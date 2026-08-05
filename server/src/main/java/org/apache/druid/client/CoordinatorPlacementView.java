/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.coordination.ChangeRequestHttpSyncer;
import org.apache.druid.server.coordination.SegmentPlacementChange;
import org.apache.druid.server.http.SegmentPlacementResource;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Follows the Coordinator's ordered stream of placement changes.
 * <p>
 * A Broker's per-data-node syncs have no ordering between them, so applying "dropped from A" before "loaded on B"
 * makes a segment that is merely moving look as though it has gone. This stream carries both halves in order from a
 * single publisher, so consuming the removal implies having consumed the addition. See apache/druid#18738.
 * <p>
 * The stream is purely additive: it publishes the destination replica ahead of the Broker's own sync with that
 * server. Removals are still applied from the per-node syncs, so a stale or broken stream can only delay the Broker
 * learning of a new replica, never strand it on a server that no longer holds the segment.
 */
public class CoordinatorPlacementView
{
  private static final EmittingLogger log = new EmittingLogger(CoordinatorPlacementView.class);

  /**
   * How long the Coordinator holds a long-poll open when it has nothing to report. Also what the Coordinator derives
   * this Broker's silence tolerance from, since it is sent on every request.
   */
  private static final long POLL_TIMEOUT_MILLIS = 60_000;

  /**
   * How long the stream may stay unhealthy before it is reported as such. Kept independent of the poll timeout, so
   * lengthening the poll cannot silently raise the threshold at which a broken stream is alerted.
   */
  private static final long UNSTABILITY_TIMEOUT_MILLIS = 60_000;

  /**
   * How often the Broker rechecks which Coordinator is the leader, so the stream is repointed after a failover.
   */
  private static final long LEADER_POLL_PERIOD_MILLIS = 30_000;

  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final CoordinatorClient coordinatorClient;
  private final String brokerId;

  /**
   * Applies changes to the timeline. Registered by {@link BrokerServerView}, which owns it.
   */
  private volatile Consumer<List<SegmentPlacementChange>> changeHandler = changes -> {};

  private final ScheduledExecutorService exec;

  /**
   * Guards the syncer and the leader it is pointed at, which are swapped together on failover.
   */
  private final Object lock = new Object();

  @Nullable
  private ChangeRequestHttpSyncer<SegmentPlacementChange> syncer;
  @Nullable
  private URI currentLeader;

  public CoordinatorPlacementView(
      ObjectMapper smileMapper,
      HttpClient httpClient,
      CoordinatorClient coordinatorClient,
      String brokerId
  )
  {
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.coordinatorClient = coordinatorClient;
    this.brokerId = brokerId;
    this.exec = Execs.scheduledSingleThreaded("CoordinatorPlacementView-%d");
  }

  public void registerChangeHandler(Consumer<List<SegmentPlacementChange>> changeHandler)
  {
    this.changeHandler = changeHandler;
  }

  public void start()
  {
    exec.scheduleWithFixedDelay(
        this::refreshLeader,
        0,
        LEADER_POLL_PERIOD_MILLIS,
        TimeUnit.MILLISECONDS
    );
  }

  public void stop()
  {
    exec.shutdownNow();
    synchronized (lock) {
      stopSyncerLocked();
    }
  }

  /**
   * Repoints the syncer when leadership moves. The Coordinator's placement endpoint answers 404 when it is not the
   * leader, so a stale target fails rather than quietly returning an empty stream, but re-resolving is what actually
   * recovers.
   */
  @VisibleForTesting
  void refreshLeader()
  {
    try {
      final URI leader = FutureUtils.getUnchecked(coordinatorClient.findCurrentLeader(), true);
      synchronized (lock) {
        if (leader == null) {
          stopSyncerLocked();
          return;
        }
        if (leader.equals(currentLeader) && syncer != null) {
          return;
        }

        log.info("Coordinator leader is now [%s]. Repointing the placement stream.", leader);
        stopSyncerLocked();
        startSyncerLocked(leader);
      }
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Could not resolve the Coordinator leader for the placement stream.");
      synchronized (lock) {
        // No leader means no authority. Falling back to per-node removals is worse than the stream but is never
        // silently stale, which is the property that matters.
        stopSyncerLocked();
      }
    }
  }

  private void startSyncerLocked(URI leader)
  {
    try {
      final ChangeRequestHttpSyncer<SegmentPlacementChange> newSyncer = new ChangeRequestHttpSyncer<>(
          smileMapper,
          httpClient,
          exec,
          leader.toURL(),
          SegmentPlacementResource.PATH,
          "broker=" + brokerId,
          SegmentPlacementResource.RESPONSE_TYPE_REF,
          POLL_TIMEOUT_MILLIS,
          UNSTABILITY_TIMEOUT_MILLIS,
          new ChangeRequestHttpSyncer.Listener<>()
          {
            @Override
            public void fullSync(List<SegmentPlacementChange> changes)
            {
              // "Full" here means "you had no position, here is the stream from now on" -- this endpoint never sends a
              // placement snapshot, because a full one is O(replicas) and must never be shipped. The Broker's own
              // per-node syncs are what rebuild placement; this just marks where the stream resumes.
              changeHandler.accept(changes);
            }

            @Override
            public void deltaSync(List<SegmentPlacementChange> changes)
            {
              changeHandler.accept(changes);
            }
          }
      );

      newSyncer.start();
      syncer = newSyncer;
      currentLeader = leader;
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Could not start the placement stream against Coordinator[%s].", leader);
      stopSyncerLocked();
    }
  }

  private void stopSyncerLocked()
  {
    currentLeader = null;
    if (syncer != null) {
      try {
        syncer.stop();
      }
      catch (Exception e) {
        log.noStackTrace().warn(e, "Could not cleanly stop the placement stream.");
      }
      syncer = null;
    }
  }

  /**
   * Delivers changes as though a sync had landed, without standing up a Coordinator to talk to.
   */
  @VisibleForTesting
  public void deliver(List<SegmentPlacementChange> changes)
  {
    changeHandler.accept(changes);
  }

  @VisibleForTesting
  @Nullable
  URL getCurrentLeaderUrl() throws Exception
  {
    synchronized (lock) {
      return currentLeader == null ? null : currentLeader.toURL();
    }
  }
}
