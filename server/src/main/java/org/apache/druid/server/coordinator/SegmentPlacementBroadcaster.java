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

package org.apache.druid.server.coordinator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.SegmentPlacementChange;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Publishes the Coordinator's ordered stream of placement changes to Brokers, and tracks how far each Broker has
 * consumed it.
 * <p>
 * The stream exists so that a Broker cannot learn that a segment was dropped from one server without first having
 * learnt of the server that replaced it: both halves are published in order on one channel. The progress tracking
 * exists so the Coordinator can wait for that to actually have happened before telling the source server to drop --
 * which is what closes the window in which a moving segment has nowhere to be queried from. See apache/druid#18738.
 * <p>
 * Progress is free to collect: {@link org.apache.druid.server.coordination.ChangeRequestHttpSyncer} already sends the
 * counter it has consumed on every long-poll, so every request is an acknowledgement.
 */
public class SegmentPlacementBroadcaster
{
  private static final EmittingLogger log = new EmittingLogger(SegmentPlacementBroadcaster.class);

  /**
   * How many changes are retained for Brokers that have fallen behind. A Broker further behind than this is told to
   * reset and rebuilds placement from the data servers directly, which is correct but slower. Sized to absorb a burst
   * -- a tier restart or a datasource-wide mark-unused produces far more events than ordinary balancing does -- and
   * not configurable, because the only consequence of the wrong value is how often that safe fallback is taken.
   */
  private static final int MAX_BUFFERED_CHANGES = 100_000;

  /**
   * How many of a Broker's own long-poll periods it may miss before the Coordinator stops waiting for it.
   */
  private static final int SILENCE_TOLERANCE_POLLS = 3;

  /**
   * Floor on the derived tolerance, so that a Broker declaring an unusually short timeout cannot make itself trivially
   * easy to skip.
   */
  private static final long MIN_SILENCE_TOLERANCE_MILLIS = 30_000;

  private final ChangeRequestHistory<SegmentPlacementChange> changes;

  /**
   * How far each Broker has consumed the stream. A Broker with no entry has not established a position -- it is new,
   * or it has just reset -- and deliberately does not gate anything.
   */
  private final Map<String, BrokerProgress> brokerProgress = new ConcurrentHashMap<>();

  /**
   * Brokers currently in node discovery. A Broker that leaves stops gating immediately.
   */
  private final Map<String, Boolean> liveBrokers = new ConcurrentHashMap<>();

  /**
   * Reads the wall clock. Injectable only so that the silence path -- the one place this gate trades safety for
   * availability -- can be tested without waiting out a real tolerance.
   */
  private final LongSupplier nowMillis;

  public SegmentPlacementBroadcaster()
  {
    this(System::currentTimeMillis);
  }

  @VisibleForTesting
  SegmentPlacementBroadcaster(LongSupplier nowMillis)
  {
    this.changes = new ChangeRequestHistory<>(MAX_BUFFERED_CHANGES);
    this.nowMillis = nowMillis;
  }

  public void registerWith(DruidNodeDiscoveryProvider discoveryProvider)
  {
    discoveryProvider.getForNodeRole(NodeRole.BROKER).registerListener(
        new DruidNodeDiscovery.Listener()
        {
          @Override
          public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
          {
            nodes.forEach(node -> brokerDiscovered(node.getDruidNode().getHostAndPortToUse()));
          }

          @Override
          public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
          {
            nodes.forEach(node -> brokerLost(node.getDruidNode().getHostAndPortToUse()));
          }
        }
    );
  }

  @VisibleForTesting
  public void brokerDiscovered(String brokerId)
  {
    liveBrokers.put(brokerId, Boolean.TRUE);
  }

  @VisibleForTesting
  public void brokerLost(String brokerId)
  {
    liveBrokers.remove(brokerId);
    // A Broker that has gone away cannot be waited for, and its remembered position is meaningless if it comes back:
    // it will bootstrap from scratch and be indistinguishable from a brand-new Broker.
    brokerProgress.remove(brokerId);
  }

  public void stop()
  {
    changes.stop();
    brokerProgress.clear();
    liveBrokers.clear();
  }

  /**
   * Appends changes to the stream and returns the counter of the last one, which is the position a Broker must reach
   * before the corresponding drop is safe to issue.
   */
  public ChangeRequestHistory.Counter publish(List<SegmentPlacementChange> placementChanges)
  {
    if (placementChanges.isEmpty()) {
      return changes.getLastCounter();
    }
    changes.addChangeRequests(placementChanges);
    return changes.getLastCounter();
  }

  /**
   * Serves one long-poll, recording the requesting Broker's position as a side effect.
   *
   * @param counter        what the Broker has already consumed; a negative counter means it is (re)starting and has
   *                       no position at all
   * @param pollTimeoutMillis how long this Broker will hold the poll open, which is how long it is expected to be
   *                       silent for between requests. Taken from the request rather than configured on this side,
   *                       so that the two processes cannot be configured into disagreeing about what silence means.
   */
  public ListenableFuture<ChangeRequestsSnapshot<SegmentPlacementChange>> getChangesSince(
      String brokerId,
      ChangeRequestHistory.Counter counter,
      long pollTimeoutMillis
  )
  {
    if (counter.getCounter() < 0) {
      // Starting or resetting. Its position is unknown until it establishes one, so it must not gate drops in the
      // meantime -- a Broker that has fallen off the buffer is exactly the sick Broker that must not stall moves.
      brokerProgress.remove(brokerId);
      return changes.getRequestsSince(changes.getLastCounter());
    }

    brokerProgress.put(
        brokerId,
        new BrokerProgress(counter.getCounter(), nowMillis.getAsLong(), pollTimeoutMillis)
    );
    return changes.getRequestsSince(counter);
  }

  /**
   * Whether every Broker that is in a position to be waited for has consumed the stream up to {@code counter}.
   * <p>
   * Three kinds of Broker deliberately do not gate, because a Broker problem must never become a cluster problem:
   * one that has left node discovery, one that has never established a position (new, or freshly reset), and one that
   * has stopped making requests for longer than the tolerance. The last of those is the only case that trades safety
   * for availability, and it is alerted.
   */
  public boolean isConsumedByAllBrokers(ChangeRequestHistory.Counter counter)
  {
    final long now = nowMillis.getAsLong();

    for (String brokerId : liveBrokers.keySet()) {
      final BrokerProgress progress = brokerProgress.get(brokerId);
      if (progress == null) {
        continue;
      }

      final long toleranceMillis = progress.silenceToleranceMillis();
      if (now - progress.lastRequestMillis >= toleranceMillis) {
        log.noStackTrace().warn(
            "Broker[%s] has not requested placement changes for over [%,d]ms. Proceeding without it; it may briefly"
            + " query a server that has already dropped a segment.",
            brokerId, toleranceMillis
        );
        continue;
      }

      if (progress.consumedCounter < counter.getCounter()) {
        return false;
      }
    }

    return true;
  }

  @VisibleForTesting
  public ChangeRequestHistory.Counter getLastCounter()
  {
    return changes.getLastCounter();
  }

  @VisibleForTesting
  public int getNumTrackedBrokers()
  {
    return brokerProgress.size();
  }

  private static class BrokerProgress
  {
    private final long consumedCounter;
    private final long lastRequestMillis;
    private final long pollTimeoutMillis;

    private BrokerProgress(long consumedCounter, long lastRequestMillis, long pollTimeoutMillis)
    {
      this.consumedCounter = consumedCounter;
      this.lastRequestMillis = lastRequestMillis;
      this.pollTimeoutMillis = pollTimeoutMillis;
    }

    /**
     * A healthy Broker only asks again when its poll returns, so the gap between requests is its own timeout. Deriving
     * the tolerance from that is what stops a Broker configured with a long poll from looking permanently silent.
     */
    private long silenceToleranceMillis()
    {
      return Math.max(MIN_SILENCE_TOLERANCE_MILLIS, pollTimeoutMillis * SILENCE_TOLERANCE_POLLS);
    }
  }
}
