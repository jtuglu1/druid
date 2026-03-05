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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.ChangeRequestHttpSyncer;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.SegmentUsedStateChangeRequest;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Broker-side component that keeps a live view of which segments the coordinator
 * considers "used" (present in metadata). It long-polls the coordinator via
 * {@link ChangeRequestHttpSyncer} so the local state is always up to date without
 * constant round-trips.
 *
 * <p>The watcher tracks coordinator leader changes through
 * {@link DruidNodeDiscovery}: when the active coordinator changes a new syncer is
 * created and the counter resets, which causes the coordinator to send a full-state
 * response that atomically replaces the local set.
 *
 * <p>Callers that need to react when segments transition from used → unused can
 * register an {@link UnusedSegmentListener}.
 */
@ManageLifecycle
public class BrokerSegmentUsedStateWatcher
{
  private static final EmittingLogger log = new EmittingLogger(BrokerSegmentUsedStateWatcher.class);

  static final TypeReference<ChangeRequestsSnapshot<SegmentUsedStateChangeRequest>> SYNC_RESPONSE_TYPE =
      new TypeReference<>() {};

  static final String REQUEST_PATH = "/druid/coordinator/v1/segment-used-state";

  /**
   * Notified after every poll cycle when one or more segments transition from
   * used → unused.
   */
  public interface UnusedSegmentListener
  {
    void onSegmentsUnused(Set<String> segmentIdStrings);
  }

  private final BrokerSegmentUnavailabilityConfig config;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final DruidNodeDiscoveryProvider discoveryProvider;

  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final ScheduledExecutorService syncExecutor =
      Execs.scheduledSingleThreaded("BrokerSegmentUsedStateWatcher-%d");

  /** Guarded by {@code this}. Maps coordinator hostAndPort → active syncer. */
  @Nullable
  private CoordinatorSyncer activeSyncer = null;

  /**
   * The current known-used segment IDs. Replaced atomically on every full or
   * delta sync.
   */
  private volatile Set<String> usedSegmentIds = Collections.emptySet();

  private final CopyOnWriteArrayList<UnusedSegmentListener> unusedListeners = new CopyOnWriteArrayList<>();

  @Inject
  public BrokerSegmentUsedStateWatcher(
      BrokerSegmentUnavailabilityConfig config,
      @Smile ObjectMapper smileMapper,
      @EscalatedGlobal HttpClient httpClient,
      DruidNodeDiscoveryProvider discoveryProvider
  )
  {
    this.config = config;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.discoveryProvider = discoveryProvider;
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("Cannot start BrokerSegmentUsedStateWatcher.");
    }
    try {
      if (!config.isEnabled()) {
        log.info("BrokerSegmentUsedStateWatcher is disabled.");
        lifecycleLock.started();
        return;
      }

      log.info("Starting BrokerSegmentUsedStateWatcher.");
      discoveryProvider.getForNodeRole(NodeRole.COORDINATOR).registerListener(
          new DruidNodeDiscovery.Listener()
          {
            @Override
            public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
            {
              for (DiscoveryDruidNode node : nodes) {
                onCoordinatorAdded(node);
              }
            }

            @Override
            public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
            {
              for (DiscoveryDruidNode node : nodes) {
                onCoordinatorRemoved(node);
              }
            }
          }
      );
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("Cannot stop BrokerSegmentUsedStateWatcher.");
    }
    try {
      log.info("Stopping BrokerSegmentUsedStateWatcher.");
      synchronized (this) {
        if (activeSyncer != null) {
          activeSyncer.syncer.stop();
          activeSyncer = null;
        }
      }
      syncExecutor.shutdownNow();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  /**
   * Returns {@code true} if the coordinator currently considers the segment used.
   * When the feature is disabled always returns {@code false} so callers can use
   * this as a direct guard.
   */
  public boolean isUsed(SegmentId segmentId)
  {
    return usedSegmentIds.contains(segmentId.toString());
  }

  /** Registers a listener that will be notified when segments become unused. */
  public void addUnusedSegmentListener(UnusedSegmentListener listener)
  {
    unusedListeners.add(listener);
  }

  // -----------------------------------------------------------------------
  // Coordinator discovery callbacks
  // -----------------------------------------------------------------------

  private void onCoordinatorAdded(DiscoveryDruidNode node)
  {
    if (!lifecycleLock.awaitStarted(1, java.util.concurrent.TimeUnit.MILLISECONDS)) {
      return;
    }
    DruidNode druidNode = node.getDruidNode();
    String hostAndPort = druidNode.getHostAndPortToUse();
    log.info("Coordinator appeared at [%s]. Starting segment-used-state sync.", hostAndPort);

    synchronized (this) {
      // Stop the previous syncer (if any) before creating a new one.
      if (activeSyncer != null) {
        log.info(
            "Stopping stale syncer for coordinator [%s] and switching to [%s].",
            activeSyncer.hostAndPort,
            hostAndPort
        );
        activeSyncer.syncer.stop();
      }
      activeSyncer = new CoordinatorSyncer(druidNode, hostAndPort);
      activeSyncer.syncer.start();
    }
  }

  private void onCoordinatorRemoved(DiscoveryDruidNode node)
  {
    DruidNode druidNode = node.getDruidNode();
    String hostAndPort = druidNode.getHostAndPortToUse();
    log.info("Coordinator at [%s] went away.", hostAndPort);

    synchronized (this) {
      if (activeSyncer != null && activeSyncer.hostAndPort.equals(hostAndPort)) {
        activeSyncer.syncer.stop();
        activeSyncer = null;
      }
    }
  }

  // -----------------------------------------------------------------------
  // Sync listener
  // -----------------------------------------------------------------------

  private void applyFullSync(List<SegmentUsedStateChangeRequest> changes)
  {
    Set<String> newIds = new HashSet<>();
    for (SegmentUsedStateChangeRequest change : changes) {
      if (change.isUsed()) {
        newIds.add(change.getSegmentId());
      }
    }
    Set<String> oldIds = usedSegmentIds;
    usedSegmentIds = ImmutableSet.copyOf(newIds);
    fireUnusedCallbacks(oldIds, newIds);
    log.debug("Full sync: [%d] used segments.", newIds.size());
  }

  private void applyDeltaSync(List<SegmentUsedStateChangeRequest> changes)
  {
    Set<String> current = new HashSet<>(usedSegmentIds);
    Set<String> nowUnused = new HashSet<>();
    for (SegmentUsedStateChangeRequest change : changes) {
      if (change.isUsed()) {
        current.add(change.getSegmentId());
      } else {
        current.remove(change.getSegmentId());
        nowUnused.add(change.getSegmentId());
      }
    }
    usedSegmentIds = ImmutableSet.copyOf(current);
    if (!nowUnused.isEmpty()) {
      fireUnusedCallbacks(nowUnused);
    }
    log.debug("Delta sync: [%d] used segments, [%d] became unused.", current.size(), nowUnused.size());
  }

  private void fireUnusedCallbacks(Set<String> oldIds, Set<String> newIds)
  {
    Set<String> removed = new HashSet<>();
    for (String id : oldIds) {
      if (!newIds.contains(id)) {
        removed.add(id);
      }
    }
    if (!removed.isEmpty()) {
      fireUnusedCallbacks(removed);
    }
  }

  private void fireUnusedCallbacks(Set<String> removedIds)
  {
    for (UnusedSegmentListener listener : unusedListeners) {
      try {
        listener.onSegmentsUnused(removedIds);
      }
      catch (Exception e) {
        log.warn(e, "UnusedSegmentListener threw an exception.");
      }
    }
  }

  // -----------------------------------------------------------------------
  // Inner holder
  // -----------------------------------------------------------------------

  private class CoordinatorSyncer
  {
    final String hostAndPort;
    final ChangeRequestHttpSyncer<SegmentUsedStateChangeRequest> syncer;

    CoordinatorSyncer(DruidNode druidNode, String hostAndPort)
    {
      this.hostAndPort = hostAndPort;
      try {
        URL baseUrl = new URL(druidNode.getServiceScheme(), druidNode.getHost(), druidNode.getPortToUse(), "/");
        this.syncer = new ChangeRequestHttpSyncer<>(
            smileMapper,
            httpClient,
            syncExecutor,
            baseUrl,
            REQUEST_PATH,
            SYNC_RESPONSE_TYPE,
            config.getServerTimeout(),
            // Use 10x serverTimeout as the unstability threshold before alerting
            config.getServerTimeout() * 10,
            new ChangeRequestHttpSyncer.Listener<>()
            {
              @Override
              public void fullSync(List<SegmentUsedStateChangeRequest> changes)
              {
                applyFullSync(changes);
              }

              @Override
              public void deltaSync(List<SegmentUsedStateChangeRequest> changes)
              {
                applyDeltaSync(changes);
              }
            }
        );
      }
      catch (Exception e) {
        throw new RuntimeException(
            StringUtils.format("Failed to create syncer for coordinator [%s]", hostAndPort),
            e
        );
      }
    }
  }
}
