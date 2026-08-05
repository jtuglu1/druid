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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.SegmentPlacementChange;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.SegmentPlacementBroadcaster;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Drops the source replica of a move once the Coordinator's inventory view confirms the destination replica.
 * <p>
 * When {@link org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig#isConfirmMoveBeforeDrop()} is set,
 * {@link org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager#moveSegment} deliberately does not drop
 * from the source when the destination acknowledges the load. The acknowledgement only means the destination has the
 * segment; Brokers do not find out until their own sync with that server lands. Dropping at ack time means a Broker
 * can process the drop before the load and briefly see no server at all for the segment, silently returning partial
 * results. See apache/druid#18738.
 * <p>
 * So the source is left marked with {@link org.apache.druid.server.coordinator.loading.SegmentAction#MOVE_FROM}
 * and this duty finishes the job on a later run,
 * once some other server is observed to be actually serving the segment. Until then the source keeps serving it, and
 * the MOVE_FROM marker keeps the Coordinator from counting the replica twice.
 */
public class CompletePendingMoves implements CoordinatorDuty
{
  private static final Logger log = new Logger(CompletePendingMoves.class);

  private final LoadQueueTaskMaster taskMaster;

  /**
   * Null when the placement stream is disabled, in which case a confirmed move drops from the source immediately, as
   * it did before the stream existed.
   */
  @Nullable
  private final SegmentPlacementBroadcaster broadcaster;

  /**
   * Moves whose placement has been published and which are waiting for Brokers to consume it, keyed by the segment on
   * the server it is leaving. Kept across runs, since a wait normally spans several.
   */
  private final Map<SegmentId, ChangeRequestHistory.Counter> awaitingBrokers = new HashMap<>();

  public CompletePendingMoves(LoadQueueTaskMaster taskMaster)
  {
    this(taskMaster, null);
  }

  public CompletePendingMoves(LoadQueueTaskMaster taskMaster, @Nullable SegmentPlacementBroadcaster broadcaster)
  {
    this.taskMaster = taskMaster;
    this.broadcaster = broadcaster;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if (!taskMaster.isConfirmMoveBeforeDrop()) {
      // Nothing will ever complete these waits while the feature is off, so they must not be carried across a toggle.
      awaitingBrokers.clear();
      return params;
    }

    final List<ServerHolder> allServers = params.getDruidCluster().getAllManagedServers();
    final Map<String, ServerHolder> serversByName = Maps.newHashMapWithExpectedSize(allServers.size());
    for (ServerHolder server : allServers) {
      serversByName.put(server.getServer().getName(), server);
    }

    int dropsQueued = 0;
    int stillPending = 0;
    long maxPendingAgeMillis = 0;
    final Set<SegmentId> marksSeenThisRun = new HashSet<>();
    for (ServerHolder source : allServers) {
      final LoadQueuePeon peon = source.getPeon();

      // Read the marks straight off the peon rather than scanning the run's whole queued-segment map. The marks are
      // exactly the pending MOVE_FROMs and there are only ever a handful, whereas the queued map is proportional to
      // load queue depth across every server in the cluster.
      for (Map.Entry<DataSegment, String> mark : peon.getPendingMoveDestinations().entrySet()) {
        final DataSegment segment = mark.getKey();
        final ServerHolder destination = serversByName.get(mark.getValue());
        marksSeenThisRun.add(segment.getId());

        // Only the recorded destination counts. A replica that happened to already exist elsewhere would otherwise
        // "complete" a move whose destination never loaded, dropping the source and leaving replication short until
        // the next balancer run. A destination that has left the cluster entirely never confirms, and the peon's own
        // expiry releases the mark.
        if (destination == null || !destination.isServingSegment(segment)) {
          ++stillPending;
          continue;
        }

        if (!brokersHaveCaughtUp(segment, source, destination, allServers)) {
          ++stillPending;
          continue;
        }

        // dropSegment retires the MOVE_FROM marker atomically, so a failed drop simply leaves the segment on the
        // source for the usual over-replication handling to pick up rather than pinning it here forever.
        peon.dropSegment(segment, null);
        awaitingBrokers.remove(segment.getId());
        ++dropsQueued;
      }

      maxPendingAgeMillis = Math.max(maxPendingAgeMillis, peon.getOldestPendingMoveAgeMillis());
    }

    // A move whose mark was force-expired by HttpLoadQueuePeon#expireStaleOperations stops appearing above, so its
    // entry would otherwise survive forever. That is not merely a leak: the next move of the same segment would find
    // the stale entry, publish nothing, and be waved through against a counter every Broker had long since consumed --
    // dropping the source without ever announcing the new replica, which is the exact race this duty exists to close.
    awaitingBrokers.keySet().retainAll(marksSeenThisRun);

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    stats.add(Stats.SegmentQueue.PENDING_MOVE_CONFIRMATION, stillPending);
    stats.updateMax(Stats.SegmentQueue.PENDING_MOVE_MAX_AGE, RowKey.empty(), maxPendingAgeMillis);

    if (dropsQueued > 0) {
      log.debug("Queued [%d] drops for moves whose destination replica is now serving.", dropsQueued);
    }

    return params;
  }

  /**
   * Whether it is safe to tell the source to drop.
   * <p>
   * Only a drop that would leave a Broker with no server at all is dangerous, so this waits only when the destination
   * is the sole remaining replica. At two or more replicas an out-of-order removal cannot empty any selector, and the
   * drop is issued immediately exactly as before -- so ordinary balancing on a replicated cluster is untouched.
   * <p>
   * When it does matter, the addition and the removal are published together and the drop waits until every Broker
   * that can be waited for has consumed them. Since the addition precedes the removal on one ordered stream, a Broker
   * that has consumed the removal has necessarily consumed the replica that replaces it. See apache/druid#18738.
   */
  private boolean brokersHaveCaughtUp(
      DataSegment segment,
      ServerHolder source,
      ServerHolder destination,
      List<ServerHolder> allServers
  )
  {
    if (broadcaster == null) {
      return true;
    }

    if (hasReplicaBesides(segment, source, destination, allServers)) {
      awaitingBrokers.remove(segment.getId());
      return true;
    }

    final ChangeRequestHistory.Counter published = awaitingBrokers.computeIfAbsent(
        segment.getId(),
        id -> broadcaster.publish(
            List.of(
                SegmentPlacementChange.replicaAdded(id, destination.getServer().getName()),
                SegmentPlacementChange.replicaRemoved(id, source.getServer().getName())
            )
        )
    );

    return broadcaster.isConsumedByAllBrokers(published);
  }

  private static boolean hasReplicaBesides(
      DataSegment segment,
      ServerHolder source,
      ServerHolder destination,
      List<ServerHolder> allServers
  )
  {
    for (ServerHolder candidate : allServers) {
      if (!candidate.equals(source) && !candidate.equals(destination) && candidate.isServingSegment(segment)) {
        return true;
      }
    }
    return false;
  }
}
