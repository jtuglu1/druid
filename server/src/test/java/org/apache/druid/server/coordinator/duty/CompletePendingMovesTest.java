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

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.SegmentPlacementBroadcaster;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/**
 * The duty must complete a move only when the server the segment was actually sent to is serving it. Anything looser
 * -- "some other server has a copy" -- drops the source replica of a move whose destination never loaded, leaving
 * replication short until the next balancer run.
 */
public class CompletePendingMovesTest
{
  private static final String TIER = "normal";

  private DataSegment segment;
  private LoadQueueTaskMaster taskMaster;

  private DruidServer source;
  private DruidServer destination;
  private DruidServer bystander;

  private TestLoadQueuePeon sourcePeon;

  @Before
  public void setUp()
  {
    segment = new DataSegment(
        "datasource",
        new Interval(DateTimes.of("2012-01-01"), DateTimes.of("2012-01-01").plusHours(1)),
        DateTimes.of("2012-03-01").toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        1L
    );

    taskMaster = EasyMock.createMock(LoadQueueTaskMaster.class);
    EasyMock.expect(taskMaster.isConfirmMoveBeforeDrop()).andReturn(true).anyTimes();
    EasyMock.replay(taskMaster);

    source = newServer("source");
    destination = newServer("destination");
    bystander = newServer("bystander");

    sourcePeon = new TestLoadQueuePeon();
  }

  @Test
  public void testDropIsQueuedOnceTheRecordedDestinationIsServing()
  {
    source.addDataSegment(segment);
    destination.addDataSegment(segment);

    sourcePeon.markSegmentToDrop(segment, destination.getName());

    runDuty(holder(source, sourcePeon), holder(destination, new TestLoadQueuePeon()));

    Assert.assertEquals(
        "Drop should be queued on the source once the destination is serving the segment",
        Set.of(segment),
        sourcePeon.getSegmentsToDrop()
    );
  }

  @Test
  public void testPreExistingReplicaElsewhereDoesNotCompleteTheMove()
  {
    // A third replica that has nothing to do with this move. Before the destination was recorded on the mark, this
    // was enough to "confirm" the move, dropping the source while the destination still had nothing.
    source.addDataSegment(segment);
    bystander.addDataSegment(segment);

    sourcePeon.markSegmentToDrop(segment, destination.getName());

    final DruidCoordinatorRuntimeParams params = runDuty(
        holder(source, sourcePeon),
        holder(destination, new TestLoadQueuePeon()),
        holder(bystander, new TestLoadQueuePeon())
    );

    Assert.assertTrue(
        "A replica on an unrelated server must not complete the move",
        sourcePeon.getSegmentsToDrop().isEmpty()
    );
    Assert.assertEquals(
        "The move should still be counted as awaiting confirmation",
        1,
        params.getCoordinatorStats().get(Stats.SegmentQueue.PENDING_MOVE_CONFIRMATION)
    );
  }

  @Test
  public void testDestinationMissingFromClusterLeavesTheMarkAlone()
  {
    // The destination was decommissioned or died mid-move. The mark must not graduate; the peon's own expiry is what
    // releases it, rather than this duty guessing.
    source.addDataSegment(segment);
    sourcePeon.markSegmentToDrop(segment, "departed");

    runDuty(holder(source, sourcePeon));

    Assert.assertTrue(sourcePeon.getSegmentsToDrop().isEmpty());
  }

  @Test
  public void testNoMarksLeavesNothingPending()
  {
    source.addDataSegment(segment);

    final DruidCoordinatorRuntimeParams params = runDuty(holder(source, sourcePeon));

    Assert.assertTrue(sourcePeon.getSegmentsToDrop().isEmpty());
    Assert.assertEquals(0, params.getCoordinatorStats().get(Stats.SegmentQueue.PENDING_MOVE_CONFIRMATION));
  }

  private DruidCoordinatorRuntimeParams runDuty(ServerHolder... servers)
  {
    return runDutyWith(new CompletePendingMoves(taskMaster, null), servers);
  }

  private DruidCoordinatorRuntimeParams runDutyWith(SegmentPlacementBroadcaster broadcaster, ServerHolder... servers)
  {
    return runDutyWith(new CompletePendingMoves(taskMaster, broadcaster), servers);
  }

  private DruidCoordinatorRuntimeParams runDutyWith(CompletePendingMoves duty, ServerHolder... servers)
  {
    final DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .builder()
        .withDruidCluster(DruidCluster.builder().addTier(TIER, servers).build())
        .withUsedSegments(segment)
        .build();

    return duty.run(params);
  }

  /**
   * The gate: with the placement stream on, the source is not told to drop until Brokers have consumed the change.
   * Otherwise a Broker could apply the removal before it had heard of the destination, which is the whole bug.
   */
  @Test
  public void testDropWaitsForBrokersToConsumeThePlacementChange()
  {
    final SegmentPlacementBroadcaster broadcaster = new SegmentPlacementBroadcaster();
    try {
      broadcaster.brokerDiscovered("broker-1");
      broadcaster.getChangesSince("broker-1", broadcaster.getLastCounter(), 60_000);

      source.addDataSegment(segment);
      destination.addDataSegment(segment);
      sourcePeon.markSegmentToDrop(segment, destination.getName());

      final CompletePendingMoves duty = new CompletePendingMoves(taskMaster, broadcaster);
      final ServerHolder[] servers = {holder(source, sourcePeon), holder(destination, new TestLoadQueuePeon())};

      runDutyWith(duty, servers);
      Assert.assertTrue(
          "The drop must wait until the Broker has consumed the placement change",
          sourcePeon.getSegmentsToDrop().isEmpty()
      );

      // The Broker catches up.
      broadcaster.getChangesSince("broker-1", broadcaster.getLastCounter(), 60_000);

      runDutyWith(duty, servers);
      Assert.assertEquals(Set.of(segment), sourcePeon.getSegmentsToDrop());
    }
    finally {
      broadcaster.stop();
    }
  }

  /**
   * With another replica already serving, no Broker can be left without a server whatever order it applies things in,
   * so the drop is issued immediately -- ordinary balancing on a replicated cluster is untouched by the stream.
   */
  @Test
  public void testDropIsNotGatedWhenAnotherReplicaRemains()
  {
    final SegmentPlacementBroadcaster broadcaster = new SegmentPlacementBroadcaster();
    try {
      broadcaster.brokerDiscovered("broker-1");
      broadcaster.getChangesSince("broker-1", broadcaster.getLastCounter(), 60_000);

      source.addDataSegment(segment);
      destination.addDataSegment(segment);
      bystander.addDataSegment(segment);
      sourcePeon.markSegmentToDrop(segment, destination.getName());

      runDutyWith(
          broadcaster,
          holder(source, sourcePeon),
          holder(destination, new TestLoadQueuePeon()),
          holder(bystander, new TestLoadQueuePeon())
      );

      Assert.assertEquals(Set.of(segment), sourcePeon.getSegmentsToDrop());
      Assert.assertEquals(
          "Nothing should have been published, since no Broker could have been left without a server",
          0,
          broadcaster.getLastCounter().getCounter()
      );
    }
    finally {
      broadcaster.stop();
    }
  }

  /**
   * A move whose mark is force-expired by the peon stops being reported, and the duty must forget it. If the published
   * counter survives, the next move of the same segment reuses it: nothing new is published, and the wait is satisfied
   * instantly by a counter every Broker consumed long ago. The source is then told to drop a replica whose replacement
   * was never announced -- the very race the duty exists to close, reintroduced by a stale map entry.
   */
  @Test
  public void testExpiredMarkIsForgottenSoALaterMoveOfTheSameSegmentStillWaits()
  {
    final SegmentPlacementBroadcaster broadcaster = new SegmentPlacementBroadcaster();
    try {
      broadcaster.brokerDiscovered("broker-1");
      broadcaster.getChangesSince("broker-1", broadcaster.getLastCounter(), 60_000);

      source.addDataSegment(segment);
      destination.addDataSegment(segment);
      sourcePeon.markSegmentToDrop(segment, destination.getName());

      final CompletePendingMoves duty = new CompletePendingMoves(taskMaster, broadcaster);
      final ServerHolder[] servers = {holder(source, sourcePeon), holder(destination, new TestLoadQueuePeon())};

      runDutyWith(duty, servers);
      final long counterAfterFirstMove = broadcaster.getLastCounter().getCounter();
      Assert.assertTrue("The first move should have published its placement change", counterAfterFirstMove > 0);
      Assert.assertTrue("The first move must still be waiting", sourcePeon.getSegmentsToDrop().isEmpty());

      // The peon force-expires the mark, as HttpLoadQueuePeon#expireStaleOperations does for a move that never
      // completed. The duty stops seeing it and must drop its record of the wait.
      sourcePeon.unmarkSegmentToDrop(segment);
      runDutyWith(duty, servers);

      // Meanwhile the Broker consumes everything published so far, including the abandoned move's changes.
      broadcaster.getChangesSince("broker-1", broadcaster.getLastCounter(), 60_000);

      // The balancer tries the same segment again.
      sourcePeon.markSegmentToDrop(segment, destination.getName());
      runDutyWith(duty, servers);

      Assert.assertTrue(
          "The new move must publish its own placement change rather than reuse the abandoned one",
          broadcaster.getLastCounter().getCounter() > counterAfterFirstMove
      );
      Assert.assertTrue(
          "The new move must wait for the Broker, not be waved through on a counter it consumed earlier",
          sourcePeon.getSegmentsToDrop().isEmpty()
      );
    }
    finally {
      broadcaster.stop();
    }
  }

  private ServerHolder holder(DruidServer server, TestLoadQueuePeon peon)
  {
    return new ServerHolder(server.toImmutableDruidServer(), peon);
  }

  private DruidServer newServer(String name)
  {
    return new DruidServer(name, name, null, 100L, null, ServerType.HISTORICAL, TIER, 0);
  }
}
