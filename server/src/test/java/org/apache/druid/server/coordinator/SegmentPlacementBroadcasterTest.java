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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.SegmentPlacementChange;
import org.apache.druid.timeline.SegmentId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The gate in {@link SegmentPlacementBroadcaster#isConsumedByAllBrokers} is what stops a source replica being dropped
 * before every Broker knows where the segment went. Its safety property and its three escape hatches both matter: it
 * must not let a drop through early, and it must not let one sick Broker stall the cluster.
 */
public class SegmentPlacementBroadcasterTest
{
  private static final SegmentId SEGMENT_ID = SegmentId.of(
      "wikipedia",
      Intervals.of("2024-01-01/2024-01-02"),
      DateTimes.of("2024-01-03").toString(),
      0
  );

  private static final long SILENCE_TOLERANCE_MILLIS = 60_000;

  private SegmentPlacementBroadcaster broadcaster;

  @Before
  public void setUp()
  {
    broadcaster = new SegmentPlacementBroadcaster();
  }

  @After
  public void tearDown()
  {
    broadcaster.stop();
  }

  @Test
  public void testNoBrokersMeansNothingToWaitFor()
  {
    final ChangeRequestHistory.Counter published = publishMove();
    Assert.assertTrue(broadcaster.isConsumedByAllBrokers(published));
  }

  @Test
  public void testBrokerBehindTheChangeGatesIt()
  {
    broadcaster.brokerDiscovered("broker-1");
    // Establishes a position at the head, before anything is published.
    consume("broker-1", broadcaster.getLastCounter());

    final ChangeRequestHistory.Counter published = publishMove();

    Assert.assertFalse(
        "A Broker that has not yet consumed the removal must hold the drop back",
        broadcaster.isConsumedByAllBrokers(published)
    );
  }

  @Test
  public void testBrokerCaughtUpReleasesTheGate()
  {
    broadcaster.brokerDiscovered("broker-1");
    consume("broker-1", broadcaster.getLastCounter());

    final ChangeRequestHistory.Counter published = publishMove();
    consume("broker-1", published);

    Assert.assertTrue(broadcaster.isConsumedByAllBrokers(published));
  }

  @Test
  public void testSlowestBrokerDecides()
  {
    broadcaster.brokerDiscovered("broker-1");
    broadcaster.brokerDiscovered("broker-2");
    consume("broker-1", broadcaster.getLastCounter());
    consume("broker-2", broadcaster.getLastCounter());

    final ChangeRequestHistory.Counter published = publishMove();
    consume("broker-1", published);

    Assert.assertFalse(broadcaster.isConsumedByAllBrokers(published));

    consume("broker-2", published);
    Assert.assertTrue(broadcaster.isConsumedByAllBrokers(published));
  }

  /**
   * A Broker that has never established a position is new or has just reset. It has nothing stale to protect, since
   * it bootstraps placement from the data nodes themselves, so it must not gate anything.
   */
  @Test
  public void testBrokerWithNoEstablishedPositionDoesNotGate()
  {
    broadcaster.brokerDiscovered("broker-1");

    final ChangeRequestHistory.Counter published = publishMove();

    Assert.assertTrue(broadcaster.isConsumedByAllBrokers(published));
    Assert.assertEquals(0, broadcaster.getNumTrackedBrokers());
  }

  /**
   * A reset -- the Broker fell off the ring buffer, or restarted -- discards its position. That is precisely the sick
   * Broker that must not stall moves.
   */
  @Test
  public void testResetDiscardsPositionAndStopsGating()
  {
    broadcaster.brokerDiscovered("broker-1");
    consume("broker-1", broadcaster.getLastCounter());

    final ChangeRequestHistory.Counter published = publishMove();
    Assert.assertFalse(broadcaster.isConsumedByAllBrokers(published));

    // counter < 0 is what ChangeRequestHttpSyncer sends after a reset.
    consume("broker-1", new ChangeRequestHistory.Counter(-1, 0));

    Assert.assertEquals(0, broadcaster.getNumTrackedBrokers());
    Assert.assertTrue(broadcaster.isConsumedByAllBrokers(published));
  }

  @Test
  public void testBrokerLeavingDiscoveryStopsGatingImmediately()
  {
    broadcaster.brokerDiscovered("broker-1");
    consume("broker-1", broadcaster.getLastCounter());

    final ChangeRequestHistory.Counter published = publishMove();
    Assert.assertFalse(broadcaster.isConsumedByAllBrokers(published));

    broadcaster.brokerLost("broker-1");

    Assert.assertTrue(broadcaster.isConsumedByAllBrokers(published));
    Assert.assertEquals(0, broadcaster.getNumTrackedBrokers());
  }

  /**
   * A Broker that is still in discovery but has stopped asking is the one case where the gate trades safety for
   * availability, so it is worth pinning: it must eventually stop blocking rather than stalling rebalancing forever.
   */
  @Test
  public void testSilentBrokerStopsGatingOnceToleranceIsExceeded()
  {
    final AtomicLong clock = new AtomicLong(0);
    final SegmentPlacementBroadcaster impatient = new SegmentPlacementBroadcaster(clock::get);
    try {
      impatient.brokerDiscovered("broker-1");
      // Declares a one-minute poll, so it is tolerated for three of them and no longer.
      impatient.getChangesSince("broker-1", impatient.getLastCounter(), 60_000);

      final ChangeRequestHistory.Counter published = impatient.publish(
          List.of(SegmentPlacementChange.replicaRemoved(SEGMENT_ID, "historical-a"))
      );

      Assert.assertFalse(
          "A Broker within its own polling interval must still gate",
          impatient.isConsumedByAllBrokers(published)
      );

      clock.set(3 * 60_000);
      Assert.assertTrue(
          "A Broker silent for longer than its own polling interval allows must stop gating",
          impatient.isConsumedByAllBrokers(published)
      );
    }
    finally {
      impatient.stop();
    }
  }

  /**
   * The addition is published before the removal, so a Broker that has consumed the removal has necessarily consumed
   * the replica that replaces it. That ordering is the whole guarantee.
   */
  @Test
  public void testAdditionIsOrderedBeforeRemoval()
  {
    final ChangeRequestHistory.Counter start = broadcaster.getLastCounter();
    broadcaster.publish(
        List.of(
            SegmentPlacementChange.replicaAdded(SEGMENT_ID, "historical-b"),
            SegmentPlacementChange.replicaRemoved(SEGMENT_ID, "historical-a")
        )
    );

    final List<SegmentPlacementChange> delivered = getChanges(start);

    Assert.assertEquals(2, delivered.size());
    Assert.assertEquals(SegmentPlacementChange.Type.REPLICA_ADDED, delivered.get(0).getType());
    Assert.assertEquals(SegmentPlacementChange.Type.REPLICA_REMOVED, delivered.get(1).getType());
  }

  private ChangeRequestHistory.Counter publishMove()
  {
    return broadcaster.publish(
        List.of(
            SegmentPlacementChange.replicaAdded(SEGMENT_ID, "historical-b"),
            SegmentPlacementChange.replicaRemoved(SEGMENT_ID, "historical-a")
        )
    );
  }

  private void consume(String brokerId, ChangeRequestHistory.Counter counter)
  {
    broadcaster.getChangesSince(brokerId, counter, 60_000);
  }

  private List<SegmentPlacementChange> getChanges(ChangeRequestHistory.Counter since)
  {
    try {
      return broadcaster.getChangesSince("reader", since, 60_000).get().getRequests();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
