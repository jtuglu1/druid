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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.HistoricalFilter;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SegmentAvailabilityTrackerTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  private RecordingCoordinatorClient coordinatorClient;

  @Before
  public void setUp()
  {
    coordinatorClient = new RecordingCoordinatorClient();
  }

  /**
   * Tracking never refuses. Returning false is what makes {@link BrokerServerView#serverRemovedSegment} drop the
   * segment from the timeline, which is the silent-partial-results bug the tracker exists to prevent, so no condition
   * short of the IGNORE policy may produce it -- least of all a burst, since a burst is exactly when the cluster is
   * unhealthy and dropping entries hurts most.
   */
  @Test
  public void testEverySegmentIsTrackedAndRetained()
  {
    final SegmentAvailabilityTracker tracker = newTracker(config("PT5S"));

    for (int i = 0; i < 5; i++) {
      final DataSegment segment = newSegment(i);
      Assert.assertTrue(
          "Every segment must be tracked so its timeline entry is kept",
          tracker.track(segment.getId(), newSelector(segment))
      );
    }

    Assert.assertEquals(5, tracker.getNumTrackedSegments());
  }

  @Test
  public void testEveryTrackedSegmentIsAskedAbout()
  {
    final SegmentAvailabilityTracker tracker = newTracker(config("PT5S"));

    for (int i = 0; i < 5; i++) {
      final DataSegment segment = newSegment(i);
      tracker.track(segment.getId(), newSelector(segment));
    }

    tracker.runChecks();

    Assert.assertEquals(
        "The sweep should ask about every tracked segment",
        5,
        coordinatorClient.segmentsAskedAbout()
    );
  }

  /**
   * The check period governs re-checks. Making it also gate the first answer puts the whole period on the path that
   * matters, which is the one where a query is about to touch a segment with no server.
   */
  @Test
  public void testFirstCheckDoesNotWaitAFullPeriod() throws Exception
  {
    // A period long enough that the scheduled sweep cannot be what satisfies this test.
    final SegmentAvailabilityTracker tracker = newTracker(config("PT1H"));
    tracker.start();

    try {
      final DataSegment segment = newSegment(0);
      tracker.track(segment.getId(), newSelector(segment));

      Assert.assertTrue(
          "A newly tracked segment should be asked about promptly, not after a full check period",
          coordinatorClient.awaitCall(10, TimeUnit.SECONDS)
      );
    }
    finally {
      tracker.stop();
    }
  }

  /**
   * Removals arrive in bursts -- a move, a handoff, or a whole server going away at once -- so the debounce must
   * fold them into one round rather than one request per segment.
   */
  @Test
  public void testBurstOfTrackedSegmentsCoalescesIntoOneRound() throws Exception
  {
    final SegmentAvailabilityTracker tracker = newTracker(config("PT1H"));
    tracker.start();

    try {
      for (int i = 0; i < 50; i++) {
        final DataSegment segment = newSegment(i);
        tracker.track(segment.getId(), newSelector(segment));
      }

      Assert.assertTrue(coordinatorClient.awaitCall(10, TimeUnit.SECONDS));
      // Let any further scheduled round land before counting.
      Thread.sleep(500);

      Assert.assertEquals(
          "A burst should coalesce into a single batched request",
          1,
          coordinatorClient.numCalls()
      );
      Assert.assertEquals(50, coordinatorClient.segmentsAskedAbout());
    }
    finally {
      tracker.stop();
    }
  }

  @Test
  public void testIgnorePolicyDoesNotTrack()
  {
    final SegmentAvailabilityTracker tracker = newTracker(config("PT5S", "IGNORE"));

    final DataSegment segment = newSegment(0);
    Assert.assertFalse(
        "IGNORE must behave exactly as before the feature existed, letting the caller drop the entry",
        tracker.track(segment.getId(), newSelector(segment))
    );
    Assert.assertEquals(0, tracker.getNumTrackedSegments());
  }

  private SegmentAvailabilityTracker newTracker(BrokerSegmentWatcherConfig config)
  {
    return new SegmentAvailabilityTracker(coordinatorClient, config);
  }

  /**
   * The retention period bounds how long the Broker waits <em>while it cannot establish</em> whether a segment should
   * be available. A segment the Coordinator has confirmed as EXPECTED_AVAILABLE is a real outage, not an unanswered
   * question: evicting it drops it from the timeline, which is the silent-partial-results behaviour this class exists
   * to prevent -- and it would do so while the outage is still going on, zeroing the metric that reports it.
   */
  @Test
  public void testResolvedOutageIsNotEvictedWhenTheRetentionPeriodPasses() throws Exception
  {
    final SegmentAvailabilityTracker tracker = newTracker(config("PT1H", "ALERT", "PT0S"));

    final Set<SegmentId> evicted = new HashSet<>();
    tracker.registerEvictionHandler(evicted::addAll);

    final DataSegment resolved = newSegment(0);
    final ServerSelector resolvedSelector = newSelector(resolved);
    tracker.track(resolved.getId(), resolvedSelector);
    resolvedSelector.setAvailability(SegmentAvailability.EXPECTED_AVAILABLE);

    // The cutoff is strictly before "now", so let the clock advance past the moment of tracking.
    Thread.sleep(5);
    tracker.runChecks();

    Assert.assertEquals("A confirmed outage must not be evicted", Set.of(), evicted);
    Assert.assertEquals(1, tracker.getNumTrackedSegments());
  }

  /**
   * The other half: a segment whose availability was never established must still be let go, or an unreachable
   * Coordinator would leave it tracked forever.
   */
  @Test
  public void testUnresolvedSegmentIsEvictedWhenTheRetentionPeriodPasses() throws Exception
  {
    final SegmentAvailabilityTracker tracker = newTracker(config("PT1H", "ALERT", "PT0S"));

    final Set<SegmentId> evicted = new HashSet<>();
    tracker.registerEvictionHandler(evicted::addAll);

    final DataSegment unresolved = newSegment(1);
    tracker.track(unresolved.getId(), newSelector(unresolved));

    Thread.sleep(5);
    tracker.runChecks();

    Assert.assertEquals(Set.of(unresolved.getId()), evicted);
    Assert.assertEquals(0, tracker.getNumTrackedSegments());
  }

  private static BrokerSegmentWatcherConfig config(String checkPeriod)
  {
    return config(checkPeriod, "ALERT");
  }

  private static BrokerSegmentWatcherConfig config(String checkPeriod, String policy)
  {
    return config(checkPeriod, policy, "PT15M");
  }

  private static BrokerSegmentWatcherConfig config(String checkPeriod, String policy, String retentionPeriod)
  {
    try {
      return MAPPER.readValue(
          StringUtils.format(
              "{\"unavailableCheckPeriod\": \"%s\", \"unavailableSegmentPolicy\": \"%s\","
              + " \"unavailableRetentionPeriod\": \"%s\"}",
              checkPeriod, policy, retentionPeriod
          ),
          BrokerSegmentWatcherConfig.class
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static ServerSelector newSelector(DataSegment segment)
  {
    return new ServerSelector(
        segment,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        HistoricalFilter.IDENTITY_FILTER
    );
  }

  private static DataSegment newSegment(int partition)
  {
    return DataSegment.builder()
                      .dataSource("datasource")
                      .interval(new Interval(DateTimes.of("2012-01-01"), DateTimes.of("2012-01-02")))
                      .version("v1")
                      .shardSpec(new NumberedShardSpec(partition, 0))
                      .size(1L)
                      .build();
  }

  /**
   * Records what the tracker asks for, and answers "expected to be available" for everything so that nothing is
   * evicted mid-test.
   */
  private static class RecordingCoordinatorClient extends NoopCoordinatorClient
  {
    private final AtomicInteger numCalls = new AtomicInteger();
    private final AtomicInteger segmentsAskedAbout = new AtomicInteger();
    private final CountDownLatch firstCall = new CountDownLatch(1);

    @Override
    public ListenableFuture<Map<SegmentId, SegmentAvailabilityStatus>> fetchSegmentAvailability(
        Set<SegmentId> segmentIds
    )
    {
      numCalls.incrementAndGet();
      segmentsAskedAbout.addAndGet(segmentIds.size());

      final Map<SegmentId, SegmentAvailabilityStatus> statuses = new HashMap<>();
      for (SegmentId segmentId : segmentIds) {
        statuses.put(segmentId, new SegmentAvailabilityStatus(true, 1));
      }
      firstCall.countDown();
      return Futures.immediateFuture(statuses);
    }

    private boolean awaitCall(long timeout, TimeUnit unit) throws InterruptedException
    {
      return firstCall.await(timeout, unit);
    }

    private int numCalls()
    {
      return numCalls.get();
    }

    private int segmentsAskedAbout()
    {
      return segmentsAskedAbout.get();
    }
  }
}
