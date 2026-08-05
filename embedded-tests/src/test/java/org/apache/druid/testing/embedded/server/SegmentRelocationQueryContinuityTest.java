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

package org.apache.druid.testing.embedded.server;

import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.ImmutableSegmentLoadInfo;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The end-to-end statement of what apache/druid#18738 and #18716 are about: while segments move between historicals,
 * queries must keep returning <em>every</em> row and must not error.
 * <p>
 * Run at 1x replication deliberately. With a second replica in play, a broker that applies a removal before the
 * matching addition still has somewhere to route, so the race these issues describe cannot be observed at all.
 * <p>
 * Every wait here is keyed on the relocation itself rather than on "some segment loaded somewhere". The cluster is
 * shared across test methods and {@code LatchableEmitter} replays already-processed events against a newly registered
 * condition, so a latch that the initial ingestion could satisfy is a latch that returns before the move has begun --
 * leaving the query loop to observe nothing at all.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SegmentRelocationQueryContinuityTest extends EmbeddedClusterTestBase
{
  /**
   * {@link Resources.InlineData#CSV_10_DAYS} at DAY granularity.
   */
  private static final int NUM_SEGMENTS = 10;

  /**
   * {@link Resources.InlineData#CSV_10_DAYS} is one row per day.
   */
  private static final int NUM_ROWS = 10;

  /**
   * Long enough to outlast the Coordinator's gap between issuing the load and issuing the drop (one {@code PT1S}
   * cycle), which is what makes the Broker apply the drop first.
   */
  private static final long SYNC_DELAY_MILLIS = 4_000;

  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.coordinator.segmentPlacement.enabled", "true")
      // Balance eagerly, so a test does not have to wait long for a move to be attempted.
      .addProperty("druid.coordinator.period", "PT1S");

  private final EmbeddedHistorical historical1 = new EmbeddedHistorical();
  private final EmbeddedHistorical historical2 = new EmbeddedHistorical()
      .addProperty("druid.plaintextPort", "7083");

  private final EmbeddedBroker broker1 = delayableBroker();
  private final EmbeddedBroker broker2 = delayableBroker()
      .addProperty("druid.plaintextPort", "7082");

  private final EmbeddedOverlord overlord = new EmbeddedOverlord();

  private static EmbeddedBroker delayableBroker()
  {
    return new EmbeddedBroker()
        .addProperty("druid.broker.segmentPlacement.enabled", "true")
        .addProperty("druid.broker.segment.unavailableSegmentPolicy", "FAIL")
        // The delay sleeps on the sync executor; extra threads keep one lagging server from holding up the other.
        .addProperty("druid.serverview.http.numThreads", "8");
  }

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .useLatchableEmitter()
                               .addExtension(DelayedSyncServerViewModule.class)
                               .addServer(overlord)
                               .addServer(coordinator)
                               .addServer(new EmbeddedIndexer())
                               .addServer(historical1)
                               .addServer(historical2)
                               .addServer(broker1)
                               .addServer(broker2);
  }

  /**
   * Makes the destination historical's per-node sync lag every broker, so that the drop from the source is applied
   * while the broker still believes the segment lives nowhere else. This is the inversion that #18738 is about; on a
   * single-JVM cluster nothing else produces it.
   */
  private void delayPlacementSyncFrom(EmbeddedHistorical historical, long delayMillis)
  {
    final String hostAndPort = historical.bindings().selfNode().getHostAndPort();
    for (EmbeddedBroker broker : List.of(broker1, broker2)) {
      delayedViewOf(broker).delayChangesFrom(hostAndPort, delayMillis);
    }
  }

  private DelayedSyncServerViewModule.DelayedSyncInventoryView delayedViewOf(EmbeddedBroker broker)
  {
    return (DelayedSyncServerViewModule.DelayedSyncInventoryView)
        broker.bindings().getInstance(FilteredServerInventoryView.class);
  }

  /**
   * Decommissioning is sticky and the cluster outlives the test method, so leaving it set would decommission
   * historical1 before the next test has ingested anything -- its segments would load straight onto historical2 and
   * there would be no move left to observe.
   */
  @AfterEach
  public void restoreClusterState() throws Exception
  {
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(CoordinatorDynamicConfig.builder().withDecommissioningNodes(Set.of()).build())
    );
    broker2.start();
    for (EmbeddedBroker broker : List.of(broker1, broker2)) {
      delayedViewOf(broker).stopDelayingChanges();
    }
  }

  /**
   * Ingest at 1x, force every segment off one historical, and query continuously throughout. Every query must see the
   * full row count; none may fail. Without an ordering guarantee between the drop and the load, a broker transiently
   * sees a segment on no server at all, and under FAIL that surfaces as a failed query.
   */
  @Order(1)
  @Test
  public void test_queriesStayCompleteAndSucceed_whileSegmentsMoveBetweenHistoricals() throws Exception
  {
    loadAtSingleReplica();
    awaitBrokerSeesAllRows(broker1);

    final int segmentsToMove = numSegmentsServedBy(historical1);
    Assertions.assertTrue(
        segmentsToMove > 0,
        "historical1 must hold segments before it is decommissioned, or there is no relocation to observe"
    );

    delayPlacementSyncFrom(historical2, SYNC_DELAY_MILLIS);

    final QueryLoop queries = new QueryLoop(broker1);
    queries.start();
    try {
      forceSegmentsOff(historical1);
      awaitRelocationOff(historical1, segmentsToMove);
    }
    finally {
      queries.stop();
    }

    queries.assertNoIncompleteOrFailedResults();
    Assertions.assertTrue(queries.completedQueries() > 0, "The query loop should have run at least once");
  }

  /**
   * The risk this design introduces: the coordinator waits for brokers before dropping a source replica, so a broker
   * going away must not stall the cluster. Stop one broker mid-move and assert the move still completes and the
   * surviving broker never returns short or errors.
   * <p>
   * Ordered last, because it is the only test that takes a server away.
   */
  @Order(2)
  @Test
  public void test_brokerStoppingMidMove_doesNotStallTheClusterOrAffectOtherBrokers() throws Exception
  {
    loadAtSingleReplica();
    awaitBrokerSeesAllRows(broker1);

    final int segmentsToMove = numSegmentsServedBy(historical1);
    Assertions.assertTrue(
        segmentsToMove > 0,
        "historical1 must hold segments before it is decommissioned, or there is no relocation to observe"
    );

    delayPlacementSyncFrom(historical2, SYNC_DELAY_MILLIS);

    final QueryLoop queries = new QueryLoop(broker1);
    queries.start();
    try {
      forceSegmentsOff(historical1);
      // Immediately, so that the coordinator is still waiting on broker2's acknowledgement when it disappears.
      broker2.stop();

      // The coordinator must stop waiting on the departed broker and finish the move. A stall fails here.
      awaitRelocationOff(historical1, segmentsToMove);
    }
    finally {
      queries.stop();
    }

    queries.assertNoIncompleteOrFailedResults();
    Assertions.assertTrue(queries.completedQueries() > 0, "The query loop should have run at least once");
  }

  private void loadAtSingleReplica()
  {
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateRulesForDatasource(
            dataSource,
            List.of(new ForeverLoadRule(Map.of("_default_tier", 1), null))
        )
    );

    final String taskId = IdUtils.getRandomId();
    final IndexTask task = TaskBuilder.ofTypeIndex()
                                      .dataSource(dataSource)
                                      .isoTimestampColumn("time")
                                      .csvInputFormatWithColumns("time", "item", "value")
                                      .inlineInputSourceWithData(Resources.InlineData.CSV_10_DAYS)
                                      .segmentGranularity("DAY")
                                      .dimensions()
                                      .withId(taskId);
    cluster.callApi().runTask(task, overlord);

    // Safe to latch on despite event replay: the datasource name is fresh for every test method.
    coordinator.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/loadQueue/success")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(NUM_SEGMENTS)
    );
  }

  /**
   * Decommissioning is the cleanest way to make the balancer relocate every segment on a server.
   */
  private void forceSegmentsOff(EmbeddedHistorical historical)
  {
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(
            CoordinatorDynamicConfig
                .builder()
                .withDecommissioningNodes(Set.of(historical.bindings().selfNode().getHostAndPort()))
                .build()
        )
    );
  }

  /**
   * Returns once the relocation has both started and finished.
   * <p>
   * The metric half asserts that a move was actually issued, so the test cannot pass by never moving anything. The
   * polled half is the completion condition, and it is polled rather than latched because "the source no longer serves
   * this datasource" is a statement about cluster state, not about any single event -- it holds only once the drop has
   * followed the load.
   */
  private void awaitRelocationOff(EmbeddedHistorical source, int expectedMoves)
  {
    coordinator.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/moved/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(expectedMoves)
    );

    cluster.callApi()
           .waitForResult(() -> numSegmentsServedBy(source), remaining -> remaining == 0)
           .withTimeoutMillis(60_000)
           .go();
  }

  /**
   * How many of this datasource's segments the given historical currently serves, as the coordinator sees it.
   */
  private int numSegmentsServedBy(EmbeddedHistorical historical)
  {
    final String hostAndPort = historical.bindings().selfNode().getHostAndPort();

    int count = 0;
    final Iterable<ImmutableSegmentLoadInfo> loadInfos = cluster.callApi().onLeaderCoordinatorSync(
        c -> c.fetchServerViewSegments(dataSource, List.of(Intervals.ETERNITY))
    );
    for (ImmutableSegmentLoadInfo loadInfo : loadInfos) {
      if (loadInfo.getServers().stream().anyMatch(server -> hostAndPort.equals(server.getHostAndPort()))) {
        ++count;
      }
    }
    return count;
  }

  /**
   * The Coordinator reporting a segment loaded does not mean a Broker has synced it yet, and a delay armed by an
   * earlier test can still be in flight. Starting the loop before the Broker is caught up would make it compare
   * against a count that was short to begin with.
   */
  private void awaitBrokerSeesAllRows(EmbeddedBroker broker)
  {
    cluster.callApi()
           .waitForResult(() -> queryRowCount(broker), rows -> rows == NUM_ROWS)
           .withTimeoutMillis(60_000)
           .go();
  }

  private long queryRowCount(EmbeddedBroker broker)
  {
    final String sql = StringUtils.format("SELECT COUNT(*) FROM %s", dataSource);
    final String result = cluster.callApi().onTargetBroker(
        broker,
        b -> b.submitSqlQuery(new ClientSqlQuery(sql, ResultFormat.CSV.name(), false, false, false, null, null))
    );
    return Long.parseLong(result.trim());
  }

  /**
   * Queries in a tight loop, recording the first result that is short or that fails. Both are failures of the same
   * requirement: a relocation must be invisible to a client.
   * <p>
   * Pinned to one broker, so that stopping the other cannot be mistaken for the failure this is looking for.
   */
  private class QueryLoop
  {
    private final EmbeddedBroker broker;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<String> firstProblem = new AtomicReference<>();
    private volatile int completed;
    private Thread thread;

    QueryLoop(EmbeddedBroker broker)
    {
      this.broker = broker;
    }

    void start()
    {
      thread = new Thread(() -> {
        while (running.get()) {
          try {
            final long rows = queryRowCount(broker);
            if (rows != NUM_ROWS) {
              firstProblem.compareAndSet(
                  null,
                  StringUtils.format("Query returned [%d] rows, expected [%d]", rows, NUM_ROWS)
              );
            }
            ++completed;
          }
          catch (Exception e) {
            firstProblem.compareAndSet(null, "Query failed: " + e);
          }
        }
      }, "relocation-query-loop");
      thread.setDaemon(true);
      thread.start();
    }

    void stop() throws InterruptedException
    {
      running.set(false);
      if (thread != null) {
        thread.join(30_000);
      }
    }

    int completedQueries()
    {
      return completed;
    }

    void assertNoIncompleteOrFailedResults()
    {
      Assertions.assertNull(
          firstProblem.get(),
          "A relocation must be invisible to queries, but: " + firstProblem.get()
      );
    }
  }
}
