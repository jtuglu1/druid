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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.IntervalCacheConfig;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

/**
 * Integration-style tests for {@link IntervalCachingQueryRunner}. Uses a mock base runner and a real
 * {@link MapCache} to verify hit/miss/partial-hit behaviour and eligibility filtering.
 */
public class IntervalCachingQueryRunnerTest extends QueryRunnerBasedOnClusteredClientTestBase
{
  /** 1-hour bucket size — aligns with HOUR granularity so each result maps to exactly one bucket. */
  private static final long BUCKET_MS = 3_600_000L;

  private Cache cache;

  @Before
  public void setUpCache()
  {
    cache = MapCache.create(1 << 22);
  }

  @After
  public void tearDownCache() throws IOException
  {
    cache.close();
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /** Config with both use and populate enabled, very large maxAgeDays so historical intervals are eligible. */
  private static IntervalCacheConfig enabledConfig()
  {
    return new IntervalCacheConfig()
    {
      @Override
      public boolean isUseIntervalCache()
      {
        return true;
      }

      @Override
      public boolean isPopulateIntervalCache()
      {
        return true;
      }

      @Override
      public long getBucketMillis()
      {
        return BUCKET_MS;
      }

      @Override
      public int getMaxAgeDays()
      {
        return Integer.MAX_VALUE;
      }
    };
  }

  private static IntervalCacheConfig disabledConfig()
  {
    return new IntervalCacheConfig(); // defaults: useIntervalCache=false, populateIntervalCache=false
  }

  /**
   * Returns one {@code Result<TimeseriesResultValue>} per hour bucket in the given interval.
   * Each result has timestamp = bucket start and rows=1.
   */
  private static List<Result<TimeseriesResultValue>> resultsForInterval(Interval interval)
  {
    final ImmutableList.Builder<Result<TimeseriesResultValue>> builder = ImmutableList.builder();
    for (long ts = IntervalCachingQueryRunner.floorToBucket(interval.getStartMillis(), BUCKET_MS);
         ts < interval.getEndMillis();
         ts += BUCKET_MS) {
      builder.add(new Result<>(DateTimes.utc(ts), new TimeseriesResultValue(ImmutableMap.of("rows", 1L))));
    }
    return builder.build();
  }

  /**
   * Builds a timeseries query spanning the given interval with HOUR granularity and a count aggregator.
   */
  private static TimeseriesQuery buildTsQuery(Interval interval)
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource("test-ds")
                 .intervals(ImmutableList.of(interval))
                 .granularity(Granularities.HOUR)
                 .aggregators(new CountAggregatorFactory("rows"))
                 .build();
  }

  @SuppressWarnings("unchecked")
  private IntervalCachingQueryRunner<Result<TimeseriesResultValue>> createRunner(
      QueryRunner<Result<TimeseriesResultValue>> baseRunner,
      TimeseriesQuery query,
      IntervalCacheConfig config
  )
  {
    return new IntervalCachingQueryRunner<>(
        baseRunner,
        (QueryToolChest<Result<TimeseriesResultValue>, Query<Result<TimeseriesResultValue>>>) (QueryToolChest<?, ?>) conglomerate.getToolChest(query),
        query,
        cache,
        config,
        objectMapper
    );
  }

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------

  @Test
  public void testDisabledConfig_passesThrough()
  {
    final Interval interval = Intervals.of("2000-01-01T00/PT2H");
    final TimeseriesQuery query = buildTsQuery(interval);
    final List<Result<TimeseriesResultValue>> expected = resultsForInterval(interval);

    @SuppressWarnings("unchecked")
    final QueryRunner<Result<TimeseriesResultValue>> mockRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any()))
           .thenReturn(Sequences.simple(expected));

    final IntervalCachingQueryRunner<Result<TimeseriesResultValue>> runner = createRunner(
        mockRunner,
        query,
        disabledConfig()
    );

    final List<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), responseContext()).toList();
    Assert.assertEquals(expected, results);
    // Base runner must have been called exactly once with the full query
    Mockito.verify(mockRunner, Mockito.times(1)).run(Mockito.any(), Mockito.any());
    // Nothing written to cache
    Assert.assertEquals(0, cache.getStats().getNumEntries());
  }

  @Test
  public void testCacheMiss_populatesCache()
  {
    final Interval interval = Intervals.of("2000-01-01T00/PT2H");
    final TimeseriesQuery query = buildTsQuery(interval);
    final List<Result<TimeseriesResultValue>> expected = resultsForInterval(interval);

    @SuppressWarnings("unchecked")
    final QueryRunner<Result<TimeseriesResultValue>> mockRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any()))
           .thenReturn(Sequences.simple(expected));

    final IntervalCachingQueryRunner<Result<TimeseriesResultValue>> runner = createRunner(
        mockRunner,
        query,
        enabledConfig()
    );

    final List<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), responseContext()).toList();
    Assert.assertEquals(expected, results);
    Mockito.verify(mockRunner, Mockito.times(1)).run(Mockito.any(), Mockito.any());
    // 2 one-hour buckets should be cached
    Assert.assertEquals(2, cache.getStats().getNumEntries());
  }

  @Test
  public void testFullCacheHit_baseRunnerNotCalled()
  {
    final Interval interval = Intervals.of("2000-01-01T00/PT2H");
    final TimeseriesQuery query = buildTsQuery(interval);
    final List<Result<TimeseriesResultValue>> expected = resultsForInterval(interval);

    @SuppressWarnings("unchecked")
    final QueryRunner<Result<TimeseriesResultValue>> mockRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any()))
           .thenReturn(Sequences.simple(expected));

    final IntervalCachingQueryRunner<Result<TimeseriesResultValue>> runner = createRunner(
        mockRunner,
        query,
        enabledConfig()
    );

    // First run — populates cache
    runner.run(QueryPlus.wrap(query), responseContext()).toList();
    Mockito.verify(mockRunner, Mockito.times(1)).run(Mockito.any(), Mockito.any());

    // Second run — full cache hit, base runner must NOT be called again
    final List<Result<TimeseriesResultValue>> results2 = runner.run(QueryPlus.wrap(query), responseContext()).toList();
    Assert.assertEquals(expected.size(), results2.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(expected.get(i).getTimestamp(), results2.get(i).getTimestamp());
    }
    Mockito.verify(mockRunner, Mockito.times(1)).run(Mockito.any(), Mockito.any()); // still 1
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPartialCacheHit_baseRunnerCalledForUncoveredSuffix()
  {
    // First populate [T0, T0+2h]; then query [T0, T0+3h] — only [T0+2h, T0+3h] should hit base runner.
    final Interval firstInterval = Intervals.of("2000-01-01T00/PT2H");
    final Interval fullInterval = Intervals.of("2000-01-01T00/PT3H");
    final Interval expectedMissInterval = Intervals.of("2000-01-01T02/PT1H");

    final TimeseriesQuery firstQuery = buildTsQuery(firstInterval);
    final TimeseriesQuery fullQuery = buildTsQuery(fullInterval);

    final QueryRunner<Result<TimeseriesResultValue>> mockRunner = Mockito.mock(QueryRunner.class);
    // Stub for the full-interval call (first run) and then the suffix call (second run)
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any()))
           .thenAnswer(invocation -> {
             QueryPlus<Result<TimeseriesResultValue>> qp = invocation.getArgument(0);
             Interval qInterval = qp.getQuery().getIntervals().get(0);
             return Sequences.simple(resultsForInterval(qInterval));
           });

    // Populate cache for [T0, T0+2h]
    final IntervalCachingQueryRunner<Result<TimeseriesResultValue>> populateRunner = createRunner(
        mockRunner,
        firstQuery,
        enabledConfig()
    );
    populateRunner.run(QueryPlus.wrap(firstQuery), responseContext()).toList();
    Mockito.verify(mockRunner, Mockito.times(1)).run(Mockito.any(), Mockito.any());

    // Now run the wider query [T0, T0+3h]
    final IntervalCachingQueryRunner<Result<TimeseriesResultValue>> fullRunner = createRunner(
        mockRunner,
        fullQuery,
        enabledConfig()
    );
    final List<Result<TimeseriesResultValue>> results = fullRunner.run(QueryPlus.wrap(fullQuery), responseContext()).toList();

    // Base runner should have been called a second time — only for the uncovered suffix
    final ArgumentCaptor<QueryPlus> captor = ArgumentCaptor.forClass(QueryPlus.class);
    Mockito.verify(mockRunner, Mockito.times(2)).run(captor.capture(), Mockito.any());
    final List<Interval> missedIntervals = captor.getValue().getQuery().getIntervals();
    Assert.assertEquals(1, missedIntervals.size());
    Assert.assertEquals(expectedMissInterval, missedIntervals.get(0));

    // Results must span the full 3-hour interval (3 hourly buckets)
    Assert.assertEquals(3, results.size());
    Assert.assertEquals(DateTimes.of("2000-01-01T00"), results.get(0).getTimestamp());
    Assert.assertEquals(DateTimes.of("2000-01-01T01"), results.get(1).getTimestamp());
    Assert.assertEquals(DateTimes.of("2000-01-01T02"), results.get(2).getTimestamp());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testIneligibleQuery_topNPassesThrough()
  {
    final TopNQuery topNQuery = new TopNQueryBuilder()
        .dataSource("test-ds")
        .intervals(ImmutableList.of(Intervals.of("2000-01-01T00/PT1H")))
        .granularity(Granularities.HOUR)
        .dimension("dim")
        .metric("rows")
        .threshold(10)
        .aggregators(new CountAggregatorFactory("rows"))
        .build();

    final QueryRunner mockRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any())).thenReturn(Sequences.empty());

    final IntervalCachingQueryRunner runner = new IntervalCachingQueryRunner(
        mockRunner,
        conglomerate.getToolChest(topNQuery),
        topNQuery,
        cache,
        enabledConfig(),
        objectMapper
    );
    runner.run(QueryPlus.wrap(topNQuery), responseContext()).toList();

    // Must pass through to base runner without caching
    Mockito.verify(mockRunner, Mockito.times(1)).run(Mockito.any(), Mockito.any());
    Assert.assertEquals(0, cache.getStats().getNumEntries());
  }

  @Test
  public void testIneligibleQuery_granularityAll()
  {
    final TimeseriesQuery allGranQuery = Druids.newTimeseriesQueryBuilder()
                                               .dataSource("test-ds")
                                               .intervals(ImmutableList.of(Intervals.of("2000-01-01T00/PT2H")))
                                               .granularity(Granularities.ALL)
                                               .aggregators(new CountAggregatorFactory("rows"))
                                               .build();

    @SuppressWarnings("unchecked")
    final QueryRunner<Result<TimeseriesResultValue>> mockRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any())).thenReturn(Sequences.empty());

    final IntervalCachingQueryRunner<Result<TimeseriesResultValue>> runner = createRunner(
        mockRunner,
        allGranQuery,
        enabledConfig()
    );
    runner.run(QueryPlus.wrap(allGranQuery), responseContext()).toList();

    Mockito.verify(mockRunner, Mockito.times(1)).run(Mockito.any(), Mockito.any());
    Assert.assertEquals(0, cache.getStats().getNumEntries());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testIneligibleQuery_groupByWithDimensions()
  {
    final GroupByQuery groupByQuery = GroupByQuery.builder()
                                                  .setDataSource("test-ds")
                                                  .setInterval(Intervals.of("2000-01-01T00/PT2H"))
                                                  .setGranularity(Granularities.HOUR)
                                                  .setDimensions(
                                                      ImmutableList.of(new DefaultDimensionSpec("dim1", "dim1"))
                                                  )
                                                  .setAggregatorSpecs(new CountAggregatorFactory("rows"))
                                                  .build();

    final QueryRunner mockRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any())).thenReturn(Sequences.empty());

    final IntervalCachingQueryRunner runner = new IntervalCachingQueryRunner(
        mockRunner,
        conglomerate.getToolChest(groupByQuery),
        groupByQuery,
        cache,
        enabledConfig(),
        objectMapper
    );
    runner.run(QueryPlus.wrap(groupByQuery), responseContext()).toList();

    Mockito.verify(mockRunner, Mockito.times(1)).run(Mockito.any(), Mockito.any());
    Assert.assertEquals(0, cache.getStats().getNumEntries());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testEligibleQuery_groupByNoDimensions()
  {
    final GroupByQuery groupByQuery = GroupByQuery.builder()
                                                  .setDataSource("test-ds")
                                                  .setInterval(Intervals.of("2000-01-01T00/PT2H"))
                                                  .setGranularity(Granularities.HOUR)
                                                  // No dimensions — eligible for interval cache
                                                  .setAggregatorSpecs(new CountAggregatorFactory("rows"))
                                                  .build();

    final QueryRunner mockRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any())).thenReturn(Sequences.empty());

    final IntervalCachingQueryRunner runner = new IntervalCachingQueryRunner(
        mockRunner,
        conglomerate.getToolChest(groupByQuery),
        groupByQuery,
        cache,
        enabledConfig(),
        objectMapper
    );
    runner.run(QueryPlus.wrap(groupByQuery), responseContext()).toList();

    // Query was eligible: the cache was consulted (2 misses for the 2-hour interval), base runner called
    Mockito.verify(mockRunner, Mockito.times(1)).run(Mockito.any(), Mockito.any());
    Assert.assertEquals(2, cache.getStats().getNumMisses());
    // No results returned → no entries written (trailing empty buckets are not negative-cached)
    Assert.assertEquals(0, cache.getStats().getNumEntries());
  }

  @Test
  public void testExpiredEntryTreatedAsMiss()
  {
    // Put an entry with ttlSeconds=0 — expires immediately
    final Cache.NamedKey key = IntervalCachingQueryRunner.makeBucketKey("test-ns", 0L);
    cache.put(key, new byte[]{1, 2, 3}, 0);

    // get() after expiry must return null
    Assert.assertNull(cache.get(key));
  }

  @Test
  public void testNoTrailingEmptyBucketsWhenResultsAreSparse()
  {
    // Query covers 3 hours; only the first 2 hours have results. The 3rd hour should NOT be
    // negative-cached, because events for that bucket may not have arrived yet.
    final Interval interval = Intervals.of("2000-01-01T00/PT3H");
    final TimeseriesQuery query = buildTsQuery(interval);
    // Only return 2 results (00:00 and 01:00); nothing for 02:00
    final List<Result<TimeseriesResultValue>> twoHourResults = resultsForInterval(Intervals.of("2000-01-01T00/PT2H"));

    @SuppressWarnings("unchecked")
    final QueryRunner<Result<TimeseriesResultValue>> mockRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any()))
           .thenReturn(Sequences.simple(twoHourResults));

    createRunner(mockRunner, query, enabledConfig()).run(QueryPlus.wrap(query), responseContext()).toList();

    // Only the 2 result buckets should be cached; the empty trailing 02:00 bucket must not be
    Assert.assertEquals(2, cache.getStats().getNumEntries());
  }

  @Test
  public void testPartialBoundaryBucketNotCached()
  {
    // Interval [00:30, 02:00) with hourly buckets: the first bucket [00:00, 01:00) is only partially
    // covered (starts at 00:30) — it must not be cached.
    final long startMs = Intervals.of("2000-01-01T00/PT1H").getStartMillis() + 30 * 60_000L;
    final long endMs = Intervals.of("2000-01-01T02/PT1H").getStartMillis();
    final Interval partialStartInterval = Intervals.utc(startMs, endMs);
    final TimeseriesQuery query = buildTsQuery(partialStartInterval);

    // Druid returns a result timestamped at 00:00 (granularity floor of 00:30)
    final List<Result<TimeseriesResultValue>> results = ImmutableList.of(
        new Result<>(DateTimes.utc(Intervals.of("2000-01-01T00/PT1H").getStartMillis()),
                     new TimeseriesResultValue(ImmutableMap.of("rows", 1L))),
        new Result<>(DateTimes.utc(Intervals.of("2000-01-01T01/PT1H").getStartMillis()),
                     new TimeseriesResultValue(ImmutableMap.of("rows", 1L)))
    );

    @SuppressWarnings("unchecked")
    final QueryRunner<Result<TimeseriesResultValue>> mockRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any()))
           .thenReturn(Sequences.simple(results));

    createRunner(mockRunner, query, enabledConfig()).run(QueryPlus.wrap(query), responseContext()).toList();

    // Bucket 00:00 is partial (interval starts at 00:30) → not cached
    // Bucket 01:00 is fully covered by [00:30, 02:00) → cached
    Assert.assertEquals(1, cache.getStats().getNumEntries());
  }

  @Test
  public void testIsEligible_queryTooOld()
  {
    final IntervalCacheConfig strictConfig = new IntervalCacheConfig()
    {
      @Override
      public int getMaxAgeDays()
      {
        return 1; // only 1 day
      }
    };
    // 2000-01-01 is ~26 years old — should be rejected by maxAgeDays check
    final TimeseriesQuery oldQuery = buildTsQuery(Intervals.of("2000-01-01T00/PT1H"));

    @SuppressWarnings("unchecked")
    final QueryRunner<Result<TimeseriesResultValue>> mockRunner = Mockito.mock(QueryRunner.class);
    Mockito.when(mockRunner.run(Mockito.any(), Mockito.any())).thenReturn(Sequences.empty());

    final IntervalCachingQueryRunner<Result<TimeseriesResultValue>> runner = createRunner(
        mockRunner,
        oldQuery,
        strictConfig
    );
    runner.run(QueryPlus.wrap(oldQuery), responseContext()).toList();

    // Too old → ineligible → passes through without caching
    Mockito.verify(mockRunner, Mockito.times(1)).run(Mockito.any(), Mockito.any());
    Assert.assertEquals(0, cache.getStats().getNumEntries());
  }
}
