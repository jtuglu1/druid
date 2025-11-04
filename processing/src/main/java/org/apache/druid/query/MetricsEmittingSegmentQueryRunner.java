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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.guava.LazySequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.context.ResponseContext;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ObjLongConsumer;

/**
 * A QueryRunner that emits segment-level query metrics. This class supports two modes of operation:
 * 
 * 1. Immediate emission mode: Metrics are emitted immediately after query execution.
 *    Used by ServerManager for historical segments.
 * 
 * 2. Accumulation mode: Metrics are accumulated across multiple sub-segments and emitted later.
 *    Used by SinkQuerySegmentWalker for realtime segments (FireHydrants within a Sink).
 *    - When segmentId is non-null, metrics are accumulated
 *    - When segmentId is null, accumulated metrics are emitted
 */
public class MetricsEmittingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final ServiceEmitter emitter;
  private final QueryToolChest<T, ? extends Query<T>> queryToolChest;
  private final QueryRunner<T> queryRunner;
  private final long creationTimeNs;
  private final Set<String> metricsToCompute;
  private final String segmentIdString;
  
  // Fields for accumulation mode (used by SinkQuerySegmentWalker)
  @Nullable
  private final ConcurrentHashMap<String, SegmentMetrics> segmentMetricsAccumulator;
  @Nullable
  private final String accumulatorSegmentId;
  
  private static final Logger log = new Logger(MetricsEmittingSegmentQueryRunner.class);
  
  // Static sets of metrics to compute for common use cases
  public static final Set<String> METRICS_SEGMENT_TIME = ImmutableSet.of(DefaultQueryMetrics.QUERY_SEGMENT_TIME);
  public static final Set<String> METRICS_WAIT_TIME = ImmutableSet.of(DefaultQueryMetrics.QUERY_WAIT_TIME);
  public static final Set<String> METRICS_SEGMENT_AND_WAIT_TIME = ImmutableSet.of(
      DefaultQueryMetrics.QUERY_SEGMENT_TIME,
      DefaultQueryMetrics.QUERY_WAIT_TIME
  );
  
  // Map of metric names to their reporting functions
  private static final Map<String, ObjLongConsumer<? super QueryMetrics<?>>> METRICS_TO_REPORT =
      Map.of(
          DefaultQueryMetrics.QUERY_SEGMENT_TIME, QueryMetrics::reportSegmentTime,
          DefaultQueryMetrics.QUERY_WAIT_TIME, QueryMetrics::reportWaitTime
      );

  /**
   * Private constructor for all modes.
   */
  private MetricsEmittingSegmentQueryRunner(
      ServiceEmitter emitter,
      QueryToolChest<T, ? extends Query<T>> queryToolChest,
      QueryRunner<T> queryRunner,
      long creationTimeNs,
      Set<String> metricsToCompute,
      String segmentIdString,
      @Nullable ConcurrentHashMap<String, SegmentMetrics> segmentMetricsAccumulator,
      @Nullable String accumulatorSegmentId,
      @Nullable AtomicBoolean cacheHit
  )
  {
    this.emitter = emitter;
    this.queryToolChest = queryToolChest;
    this.queryRunner = queryRunner;
    this.creationTimeNs = creationTimeNs;
    this.metricsToCompute = metricsToCompute;
    this.segmentIdString = segmentIdString;
    this.segmentMetricsAccumulator = segmentMetricsAccumulator;
    this.accumulatorSegmentId = accumulatorSegmentId;
    this.cacheHit = cacheHit;
  }

  /**
   * Constructor for immediate emission mode (used by ServerManager).
   * 
   * @param segmentIdString Segment identifier to set as dimension when emitting metrics
   */
  public MetricsEmittingSegmentQueryRunner(
      ServiceEmitter emitter,
      QueryToolChest<T, ? extends Query<T>> queryToolChest,
      QueryRunner<T> queryRunner,
      Set<String> metricsToCompute,
      String segmentIdString,
      @Nullable AtomicBoolean cacheHit
  )
  {
    this(emitter, queryToolChest, queryRunner, -1, metricsToCompute, segmentIdString, null, null, cacheHit);
  }

  /**
   * Constructor for accumulation mode (used by SinkQuerySegmentWalker).
   * 
   * @param segmentMetricsAccumulator Map to accumulate metrics across sub-segments
   * @param metricsToCompute Set of metric names to compute (e.g., QUERY_SEGMENT_TIME, QUERY_WAIT_TIME)
   * @param accumulatorSegmentId Segment identifier for accumulation; null to emit accumulated metrics
   * @param cacheHit AtomicBoolean to track cache hit status
   */
  public MetricsEmittingSegmentQueryRunner(
      ServiceEmitter emitter,
      QueryToolChest<T, ? extends Query<T>> queryToolChest,
      QueryRunner<T> queryRunner,
      ConcurrentHashMap<String, SegmentMetrics> segmentMetricsAccumulator,
      Set<String> metricsToCompute,
      @Nullable String accumulatorSegmentId,
      @Nullable AtomicBoolean cacheHit
  )
  {
    this(
        emitter,
        queryToolChest,
        queryRunner,
        System.nanoTime(),
        metricsToCompute,
        accumulatorSegmentId != null ? accumulatorSegmentId : "",
        segmentMetricsAccumulator,
        accumulatorSegmentId,
        cacheHit
    );
  }

  public MetricsEmittingSegmentQueryRunner<T> withWaitMeasuredFromNow()
  {
    return new MetricsEmittingSegmentQueryRunner<>(
        emitter,
        queryToolChest,
        queryRunner,
        System.nanoTime(),
        metricsToCompute,
        segmentIdString,
        segmentMetricsAccumulator,
        accumulatorSegmentId,
        cacheHit
    );
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
  {
    final boolean isAccumulationMode = segmentMetricsAccumulator != null;
    
    if (isAccumulationMode && accumulatorSegmentId != null) {
      // Accumulation mode: accumulate metrics for this segment
      return runAndAccumulateMetrics(queryPlus, responseContext);
    } else if (isAccumulationMode) {
      // Accumulation mode: emit all accumulated metrics
      return runAndEmitAccumulatedMetrics(queryPlus, responseContext);
    } else {
      // Immediate emission mode: emit metrics right after execution
      return runAndEmitImmediate(queryPlus, responseContext);
    }
  }

  /**
   * Accumulation mode: Run query and accumulate metrics for a specific segment.
   */
  private Sequence<T> runAndAccumulateMetrics(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
  {
    return Sequences.wrap(
        new LazySequence<>(() -> queryRunner.run(queryPlus, responseContext)),
        new SequenceWrapper()
        {
          private long startTimeNs;

          @Override
          public void before()
          {
            startTimeNs = System.nanoTime();
          }

          @Override
          public void after(boolean isDone, Throwable thrown)
          {
            final SegmentMetrics metrics = segmentMetricsAccumulator.computeIfAbsent(
                accumulatorSegmentId,
                id -> new SegmentMetrics()
            );
            
            if (metricsToCompute.contains(DefaultQueryMetrics.QUERY_WAIT_TIME)) {
              metrics.setWaitTime(startTimeNs - creationTimeNs);
            }
            if (metricsToCompute.contains(DefaultQueryMetrics.QUERY_SEGMENT_TIME)) {
              metrics.addSegmentTime(System.nanoTime() - startTimeNs);
            }

            // Track cache hit status for this segment
            if (cacheHit != null) {
              metrics.setCacheHit(cacheHit.get());
            }
          }
        }
    );
  }

  /**
   * Accumulation mode: Run query and emit all accumulated metrics.
   */
  private Sequence<T> runAndEmitAccumulatedMetrics(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
  {
    final QueryPlus<T> queryWithMetrics = queryPlus.withQueryMetrics(queryToolChest);
    
    return Sequences.wrap(
        new LazySequence<>(() -> queryRunner.run(queryWithMetrics, responseContext)),
        new SequenceWrapper()
        {
          @Override
          public void before()
          {
            // Nothing to do
          }

          @Override
          public void after(boolean isDone, Throwable thrown)
          {
            final QueryMetrics<?> queryMetrics = queryWithMetrics.getQueryMetrics();
            
            // Emit accumulated metrics for each segment
            for (Map.Entry<String, SegmentMetrics> segmentAndMetrics : segmentMetricsAccumulator.entrySet()) {
              queryMetrics.segment(segmentAndMetrics.getKey());
              
              final SegmentMetrics metrics = segmentAndMetrics.getValue();

              if (cacheHit != null) {
                queryMetrics.segmentCacheHit(metrics.getCacheHit());
              }
              
              // Report each metric that was computed
              for (Map.Entry<String, ObjLongConsumer<? super QueryMetrics<?>>> reportMetric : METRICS_TO_REPORT.entrySet()) {
                final String metricName = reportMetric.getKey();
                if (!metricsToCompute.isEmpty() && !metricsToCompute.contains(metricName)) {
                  continue;
                }
                
                switch (metricName) {
                  case DefaultQueryMetrics.QUERY_SEGMENT_TIME:
                    reportMetric.getValue().accept(queryMetrics, metrics.getSegmentTime());
                    break;
                  case DefaultQueryMetrics.QUERY_WAIT_TIME:
                    reportMetric.getValue().accept(queryMetrics, metrics.getWaitTime());
                    break;
                }
              }
              
              try {
                queryMetrics.emit(emitter);
              }
              catch (Exception e) {
                // Query should not fail, because of emitter failure. Swallowing the exception.
                log.error(e, "Failed to emit metrics for segment[%s]", segmentAndMetrics.getKey());
              }
            }
          }
        }
    );
  }

  /**
   * Immediate emission mode: Run query and emit metrics immediately.
   */
  private Sequence<T> runAndEmitImmediate(final QueryPlus<T> queryPlus, final ResponseContext responseContext)
  {
    QueryPlus<T> queryWithMetrics = queryPlus.withQueryMetrics(queryToolChest);
    final QueryMetrics<?> queryMetrics = queryWithMetrics.getQueryMetrics();

    // Set segment ID dimension
    queryMetrics.segment(segmentIdString);

    return Sequences.wrap(
        // Use LazySequence because want to account execution time of queryRunner.run() (it prepares the underlying
        // Sequence) as part of the reported query time, i. e. we want to execute queryRunner.run() after
        // `startTime = System.nanoTime();` (see below).
        new LazySequence<>(() -> queryRunner.run(queryWithMetrics, responseContext)),
        new SequenceWrapper()
        {
          private long startTimeNs;

          @Override
          public void before()
          {
            startTimeNs = System.nanoTime();
          }

          @Override
          public void after(boolean isDone, Throwable thrown)
          {
            if (thrown != null) {
              queryMetrics.status("failed");
            } else if (!isDone) {
              queryMetrics.status("short");
            }
            
            long timeTakenNs = System.nanoTime() - startTimeNs;
            
            // Add cache hit dimension if available
            if (cacheHit != null) {
              queryMetrics.segmentCacheHit(cacheHit.get());
            }
            
            // Report each metric that was requested
            for (Map.Entry<String, ObjLongConsumer<? super QueryMetrics<?>>> reportMetric : METRICS_TO_REPORT.entrySet()) {
              final String metricName = reportMetric.getKey();
              if (!metricsToCompute.isEmpty() && !metricsToCompute.contains(metricName)) {
                continue;
              }
              
              switch (metricName) {
                case DefaultQueryMetrics.QUERY_SEGMENT_TIME:
                  reportMetric.getValue().accept(queryMetrics, timeTakenNs);
                  break;
                case DefaultQueryMetrics.QUERY_WAIT_TIME:
                  if (creationTimeNs > 0) {
                    queryMetrics.reportWaitTime(startTimeNs - creationTimeNs);
                  }
                  break;
              }
            }
            
            try {
              queryMetrics.emit(emitter);
            }
            catch (Exception e) {
              // Query should not fail, because of emitter failure. Swallowing the exception.
              log.error("Failure while trying to emit [%s] with stacktrace [%s]", emitter.toString(), e);
            }
          }
        }
    );
  }

  /**
   * Class to track segment related metrics during query execution.
   * Used in accumulation mode to aggregate metrics across multiple sub-segments.
   */
  public static class SegmentMetrics
  {
    private final AtomicLong querySegmentTime = new AtomicLong(0);
    private final AtomicLong queryWaitTime = new AtomicLong(0);
    private final AtomicBoolean querySegmentCacheHit = new AtomicBoolean(false);

    public void addSegmentTime(long time)
    {
      querySegmentTime.addAndGet(time);
    }

    public void setWaitTime(long time)
    {
      queryWaitTime.set(time);
    }

    public void setCacheHit(boolean hit)
    {
      this.querySegmentCacheHit.set(hit);
    }

    public long getSegmentTime()
    {
      return querySegmentTime.get();
    }

    public long getWaitTime()
    {
      return queryWaitTime.get();
    }

    public boolean getCacheHit()
    {
      return querySegmentCacheHit.get();
    }
  }
}
