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

package org.apache.druid.benchmark.indexing;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorTester;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for StreamAppenderator add() throughput.
 * <p>
 * This measures the hot path of adding rows to the appenderator, which is
 * critical for streaming ingestion performance.
 * <p>
 * Run with:
 * <pre>
 * java -jar benchmarks/target/benchmarks.jar StreamAppenderatorBenchmark
 * </pre>
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
public class StreamAppenderatorBenchmark
{
  private static final Logger log = new Logger(StreamAppenderatorBenchmark.class);
  // Must match StreamAppenderatorTester.DATASOURCE
  private static final String DATASOURCE = StreamAppenderatorTester.DATASOURCE;

  @Param({"100000"})
  private int rowsPerIteration;

  @Param({"5"})
  private int numSegments;

  @Param({"100000"})
  private int maxRowsInMemory;

  private StreamAppenderatorTester appenderatorTester;
  private Appenderator appenderator;
  private List<InputRow> rows;
  private List<SegmentIdWithShardSpec> segmentIds;
  private File tmpDir;

  @Setup(Level.Trial)
  public void setupTrial()
  {
    log.info("Setting up trial with rowsPerIteration=%d, numSegments=%d", rowsPerIteration, numSegments);

    // Pre-generate all rows to avoid row generation overhead in benchmark
    rows = new ArrayList<>(rowsPerIteration);
    long baseTime = DateTimes.of("2024-01-01T00:00:00Z").getMillis();

    for (int i = 0; i < rowsPerIteration; i++) {
      rows.add(new MapBasedInputRow(
          baseTime + (i * 1000L), // 1 second apart
          Arrays.asList("dim1", "dim2"),
          ImmutableMap.of(
              "ts", baseTime + (i * 1000L),
              "dim1", "value_" + (i % 1000),
              "dim2", "category_" + (i % 100),
              "met", (long) (i % 10000)
          )
      ));
    }

    // Pre-generate segment IDs
    segmentIds = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      segmentIds.add(new SegmentIdWithShardSpec(
          DATASOURCE,
          Intervals.of("2024-01-01/2024-01-02"),
          "version_" + i,
          new LinearShardSpec(i)
      ));
    }
  }

  @Setup(Level.Iteration)
  public void setupIteration()
  {
    tmpDir = FileUtils.createTempDir();
    log.info("Using temp dir: %s", tmpDir.getAbsolutePath());

    appenderatorTester = new StreamAppenderatorTester.Builder()
        .maxRowsInMemory(maxRowsInMemory)
        .maxSizeInBytes(50_000_000L) // 50MB
        .basePersistDirectory(tmpDir)
        .rowIngestionMeters(new SimpleRowIngestionMeters())
        .skipBytesInMemoryOverheadCheck(true)
        .build();

    appenderator = appenderatorTester.getAppenderator();
    appenderator.startJob();
  }

  @TearDown(Level.Iteration)
  public void teardownIteration() throws Exception
  {
    if (appenderatorTester != null) {
      appenderatorTester.close();
      appenderatorTester = null;
    }
    if (tmpDir != null) {
      FileUtils.deleteDirectory(tmpDir);
      tmpDir = null;
    }
  }

//  /**
//   * Benchmark add() throughput with a single segment.
//   * This tests the common case where rows are continuously added to one active segment.
//   */
//  @Benchmark
//  @BenchmarkMode(Mode.Throughput)
//  @OutputTimeUnit(TimeUnit.SECONDS)
//  public void addRowsSingleSegment(Blackhole blackhole) throws Exception
//  {
//    SegmentIdWithShardSpec segmentId = segmentIds.get(0);
//    for (InputRow row : rows) {
//      blackhole.consume(appenderator.add(segmentId, row, null, false));
//    }
//  }
//
//  /**
//   * Benchmark add() throughput with multiple segments (round-robin distribution).
//   * This tests scenarios with multiple active segments, exercising sink lookup.
//   */
//  @Benchmark
//  @BenchmarkMode(Mode.Throughput)
//  @OutputTimeUnit(TimeUnit.SECONDS)
//  public void addRowsMultipleSegments(Blackhole blackhole) throws Exception
//  {
//    int numSegs = segmentIds.size();
//    for (int i = 0; i < rows.size(); i++) {
//      SegmentIdWithShardSpec segmentId = segmentIds.get(i % numSegs);
//      blackhole.consume(appenderator.add(segmentId, rows.get(i), null, false));
//    }
//  }
//
//  /**
//   * Benchmark add() with incremental persist enabled.
//   * This tests the full path including persist threshold checks.
//   */
//  @Benchmark
//  @BenchmarkMode(Mode.Throughput)
//  @OutputTimeUnit(TimeUnit.SECONDS)
//  public void addRowsWithIncrementalPersist(Blackhole blackhole) throws Exception
//  {
//    SegmentIdWithShardSpec segmentId = segmentIds.get(0);
//    for (InputRow row : rows) {
//      blackhole.consume(appenderator.add(segmentId, row, null, true));
//    }
//  }

  /**
   * Benchmark average time per add() call.
   * Useful for understanding per-row latency.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void addRowAverageTime(Blackhole blackhole) throws Exception
  {
    // Add a single row to measure per-row overhead
    SegmentIdWithShardSpec segmentId = segmentIds.get(0);
    InputRow row = rows.get(0);
    blackhole.consume(appenderator.add(segmentId, row, null, false));
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(StreamAppenderatorBenchmark.class.getSimpleName())
        .forks(1)
        .warmupIterations(5)
        .measurementIterations(10)
        .measurementTime(TimeValue.seconds(5))
        .build();
    new Runner(opt).run();
  }
}
