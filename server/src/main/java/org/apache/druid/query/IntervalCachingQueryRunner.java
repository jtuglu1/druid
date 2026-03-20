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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.IntervalCacheConfig;
import org.apache.druid.client.cache.IntervalCacheTtlCalculator;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.joda.time.Interval;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A broker-level query runner that provides partial interval-based caching on top of the existing
 * segment-level cache. Unlike {@link ResultLevelCachingQueryRunner} (which is all-or-nothing), this
 * runner can satisfy a sub-range of a query from cache and issue a reduced query to the cluster for
 * the uncovered suffix.
 *
 * <p>Sits between {@link ResultLevelCachingQueryRunner} (outer) and the base cluster runner (inner):
 * <pre>
 *   ResultLevelCachingQueryRunner
 *     └─ IntervalCachingQueryRunner   ← this class
 *          └─ base cluster runner (segment-level cache applies here)
 * </pre>
 *
 * <p>Only eligible for queries where results are strictly one row per time bucket:
 * {@link TimeseriesQuery} (always) and {@link GroupByQuery} with no dimensions.
 */
public class IntervalCachingQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(IntervalCachingQueryRunner.class);

  /** Cache namespace prefix distinguishing interval-cache entries from result-level cache entries. */
  static final String NAMESPACE_PREFIX = "INT:";

  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;
  private final Query<T> query;
  private final Cache cache;
  private final IntervalCacheConfig config;
  private final ObjectMapper mapper;

  public IntervalCachingQueryRunner(
      QueryRunner<T> baseRunner,
      QueryToolChest<T, Query<T>> toolChest,
      Query<T> query,
      Cache cache,
      IntervalCacheConfig config,
      ObjectMapper mapper
  )
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
    this.query = query;
    this.cache = cache;
    this.config = config;
    this.mapper = mapper;
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    if (!config.isUseIntervalCache() && !config.isPopulateIntervalCache()) {
      return baseRunner.run(queryPlus, responseContext);
    }

    final CacheStrategy<T, Object, Query<T>> strategy = toolChest.getCacheStrategy(query, mapper);
    if (!isEligible(query, strategy)) {
      return baseRunner.run(queryPlus, responseContext);
    }

    final String namespace = NAMESPACE_PREFIX + computeFingerprint(
        query.getDataSource().toString(),
        strategy.computeResultLevelCacheKey(query)
    );
    final long bucketMillis = config.getBucketMillis();
    final List<Interval> queryIntervals = query.getIntervals();
    if (queryIntervals.isEmpty()) {
      return baseRunner.run(queryPlus, responseContext);
    }

    // Enumerate all bucket keys for all query intervals (one Cache.NamedKey per bucket)
    final List<Cache.NamedKey> allBucketKeys = enumerateBucketKeys(namespace, queryIntervals, bucketMillis);

    // Single round-trip to fetch whatever is cached
    final Map<Cache.NamedKey, byte[]> cachedMap = config.isUseIntervalCache()
                                                  ? cache.getBulk(allBucketKeys)
                                                  : Collections.emptyMap();

    // For each query interval find the contiguous cached prefix and build the list of uncovered intervals
    final List<T> cachedResults = new ArrayList<>();
    final List<Interval> missingIntervals = new ArrayList<>();

    for (Interval interval : queryIntervals) {
      final long bucketStart = floorToBucket(interval.getStartMillis(), bucketMillis);
      // splitMs tracks where the uncovered suffix starts. Initialise to the actual query start
      // (not bucketStart) so that a full miss never widens the Druid query backwards past the
      // original interval boundary.
      long splitMs = interval.getStartMillis();

      for (long ts = bucketStart; ts < interval.getEndMillis(); ts += bucketMillis) {
        final byte[] raw = cachedMap.get(makeBucketKey(namespace, ts));
        if (raw == null) {
          break; // gap found — stop prefix scan
        }
        cachedResults.addAll(decodeBucket(raw, strategy));
        splitMs = ts + bucketMillis;
      }

      if (splitMs < interval.getEndMillis()) {
        missingIntervals.add(Intervals.utc(splitMs, interval.getEndMillis()));
      }
    }

    if (missingIntervals.isEmpty()) {
      // Full cache hit
      return Sequences.simple(cachedResults);
    }

    // Run base runner only for the uncovered intervals
    final Query<T> reducedQuery = query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(missingIntervals));
    final Sequence<T> freshSequence = baseRunner.run(queryPlus.withQuery(reducedQuery), responseContext);

    if (config.isPopulateIntervalCache()) {
      // Materialize fresh results so we can write them to cache before returning
      final List<T> freshResults;
      try {
        freshResults = freshSequence.toList();
      }
      catch (Exception e) {
        log.warn(e, "Failed to materialize fresh results for interval cache population; skipping cache write");
        return Sequences.concat(Sequences.simple(cachedResults), baseRunner.run(queryPlus.withQuery(reducedQuery), responseContext));
      }
      populateCache(freshResults, namespace, missingIntervals, bucketMillis, strategy);
      return Sequences.simple(merge(cachedResults, freshResults));
    }

    // No population: concatenate lazily (cached prefix is already in order before fresh results)
    return Sequences.concat(Sequences.simple(cachedResults), freshSequence);
  }

  // -------------------------------------------------------------------------
  // Eligibility
  // -------------------------------------------------------------------------

  /**
   * Returns {@code true} if the query type and configuration allow interval caching.
   */
  boolean isEligible(Query<T> q, CacheStrategy<T, Object, Query<T>> strategy)
  {
    if (strategy == null || !strategy.isCacheable(q, false, false)) {
      return false;
    }

    if (Granularities.ALL.equals(q.getGranularity())) {
      return false;
    }

    if (q instanceof TimeseriesQuery) {
      // Always eligible (one row per time bucket by definition)
    } else if (q instanceof GroupByQuery) {
      // Only eligible when there are no dimensions (effectively GROUP BY __time only)
      if (!((GroupByQuery) q).getDimensions().isEmpty()) {
        return false;
      }
    } else {
      return false;
    }

    // Skip queries whose earliest interval is older than maxAgeDays
    final long maxAgeMs = (long) config.getMaxAgeDays() * 24L * 60L * 60L * 1_000L;
    final long nowMs = System.currentTimeMillis();
    for (Interval interval : q.getIntervals()) {
      if (nowMs - interval.getStartMillis() > maxAgeMs) {
        return false;
      }
    }

    return true;
  }

  // -------------------------------------------------------------------------
  // Key helpers
  // -------------------------------------------------------------------------

  /**
   * Computes a hex-encoded SHA-256 fingerprint that identifies the "query shape" — everything that
   * affects results except the time interval.
   *
   * <p>{@code CacheStrategy.computeResultLevelCacheKey} intentionally omits datasource and interval
   * (the result-level runner adds those separately). We must include the datasource here so that
   * two different datasources with identical query structures do not share a cache namespace.
   * The interval is still intentionally excluded: that is what enables partial-interval hits.
   */
  static String computeFingerprint(String datasource, byte[] resultLevelCacheKey)
  {
    try {
      final MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      sha256.update(datasource.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      sha256.update((byte) 0); // null separator to prevent prefix collisions
      return bytesToHex(sha256.digest(resultLevelCacheKey));
    }
    catch (NoSuchAlgorithmException e) {
      // SHA-256 is guaranteed to be available on all Java platforms
      throw new RE(e, "SHA-256 not available");
    }
  }

  private static String bytesToHex(byte[] bytes)
  {
    final StringBuilder sb = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      sb.append(StringUtils.format("%02x", b & 0xFF));
    }
    return sb.toString();
  }

  /**
   * Returns one {@link Cache.NamedKey} per bucket covering all given intervals.
   */
  private static List<Cache.NamedKey> enumerateBucketKeys(
      String namespace,
      List<Interval> intervals,
      long bucketMillis
  )
  {
    final List<Cache.NamedKey> keys = new ArrayList<>();
    for (Interval interval : intervals) {
      final long start = floorToBucket(interval.getStartMillis(), bucketMillis);
      for (long ts = start; ts < interval.getEndMillis(); ts += bucketMillis) {
        keys.add(makeBucketKey(namespace, ts));
      }
    }
    return keys;
  }

  static Cache.NamedKey makeBucketKey(String namespace, long bucketStartMs)
  {
    return new Cache.NamedKey(namespace, ByteBuffer.allocate(Long.BYTES).putLong(bucketStartMs).array());
  }

  static long floorToBucket(long epochMs, long bucketMillis)
  {
    return (epochMs / bucketMillis) * bucketMillis;
  }

  // -------------------------------------------------------------------------
  // Bucket helpers
  // -------------------------------------------------------------------------

  /**
   * Returns {@code true} if {@code [bucket, bucket + bucketMillis)} is completely contained within
   * at least one interval in {@code intervals}.
   */
  private static boolean isFullBucket(long bucket, long bucketMillis, List<Interval> intervals)
  {
    for (Interval interval : intervals) {
      if (isFullBucket(bucket, bucketMillis, interval)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {@code true} if {@code [bucket, bucket + bucketMillis)} is completely contained within
   * {@code interval}.
   */
  private static boolean isFullBucket(long bucket, long bucketMillis, Interval interval)
  {
    return interval.getStartMillis() <= bucket && interval.getEndMillis() >= bucket + bucketMillis;
  }

  // -------------------------------------------------------------------------
  // Serialization
  // -------------------------------------------------------------------------

  /**
   * Deserializes raw bytes from the cache into a list of result objects for one bucket.
   */
  @SuppressWarnings("unchecked")
  private List<T> decodeBucket(byte[] raw, CacheStrategy<T, Object, Query<T>> strategy)
  {
    final Function<Object, T> pullFn = strategy.pullFromCache(true);
    final TypeReference<Object> clazz = (TypeReference<Object>) strategy.getCacheObjectClazz();
    final List<T> results = new ArrayList<>();
    try {
      final MappingIterator<Object> iter = mapper.readValues(
          mapper.getFactory().createParser(raw),
          clazz
      );
      while (iter.hasNext()) {
        results.add(pullFn.apply(iter.next()));
      }
    }
    catch (IOException e) {
      log.warn(e, "Failed to decode interval cache bucket; treating as miss");
      return Collections.emptyList();
    }
    return results;
  }

  /**
   * Serializes a list of result objects into bytes for storage in one cache bucket.
   */
  private byte[] encodeBucket(List<T> results, CacheStrategy<T, Object, Query<T>> strategy)
      throws IOException
  {
    final Function<T, Object> prepareFn = strategy.prepareForCache(true);
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (JsonGenerator gen = mapper.getFactory().createGenerator(baos)) {
      for (T result : results) {
        JacksonUtils.writeObjectUsingSerializerProvider(gen, mapper.getSerializerProviderInstance(), prepareFn.apply(result));
      }
    }
    return baos.toByteArray();
  }

  // -------------------------------------------------------------------------
  // Cache population
  // -------------------------------------------------------------------------

  /**
   * Groups fresh results by bucket, encodes each bucket, and writes it to cache with a per-bucket TTL.
   *
   * <p>Two correctness constraints are enforced:
   * <ol>
   *   <li><b>No partial boundary buckets.</b> A bucket is only written if the covered interval fully
   *       contains it — i.e., {@code intervalStart <= bucketStart} and
   *       {@code bucketStart + bucketMillis <= intervalEnd}. Partial buckets (where the query interval
   *       clips one end of the bucket) hold incomplete aggregations and must not be served as complete.</li>
   *   <li><b>No trailing empty buckets.</b> Empty sentinel entries (for gap-filling) are written only up
   *       to the last bucket that contained actual results. Buckets beyond the last data point are not
   *       cached: they may represent time periods where events have not yet arrived, and caching a
   *       "no data" entry there would suppress future queries for that data.</li>
   * </ol>
   */
  private void populateCache(
      List<T> freshResults,
      String namespace,
      List<Interval> coveredIntervals,
      long bucketMillis,
      CacheStrategy<T, Object, Query<T>> strategy
  )
  {
    // Group results by bucket start, skipping any bucket only partially covered by its interval.
    final TreeMap<Long, List<T>> byBucket = new TreeMap<>();
    for (T result : freshResults) {
      final long ts = getResultTimestamp(result);
      if (ts == Long.MIN_VALUE) {
        continue; // unable to determine timestamp; skip
      }
      final long bucket = floorToBucket(ts, bucketMillis);
      if (isFullBucket(bucket, bucketMillis, coveredIntervals)) {
        byBucket.computeIfAbsent(bucket, k -> new ArrayList<>()).add(result);
      }
    }

    // Write empty sentinel entries for fully-covered buckets up to (and including) the last result
    // bucket. Buckets after the last result are not written to avoid caching "no data" for time
    // periods where events may not have arrived yet.
    final long lastResultBucket = byBucket.isEmpty() ? Long.MIN_VALUE : byBucket.lastKey();
    for (Interval interval : coveredIntervals) {
      final long start = floorToBucket(interval.getStartMillis(), bucketMillis);
      for (long ts = start; ts < interval.getEndMillis(); ts += bucketMillis) {
        if (ts > lastResultBucket) {
          break; // do not negative-cache trailing empty buckets
        }
        if (isFullBucket(ts, bucketMillis, interval)) {
          byBucket.putIfAbsent(ts, Collections.emptyList());
        }
      }
    }

    final long nowMs = System.currentTimeMillis();
    for (Map.Entry<Long, List<T>> entry : byBucket.entrySet()) {
      final long bucketMs = entry.getKey();
      final long ttlMs = IntervalCacheTtlCalculator.computeTtlMillis(bucketMs, nowMs, config);
      final int ttlSeconds = (int) Math.min(ttlMs / 1_000L, Integer.MAX_VALUE);
      try {
        final byte[] encoded = encodeBucket(entry.getValue(), strategy);
        cache.put(makeBucketKey(namespace, bucketMs), encoded, ttlSeconds);
      }
      catch (IOException e) {
        log.warn(e, "Failed to encode interval cache bucket at %d; skipping", bucketMs);
      }
    }
  }

  /**
   * Returns the result timestamp in epoch milliseconds, or {@link Long#MIN_VALUE} if unknown.
   * Works for {@link Result}-wrapped types (Timeseries, TopN) and for {@link org.apache.druid.query.groupby.ResultRow}.
   */
  private long getResultTimestamp(T result)
  {
    if (result instanceof Result) {
      return ((Result<?>) result).getTimestamp().getMillis();
    }
    // GroupByQuery returns ResultRow; timestamp is the first field (index 0)
    if (result instanceof ResultRow) {
      final Object ts = ((ResultRow) result).get(0);
      if (ts instanceof Number) {
        return ((Number) ts).longValue();
      }
    }
    return Long.MIN_VALUE;
  }

  // -------------------------------------------------------------------------
  // Merge
  // -------------------------------------------------------------------------

  /**
   * Concatenates cached (prefix) and fresh (suffix) results. Because the prefix covers earlier
   * timestamps and the suffix covers later ones, simple concatenation preserves order.
   */
  private List<T> merge(List<T> cached, List<T> fresh)
  {
    if (cached.isEmpty()) {
      return fresh;
    }
    if (fresh.isEmpty()) {
      return cached;
    }
    final List<T> merged = new ArrayList<>(cached.size() + fresh.size());
    merged.addAll(cached);
    merged.addAll(fresh);
    return merged;
  }
}
