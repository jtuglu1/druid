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

package org.apache.druid.client.cache;

import org.junit.Assert;
import org.junit.Test;

public class IntervalCacheTtlCalculatorTest
{
  private static IntervalCacheConfig config()
  {
    return new IntervalCacheConfig();
    // defaults: ttlMin=5s, ttlMax=3600s, threshold=2min
  }

  @Test
  public void testYoungBucketUsesTtlMin()
  {
    final IntervalCacheConfig cfg = config();
    final long nowMs = 10_000_000L;
    // bucket is 1 minute old — below threshold (2 min)
    final long bucketMs = nowMs - 60_000L;
    Assert.assertEquals(cfg.getTtlMinSeconds() * 1000L,
                        IntervalCacheTtlCalculator.computeTtlMillis(bucketMs, nowMs, cfg));
  }

  @Test
  public void testBucketAtThresholdDoubles()
  {
    final IntervalCacheConfig cfg = config();
    final long nowMs = 10_000_000L;
    // bucket is exactly at threshold age (2 min)
    final long bucketMs = nowMs - (long) cfg.getAgeThresholdMinutes() * 60_000L;
    final long expected = (long) cfg.getTtlMinSeconds() * 1000L; // ageMinutes == threshold → ttlMin (< threshold)
    Assert.assertEquals(expected, IntervalCacheTtlCalculator.computeTtlMillis(bucketMs, nowMs, cfg));
  }

  @Test
  public void testBucketJustOverThresholdDoubles()
  {
    final IntervalCacheConfig cfg = config();
    final long nowMs = 10_000_000L;
    // bucket is threshold+1 minutes old — first doubling
    final long bucketMs = nowMs - (long) (cfg.getAgeThresholdMinutes() + 1) * 60_000L;
    // ageMinutes = threshold+1; ttl = ttlMin * 2^(ageMinutes-1) = ttlMin * 2^threshold
    long expected = (long) cfg.getTtlMinSeconds() * 1000L;
    for (int i = 1; i < cfg.getAgeThresholdMinutes() + 1; i++) {
      expected = Math.min(expected * 2, (long) cfg.getTtlMaxSeconds() * 1000L);
    }
    Assert.assertEquals(expected, IntervalCacheTtlCalculator.computeTtlMillis(bucketMs, nowMs, cfg));
  }

  @Test
  public void testOldBucketCapsAtMax()
  {
    final IntervalCacheConfig cfg = config();
    final long nowMs = 10_000_000L;
    // bucket is 1 day old — well past the doubling threshold; should be capped at ttlMax
    final long bucketMs = nowMs - 24L * 60L * 60_000L;
    Assert.assertEquals((long) cfg.getTtlMaxSeconds() * 1000L,
                        IntervalCacheTtlCalculator.computeTtlMillis(bucketMs, nowMs, cfg));
  }

  @Test
  public void testFutureBucketUsesTtlMin()
  {
    // If bucket timestamp is in the future (clock skew), ageMs is negative — treat as young
    final IntervalCacheConfig cfg = config();
    final long nowMs = 10_000_000L;
    final long bucketMs = nowMs + 60_000L; // future bucket
    Assert.assertEquals((long) cfg.getTtlMinSeconds() * 1000L,
                        IntervalCacheTtlCalculator.computeTtlMillis(bucketMs, nowMs, cfg));
  }
}
