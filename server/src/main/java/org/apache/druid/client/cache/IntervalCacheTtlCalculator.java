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

/**
 * Stateless TTL calculator for interval-cache buckets.
 *
 * <p>TTL grows exponentially with bucket age:
 * <ul>
 *   <li>If {@code ageMinutes <= ageThresholdMinutes}: TTL = {@code ttlMinSeconds}.</li>
 *   <li>Otherwise: TTL = min({@code ttlMinSeconds} * 2^(ageMinutes&minus;1), {@code ttlMaxSeconds}).</li>
 * </ul>
 * This keeps recently-written buckets short-lived (they may still change due to realtime ingestion)
 * while allowing older, stable buckets to stay cached longer.
 */
public final class IntervalCacheTtlCalculator
{
  private IntervalCacheTtlCalculator()
  {
  }

  /**
   * Computes the TTL in milliseconds for a bucket whose start time is {@code bucketEpochMs}.
   *
   * @param bucketEpochMs bucket start timestamp in epoch milliseconds
   * @param nowMs         current time in epoch milliseconds (injectable for testing)
   * @param config        interval cache configuration
   * @return TTL in milliseconds, always in [{@code ttlMinSeconds * 1000}, {@code ttlMaxSeconds * 1000}]
   */
  public static long computeTtlMillis(long bucketEpochMs, long nowMs, IntervalCacheConfig config)
  {
    final long ageMs = nowMs - bucketEpochMs;
    final long ageMinutes = ageMs / 60_000L;
    final long ttlMinMs = (long) config.getTtlMinSeconds() * 1_000L;
    final long ttlMaxMs = (long) config.getTtlMaxSeconds() * 1_000L;

    if (ageMinutes <= config.getAgeThresholdMinutes()) {
      return ttlMinMs;
    }

    // ttl = ttlMin * 2^(ageMinutes - 1); loop to avoid overflow
    long ttlMs = ttlMinMs;
    for (long i = 1; i < ageMinutes && ttlMs < ttlMaxMs; i++) {
      ttlMs = Math.min(ttlMs * 2, ttlMaxMs);
    }
    return ttlMs;
  }
}
