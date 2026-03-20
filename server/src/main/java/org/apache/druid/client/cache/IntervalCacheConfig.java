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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for the broker-level interval cache ({@code IntervalCachingQueryRunner}).
 * Bound to the property prefix {@code druid.broker.cache.interval}.
 */
public class IntervalCacheConfig
{
  /** Whether to serve partial or full cache hits from the interval cache. */
  @JsonProperty
  private boolean useIntervalCache = false;

  /** Whether to populate the interval cache with results fetched from Druid. */
  @JsonProperty
  private boolean populateIntervalCache = false;

  /** Size of each cache bucket in milliseconds. Defaults to 1 minute. */
  @JsonProperty
  private long bucketMillis = 60_000L;

  /** Minimum TTL applied to any bucket, in seconds. */
  @JsonProperty
  private int ttlMinSeconds = 5;

  /** Maximum TTL applied to any bucket, in seconds. */
  @JsonProperty
  private int ttlMaxSeconds = 3_600;

  /**
   * Queries whose earliest interval start is older than this many days are skipped — they are
   * unlikely to contain realtime data and the cache cost-benefit ratio is poor.
   */
  @JsonProperty
  private int maxAgeDays = 7;

  /**
   * Age threshold (in minutes) after which the TTL starts doubling each minute.
   * Buckets younger than this always use {@link #ttlMinSeconds}.
   */
  @JsonProperty
  private int ageThresholdMinutes = 2;

  public boolean isUseIntervalCache()
  {
    return useIntervalCache;
  }

  public boolean isPopulateIntervalCache()
  {
    return populateIntervalCache;
  }

  public long getBucketMillis()
  {
    return bucketMillis;
  }

  public int getTtlMinSeconds()
  {
    return ttlMinSeconds;
  }

  public int getTtlMaxSeconds()
  {
    return ttlMaxSeconds;
  }

  public int getMaxAgeDays()
  {
    return maxAgeDays;
  }

  public int getAgeThresholdMinutes()
  {
    return ageThresholdMinutes;
  }
}
