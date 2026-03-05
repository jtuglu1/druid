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

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for broker-side detection of unavailable (used-but-unserved) segments.
 *
 * <p>Bound from {@code druid.broker.unavailableSegments.*}.
 */
public class BrokerSegmentUnavailabilityConfig
{
  /**
   * Master switch. When false the feature is entirely disabled and broker behavior is unchanged.
   */
  @JsonProperty
  private boolean enabled = false;

  /**
   * How long (ms) to wait between coordinator sync requests. Only relevant when
   * {@code enabled=true}. The coordinator endpoint is long-polled so this is
   * effectively a ceiling on how stale the used-segment view can be.
   */
  @JsonProperty
  private long serverTimeout = 30_000L;

  /**
   * When true, queries that encounter an unavailable segment throw a
   * {@link org.apache.druid.query.QueryUnavailableException} (HTTP 503).
   * When false, the existing "emit alert and skip" behavior is preserved even
   * when the feature is enabled.
   */
  @JsonProperty
  private boolean failOnUnavailable = true;

  public boolean isEnabled()
  {
    return enabled;
  }

  public long getServerTimeout()
  {
    return serverTimeout;
  }

  public boolean isFailOnUnavailable()
  {
    return failOnUnavailable;
  }
}
