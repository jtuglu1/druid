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
 * Stores a cached payload alongside an optional per-entry expiry time.
 * Used by both {@link MapCache} and {@link CaffeineCache}.
 *
 * <p>{@link #expiryNanos} is an absolute timestamp obtained from {@link System#nanoTime()}.
 * {@link #NO_EXPIRY} ({@value Long#MAX_VALUE}) means the entry has no per-entry TTL.
 */
record CachedEntry(byte[] data, long expiryNanos)
{
  /**
   * Sentinel value for {@link #expiryNanos} indicating that the entry never expires on its own.
   */
  static final long NO_EXPIRY = Long.MAX_VALUE;

}
