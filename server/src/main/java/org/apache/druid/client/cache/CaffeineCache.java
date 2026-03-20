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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.utils.JvmUtils;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CaffeineCache implements org.apache.druid.client.cache.Cache
{
  private static final Logger log = new Logger(CaffeineCache.class);
  private static final int FIXED_COST = 8; // Minimum cost in "weight" per entry;
  private static final int MAX_DEFAULT_BYTES = 1024 * 1024 * 1024;
  private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final LZ4FastDecompressor LZ4_DECOMPRESSOR = LZ4_FACTORY.fastDecompressor();
  private static final LZ4Compressor LZ4_COMPRESSOR = LZ4_FACTORY.fastCompressor();

  private final Cache<NamedKey, CachedEntry> cache;
  private final AtomicReference<CacheStats> priorStats = new AtomicReference<>(CacheStats.empty());
  private final CaffeineCacheConfig config;

  public static CaffeineCache create(final CaffeineCacheConfig config)
  {
    return create(config, config.createExecutor());
  }

  // Used in testing
  public static CaffeineCache create(final CaffeineCacheConfig config, final Executor executor)
  {
    final long globalAccessNanos = config.getExpireAfter() >= 0
                                   ? TimeUnit.MILLISECONDS.toNanos(config.getExpireAfter())
                                   : Long.MAX_VALUE;

    Caffeine<Object, Object> builder = Caffeine.newBuilder().recordStats();
    builder.expireAfter(new Expiry<NamedKey, CachedEntry>()
    {
      @Override
      public long expireAfterCreate(NamedKey key, CachedEntry entry, long currentTime)
      {
        if (entry.expiryNanos() != CachedEntry.NO_EXPIRY) {
          return Math.max(0, entry.expiryNanos() - currentTime);
        }
        return globalAccessNanos;
      }

      @Override
      public long expireAfterUpdate(NamedKey key, CachedEntry entry, long currentTime, long currentDuration)
      {
        return expireAfterCreate(key, entry, currentTime);
      }

      @Override
      public long expireAfterRead(NamedKey key, CachedEntry entry, long currentTime, long currentDuration)
      {
        if (entry.expiryNanos() != CachedEntry.NO_EXPIRY) {
          // TTL entries: do not renew on read — use remaining duration
          return currentDuration;
        }
        // Access-based: renew on read
        return globalAccessNanos;
      }
    });
    if (config.getSizeInBytes() >= 0) {
      builder.maximumWeight(config.getSizeInBytes());
    } else {
      builder.maximumWeight(Math.min(MAX_DEFAULT_BYTES, JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes() / 20));
    }
    builder
        .weigher((NamedKey key, CachedEntry entry) -> entry.data().length
                                                      + key.key.length
                                                      + key.namespace.length() * Character.BYTES
                                                      + FIXED_COST)
        .executor(executor);
    return new CaffeineCache(builder.build(), config);
  }

  private CaffeineCache(final Cache<NamedKey, CachedEntry> cache, CaffeineCacheConfig config)
  {
    this.cache = cache;
    this.config = config;
  }

  @Override
  public byte[] get(NamedKey key)
  {
    final CachedEntry entry = cache.getIfPresent(key);
    return entry == null ? null : deserialize(entry.data());
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    cache.put(key, new CachedEntry(serialize(value), CachedEntry.NO_EXPIRY));
  }

  @Override
  public void put(NamedKey key, byte[] value, int ttlSeconds)
  {
    cache.put(key, new CachedEntry(serialize(value), System.nanoTime() + (long) ttlSeconds * 1_000_000_000L));
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    // The assumption here is that every value is accessed at least once. Materializing here ensures deserialize is only
    // called *once* per value.
    return ImmutableMap.copyOf(
        Maps.transformValues(cache.getAllPresent(keys), entry -> deserialize(entry.data()))
    );
  }

  // This is completely racy with put. Any values missed should be evicted later anyways. So no worries.
  @Override
  public void close(String namespace)
  {
    if (config.isEvictOnClose()) {
      cache.asMap().keySet().removeIf(key -> key.namespace.equals(namespace));
    }
  }

  @Override
  @LifecycleStop
  public void close()
  {
    cache.cleanUp();
  }

  @Override
  public org.apache.druid.client.cache.CacheStats getStats()
  {
    final CacheStats stats = cache.stats();
    final long size = cache
        .policy().eviction()
        .map(eviction -> eviction.isWeighted() ? eviction.weightedSize() : OptionalLong.empty())
        .orElse(OptionalLong.empty()).orElse(-1);
    return new org.apache.druid.client.cache.CacheStats(
        stats.hitCount(),
        stats.missCount(),
        cache.estimatedSize(),
        size,
        stats.evictionCount(),
        0,
        stats.loadFailureCount()
    );
  }

  @Override
  public boolean isLocal()
  {
    return true;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    final CacheStats oldStats = priorStats.get();
    final CacheStats newStats = cache.stats();
    final CacheStats deltaStats = newStats.minus(oldStats);

    final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
    emitter.emit(builder.setMetric("query/cache/caffeine/delta/requests", deltaStats.requestCount()));
    emitter.emit(builder.setMetric("query/cache/caffeine/total/requests", newStats.requestCount()));
    emitter.emit(builder.setMetric("query/cache/caffeine/delta/loadTime", deltaStats.totalLoadTime()));
    emitter.emit(builder.setMetric("query/cache/caffeine/total/loadTime", newStats.totalLoadTime()));
    emitter.emit(builder.setMetric("query/cache/caffeine/delta/evictionBytes", deltaStats.evictionWeight()));
    emitter.emit(builder.setMetric("query/cache/caffeine/total/evictionBytes", newStats.evictionWeight()));
    if (!priorStats.compareAndSet(oldStats, newStats)) {
      // ISE for stack trace
      log.warn(
          new IllegalStateException("Multiple monitors"),
          "Multiple monitors on the same cache causing race conditions and unreliable stats reporting"
      );
    }
  }

  @VisibleForTesting
  Cache<NamedKey, CachedEntry> getCache()
  {
    return cache;
  }

  private byte[] deserialize(byte[] bytes)
  {
    if (bytes == null) {
      return null;
    }
    final int decompressedLen = ByteBuffer.wrap(bytes).getInt();
    final byte[] out = new byte[decompressedLen];
    LZ4_DECOMPRESSOR.decompress(bytes, Integer.BYTES, out, 0, out.length);
    return out;
  }

  private byte[] serialize(byte[] value)
  {
    final int len = LZ4_COMPRESSOR.maxCompressedLength(value.length);
    final byte[] out = new byte[len];
    final int compressedSize = LZ4_COMPRESSOR.compress(value, 0, value.length, out, 0);
    return ByteBuffer.allocate(compressedSize + Integer.BYTES)
                     .putInt(value.length)
                     .put(out, 0, compressedSize)
                     .array();
  }
}
