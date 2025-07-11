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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.apache.druid.query.BrokerParallelMergeConfig;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.utils.RuntimeInfo;

import java.nio.ByteBuffer;
import java.util.concurrent.ForkJoinPool;

/**
 * This module is used to fulfill dependency injection of query processing and caching resources: buffer pools and
 * thread pools on Broker. Broker does not need to be allocated an intermediate results pool.
 * This is separated from DruidProcessingModule to separate the needs of the broker from the historicals
 *
 * @see DruidProcessingModule
 */
public class BrokerProcessingModule implements Module
{
  private static final Logger log = new Logger(BrokerProcessingModule.class);

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, DruidProcessingModule.PROCESSING_PROPERTY_PREFIX + ".merge", BrokerParallelMergeConfig.class);
    DruidProcessingModule.registerConfigsAndMonitor(binder);
  }

  @Provides
  @LazySingleton
  public CachePopulator getCachePopulator(
      @Smile ObjectMapper smileMapper,
      CachePopulatorStats cachePopulatorStats,
      CacheConfig cacheConfig
  )
  {
    return DruidProcessingModule.createCachePopulator(smileMapper, cachePopulatorStats, cacheConfig);
  }

  @Provides
  @ManageLifecycle
  public QueryProcessingPool getProcessingExecutorPool(
      DruidProcessingConfig config
  )
  {
    return new ForwardingQueryProcessingPool(Execs.dummy());
  }

  @Provides
  @LazySingleton
  @Global
  public NonBlockingPool<ByteBuffer> getIntermediateResultsPool(DruidProcessingConfig config, RuntimeInfo runtimeInfo)
  {
    verifyDirectMemory(config, runtimeInfo);
    return new StupidPool<>(
        "intermediate processing pool",
        new OffheapBufferGenerator("intermediate processing", config.intermediateComputeSizeBytes()),
        config.getNumInitalBuffersForIntermediatePool(),
        config.poolCacheMaxCount()
    );
  }

  @Provides
  @LazySingleton
  @Merging
  public BlockingPool<ByteBuffer> getMergeBufferPool(DruidProcessingConfig config, RuntimeInfo runtimeInfo)
  {
    verifyDirectMemory(config, runtimeInfo);
    return new DefaultBlockingPool<>(
        new OffheapBufferGenerator("result merging", config.intermediateComputeSizeBytes()),
        config.getNumMergeBuffers()
    );
  }

  @Provides
  @LazySingleton
  @Merging
  public GroupByResourcesReservationPool getGroupByResourcesReservationPool(
      @Merging BlockingPool<ByteBuffer> mergeBufferPool,
      GroupByQueryConfig groupByQueryConfig
  )
  {
    return new GroupByResourcesReservationPool(mergeBufferPool, groupByQueryConfig);
  }

  @Provides
  @ManageLifecycle
  public LifecycleForkJoinPoolProvider getMergeProcessingPoolProvider(BrokerParallelMergeConfig config)
  {
    return new LifecycleForkJoinPoolProvider(
        config.getParallelism(),
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        (t, e) -> log.error(e, "Unhandled exception in thread [%s]", t),
        true,
        config.getAwaitShutdownMillis()
    );
  }

  @Provides
  @Merging
  public ForkJoinPool getMergeProcessingPool(LifecycleForkJoinPoolProvider poolProvider)
  {
    return poolProvider.getPool();
  }

  private void verifyDirectMemory(DruidProcessingConfig config, RuntimeInfo runtimeInfo)
  {
    final long memoryNeeded = (long) config.intermediateComputeSizeBytes() *
                              (config.getNumMergeBuffers() + 1);

    try {
      final long maxDirectMemory = runtimeInfo.getDirectMemorySizeBytes();

      if (maxDirectMemory < memoryNeeded) {
        throw new ProvisionException(
            StringUtils.format(
                "Not enough direct memory.  Please adjust -XX:MaxDirectMemorySize, druid.processing.buffer.sizeBytes, or druid.processing.numMergeBuffers: "
                + "maxDirectMemory[%,d], memoryNeeded[%,d] = druid.processing.buffer.sizeBytes[%,d] * (druid.processing.numMergeBuffers[%,d] + 1)",
                maxDirectMemory,
                memoryNeeded,
                config.intermediateComputeSizeBytes(),
                config.getNumMergeBuffers()
            )
        );
      }
    }
    catch (UnsupportedOperationException e) {
      log.debug("Checking for direct memory size is not support on this platform: %s", e);
      log.info(
          "Your memory settings require at least %,d bytes of direct memory. "
          + "Your machine must have at least this much memory available, and your JVM "
          + "-XX:MaxDirectMemorySize parameter must be at least this high. "
          + "If it is, you may safely ignore this message. "
          + "Otherwise, consider adjusting your memory settings. "
          + "Calculation: druid.processing.buffer.sizeBytes[%,d] * (druid.processing.numMergeBuffers[%,d] + 1).",
          memoryNeeded,
          config.intermediateComputeSizeBytes(),
          config.getNumMergeBuffers()
      );
    }
  }
}
