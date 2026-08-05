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

package org.apache.druid.testing.embedded.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.HttpServerInventoryView;
import org.apache.druid.client.HttpServerInventoryViewConfig;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.coordination.ChangeRequestHttpSyncer;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;

import java.util.List;

/**
 * Makes one data server's segment changes reach a Broker late.
 * <p>
 * A Broker syncs each data server independently, so "loaded on B" and "dropped from A" arrive on two unordered
 * channels. On a single-JVM cluster both channels are sub-millisecond while the Coordinator leaves a full cycle
 * between the load and the drop, so the Broker always happens to see them in the helpful order and the bug of
 * apache/druid#18738 cannot be observed at all. Holding back the destination server's changes reproduces the sync lag
 * that a large cluster has naturally, and with it the inversion: the Broker applies the drop while it still does not
 * know the segment now lives somewhere else.
 * <p>
 * Test-only. Installed as an extension, which {@code ExtensionInjectorBuilder} layers over the core modules with
 * {@code Modules.override}, so this replaces the stock {@link FilteredServerInventoryView} binding. The replacement
 * behaves exactly like the stock view until a test arms it, so it is inert on every server that merely has the
 * extension loaded.
 * <p>
 * {@code druid.serverview.type} is deliberately not used to select this: {@code StartupInjectorBuilder} rejects every
 * value but {@code http}.
 *
 * @see SegmentRelocationQueryContinuityTest
 */
public class DelayedSyncServerViewModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    // Nothing to bind here; the view is supplied by the @Provides method below.
  }

  /**
   * Replaces the binding that {@code ServerViewModule} makes from {@code FilteredServerInventoryViewProvider}. The
   * filtered view is the one {@code BrokerServerView} drives its timeline from; the unfiltered binding is left alone.
   */
  @Provides
  @ManageLifecycle
  public FilteredServerInventoryView getFilteredServerInventoryView(
      @Smile ObjectMapper smileMapper,
      @EscalatedClient HttpClient httpClient,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      HttpServerInventoryViewConfig config,
      ServiceEmitter serviceEmitter,
      ScheduledExecutorFactory executorFactory
  )
  {
    return new DelayedSyncInventoryView(
        smileMapper,
        httpClient,
        druidNodeDiscoveryProvider,
        // As in FilteredHttpServerInventoryViewProvider: the Broker installs its own filter from watched tiers.
        Predicates.alwaysFalse(),
        config,
        serviceEmitter,
        executorFactory,
        "DelayedSyncFilteredInventoryView"
    );
  }

  /**
   * A view that can be told to hold back one server's changes.
   */
  public static class DelayedSyncInventoryView extends HttpServerInventoryView
  {
    /**
     * The server whose changes are held back, as {@code host:port}. Null means no delay, which is the state until a
     * test arms it and the state this view is left in afterwards.
     */
    private volatile String delayedServer = null;
    private volatile long delayMillis = 0;

    DelayedSyncInventoryView(
        ObjectMapper smileMapper,
        HttpClient httpClient,
        DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
        Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter,
        HttpServerInventoryViewConfig config,
        ServiceEmitter serviceEmitter,
        ScheduledExecutorFactory executorFactory,
        String execNamePrefix
    )
    {
      super(
          smileMapper,
          httpClient,
          druidNodeDiscoveryProvider,
          defaultFilter,
          config,
          serviceEmitter,
          executorFactory,
          execNamePrefix
      );
    }

    /**
     * Delays every subsequent change from the given server. Arm this only once the segments under test are loaded, so
     * that the delay lands on the relocation and nothing else.
     */
    public void delayChangesFrom(String hostAndPort, long delayMillis)
    {
      this.delayMillis = delayMillis;
      this.delayedServer = hostAndPort;
    }

    public void stopDelayingChanges()
    {
      this.delayedServer = null;
    }

    @Override
    protected ChangeRequestHttpSyncer.Listener<DataSegmentChangeRequest> decorateSyncListener(
        DruidServerMetadata server,
        ChangeRequestHttpSyncer.Listener<DataSegmentChangeRequest> listener
    )
    {
      return new ChangeRequestHttpSyncer.Listener<>()
      {
        @Override
        public void fullSync(List<DataSegmentChangeRequest> changes)
        {
          // Never delayed. A full sync is how the view bootstraps, so holding it back would slow the initial load
          // rather than the relocation.
          listener.fullSync(changes);
        }

        @Override
        public void deltaSync(List<DataSegmentChangeRequest> changes)
        {
          sleepIfDelayed(server);
          listener.deltaSync(changes);
        }
      };
    }

    /**
     * Sleeps on the sync executor, which is what a genuinely lagging server does to its own sync. Safe for the other
     * servers only because that executor is sized by {@code druid.serverview.http.numThreads}, which the test raises.
     */
    private void sleepIfDelayed(DruidServerMetadata server)
    {
      final String delayed = delayedServer;
      if (delayed == null || !delayed.equals(server.getHostAndPort())) {
        return;
      }

      try {
        Thread.sleep(delayMillis);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
