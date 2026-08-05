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
import org.apache.druid.client.BrokerPlacementStreamConfig;
import org.apache.druid.client.CoordinatorPlacementView;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.DruidNode;

import javax.annotation.Nullable;

/**
 * Wires the Broker's side of the Coordinator's placement stream. See {@link CoordinatorPlacementView}.
 */
public class BrokerPlacementStreamModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.broker.segmentPlacement", BrokerPlacementStreamConfig.class);
  }

  /**
   * Null when the stream is disabled, which leaves the Broker driving its timeline purely from per-data-node syncs,
   * exactly as before this existed.
   */
  @Provides
  @LazySingleton
  @Nullable
  public CoordinatorPlacementView getCoordinatorPlacementView(
      BrokerPlacementStreamConfig config,
      @Smile ObjectMapper smileMapper,
      @EscalatedGlobal HttpClient httpClient,
      CoordinatorClient coordinatorClient,
      @Self DruidNode self,
      Lifecycle lifecycle
  )
  {
    if (!config.isEnabled()) {
      return null;
    }

    final CoordinatorPlacementView view = new CoordinatorPlacementView(
        smileMapper,
        httpClient,
        coordinatorClient,
        self.getHostAndPortToUse()
    );

    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start()
          {
            view.start();
          }

          @Override
          public void stop()
          {
            view.stop();
          }
        }
    );
    return view;
  }
}
