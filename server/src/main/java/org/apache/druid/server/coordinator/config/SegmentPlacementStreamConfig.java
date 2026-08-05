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

package org.apache.druid.server.coordinator.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Settings for the Coordinator's placement stream, which Brokers follow so that they learn where a segment has moved
 * to before they learn that it left its old server. See apache/druid#18738.
 */
public class SegmentPlacementStreamConfig
{
  @JsonProperty
  private boolean enabled = false;



  /**
   * Whether the Coordinator publishes placement changes and waits for Brokers to consume them before dropping a
   * segment's last replica from its old server.
   */
  public boolean isEnabled()
  {
    return enabled;
  }


}
