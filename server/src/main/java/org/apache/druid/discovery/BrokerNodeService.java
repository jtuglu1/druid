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

package org.apache.druid.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import org.apache.druid.client.DruidServerConfig;

import java.util.Objects;

/**
 * Broker metadata announced with the Broker node.
 */
public class BrokerNodeService extends DruidService
{
  public static final String DISCOVERY_SERVICE_KEY = "brokerNodeService";

  private final String tier;

  @Inject
  public BrokerNodeService(final DruidServerConfig config)
  {
    this(config.getTier());
  }

  @JsonCreator
  public BrokerNodeService(
      @JsonProperty("tier") final String tier
  )
  {
    this.tier = tier;
  }

  @Override
  public String getName()
  {
    return DISCOVERY_SERVICE_KEY;
  }

  @JsonProperty
  public String getTier()
  {
    return tier;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BrokerNodeService that = (BrokerNodeService) o;
    return Objects.equals(tier, that.tier);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tier);
  }

  @Override
  public String toString()
  {
    return "BrokerNodeService{" +
           "tier='" + tier + '\'' +
           '}';
  }
}
