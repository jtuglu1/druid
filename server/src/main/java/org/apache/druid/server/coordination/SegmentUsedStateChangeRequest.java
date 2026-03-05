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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * A change request for the coordinator→broker segment-used-state sync.
 * Carries a segment identifier string and whether the segment is used in metadata.
 */
public class SegmentUsedStateChangeRequest
{
  private final String segmentId;
  private final boolean used;

  @JsonCreator
  public SegmentUsedStateChangeRequest(
      @JsonProperty("segmentId") String segmentId,
      @JsonProperty("used") boolean used
  )
  {
    this.segmentId = segmentId;
    this.used = used;
  }

  @JsonProperty
  public String getSegmentId()
  {
    return segmentId;
  }

  @JsonProperty
  public boolean isUsed()
  {
    return used;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentUsedStateChangeRequest that = (SegmentUsedStateChangeRequest) o;
    return used == that.used && Objects.equals(segmentId, that.segmentId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segmentId, used);
  }

  @Override
  public String toString()
  {
    return "SegmentUsedStateChangeRequest{segmentId='" + segmentId + "', used=" + used + '}';
  }
}
