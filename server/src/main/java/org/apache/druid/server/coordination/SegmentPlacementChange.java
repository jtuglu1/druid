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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * One entry on the Coordinator's ordered placement stream: a statement about where a segment lives, or about whether
 * it should exist at all.
 * <p>
 * The stream exists because a Broker's per-data-node syncs have no ordering between them, so a Broker can apply
 * "dropped from A" before "loaded on B" and briefly believe a segment that is merely moving has gone away. Publishing
 * both halves on a single ordered channel makes that impossible: a Broker that has consumed the removal has
 * necessarily consumed the addition that precedes it. See apache/druid#18738.
 * <p>
 * Deliberately carries a {@link SegmentId} and a server name rather than a {@link org.apache.druid.timeline.DataSegment}.
 * The Broker already holds the segment -- it is in its timeline, which is what makes the removal dangerous in the
 * first place -- so shipping it again would multiply the size of the stream for nothing.
 */
public class SegmentPlacementChange
{
  public enum Type
  {
    /**
     * A server is now serving this segment. Emitted so that a Broker consuming the stream in order cannot learn of a
     * removal without first having learnt of the replica that replaces it.
     */
    REPLICA_ADDED,

    /**
     * A server is no longer serving this segment.
     */
    REPLICA_REMOVED,

    /**
     * The segment is no longer used, so its absence is correct and any timeline entry for it should go away. This is
     * what distinguishes "legitimately gone" from "should be here but is not", which a Broker cannot tell on its own.
     */
    SEGMENT_UNUSED
  }

  private final Type type;
  private final SegmentId segmentId;
  @Nullable
  private final String serverName;

  public static SegmentPlacementChange replicaAdded(SegmentId segmentId, String serverName)
  {
    return new SegmentPlacementChange(Type.REPLICA_ADDED, segmentId, serverName);
  }

  public static SegmentPlacementChange replicaRemoved(SegmentId segmentId, String serverName)
  {
    return new SegmentPlacementChange(Type.REPLICA_REMOVED, segmentId, serverName);
  }

  public static SegmentPlacementChange segmentUnused(SegmentId segmentId)
  {
    return new SegmentPlacementChange(Type.SEGMENT_UNUSED, segmentId, null);
  }

  private SegmentPlacementChange(Type type, SegmentId segmentId, @Nullable String serverName)
  {
    this.type = type;
    this.segmentId = segmentId;
    this.serverName = serverName;
  }

  /**
   * {@link SegmentId} serializes as a bare string and can only be parsed back with the datasource in hand, so the
   * datasource travels alongside it.
   */
  @JsonCreator
  public static SegmentPlacementChange fromJson(
      @JsonProperty("type") Type type,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segmentId") String segmentId,
      @JsonProperty("serverName") @Nullable String serverName
  )
  {
    final SegmentId parsed = SegmentId.tryParse(dataSource, segmentId);
    if (parsed == null) {
      throw InvalidInput.exception("Could not parse segment id[%s] of datasource[%s].", segmentId, dataSource);
    }
    return new SegmentPlacementChange(type, parsed, serverName);
  }

  @JsonProperty
  public Type getType()
  {
    return type;
  }

  @JsonProperty
  public String getDataSource()
  {
    return segmentId.getDataSource();
  }

  @JsonProperty("segmentId")
  public String getSerializedSegmentId()
  {
    return segmentId.toString();
  }

  /**
   * The server this change is about, or null for {@link Type#SEGMENT_UNUSED}, which concerns the segment itself
   * rather than any one server.
   */
  @JsonProperty
  @Nullable
  public String getServerName()
  {
    return serverName;
  }

  public SegmentId getSegmentId()
  {
    return segmentId;
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
    SegmentPlacementChange that = (SegmentPlacementChange) o;
    return type == that.type
           && segmentId.equals(that.segmentId)
           && Objects.equals(serverName, that.serverName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type, segmentId, serverName);
  }

  @Override
  public String toString()
  {
    return "SegmentPlacementChange{"
           + "type=" + type
           + ", segmentId=" + segmentId
           + (serverName == null ? "" : ", serverName=" + serverName)
           + '}';
  }
}
