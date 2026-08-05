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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SegmentPlacementChangeTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  private static final SegmentId SEGMENT_ID = SegmentId.of(
      "wikipedia",
      Intervals.of("2024-01-01/2024-01-02"),
      DateTimes.of("2024-01-03").toString(),
      3
  );

  @Test
  public void testReplicaAddedRoundTrip() throws Exception
  {
    assertRoundTrips(SegmentPlacementChange.replicaAdded(SEGMENT_ID, "historical-1"));
  }

  @Test
  public void testReplicaRemovedRoundTrip() throws Exception
  {
    assertRoundTrips(SegmentPlacementChange.replicaRemoved(SEGMENT_ID, "historical-1"));
  }

  /**
   * Carries no server, since it is a statement about the segment rather than about any one server.
   */
  @Test
  public void testSegmentUnusedRoundTripsWithNoServer() throws Exception
  {
    final SegmentPlacementChange change = SegmentPlacementChange.segmentUnused(SEGMENT_ID);
    Assert.assertNull(change.getServerName());
    assertRoundTrips(change);
  }

  /**
   * The stream is shipped as a batch, so a mixed list has to survive the round trip as one.
   */
  @Test
  public void testBatchRoundTrip() throws Exception
  {
    final List<SegmentPlacementChange> batch = List.of(
        SegmentPlacementChange.replicaAdded(SEGMENT_ID, "historical-2"),
        SegmentPlacementChange.replicaRemoved(SEGMENT_ID, "historical-1"),
        SegmentPlacementChange.segmentUnused(SEGMENT_ID)
    );

    final List<SegmentPlacementChange> read = MAPPER.readValue(
        MAPPER.writeValueAsBytes(batch),
        MAPPER.getTypeFactory().constructCollectionType(List.class, SegmentPlacementChange.class)
    );
    Assert.assertEquals(batch, read);
  }

  /**
   * A SegmentId serializes as a bare string and cannot be parsed back without its datasource, which is why the
   * datasource travels alongside it. Guard that it actually does.
   */
  @Test
  public void testSegmentIdSurvivesWithItsDataSource() throws Exception
  {
    final SegmentPlacementChange read = roundTrip(SegmentPlacementChange.replicaAdded(SEGMENT_ID, "historical-1"));

    Assert.assertEquals(SEGMENT_ID, read.getSegmentId());
    Assert.assertEquals("wikipedia", read.getSegmentId().getDataSource());
    Assert.assertEquals(3, read.getSegmentId().getPartitionNum());
  }

  @Test
  public void testUnparseableSegmentIdIsRejected() throws Exception
  {
    final String json = MAPPER.writeValueAsString(
        SegmentPlacementChange.replicaAdded(SEGMENT_ID, "historical-1")
    ).replace(SEGMENT_ID.toString(), "not-a-segment-id");

    Assert.assertThrows(
        Exception.class,
        () -> MAPPER.readValue(json, SegmentPlacementChange.class)
    );
  }

  private static void assertRoundTrips(SegmentPlacementChange change) throws Exception
  {
    Assert.assertEquals(change, roundTrip(change));
    Assert.assertEquals(change.hashCode(), roundTrip(change).hashCode());
  }

  private static SegmentPlacementChange roundTrip(SegmentPlacementChange change) throws Exception
  {
    return MAPPER.readValue(MAPPER.writeValueAsBytes(change), SegmentPlacementChange.class);
  }
}
