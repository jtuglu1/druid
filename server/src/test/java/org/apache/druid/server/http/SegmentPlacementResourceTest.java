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

package org.apache.druid.server.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.SegmentPlacementChange;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.SegmentPlacementBroadcaster;
import org.apache.druid.server.mocks.MockAsyncContext;
import org.apache.druid.server.mocks.MockHttpServletRequest;
import org.apache.druid.server.mocks.MockHttpServletResponse;
import org.apache.druid.timeline.SegmentId;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

/**
 * The Coordinator endpoint that serves the placement stream.
 * <p>
 * The rejections matter as much as the success path. A Broker cannot tell an empty stream from a stream it is not
 * entitled to read, so every case where this Coordinator has nothing legitimate to say must be an error the Broker can
 * react to, never a 200 with no changes in it.
 */
public class SegmentPlacementResourceTest
{
  private static final SegmentId SEGMENT_ID = SegmentId.of(
      "wikipedia",
      Intervals.of("2024-01-01/2024-01-02"),
      DateTimes.of("2024-01-03").toString(),
      0
  );

  private DruidCoordinator coordinator;
  private SegmentPlacementBroadcaster broadcaster;

  @Before
  public void setUp()
  {
    coordinator = Mockito.mock(DruidCoordinator.class);
    broadcaster = Mockito.mock(SegmentPlacementBroadcaster.class);
  }

  private SegmentPlacementResource resourceWith(SegmentPlacementBroadcaster broadcaster)
  {
    return new SegmentPlacementResource(
        TestHelper.JSON_MAPPER,
        TestHelper.makeSmileMapper(),
        coordinator,
        broadcaster
    );
  }

  @Test
  public void test_getPlacementChanges_servesTheStream_whenLeader() throws Exception
  {
    Mockito.when(coordinator.isLeader()).thenReturn(true);

    final List<SegmentPlacementChange> changes = List.of(
        SegmentPlacementChange.replicaAdded(SEGMENT_ID, "historical-2"),
        SegmentPlacementChange.replicaRemoved(SEGMENT_ID, "historical-1")
    );
    Mockito.when(broadcaster.getChangesSince(Mockito.eq("broker-1"), Mockito.any(), Mockito.anyLong())).thenReturn(
        Futures.immediateFuture(
            ChangeRequestsSnapshot.success(new ChangeRequestHistory.Counter(2, 0), changes)
        )
    );

    final MockHttpServletResponse response = new MockHttpServletResponse();
    final HttpServletRequest request = createMockRequest(response);

    resourceWith(broadcaster).getPlacementChanges("broker-1", 1, 0, 10L, request);

    Assertions.assertEquals(HttpServletResponse.SC_OK, response.getStatus());
    final ChangeRequestsSnapshot<SegmentPlacementChange> payload = getResponsePayload(response);
    Assertions.assertEquals(new ChangeRequestHistory.Counter(2, 0), payload.getCounter());
    Assertions.assertEquals(changes, payload.getRequests());
  }

  /**
   * The Broker's counter is passed through untouched, since that is what the Coordinator records as this Broker's
   * position and later waits on before allowing a drop.
   */
  @Test
  public void test_getPlacementChanges_passesTheBrokersPositionToTheBroadcaster() throws Exception
  {
    Mockito.when(coordinator.isLeader()).thenReturn(true);
    Mockito.when(broadcaster.getChangesSince(Mockito.any(), Mockito.any(), Mockito.anyLong())).thenReturn(
        Futures.immediateFuture(
            ChangeRequestsSnapshot.success(new ChangeRequestHistory.Counter(7, 99), List.of())
        )
    );

    final MockHttpServletResponse response = new MockHttpServletResponse();
    final HttpServletRequest request = createMockRequest(response);

    resourceWith(broadcaster).getPlacementChanges("broker-1", 7, 99, 10L, request);

    Mockito.verify(broadcaster).getChangesSince("broker-1", new ChangeRequestHistory.Counter(7, 99), 10L);
  }

  /**
   * A Broker following a Coordinator that has lost leadership must be told to look elsewhere. Answering 200 with an
   * empty stream would read as "nothing has changed", and the Broker would go on believing it is up to date.
   */
  @Test
  public void test_getPlacementChanges_is404_whenNotLeader() throws Exception
  {
    Mockito.when(coordinator.isLeader()).thenReturn(false);

    final MockHttpServletResponse response = new MockHttpServletResponse();
    final HttpServletRequest request = createMockRequest(response);

    resourceWith(broadcaster).getPlacementChanges("broker-1", 0, 0, 10L, request);

    Assertions.assertEquals(HttpServletResponse.SC_NOT_FOUND, response.getStatus());
    Mockito.verify(broadcaster, Mockito.never()).getChangesSince(Mockito.any(), Mockito.any(), Mockito.anyLong());
  }

  /**
   * Same reasoning when the stream is switched off entirely: the Broker must fall back, not conclude it is current.
   */
  @Test
  public void test_getPlacementChanges_is404_whenTheStreamIsDisabled() throws Exception
  {
    Mockito.when(coordinator.isLeader()).thenReturn(true);

    final MockHttpServletResponse response = new MockHttpServletResponse();
    final HttpServletRequest request = createMockRequest(response);

    resourceWith(null).getPlacementChanges("broker-1", 0, 0, 10L, request);

    Assertions.assertEquals(HttpServletResponse.SC_NOT_FOUND, response.getStatus());
  }

  /**
   * Without an identity the Coordinator could serve the stream but could never know anyone had consumed it, which
   * would silently disable the very gating this endpoint exists for.
   */
  @Test
  public void test_getPlacementChanges_is400_whenBrokerIdIsMissing() throws Exception
  {
    Mockito.when(coordinator.isLeader()).thenReturn(true);

    final MockHttpServletResponse response = new MockHttpServletResponse();
    final HttpServletRequest request = createMockRequest(response);

    resourceWith(broadcaster).getPlacementChanges(null, 0, 0, 10L, request);

    Assertions.assertEquals(HttpServletResponse.SC_BAD_REQUEST, response.getStatus());
    Mockito.verify(broadcaster, Mockito.never()).getChangesSince(Mockito.any(), Mockito.any(), Mockito.anyLong());
  }

  @Test
  public void test_getPlacementChanges_is400_whenBrokerIdIsEmpty() throws Exception
  {
    Mockito.when(coordinator.isLeader()).thenReturn(true);

    final MockHttpServletResponse response = new MockHttpServletResponse();
    final HttpServletRequest request = createMockRequest(response);

    resourceWith(broadcaster).getPlacementChanges("", 0, 0, 10L, request);

    Assertions.assertEquals(HttpServletResponse.SC_BAD_REQUEST, response.getStatus());
  }

  /**
   * A non-positive timeout would make the long-poll return immediately, turning the stream into a busy loop.
   */
  @Test
  public void test_getPlacementChanges_is400_whenTimeoutIsNotPositive() throws Exception
  {
    Mockito.when(coordinator.isLeader()).thenReturn(true);

    final MockHttpServletResponse response = new MockHttpServletResponse();
    final HttpServletRequest request = createMockRequest(response);

    resourceWith(broadcaster).getPlacementChanges("broker-1", 0, 0, 0L, request);

    Assertions.assertEquals(HttpServletResponse.SC_BAD_REQUEST, response.getStatus());
    Mockito.verify(broadcaster, Mockito.never()).getChangesSince(Mockito.any(), Mockito.any(), Mockito.anyLong());
  }

  /**
   * Leadership is checked before the broker parameter is, so a Broker talking to a follower gets told about the
   * follower rather than about its own request.
   */
  @Test
  public void test_getPlacementChanges_prefers404_whenNotLeaderAndBrokerIdMissing() throws Exception
  {
    Mockito.when(coordinator.isLeader()).thenReturn(false);

    final MockHttpServletResponse response = new MockHttpServletResponse();
    final HttpServletRequest request = createMockRequest(response);

    resourceWith(broadcaster).getPlacementChanges(null, 0, 0, 10L, request);

    Assertions.assertEquals(HttpServletResponse.SC_NOT_FOUND, response.getStatus());
  }

  private ChangeRequestsSnapshot<SegmentPlacementChange> getResponsePayload(
      MockHttpServletResponse response
  ) throws IOException
  {
    return TestHelper.JSON_MAPPER.readValue(response.baos.toByteArray(), new TypeReference<>() {});
  }

  private HttpServletRequest createMockRequest(MockHttpServletResponse response)
  {
    final MockHttpServletRequest request = new MockHttpServletRequest();

    final MockAsyncContext asyncContext = new MockAsyncContext();
    asyncContext.request = request;
    asyncContext.response = response;

    request.asyncContextSupplier = () -> asyncContext;
    return request;
  }
}
