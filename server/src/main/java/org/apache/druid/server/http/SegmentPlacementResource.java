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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.SegmentPlacementChange;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.SegmentPlacementBroadcaster;
import org.apache.druid.server.http.security.StateResourceFilter;

import javax.annotation.Nullable;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Serves the Coordinator's ordered stream of placement changes to Brokers.
 * <p>
 * Mirrors {@link SegmentListerResource#getSegments} deliberately: same long-poll shape, same counter/hash gap
 * detection, so a Broker can drive it with the existing
 * {@link org.apache.druid.server.coordination.ChangeRequestHttpSyncer}.
 * <p>
 * The one addition is the {@code broker} parameter. Every request states the position its sender has already
 * consumed, so serving a request is also collecting an acknowledgement -- which is what lets the Coordinator hold a
 * drop back until every Broker knows where the segment went. See apache/druid#18738.
 */
@Path(SegmentPlacementResource.PATH)
@ResourceFilters(StateResourceFilter.class)
public class SegmentPlacementResource
{
  public static final String PATH = "/druid-internal/v1/segmentPlacement";

  public static final TypeReference<ChangeRequestsSnapshot<SegmentPlacementChange>> RESPONSE_TYPE_REF =
      new TypeReference<>() {};

  private static final EmittingLogger log = new EmittingLogger(SegmentPlacementResource.class);

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final DruidCoordinator coordinator;
  @Nullable
  private final SegmentPlacementBroadcaster broadcaster;

  @Inject
  public SegmentPlacementResource(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      DruidCoordinator coordinator,
      @Nullable SegmentPlacementBroadcaster broadcaster
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.coordinator = coordinator;
    this.broadcaster = broadcaster;
  }

  /**
   * @param brokerId identifies the caller, so its progress through the stream can be tracked. Without it the
   *                 Coordinator could serve the stream but could never know whether anyone had consumed it.
   * @param counter  the position the caller has already consumed; negative means it is starting or has reset and has
   *                 no position at all.
   */
  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Void getPlacementChanges(
      @QueryParam("broker") String brokerId,
      @QueryParam("counter") long counter,
      @QueryParam("hash") long hash,
      @QueryParam("timeout") long timeout,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    if (broadcaster == null || !coordinator.isLeader()) {
      // Only the leader publishes placement, so a Broker following a stale leader must be told to look elsewhere
      // rather than be handed an empty stream it would mistake for "nothing has changed".
      sendError(req, HttpServletResponse.SC_NOT_FOUND, "This Coordinator is not the leader.");
      return null;
    }

    if (brokerId == null || brokerId.isEmpty()) {
      sendError(req, HttpServletResponse.SC_BAD_REQUEST, "broker must be specified.");
      return null;
    }

    if (timeout <= 0) {
      sendError(req, HttpServletResponse.SC_BAD_REQUEST, "timeout must be positive.");
      return null;
    }

    final ObjectMapper mapper = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(req.getHeader("Accept"))
                                ? smileMapper
                                : jsonMapper;

    final ListenableFuture<ChangeRequestsSnapshot<SegmentPlacementChange>> future =
        broadcaster.getChangesSince(brokerId, new ChangeRequestHistory.Counter(counter, hash), timeout);

    final AsyncContext asyncContext = req.startAsync();
    asyncContext.addListener(
        ServletResourceUtils.createAsyncTimeoutListener(event -> {
          future.cancel(true);
          event.getAsyncContext().complete();
        })
    );

    Futures.addCallback(
        future,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(ChangeRequestsSnapshot<SegmentPlacementChange> result)
          {
            try {
              final HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              response.setStatus(HttpServletResponse.SC_OK);
              mapper.writerFor(RESPONSE_TYPE_REF).writeValue(asyncContext.getResponse().getOutputStream(), result);
              asyncContext.complete();
            }
            catch (Exception ex) {
              log.debug(ex, "Request timed out or closed already.");
            }
          }

          @Override
          public void onFailure(Throwable th)
          {
            try {
              final HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              if (th instanceof IllegalArgumentException) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, th.getMessage());
              } else {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, th.getMessage());
              }
              asyncContext.complete();
            }
            catch (Exception ex) {
              log.debug(ex, "Request timed out or closed already.");
            }
          }
        },
        MoreExecutors.directExecutor()
    );

    asyncContext.setTimeout(timeout);
    return null;
  }

  private void sendError(HttpServletRequest req, int code, String message) throws IOException
  {
    final AsyncContext asyncContext = req.startAsync();
    final HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
    response.sendError(code, message);
    asyncContext.complete();
  }

}
