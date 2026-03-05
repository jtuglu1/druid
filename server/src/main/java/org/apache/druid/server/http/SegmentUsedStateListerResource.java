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
import org.apache.druid.server.coordination.SegmentUsedStateChangeRequest;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.http.security.StateResourceFilter;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 * Coordinator-side endpoint for broker→coordinator delta sync of the used-segment set.
 *
 * <ul>
 *   <li>{@code counter = -1}: returns the full set of currently used segment IDs
 *       together with the latest counter so the client can switch to delta polling.</li>
 *   <li>{@code counter ≥ 0}: long-polls for changes since the given counter,
 *       returning at most once the history has new entries or the timeout elapses.</li>
 * </ul>
 */
@Path("/druid/coordinator/v1/segment-used-state")
@ResourceFilters(StateResourceFilter.class)
public class SegmentUsedStateListerResource
{
  private static final EmittingLogger log = new EmittingLogger(SegmentUsedStateListerResource.class);

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final DruidCoordinator coordinator;

  @Inject
  public SegmentUsedStateListerResource(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      DruidCoordinator coordinator
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.coordinator = coordinator;
  }

  /**
   * Long-poll endpoint for broker-side used-segment tracking.
   *
   * @param counter counter from the last response, or {@code -1} for the first request
   * @param hash    hash from the last response (ignored when {@code counter = -1})
   * @param timeout max milliseconds to wait before returning an empty delta
   */
  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Void getSegmentUsedState(
      @QueryParam("counter") long counter,
      @QueryParam("hash") long hash,
      @QueryParam("timeout") long timeout,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    if (!coordinator.isLeader()) {
      sendErrorResponse(req, HttpServletResponse.SC_SERVICE_UNAVAILABLE, "Not the coordinator leader.");
      return null;
    }

    if (timeout <= 0) {
      sendErrorResponse(req, HttpServletResponse.SC_BAD_REQUEST, "timeout must be positive.");
      return null;
    }

    // counter=-1 → full sync. Return the current snapshot immediately (no need to long-poll).
    if (counter < 0) {
      final ChangeRequestsSnapshot<SegmentUsedStateChangeRequest> snapshot =
          coordinator.getFullSegmentUsedStateSnapshot();
      final AsyncContext asyncContext = req.startAsync();
      try {
        final HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
        response.setStatus(HttpServletResponse.SC_OK);
        pickMapper(req).writeValue(asyncContext.getResponse().getOutputStream(), snapshot);
      }
      catch (Exception ex) {
        log.debug(ex, "Error writing full-sync response.");
      }
      finally {
        asyncContext.complete();
      }
      return null;
    }

    // counter≥0 → delta sync via long-poll.
    final ListenableFuture<ChangeRequestsSnapshot<SegmentUsedStateChangeRequest>> future =
        coordinator.getSegmentUsedStateDeltaSince(new ChangeRequestHistory.Counter(counter, hash));

    final AsyncContext asyncContext = req.startAsync();
    asyncContext.addListener(
        new AsyncListener()
        {
          @Override
          public void onComplete(AsyncEvent event)
          {
          }

          @Override
          public void onTimeout(AsyncEvent event)
          {
            future.cancel(true);
            event.getAsyncContext().complete();
          }

          @Override
          public void onError(AsyncEvent event)
          {
          }

          @Override
          public void onStartAsync(AsyncEvent event)
          {
          }
        }
    );

    Futures.addCallback(
        future,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(ChangeRequestsSnapshot<SegmentUsedStateChangeRequest> result)
          {
            try {
              final HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              response.setStatus(HttpServletResponse.SC_OK);
              pickMapper(req).writeValue(asyncContext.getResponse().getOutputStream(), result);
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

  private ObjectMapper pickMapper(HttpServletRequest req)
  {
    return SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(req.getHeader("Accept"))
           ? smileMapper
           : jsonMapper;
  }

  private void sendErrorResponse(HttpServletRequest req, int code, String error) throws IOException
  {
    final AsyncContext asyncContext = req.startAsync();
    final HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
    response.sendError(code, error);
    asyncContext.complete();
  }
}
