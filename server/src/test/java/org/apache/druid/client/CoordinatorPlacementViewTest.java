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

package org.apache.druid.client;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.SegmentPlacementChange;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Broker's side of the Coordinator's placement stream.
 * <p>
 * The stream is purely additive -- it publishes a destination replica ahead of the Broker's own sync with that server
 * -- so what matters here is that it keeps pointing at the current leader and never silently follows a stale one. A
 * Broker whose stream is broken simply learns of new replicas late; removals still arrive on the per-node syncs.
 */
public class CoordinatorPlacementViewTest
{
  private static final SegmentId SEGMENT_ID = SegmentId.of(
      "wikipedia",
      Intervals.of("2024-01-01/2024-01-02"),
      DateTimes.of("2024-01-03").toString(),
      3
  );

  private static final URI LEADER_A = URI.create("http://coordinator-a:8081");
  private static final URI LEADER_B = URI.create("http://coordinator-b:8081");

  private TestCoordinatorClient coordinatorClient;
  private CoordinatorPlacementView view;

  @Before
  public void setUp()
  {
    coordinatorClient = new TestCoordinatorClient();
    view = newView();
  }

  private CoordinatorPlacementView newView()
  {
    return new CoordinatorPlacementView(
        TestHelper.makeSmileMapper(),
        new UnusedHttpClient(),
        coordinatorClient,
        "broker-1"
    );
  }

  @Test
  public void test_refreshLeader_pointsTheStreamAtTheLeader() throws Exception
  {
    coordinatorClient.leader = LEADER_A;
    view.refreshLeader();

    Assert.assertEquals(LEADER_A.toURL(), view.getCurrentLeaderUrl());
  }

  @Test
  public void test_refreshLeader_repointsWhenLeadershipMoves() throws Exception
  {
    coordinatorClient.leader = LEADER_A;
    view.refreshLeader();
    Assert.assertEquals(LEADER_A.toURL(), view.getCurrentLeaderUrl());

    coordinatorClient.leader = LEADER_B;
    view.refreshLeader();
    Assert.assertEquals(LEADER_B.toURL(), view.getCurrentLeaderUrl());
  }

  /**
   * Repointing tears the syncer down and builds a new one, which resets the position. Doing that on every poll would
   * mean the stream never establishes at all, so an unchanged leader must be left alone.
   */
  @Test
  public void test_refreshLeader_doesNotRepointWhenTheLeaderIsUnchanged() throws Exception
  {
    coordinatorClient.leader = LEADER_A;
    view.refreshLeader();

    view.refreshLeader();

    Assert.assertEquals(LEADER_A.toURL(), view.getCurrentLeaderUrl());
  }

  @Test
  public void test_refreshLeader_revokesAuthorityWhenThereIsNoLeader() throws Exception
  {
    coordinatorClient.leader = LEADER_A;
    view.refreshLeader();

    coordinatorClient.leader = null;
    view.refreshLeader();

    Assert.assertNull(view.getCurrentLeaderUrl());
  }

  @Test
  public void test_refreshLeader_revokesAuthorityWhenTheCoordinatorCannotBeReached() throws Exception
  {
    coordinatorClient.leader = LEADER_A;
    view.refreshLeader();

    coordinatorClient.failure = new RuntimeException("Coordinator unreachable");
    view.refreshLeader();

    Assert.assertNull(view.getCurrentLeaderUrl());
  }

  /**
   * A failure while building the syncer must leave the view in the same state as having no leader at all, rather than
   * holding a leader it is not actually following.
   */
  @Test
  public void test_refreshLeader_revokesAuthorityWhenTheStreamCannotBeStarted() throws Exception
  {
    coordinatorClient.leader = LEADER_A;
    view.refreshLeader();

    // No scheme, so URI.toURL() throws inside startSyncerLocked.
    coordinatorClient.leader = URI.create("coordinator-a:8081");
    view.refreshLeader();

    Assert.assertNull(view.getCurrentLeaderUrl());
  }

  /**
   * Losing the leader and getting it back must work, since that is what an ordinary Coordinator restart looks like.
   */
  @Test
  public void test_refreshLeader_recoversAfterTheLeaderComesBack() throws Exception
  {
    coordinatorClient.leader = LEADER_A;
    view.refreshLeader();

    coordinatorClient.leader = null;
    view.refreshLeader();
    Assert.assertNull(view.getCurrentLeaderUrl());

    coordinatorClient.leader = LEADER_A;
    view.refreshLeader();
    Assert.assertEquals(LEADER_A.toURL(), view.getCurrentLeaderUrl());
  }

  @Test
  public void test_registeredChangeHandler_receivesChangesInOrder()
  {
    final List<SegmentPlacementChange> received = new ArrayList<>();
    view.registerChangeHandler(received::addAll);

    // Order is the whole point of the stream: the addition must not be separable from the removal it justifies.
    final List<SegmentPlacementChange> changes = List.of(
        SegmentPlacementChange.replicaAdded(SEGMENT_ID, "historical-2"),
        SegmentPlacementChange.replicaRemoved(SEGMENT_ID, "historical-1")
    );
    view.deliver(changes);

    Assert.assertEquals(changes, received);
  }

  /**
   * A later registration replaces the earlier one rather than accumulating, so a re-registering Broker view does not
   * end up applying every change twice.
   */
  @Test
  public void test_registerChangeHandler_replacesThePreviousHandler()
  {
    final List<SegmentPlacementChange> first = new ArrayList<>();
    final List<SegmentPlacementChange> second = new ArrayList<>();
    view.registerChangeHandler(first::addAll);
    view.registerChangeHandler(second::addAll);

    final List<SegmentPlacementChange> changes = List.of(
        SegmentPlacementChange.replicaAdded(SEGMENT_ID, "historical-2")
    );
    view.deliver(changes);

    Assert.assertEquals(List.of(), first);
    Assert.assertEquals(changes, second);
  }

  /**
   * The view is constructed before {@code BrokerServerView} registers its handler, so anything arriving in between
   * must be dropped rather than throw on the sync thread.
   */
  @Test
  public void test_deliver_beforeAnyHandlerIsRegistered_doesNotThrow()
  {
    view.deliver(List.of());
  }

  @Test
  public void test_stop_clearsTheLeader() throws Exception
  {
    coordinatorClient.leader = LEADER_A;
    view.refreshLeader();

    view.stop();

    Assert.assertNull(view.getCurrentLeaderUrl());
  }

  private static class TestCoordinatorClient extends NoopCoordinatorClient
  {
    @Nullable
    private URI leader;
    @Nullable
    private RuntimeException failure;
    private final AtomicInteger findLeaderCalls = new AtomicInteger();

    @Override
    public ListenableFuture<URI> findCurrentLeader()
    {
      findLeaderCalls.incrementAndGet();
      if (failure != null) {
        return Futures.immediateFailedFuture(failure);
      }
      return Futures.immediateFuture(leader);
    }
  }

  /**
   * The syncer only issues requests once it is scheduled, which these tests never let happen, so any call here is a
   * test that is not testing what it thinks it is.
   */
  private static class UnusedHttpClient implements HttpClient
  {
    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> handler
    )
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> handler,
        Duration readTimeout
    )
    {
      throw new UnsupportedOperationException();
    }
  }
}
