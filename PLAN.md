# Broker Segment Unavailability Detection

## Context

Druid brokers silently serve partial results when data nodes are unavailable. When a historical goes down, `BrokerServerView.serverRemovedSegment()` removes the segment from the timeline once no servers remain, causing queries to return incomplete data without any error.

The fix requires the broker to distinguish "segment is unused (Case 1: remove from timeline)" from "segment is used but temporarily unavailable (Cases 2-4: keep in timeline, fail queries)". The broker can't make this distinction from historical callbacks alone — it needs the coordinator to provide the expected segment state.

**Issue**: https://github.com/apache/druid/issues/18716

---

## Phase 1: Coordinator Segment State Endpoint

Build the coordinator-side infrastructure to expose used segment state with delta-based sync.

### 1a. Create `SegmentUsedStateChangeRequest`

New change request type for the coordinator→broker sync changelog.

**New file**: `server/src/main/java/org/apache/druid/server/coordination/SegmentUsedStateChangeRequest.java`

- Two subtypes (similar to `SegmentChangeRequestLoad`/`SegmentChangeRequestDrop`):
  - `SegmentUsedStateChange.USED` — segment is used in metadata
  - `SegmentUsedStateChange.UNUSED` — segment marked unused
- Payload: `SegmentId` (lightweight — no full `DataSegment` needed since broker already has it from historical callbacks)

### 1b. Create `SegmentUsedStateHistory` in the Coordinator

Maintain a `ChangeRequestHistory<SegmentUsedStateChangeRequest>` within the coordinator that tracks changes to segment used status.

**Modify**: `server/src/main/java/org/apache/druid/server/coordinator/DruidCoordinator.java`

- Add a `ChangeRequestHistory<SegmentUsedStateChangeRequest>` field
- On each coordinator duty cycle, diff current used segments vs. previous run and push changes to the history
- On startup, populate initial state from `SegmentsMetadataManager`
- Expose a method `getSegmentUsedStateChangesSince(Counter)` for the endpoint

### 1c. Create Coordinator HTTP Endpoint

**New file**: `server/src/main/java/org/apache/druid/server/http/SegmentUsedStateListerResource.java`

- Endpoint: `GET /druid/coordinator/v1/segment-used-state`
- Query params: `counter`, `hash`, `timeout` (same pattern as `SegmentListerResource`)
- On `counter=-1`: return full set of all currently used `SegmentId`s
- On `counter=N`: return delta from `ChangeRequestHistory`
- Uses `AsyncContext` for long-polling (same as `SegmentListerResource`)

---

## Phase 2: Broker Sync Client

Build the broker-side polling client that syncs expected segment state from the coordinator.

### 2a. Create `BrokerSegmentUsedStateWatcher`

**New file**: `server/src/main/java/org/apache/druid/client/BrokerSegmentUsedStateWatcher.java`

- Managed lifecycle component (inject, `@LifecycleStart`)
- Uses `ChangeRequestHttpSyncer` to poll the coordinator endpoint from Phase 1
- Maintains `Set<SegmentId> expectedUsedSegments` (the coordinator's view of what should exist)
- On `fullSync`: replace entire set
- On `deltaSync`: apply adds/removes
- Blocks startup until initial full sync completes (`CountDownLatch`, like `BrokerServerView`)
- Exposes `boolean isUsed(SegmentId)` for query-time checks
- Exposes `void registerCallback(SegmentUsedStateCallback)` so `BrokerServerView` can react to state changes

### 2b. Create Config

**New file**: `server/src/main/java/org/apache/druid/client/BrokerSegmentUnavailabilityConfig.java`

- `boolean enabled` (default: `false`) — feature flag, opt-in initially
- `long pollPeriodMs` (default: `30000`) — coordinator sync interval
- `boolean failOnUnavailable` (default: `true`) — when enabled, whether to hard-fail queries or just add response context warning

### 2c. Wire into Guice

**Modify**: `server/src/main/java/org/apache/druid/guice/BrokerModule.java` (or `CliBroker`)

- Bind `BrokerSegmentUnavailabilityConfig`
- Bind `BrokerSegmentUsedStateWatcher` as managed lifecycle

---

## Phase 3: BrokerServerView Changes

Modify the broker's segment timeline management to retain segments that are used but unavailable.

### 3a. Modify `BrokerServerView.serverRemovedSegment()`

**Modify**: `server/src/main/java/org/apache/druid/client/BrokerServerView.java`

Current behavior (lines 353-370): when `selector.isEmpty()`, remove segment from timeline + selectors map.

New behavior:
```java
if (selector.isEmpty()) {
  if (unavailabilityDetectionEnabled && segmentUsedStateWatcher.isUsed(segmentId)) {
    // Cases 2, 3, 4: keep segment in timeline, do NOT remove
    // Queries will fail at groupSegmentsByServer() when pick() returns null
    log.info("Segment[%s] has no available servers but is still used. Keeping in timeline.", segmentId);
  } else {
    // Case 1: unused, or feature disabled — remove from timeline (current behavior)
    timeline.remove(...);
    selectors.remove(segmentId);
    runTimelineCallbacks(callback -> callback.segmentRemoved(segment));
  }
}
```

### 3b. React to Coordinator Sync Updates

Register a callback with `BrokerSegmentUsedStateWatcher` so that when the coordinator sync reports a segment as unused, `BrokerServerView` can clean it up from the timeline (if its selector is empty).

```java
segmentUsedStateWatcher.registerCallback(new SegmentUsedStateCallback() {
  void onSegmentUnused(SegmentId segmentId) {
    synchronized (lock) {
      ServerSelector selector = selectors.get(segmentId);
      if (selector != null && selector.isEmpty()) {
        // Coordinator confirmed unused + no servers → safe to remove
        timeline.remove(...);
        selectors.remove(segmentId);
        runTimelineCallbacks(callback -> callback.segmentRemoved(segment));
      }
    }
  }
});
```

---

## Phase 4: Query Failure on Unavailable Segments

Surface unavailability errors at query time.

### 4a. Modify `CachingClusteredClient.groupSegmentsByServer()`

**Modify**: `server/src/main/java/org/apache/druid/client/CachingClusteredClient.java`

Current behavior (line ~600): when `pick()` returns null, emits alert and silently skips the segment.

New behavior:
```java
if (queryableDruidServer == null) {
  if (unavailabilityDetectionEnabled) {
    throw new QueryUnavailableException(
      "Segment[%s] for DataSource[%s] is unavailable — no servers are currently serving it.",
      segmentServer.getSegmentDescriptor(),
      query.getDataSource()
    );
  } else {
    // Current behavior: alert and skip
    log.makeAlert(...).emit();
  }
}
```

### 4b. Create `QueryUnavailableException`

**New file**: `processing/src/main/java/org/apache/druid/query/QueryUnavailableException.java`

- Extends `QueryException` or similar
- HTTP status: `503 Service Unavailable`
- Error code: `UNAVAILABLE_SEGMENTS`
- Include segment descriptors in the error message for debuggability

### 4c. Emit Metrics

Add metric emission when unavailable segments are detected:
- `query/segment/unavailable/count` — count of unavailable segments encountered per query
- Emit in `groupSegmentsByServer()` before throwing

---

## Phase 5: Testing

### Unit Tests

- **`BrokerServerViewTest`**: Test that segments with empty selectors are retained in the timeline when `isUsed()` returns true, and removed when `isUsed()` returns false
- **`CachingClusteredClientTest`**: Test that queries fail with `QueryUnavailableException` when segments are unavailable (pick returns null) and feature is enabled; test that queries succeed silently when feature is disabled
- **`BrokerSegmentUsedStateWatcherTest`**: Test full sync, delta sync, reset mechanism, coordinator failure handling
- **`SegmentUsedStateListerResourceTest`**: Test endpoint returns correct full/delta responses
- **`ChangeRequestHistory` with new change type**: Verify serialization/deserialization

### Integration / Embedded Tests

- Extend/create embedded test (reference PR #18737) that:
  1. Starts a mini-cluster (coordinator + broker + historical)
  2. Loads data onto historical
  3. Verifies queries succeed
  4. Stops the historical
  5. Verifies queries fail with `UNAVAILABLE_SEGMENTS` error (not partial results)
  6. Restarts the historical
  7. Verifies queries succeed again

---

## PR Strategy

| PR | Scope | Dependencies |
|----|-------|-------------|
| **PR 1** | Phase 1 (coordinator endpoint) + Phase 2 (broker sync client) | None |
| **PR 2** | Phase 3 (BrokerServerView changes) + Phase 4 (query failure) + Phase 5 (tests) | PR 1 |

PR 1 is purely additive (new classes, new endpoint) with no behavioral changes. PR 2 is where the behavior changes behind the feature flag.

---

## Key Files Summary

| File | Action | Purpose |
|------|--------|---------|
| `server/.../coordination/SegmentUsedStateChangeRequest.java` | Create | Change request type for used/unused deltas |
| `server/.../http/SegmentUsedStateListerResource.java` | Create | Coordinator endpoint for segment state sync |
| `server/.../coordinator/DruidCoordinator.java` | Modify | Add ChangeRequestHistory for segment used state |
| `server/.../client/BrokerSegmentUsedStateWatcher.java` | Create | Broker-side sync client |
| `server/.../client/BrokerSegmentUnavailabilityConfig.java` | Create | Feature flag + config |
| `server/.../client/BrokerServerView.java` | Modify | Stop removing used segments from timeline |
| `server/.../client/CachingClusteredClient.java` | Modify | Fail queries on unavailable segments |
| `processing/.../query/QueryUnavailableException.java` | Create | Exception for unavailable segments |
| `server/.../guice/BrokerModule.java` | Modify | Wire new components |

---

## Open Questions / Risks

1. **Coordinator duty cycle frequency**: The coordinator runs duties on a configurable period (default 60s). Changes to segment used status are detected per duty run. This means the coordinator's changelog granularity is bounded by the duty cycle — deltas won't be finer than this.

2. **Memory overhead**: Keeping segments with empty selectors in the broker timeline adds memory pressure. Bounded by the number of unavailable segments, which should be small in a healthy cluster.

3. **Segment moves during rebalancing**: Bulk segment moves can cause many segments to briefly have empty selectors. This could cause a burst of query failures. Consider adding a short grace period or configurable tolerance before failing.
