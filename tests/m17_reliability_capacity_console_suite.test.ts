/**
 * @fileoverview M17 (TASK-84 ~ TASK-89) Verification Suite:
 * - Sizing & SLA Capacity Admission Enforcer (Brain 128MB/RTO<3s, Body 32-sandbox/<1.5ms, Tunnel 20k streams/<15ms, RustFS EC:4)
 * - Queue-backed Agent Stream Publisher & TurnWAL `Last-Event-ID` Incremental Catch-up
 * - `TunnelGatewayCell` Co-located System Cell & Full-Jitter Reconnect Backoff Guard
 * - TanStack Management Console BFF Controller end-to-end integration
 */

import test from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import {
  CapacitySlaEnforcer,
  AgentStreamQueuePublisher,
  SseEventStreamGateway,
  TurnWalEngine,
  FenceToken,
  TaskState,
  TunnelGatewayCell,
  EdgeTunnelHaDynamicRouter,
  MultiRegionGeoRouter,
  ConsoleBffController,
  CloudEventsBus,
  OtelAndPrometheusExporter,
  FaultTolerantObservabilityCoordinator,
  UnifiedComponentPersistenceStore,
  InMemorySqlStorage,
  CanaryRolloutController,
  ProductionChaosDrillRunner,
  HaPartitionedSchedulerMeshCoordinator,
} from "../src/index";

test("test 61: [TASK-84] CapacitySlaEnforcer enforces Brain 128MB watermarks, Body 32-sandbox cap, Tunnel 20k stream cap, and RustFS EC:4 quorum", () => {
  const enforcer = new CapacitySlaEnforcer();

  // 1. Brain Isolate: 100MB / 128MB (78.1%) -> triggers GC (>=75%) but allows admission (<90%)
  const brainOk = enforcer.evaluateBrainIsolateAdmission({
    isolateId: "iso-01",
    heapUsedMb: 100,
    heapLimitMb: 128,
    activeCells: 120,
    lastRehydrateRtoMs: 850,
  });
  assert.equal(brainOk.allowed, true);
  assert.equal(brainOk.gcTriggered, true);
  assert.equal(brainOk.circuitBreakerTripped, false);
  assert.equal(brainOk.slaViolations.length, 0);

  // Brain Isolate: 118MB / 128MB (92.1%) -> trips circuit breaker (>=90%)
  const brainOverloaded = enforcer.evaluateBrainIsolateAdmission({
    isolateId: "iso-01",
    heapUsedMb: 118,
    heapLimitMb: 128,
    activeCells: 200,
    lastRehydrateRtoMs: 3400,
  });
  assert.equal(brainOverloaded.allowed, false);
  assert.equal(brainOverloaded.circuitBreakerTripped, true);
  assert.equal(brainOverloaded.slaViolations.length, 2);

  // 2. Body Host: 32/32 sandboxes -> rejects 33rd sandbox
  const bodyFull = enforcer.evaluateBodyHostAdmission({
    hostId: "body-01",
    vcpuCount: 16,
    ramGb: 64,
    activeSandboxes: 32,
    maxSandboxesLimit: 32,
    lastCgroupBoostLatencyMs: 1.1,
    lastVsockFastpathLatencyMs: 0.11,
  });
  assert.equal(bodyFull.allowed, false);
  assert.equal(bodyFull.availableSlots, 0);

  // 3. RustFS EC:4: 4 failed drives out of 12 -> still RPO=0 guaranteed; 5 failed drives -> quorum lost
  const ec4Survives4 = enforcer.evaluateRustFsEc4Reliability({
    totalDrives: 12,
    dataBlocks: 8,
    parityBlocks: 4,
    healthyDrives: 8,
    failedDrives: 4,
    lastEtagCasLatencyMs: 8.2,
  });
  assert.equal(ec4Survives4.rpoZeroGuaranteed, true);
  assert.equal(ec4Survives4.remainingFaultToleranceDrives, 0);

  const ec4Fails5 = enforcer.evaluateRustFsEc4Reliability({
    totalDrives: 12,
    dataBlocks: 8,
    parityBlocks: 4,
    healthyDrives: 7,
    failedDrives: 5,
    lastEtagCasLatencyMs: 14.5,
  });
  assert.equal(ec4Fails5.rpoZeroGuaranteed, false);
  assert.equal(ec4Fails5.slaViolations.length, 2);
});

test("test 62: [TASK-85 & TASK-86] AgentStreamQueuePublisher persists TurnWAL frames and replays missed events on Last-Event-ID reconnect", () => {
  const sseGw = new SseEventStreamGateway();
  // Isolated per-run WAL root: frames are create-once, so a shared root 412s on re-runs.
  const walEngine = new TurnWalEngine(fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m17-wal-")));
  const publisher = new AgentStreamQueuePublisher(sseGw, walEngine);

  sseGw.registerClient("client-ui-01", 100);

  // Publish turns 1, 2, 3 while stream is active or interrupted
  publisher.publishTaskStreamEvent({
    clientId: "client-ui-01",
    taskId: "task-stream-202",
    eventType: "TASK_CLAIMED",
    state: TaskState.THINKING_GEN,
    fence: new FenceToken(2, 1, "task-stream-202"),
    payload: { message: "Claimed by HarnessCell" },
  });
  publisher.publishTaskStreamEvent({
    clientId: "client-ui-01",
    taskId: "task-stream-202",
    eventType: "REASONING_CHUNK",
    state: TaskState.THINKING_GEN,
    fence: new FenceToken(2, 2, "task-stream-202"),
    payload: { delta: "Analyzing sandbox cgroups..." },
  });
  publisher.publishTaskStreamEvent({
    clientId: "client-ui-01",
    taskId: "task-stream-202",
    eventType: "TOOL_EXEC_PULSE",
    state: TaskState.TOOL_EXEC,
    fence: new FenceToken(2, 3, "task-stream-202"),
    payload: { tool: "bash", boost: "8.0C" },
  });

  // Client reconnects with Last-Event-ID / lastReceivedTurnSeq = 1 -> must receive turns 2 and 3
  const catchup = publisher.resumeAndCatchUpFromWal({
    clientId: "client-ui-01",
    taskId: "task-stream-202",
    lastReceivedTurnSeq: 1,
  });

  assert.equal(catchup.replayedWalFrames.length, 2);
  assert.equal(catchup.replayedWalFrames[0].fence.turnSeq, 2);
  assert.equal(catchup.replayedWalFrames[1].fence.turnSeq, 3);
  assert.equal(catchup.currentEpoch, 2);
  assert.equal(catchup.currentTurnSeq, 3);
  assert.equal(catchup.liveStreamResumed, true);
});

test("test 63: [TASK-87 & TASK-88] TunnelGatewayCell heartbeat eviction, failover, and ConsoleBffController integration", () => {
  const haRouter = new EdgeTunnelHaDynamicRouter("celld-gw-01");
  haRouter.registerTunnelGateway({
    gatewayId: "celld-gw-01",
    internalRelayUrl: "https://10.240.16.11:9444/relay",
    publicEdgeIngressUrl: "https://tunnel-01.edge.ansts.io:9443",
    healthy: true,
    draining: false,
  });
  haRouter.registerTunnelGateway({
    gatewayId: "celld-gw-02",
    internalRelayUrl: "https://10.240.16.12:9444/relay",
    publicEdgeIngressUrl: "https://tunnel-02.edge.ansts.io:9443",
    healthy: true,
    draining: false,
  });

  const tunnelCell = new TunnelGatewayCell("celld-gw-01", haRouter);
  tunnelCell.acceptDaemonTunnelStream({
    hostId: "body-edge-09",
    gatewayId: "celld-gw-01",
    role: "PRIMARY",
    connGeneration: 1,
    epoch: 1,
    nowMs: 1000,
  });
  tunnelCell.acceptDaemonTunnelStream({
    hostId: "body-edge-09",
    gatewayId: "celld-gw-02",
    role: "SECONDARY",
    connGeneration: 1,
    epoch: 1,
    nowMs: 1000,
  });

  // Simulate primary stream missing >3 heartbeats (nowMs = 20000 > 1000 + 15000) while secondary stays fresh
  tunnelCell.recordDaemonHeartbeat("body-edge-09", "celld-gw-02", 19000);
  const evicted = tunnelCell.evictTimedOutStreams(20000);
  assert.deepEqual(evicted, ["celld-gw-01:body-edge-09"]);

  // Next command automatically fails over to celld-gw-02
  const relayRes = tunnelCell.relayBrainCommand({
    commandId: "cmd-ha-9",
    taskId: "task-ha-9",
    sandboxId: "sbx-9",
    targetBodyHostId: "body-edge-09",
    fence: { epoch: 1, turnSeq: 1 },
    action: "CGROUP_BOOST_8C",
    payload: {},
  });
  assert.equal(relayRes.routedViaGatewayId, "celld-gw-02");
  assert.equal(relayRes.failoverTriggered, true);

  // Verify Console BFF Controller overview
  const geoRouter = new MultiRegionGeoRouter("us-west-2");
  geoRouter.registerRegion({
    regionId: "us-west-2",
    gatewayEndpoint: "us.edge.ansts.io",
    pingEwmaMs: 32,
    healthy: true,
    activeSessions: 410,
  });
  const publisher = new AgentStreamQueuePublisher(
    new SseEventStreamGateway(),
    new TurnWalEngine(fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m17-bff-wal-"))),
  );
  const bff = new ConsoleBffController(haRouter, geoRouter, publisher);

  const overview = bff.getClusterOverview({
    clientRegion: "us-west-2",
    activeSandboxes: 28,
    maxSandboxes: 32,
    heapUsedMb: 54,
    activeCells: 110,
    failedRustFsDrives: 1,
  });
  assert.equal(overview.clusterHealthy, true);
  assert.equal(overview.rpoZeroGuaranteed, true);
  assert.equal(overview.storageEc4.remainingFaultToleranceDrives, 3);
});

test("test 64: [TASK-90 & TASK-91] FaultTolerantObservabilityCoordinator bounded ring spooling during collector outage and SQLite RPO=0 crash rehydration", () => {
  const sql = new InMemorySqlStorage();
  const stateStore = new UnifiedComponentPersistenceStore(sql);

  const otel = new OtelAndPrometheusExporter();
  const ceBus = new CloudEventsBus();
  const obsCoord = new FaultTolerantObservabilityCoordinator(otel, ceBus, 3); // small ring capacity = 3 to test bounded eviction

  // Emit 4 correlated events while OTLP Collector is offline (503)
  for (let i = 1; i <= 4; i++) {
    obsCoord.recordAndSpoolCorrelatedEvent({
      taskId: "task-obs-01",
      tenantId: "tenant-alpha",
      spanName: `turn.step.${i}`,
      cloudEventType: "io.ansts.turn.executed",
      durationMs: 12,
      data: { step: i },
    });
  }

  // Ring buffer capacity is 3 -> 1 oldest item evicted, 3 newest retained
  assert.equal(obsCoord.spooler.spooledSpans.length, 3);
  assert.equal(obsCoord.spooler.droppedSpansCount, 1);

  // Collector outage (returns 503) -> spool retains all 3 items
  const failedFlush = obsCoord.spooler.flushSpooledTelemetry(() => 503);
  assert.equal(failedFlush.collectorHealthy, false);
  assert.equal(failedFlush.remainingSpooledSpans, 3);

  // Persist to SQLite before Isolate hibernation / node crash
  stateStore.persistObservabilityState(obsCoord, "cell-obs-1");

  // Rehydrate into a brand-new coordinator instance after crash
  const recoveredCoord = new FaultTolerantObservabilityCoordinator(
    new OtelAndPrometheusExporter(),
    new CloudEventsBus(),
    3,
  );
  const restored = stateStore.rehydrateObservabilityState(recoveredCoord, "cell-obs-1");
  assert.equal(restored, true);
  assert.equal(recoveredCoord.spooler.spooledSpans.length, 3);
  assert.equal(recoveredCoord.spooler.spooledCloudEvents.length, 3);

  // Collector recovers (returns 200) -> all spooled spans & CloudEvents flush cleanly
  const okFlush = recoveredCoord.spooler.flushSpooledTelemetry(() => 200);
  assert.equal(okFlush.collectorHealthy, true);
  assert.equal(okFlush.flushedSpansCount, 3);
  assert.equal(okFlush.flushedCloudEventsCount, 3);
  assert.equal(okFlush.remainingSpooledSpans, 0);
});

test("test 65: [TASK-92] W3cTraceContinuityPropagator preserves root traceId and attaches causal SpanLink across Secondary Tunnel Failover and SSE Reconnect", () => {
  const obsCoord = new FaultTolerantObservabilityCoordinator(
    new OtelAndPrometheusExporter(),
    new CloudEventsBus(),
  );

  // 1. Initial span before tunnel interruption
  const initial = obsCoord.recordAndSpoolCorrelatedEvent({
    taskId: "task-trace-77",
    tenantId: "tenant-beta",
    spanName: "tunnel.primary.dispatch",
    cloudEventType: "io.ansts.tunnel.command",
    durationMs: 0.9,
    data: { gateway: "celld-gw-01" },
  });
  assert.equal(initial.span.spanLinks.length, 0);

  // 2. Primary gateway crashes -> <15ms Secondary Tunnel Failover occurs
  const failover = obsCoord.recordAndSpoolCorrelatedEvent({
    taskId: "task-trace-77",
    tenantId: "tenant-beta",
    spanName: "tunnel.secondary.failover",
    cloudEventType: "io.ansts.tunnel.failover",
    durationMs: 1.3,
    recoveryLink: {
      reason: "SECONDARY_TUNNEL_FAILOVER",
      fenceEpoch: 2,
      fenceTurnSeq: 8,
    },
    data: { failoverGateway: "celld-gw-02" },
  });

  // Root traceId is strictly preserved and causal SpanLink points to initial.span.spanId
  assert.equal(failover.span.traceId, initial.span.traceId);
  assert.equal(failover.span.parentSpanId, initial.span.spanId);
  assert.equal(failover.span.spanLinks.length, 1);
  assert.equal(failover.span.spanLinks[0].linkedTraceId, initial.span.traceId);
  assert.equal(failover.span.spanLinks[0].linkedSpanId, initial.span.spanId);
  assert.equal(failover.span.spanLinks[0].recoveryReason, "SECONDARY_TUNNEL_FAILOVER");
  assert.equal(failover.cloudEvent.traceparent, failover.span.traceparent);
});

test("test 66: [TASK-93 & TASK-94] CanaryRolloutController session version pinning and 4-phase progressive virtual slot cutover", () => {
  const rollout = new CanaryRolloutController("v2.3.0", "v2.3.1-canary");

  // 1. Existing task `task-active-1` starts on Stable v2.3.0 in slot 250
  const initialRoute = rollout.resolveTaskVersionAndNode({
    taskId: "task-active-1",
    tenantId: "tenant-std",
    slotId: 250,
    isExistingActiveTask: false,
  });
  assert.equal(initialRoute.routedVersion, "v2.3.0");

  // 2. Advance to Phase 2 Canary (8 slots: 248..255 cut over to v2.3.1-canary)
  const phase2 = rollout.advanceRolloutPhase("PHASE_2_CANARY_8_SLOTS");
  assert.equal(phase2.canarySlotCount, 8);
  assert.equal(rollout.slots.get(250)?.ownerVersion, "v2.3.1-canary");
  assert.equal(rollout.slots.get(100)?.ownerVersion, "v2.3.0");

  // 3. `task-active-1` is an existing active task -> MUST stay pinned to v2.3.0 despite slot 250 moving to Canary
  const pinnedRoute = rollout.resolveTaskVersionAndNode({
    taskId: "task-active-1",
    tenantId: "tenant-std",
    slotId: 250,
    isExistingActiveTask: true,
  });
  assert.equal(pinnedRoute.routedVersion, "v2.3.0");

  // 4. Newly submitted task `task-new-2` in slot 250 routes to Canary v2.3.1-canary
  const newRoute = rollout.resolveTaskVersionAndNode({
    taskId: "task-new-2",
    tenantId: "tenant-std",
    slotId: 250,
    isExistingActiveTask: false,
  });
  assert.equal(newRoute.routedVersion, "v2.3.1-canary");
});

test("test 67: [TASK-95 & TASK-96] CanaryRolloutController automated SLA breach rollback, Make-Before-Break gateway drain, and N-1 WAL schema compatibility", () => {
  const rollout = new CanaryRolloutController("v2.3.0", "v2.3.1-canary");
  rollout.advanceRolloutPhase("PHASE_3_PARTIAL_64_SLOTS");
  assert.equal(rollout.currentPhase, "PHASE_3_PARTIAL_64_SLOTS");

  const haRouter = new EdgeTunnelHaDynamicRouter("celld-gw-stable-01");
  haRouter.registerTunnelGateway({
    gatewayId: "celld-gw-stable-01",
    internalRelayUrl: "https://10.240.16.11:9444/relay",
    publicEdgeIngressUrl: "https://tunnel-01.edge.ansts.io:9443",
    healthy: true,
    draining: false,
  });
  haRouter.registerTunnelGateway({
    gatewayId: "celld-gw-canary-01",
    internalRelayUrl: "https://10.240.16.99:9444/relay",
    publicEdgeIngressUrl: "https://tunnel-canary.edge.ansts.io:9443",
    healthy: true,
    draining: false,
  });
  haRouter.registerInboundDaemonStream({
    hostId: "body-edge-canary-host",
    gatewayId: "celld-gw-canary-01",
    role: "PRIMARY",
    connGeneration: 1,
    epoch: 1,
  });

  // Simulate Canary SLA violation: Brain heap spikes to 112MB (>=75% GC watermark) & 1 slow SQL abort
  const verdict = rollout.evaluateHealthAndAutoRollback(
    {
      brainSnapshot: {
        isolateId: "canary-iso-1",
        heapUsedMb: 112,
        heapLimitMb: 128,
        activeCells: 95,
        lastRehydrateRtoMs: 610,
      },
      tunnelSnapshot: {
        gatewayId: "celld-gw-canary-01",
        activeReverseStreams: 1200,
        maxStreamsLimit: 20000,
        lastOneHopRelayLatencyMs: 0.65,
        lastFailoverLatencyMs: 8.5,
      },
      rustFsStatus: {
        totalDrives: 12,
        dataBlocks: 8,
        parityBlocks: 4,
        healthyDrives: 12,
        failedDrives: 0,
        lastEtagCasLatencyMs: 5.2,
      },
      staleFenceErrorCount: 0,
      slowSqlAbortCount: 1,
      droppedOtelSpansCount: 0,
    },
    haRouter,
    "celld-gw-canary-01",
  );

  assert.equal(verdict.healthy, false);
  assert.equal(verdict.rollbackTriggered, true);
  assert.equal(rollout.currentPhase, "ROLLED_BACK_TO_STABLE");
  // All 256 slots atomically reverted to v2.3.0
  assert.equal(rollout.slots.get(255)?.ownerVersion, "v2.3.0");
  // Canary tunnel gateway placed into Make-Before-Break draining state
  assert.equal(haRouter.gateways.get("celld-gw-canary-01")?.draining, true);

  // Verify N-1 WAL schema compatibility with extra Canary fields
  const compat = rollout.verifyNMinusOneWalSchemaCompatibility(
    JSON.stringify({
      taskId: "task-99",
      fence: { epoch: 3, turnSeq: 12 },
      state: "THINKING_GEN",
      diffSnapshot: "{}",
      canaryExperimentalTraceTag: "v2.3.1-trace-ext",
    }),
  );
  assert.equal(compat.compatible, true);
  assert.deepEqual(compat.preservedUnknownFields, ["canaryExperimentalTraceTag"]);
});

test("test 68: [TASK-97 ~ TASK-100] ProductionChaosDrillRunner executes all 4 end-to-end fault injection drills with RPO=0 and trace continuity", () => {
  const runner = new ProductionChaosDrillRunner();
  const summary = runner.runAllProductionDrills();

  assert.equal(summary.totalDrills, 4);
  assert.equal(summary.allPassed, true);
  for (const report of summary.reports) {
    assert.equal(report.passed, true, `Chaos drill ${report.drillId} failed`);
    assert.equal(report.rpoZeroVerified, true);
    assert.equal(report.traceContinuityVerified, true);
  }
});

test("test 69: [TASK-101 & TASK-102] HaPartitionedSchedulerMeshCoordinator 0-Hop/1-Hop slot routing, DRAINING micro-buffer, and 3-phase online scale-out", () => {
  const mesh = new HaPartitionedSchedulerMeshCoordinator([
    { shardId: "scheduler-shard-00", hostBrainNodeId: "brain-node-01" },
    { shardId: "scheduler-shard-01", hostBrainNodeId: "brain-node-02" },
  ]);

  const slotForTenantA = mesh.computeSha256VirtualSlot("tenant-enterprise-a");
  const initialOwner = mesh.slotRing.slots.get(slotForTenantA)!.ownerShardId;
  const initialHost = mesh.shardCells.get(initialOwner)!.hostBrainNodeId;

  // 1. Dispatch from co-located brain node -> ZERO_HOP_COLOCATED_ISOLATE
  const res1 = mesh.routeScheduleRequest(initialHost, {
    requestId: "req-1",
    tenantId: "tenant-enterprise-a",
    taskId: "task-sched-101",
    payload: { cpu: 4 },
  });
  assert.equal(res1.status, "DISPATCHED");
  assert.equal(res1.envelope?.hopMode, "ZERO_HOP_COLOCATED_ISOLATE");
  assert.equal(res1.envelope?.targetShardId, initialOwner);
  assert.ok(res1.envelope?.cellObjectId.startsWith("do_sched_"));

  // 2. Begin online slot drain targeting new scale-out shard `scheduler-shard-02` on `brain-node-03`
  mesh.slotRing.beginSlotDrain(slotForTenantA, "scheduler-shard-02");

  // Requests arriving during DRAINING are safely micro-buffered (zero double-spending)
  const bufferedRes = mesh.routeScheduleRequest("brain-node-01", {
    requestId: "req-2-buffered",
    tenantId: "tenant-enterprise-a",
    taskId: "task-sched-102",
    payload: { cpu: 8 },
  });
  assert.equal(bufferedRes.status, "BUFFERED_DURING_DRAIN");

  // 3. Complete 3-phase online migration & flush micro-buffer into `scheduler-shard-02`
  const migration = mesh.migrateSlotOnlineAndFlushBuffer(
    slotForTenantA,
    "scheduler-shard-02",
    "brain-node-03",
  );
  assert.equal(migration.cutoverEntry.ownerShardId, "scheduler-shard-02");
  assert.equal(migration.flushedEnvelopes.length, 1);
  assert.equal(migration.flushedEnvelopes[0].targetShardId, "scheduler-shard-02");
  assert.equal(migration.flushedEnvelopes[0].bufferedDuringDrain, true);
  // Both task-sched-101 (transferred via snapshot) and task-sched-102 (flushed from buffer) exist on shard-02
  const newShard = mesh.shardCells.get("scheduler-shard-02")!;
  assert.equal(newShard.queuedTaskIds.has("task-sched-101"), true);
  assert.equal(newShard.queuedTaskIds.has("task-sched-102"), true);
});

test("test 70: [TASK-103 & TASK-104] HaPartitionedSchedulerMeshCoordinator crash failover SQLite RPO=0 rehydration (<3s RTO) and Stale-Route 409 self-healing", () => {
  const mesh = new HaPartitionedSchedulerMeshCoordinator([
    { shardId: "scheduler-shard-00", hostBrainNodeId: "brain-node-01" },
    { shardId: "scheduler-shard-01", hostBrainNodeId: "brain-node-02" },
  ]);

  // Seed state on `scheduler-shard-00`
  const slot = mesh.computeSha256VirtualSlot("tenant-ha-test");
  mesh.slotRing.slots.get(slot)!.ownerShardId = "scheduler-shard-00";
  mesh.syncRouterNodeL1Cache("brain-router-edge");

  mesh.routeScheduleRequest("brain-router-edge", {
    requestId: "req-ha-1",
    tenantId: "tenant-ha-test",
    taskId: "task-ha-persisted-1",
    payload: {},
  });

  // Simulate sudden crash of `scheduler-shard-00` -> failover to `scheduler-shard-standby-09`
  // Note: `brain-router-edge` still holds the OLD L1 cache pointing to `scheduler-shard-00` (epoch 1)
  const failover = mesh.failoverCrashedSchedulerShard(
    "scheduler-shard-00",
    "scheduler-shard-standby-09",
    "brain-node-09",
  );
  assert.equal(failover.rehydratedTasksCount, 1);
  assert.equal(failover.rehydratedTenantsCount, 1);
  assert.ok(failover.rehydrateRtoMs < 3000);

  // `brain-router-edge` sends next request with stale L1 cache -> automatically catches 409 StaleSlotEpochError,
  // refreshes L1 cache from L2 topology, and routes to `scheduler-shard-standby-09`
  const postFailoverRes = mesh.routeScheduleRequest("brain-router-edge", {
    requestId: "req-ha-2",
    tenantId: "tenant-ha-test",
    taskId: "task-ha-persisted-2",
    payload: {},
  });

  assert.equal(postFailoverRes.status, "DISPATCHED");
  assert.equal(postFailoverRes.envelope?.selfHealedFromStaleCache, true);
  assert.equal(postFailoverRes.envelope?.targetShardId, "scheduler-shard-standby-09");
  assert.equal(postFailoverRes.envelope?.slotEpoch, failover.newGeneration);

  const standbyShard = mesh.shardCells.get("scheduler-shard-standby-09")!;
  assert.equal(standbyShard.queuedTaskIds.has("task-ha-persisted-1"), true);
  assert.equal(standbyShard.queuedTaskIds.has("task-ha-persisted-2"), true);
});
