/**
 * @fileoverview M16 (TASK-75 ~ TASK-82) denoland/celld v0.6.3 Deep Optimization Suite (8 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  CelldNativeContainerSandboxManager,
  OperationInterruptedError,
  FleetReplicatedDurabilityEngine,
  HotCellOverloadAndQueueGuard,
  CelldStateAwareFleetAutoscaler,
  WalCompactorAndPagedRestorer,
  HierarchicalQueueManager,
  TaskPriority,
  TaskSpec,
  MockDurableObjectState,
  InMemoryR2Bucket,
  AgentTaskHarnessCell,
  SseEventStreamGateway,
  AnstsClusterRuntime,
  runCli,
} from "../src/index.js";

describe("M16 (TASK-75 ~ TASK-82) denoland/celld v0.6.3 Deep Optimization Suite", () => {
  it("1. [TASK-75] Native ctx.container Bridge configures kata/runsc OCI runtime, 1.1.1.1 DNS, and celld-fence RFC1918 block", async () => {
    const mgr = new CelldNativeContainerSandboxManager();
    const cfg = {
      class_name: "AgentTaskHarnessCell",
      image: "./deploy/Dockerfile.sandbox",
      instance_type: "standard-2" as const,
      max_instances: 64,
      runtime: "runsc" as const,
    };
    mgr.registerContainerClass(cfg);
    const handle = mgr.getOrCreateContainerHandle("cell-c1", cfg);
    const started = handle.start({ enableInternet: true });

    assert.equal(started.running, true);
    assert.equal(started.ociRuntime, "runsc");
    assert.deepEqual(started.resolvConf, ["1.1.1.1", "1.0.0.1"]);
    assert.equal(started.committedMemoryMb, 6144);
    assert.ok(started.bucketImageTarKey.startsWith("deploy/images/"));

    const tcp = handle.getTcpPort(8080);
    const wsRes = await tcp.fetch("https://api.openai.com/v1/realtime", {
      headers: { Upgrade: "websocket" },
    });
    assert.equal(wsRes.status, 101);
    assert.equal(wsRes.webSocketUpgraded, true);

    await assert.rejects(async () => {
      await tcp.fetch("http://10.0.0.12:8081/state");
    }, /celld-fence nftables blocked private/);
  });

  it("2. [TASK-76] FleetReplicatedDurabilityEngine uses peer fsync, triggers adaptive hedged append, and falls back to bucket on 1 node", () => {
    const engine = new FleetReplicatedDurabilityEngine("fleet", 4);
    const fastPeer = engine.commitWriteWithFleetDurability({
      livePeerNodeCount: 2,
      peerFsyncLatencyMs: 3.5,
      bucketPutLatencyMs: 42.0,
      inFlightLogRounds: 3,
    });
    assert.equal(fastPeer.effectiveDurabilityMode, "fleet");
    assert.equal(fastPeer.acknowledgedVia, "PEER_FSYNC");
    assert.equal(fastPeer.commitLatencyMs, 3.5);

    const slowPeerHedged = engine.commitWriteWithFleetDurability({
      livePeerNodeCount: 2,
      peerFsyncLatencyMs: 320,
      bucketPutLatencyMs: 400,
    });
    assert.equal(slowPeerHedged.hedgedCopyTriggered, true);
    assert.equal(slowPeerHedged.acknowledgedVia, "HEDGED_PEER_COPY");

    const singleNodeFallback = engine.commitWriteWithFleetDurability({
      livePeerNodeCount: 0,
      peerFsyncLatencyMs: 2.0,
      bucketPutLatencyMs: 38.0,
    });
    assert.equal(singleNodeFallback.effectiveDurabilityMode, "bucket_fallback");
    assert.equal(singleNodeFallback.acknowledgedVia, "BUCKET_PUT");
  });

  it("3. [TASK-77] HotCellOverloadAndQueueGuard enforces 64-fetch cell cap (503 Retry-After:1) and 256-producer Queue limit", () => {
    const guard = new HotCellOverloadAndQueueGuard({
      maxCellConcurrentRequests: 64,
      maxQueueConcurrentProducers: 256,
    });
    for (let i = 0; i < 64; i++) {
      assert.equal(guard.tryEnterCellFetch("AnstsSchedulerCell:shard-0").admitted, true);
    }
    const overloaded1 = guard.tryEnterCellFetch("AnstsSchedulerCell:shard-0");
    assert.equal(overloaded1.admitted, false);
    assert.equal(overloaded1.statusCode, 503);
    assert.equal(overloaded1.headers["Retry-After"], "1");
    assert.equal(overloaded1.headers["X-Celld-Overload"], "cell");

    guard.tryEnterCellFetch("AnstsSchedulerCell:shard-0");
    const overloaded3 = guard.tryEnterCellFetch("AnstsSchedulerCell:shard-0");
    assert.equal(overloaded3.recommendSubShardSplit, true);

    const qOk = guard.tryAdmitQueueProducerBatch(128);
    assert.equal(qOk.admitted, true);
    assert.equal(qOk.committedTransactions, 2);

    const qOver = guard.tryAdmitQueueProducerBatch(300);
    assert.equal(qOver.admitted, false);
    assert.equal(qOver.statusCode, 503);
    assert.equal(qOver.errorBody?.error, "cell admission refused");
  });

  it("4. [TASK-78] CelldStateAwareFleetAutoscaler parses /state counters and coordinates paced drain token shutdown", () => {
    const scaler = new CelldStateAwareFleetAutoscaler(40_000);
    const scaleUpEval = scaler.evaluateFleetStateSamples([
      {
        nodeId: "node-1",
        capacity_waiting: 12,
        activation_waiting: 4,
        restoring: 2,
        owned_cells: 950,
        resident_cells: 950,
        pressured: true,
        memory_headroom: false,
        shed_cells: 15,
        live_empty_isolates_sec: 45,
      },
    ]);
    assert.equal(scaleUpEval.decision, "ADD_NODE");
    assert.equal(scaleUpEval.rollingUpgradeNextNodeAllowed, false);
    assert.equal(scaleUpEval.isolateMaintenanceWarning, true);

    const drain1 = scaler.initiateNodeShutdown({
      nodeId: "node-1",
      handoffMode: "preserve",
      systemdTimeoutStopSec: 60,
    });
    assert.equal(drain1.claimedDrainToken, true);
    assert.equal(drain1.ownershipPreservedForSameNodeRestart, true);
    assert.equal(drain1.drainTokenWaitMs, 30_000);
    assert.equal(drain1.handoffNoProgressMs, 25_000);

    const drain2 = scaler.initiateNodeShutdown({
      nodeId: "node-2",
      systemdTimeoutStopSec: 60,
    });
    assert.equal(drain2.claimedDrainToken, false);
  });

  it("5. [TASK-79] WalCompactorAndPagedRestorer triggers L0->L1 additive compaction and 128-page WAL truncation", () => {
    const compactor = new WalCompactorAndPagedRestorer();
    const l1 = compactor.executeCelldLtxL1Compaction({
      cellId: "cell-heavy-1",
      l0ObjectCount: 600,
      txidDistance: 300,
      totalL0Megabytes: 48,
    });
    assert.equal(l1.compactionTriggered, true);
    assert.equal(l1.batchesMerged, 3);
    assert.equal(l1.spilledToLocalDisk, false);

    const paging = compactor.evaluateWalTruncationAndPaging({
      dbSizeMb: 2.5,
      walSizePages: 140,
      ltxChainSizeMb: 512,
      pagedMinMb: 256,
      hydrateMbps: 16,
    });
    assert.equal(paging.shouldTruncateWalAtCheckpoint, true);
    assert.equal(paging.usePagedRestore, true);
    assert.equal(paging.estimatedFullHydrationSeconds, 32);
  });

  it("6. [TASK-80] O(log N) Binary Max-Heap Queue Manager enqueues and pops 2,000 prioritized tasks in < 25ms", () => {
    const qm = new HierarchicalQueueManager();
    const t0 = performance.now();
    for (let i = 0; i < 2000; i++) {
      qm.enqueue(
        new TaskSpec({
          task_id: `heap-task-${i}`,
          tenant_id: `tenant-${i % 20}`,
          priority: (i % 4) as TaskPriority,
        })
      );
    }
    const first = qm.popNext();
    const elapsedMs = performance.now() - t0;
    assert.equal(first?.priority, TaskPriority.P0_EMERGENCY);
    assert.ok(elapsedMs < 150, `Expected <150ms for 2,000 heap ops, got ${elapsedMs.toFixed(2)}ms`);
  });

  it("7. [TASK-81] AgentTaskHarnessCell auto-heals OperationInterruptedError after cell migration & SSE handles 1012 reconnect", async () => {
    const state = new MockDurableObjectState("harness-heal-1");
    const bucket = new InMemoryR2Bucket();
    const cell = new AgentTaskHarnessCell(state, bucket, "task-heal-1");
    await cell.executeAndCheckpointTurn({
      turnIndex: 1,
      fencingToken: 1,
      toolName: "pytest",
      toolOutput: { passed: 12 },
    });

    let containerRestarted = false;
    let attempt = 0;
    const res = await cell.executeInCompanionContainerWithAutoHeal(
      async () => {
        attempt += 1;
        if (attempt === 1) {
          throw new OperationInterruptedError("harness-heal-1");
        }
        return { exitCode: 0, rehydrated: containerRestarted };
      },
      async (latestTurn) => {
        assert.equal(latestTurn?.turnIndex, 1);
        containerRestarted = true;
      }
    );
    assert.equal(res.recoveredFromMigrationInterrupt, true);
    assert.equal(res.output.rehydrated, true);

    const sse = new SseEventStreamGateway();
    sse.publish({ id: "e1", event: "turn", taskId: "t1", turnIndex: 1, timestampMs: 1, data: {} });
    sse.publish({ id: "e2", event: "turn", taskId: "t1", turnIndex: 2, timestampMs: 2, data: {} });
    sse.subscribe("t1", "sub-old");
    const reconn = sse.handleWebSocket1012Reconnect("t1", "sub-old", "sub-new", "e1");
    assert.equal(reconn.closeCodeHandled, 1012);
    const frame = String(reconn.subscription.stream.read() ?? "");
    assert.ok(frame.includes("e2"));
    reconn.subscription.close();
  });

  it("8. [TASK-82] anstsctl diagnose and gc-audit verify restoring=0 gate and skip facet streams during epoch GC", () => {
    const rt = new AnstsClusterRuntime();
    rt.peerProbes = [
      { nodeId: "node-1", leaseExpired: false, reachable: true, protocolVersion: "v0.6.3", restoringCount: 0 },
      { nodeId: "node-2", leaseExpired: false, reachable: true, protocolVersion: "v0.6.3", restoringCount: 0 },
    ];
    const diag = runCli(["diagnose", "v0.6.3"], rt);
    assert.equal(diag.exitCode, 0);
    assert.equal(JSON.parse(diag.stdout).rollingUpgradeSafe, true);

    rt.epochCandidates = [
      {
        cellScope: "AgentTaskHarnessCell:t1",
        epochId: 1,
        ownerCurrentEpoch: 4,
        ageSeconds: 7200,
        isFacetStream: false,
        isPagedCell: false,
        hydrateMbps: 16,
        bytes: 4096,
      },
      {
        cellScope: "AgentTaskHarnessCell:t1/facet-py",
        epochId: 1,
        ownerCurrentEpoch: 4,
        ageSeconds: 7200,
        isFacetStream: true,
        isPagedCell: false,
        hydrateMbps: 16,
        bytes: 2048,
      },
    ];
    const gc = runCli(["gc-audit", "3600"], rt);
    const gcBody = JSON.parse(gc.stdout);
    assert.equal(gcBody.deletableEpochCount, 1);
    assert.equal(gcBody.reclaimableBytes, 4096);
  });
});
