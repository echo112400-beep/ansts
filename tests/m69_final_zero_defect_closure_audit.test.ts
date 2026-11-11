/**
 * @fileoverview M69 E2E Verification Suite: Final Deep Edge-Case Sweep & Zero-Residual-Defect Closure Suite
 * Covers:
 * - [M69-1] `CognitiveRouter` Half-Open Single-Probe Stampede Lock (`tryAcquireRequestSlot`/`releaseHalfOpenProbe`) & Bounded `prefixRadixIndex` FIFO Eviction
 * - [M69-2] `AnstsSchedulerCell` & `UnifiedComponentPersistenceStore` Cold-Start Interrupted-Lease Node Capacity Refund & Active-Only `contentFingerprintIndex` Rehydration
 * - [M69-3] `DagEscrowCoordinator` Terminal-Parent Resurrection Guard in `registerChildSubAgent` & `completeChildSubAgent`
 * - [M69-4] `SyntacticAndVelocityBreaker` Non-Negative Finite `tokensConsumed` & Constructor Threshold Sanitization
 * - [M69-5] `AgentTaskHarnessCell` Foreign-TaskId R2 Snapshot Rejection + Positive Turn/Fence Guard + Turn Rollback Truncation & `HaSchedulerShardCoordinator` Per-Brain `shardEpochs` SQLite Persistence
 * - [M69-6] Management Plane Endpoint (`POST /v1beta1/management/system/final-closure-audit`) & CLI (`anstsctl mgmt-final-closure-audit`)
 */
import * as assert from "node:assert/strict";
import { describe, it } from "node:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { InMemoryCellSqlStorage, InMemoryR2Bucket } from "../src/cloudflare/env.js";
import { CognitiveRouter } from "../src/cognitive_router.js";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell.js";
import { UnifiedComponentPersistenceStore } from "../src/checkpoint/unified_state_store.js";
import { DagEscrowCoordinator } from "../src/dag/escrow.js";
import { SyntacticAndVelocityBreaker } from "../src/harness/velocity_breaker.js";
import { AgentTaskHarnessCell } from "../src/cloudflare/harness_cell.js";
import { HaSchedulerShardCoordinator } from "../src/cloudflare/ha_scheduler_shard_coordinator.js";
import { CoreScheduler5D } from "../src/scheduler.js";
import { LlmEndpointMetrics, NodeHeartbeat, TaskPriority, TaskSpec, TaskState } from "../src/models.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M69 Final Deep Edge-Case Sweep & Zero-Residual-Defect Closure Suite", () => {
  it("[M69-1] CognitiveRouter enforces Half-Open single-probe stampede lock on winning endpoint and bounds prefixRadixIndex", () => {
    const sql = new InMemoryCellSqlStorage();
    const router = new CognitiveRouter(0.3, sql, 3);
    router.registerEndpoint(
      new LlmEndpointMetrics({
        endpoint_id: "ep-halfopen-1",
        provider: "deepseek",
        model_name: "DeepSeek-R1",
        ttft_ewma_ms: 100,
      }),
      1000,
      2_000_000,
      1000
    );
    router.registerEndpoint(
      new LlmEndpointMetrics({
        endpoint_id: "ep-healthy-2",
        provider: "qwen",
        model_name: "Qwen-3.8",
        ttft_ewma_ms: 300,
      }),
      1000,
      2_000_000,
      1000
    );

    // Seed prefix affinity on ep-halfopen-1 first
    const [warmEp] = router.selectEndpoint("pfx-1", 100, TaskPriority.P1_HIGH, 900);
    router.releaseEndpoint(warmEp.endpoint_id);
    // Trip ep-halfopen-1 into OPEN at t=1000
    router.reportInvocationMetrics("ep-halfopen-1", 500, 100, true, 1000);
    router.reportInvocationMetrics("ep-halfopen-1", 500, 100, true, 1000);
    router.reportInvocationMetrics("ep-halfopen-1", 500, 100, true, 1000);
    assert.equal(router.getEndpointBreaker("ep-halfopen-1")?.getState(1000), "OPEN");

    // Advance past 15s cooldown (t=17000): first request enters HALF_OPEN on ep-halfopen-1 and locks the single-probe slot
    const [firstEp] = router.selectEndpoint("pfx-1", 500, TaskPriority.P1_HIGH, 17_000);
    assert.equal(firstEp.endpoint_id, "ep-halfopen-1");

    // Second concurrent request at t=17001 while first probe is still in-flight MUST NOT stampede ep-halfopen-1!
    const [secondEp] = router.selectEndpoint("pfx-1", 500, TaskPriority.P1_HIGH, 17_001);
    assert.equal(secondEp.endpoint_id, "ep-healthy-2");

    router.releaseEndpoint("ep-halfopen-1");
    router.releaseEndpoint("ep-healthy-2");

    // Bounded prefixRadixIndex (maxPrefixCacheEntries = 3)
    for (let i = 1; i <= 6; i++) {
      const [ep] = router.selectEndpoint(`pfx-cap-${i}`, 100, TaskPriority.P2_STANDARD, 20_000);
      router.releaseEndpoint(ep.endpoint_id);
    }
    assert.equal(router.getPrefixIndexSize(), 3);
  });

  it("[M69-2] AnstsSchedulerCell & UnifiedComponentPersistenceStore refund interrupted-lease node capacity on cold start and only index active task content fingerprints", async () => {
    const sql = new InMemoryCellSqlStorage();
    const makeCellState = () => ({
      id: { toString: () => "cell-m69-01" },
      storage: {
        sql,
        transactionSync: <T>(fn: () => T) => fn(),
        getAlarm: async () => null,
        setAlarm: async () => {},
      },
      blockConcurrencyWhile: async <T>(fn: () => Promise<T>) => fn(),
    });

    const cell1 = new AnstsSchedulerCell(makeCellState());
    cell1.registerNode(
      new NodeHeartbeat({
        node_id: "body-m69-01",
        total_cpu_cores: 8,
        free_cpu_cores: 8,
        total_memory_mb: 16384,
        free_memory_mb: 16384,
        running_tasks: 0,
      })
    );

    const t1 = new TaskSpec({
      task_id: "t-m69-done",
      tenant_id: "Northstar",
      prompt: "shared prompt",
    });
    await cell1.submitTask(t1, { mergeDuplicateInFlight: true });
    await cell1.alarm();
    cell1.scheduler.completeTaskExecution("t-m69-done", "body-m69-01", t1.lease_fencing_token);

    const t2 = new TaskSpec({
      task_id: "t-m69-leased",
      tenant_id: "Northstar",
      prompt: "shared prompt",
      resource_profile: { think_cpu_cores: 2, act_cpu_cores: 4, act_memory_mb: 4096, disk_mb: 10240 },
    });
    await cell1.submitTask(t2, { mergeDuplicateInFlight: true });
    await cell1.alarm();
    // Persist the node's reduced capacity while t-m69-leased is LEASED, simulating a crash right after placement
    cell1.registerNode(cell1.scheduler.nodes.get("body-m69-01")!);
    assert.equal(cell1.scheduler.nodes.get("body-m69-01")?.running_tasks, 1);
    assert.equal(cell1.scheduler.nodes.get("body-m69-01")?.free_cpu_cores, 6);

    // Cold-start cell2 over the same SQLite: t-m69-leased is reset to QUEUED, so body-m69-01 must have its 2 CPU cores & running_tasks refunded!
    const cell2 = new AnstsSchedulerCell(makeCellState());
    const rehydratedNode = cell2.scheduler.nodes.get("body-m69-01")!;
    assert.equal(rehydratedNode.running_tasks, 0);
    assert.equal(rehydratedNode.free_cpu_cores, 8);
    // And contentFingerprintIndex must point to active t-m69-leased (never completed t-m69-done)
    assert.equal(
      cell2.scheduler.contentFingerprintIndex.get(t2.content_fingerprint),
      "t-m69-leased"
    );

    // Also verify UnifiedComponentPersistenceStore active-only fingerprint & node capacity refund
    const uSql = new InMemoryCellSqlStorage();
    const uStore = new UnifiedComponentPersistenceStore(uSql);
    uStore.snapshotSchedulerState(cell1.scheduler);
    const sRestored = new CoreScheduler5D();
    sRestored.registerOrUpdateNode(
      new NodeHeartbeat({
        node_id: "body-m69-01",
        total_cpu_cores: 8,
        free_cpu_cores: 6,
        total_memory_mb: 16384,
        free_memory_mb: 15360,
        running_tasks: 1,
      })
    );
    uStore.rehydrateSchedulerFromSql(sRestored, { requeueInterruptedLeases: true });
    assert.equal(sRestored.nodes.get("body-m69-01")?.running_tasks, 0);
    assert.equal(sRestored.nodes.get("body-m69-01")?.free_cpu_cores, 8);
    assert.equal(sRestored.contentFingerprintIndex.get(t2.content_fingerprint), "t-m69-leased");
  });

  it("[M69-3] DagEscrowCoordinator blocks registering children under a terminal parent and never resurrects a dead-lettered parent on completeChildSubAgent", () => {
    const escrow = new DagEscrowCoordinator();
    const parent = new TaskSpec({
      task_id: "dag-p-m69",
      tenant_id: "Northstar",
      expected_SubAgent_count: 2,
    });
    const c1 = new TaskSpec({
      task_id: "dag-c1-m69",
      tenant_id: "Northstar",
      parent_task_id: "dag-p-m69",
    });
    const c2 = new TaskSpec({
      task_id: "dag-c2-m69",
      tenant_id: "Northstar",
      parent_task_id: "dag-p-m69",
    });
    escrow.reserveParentEscrow(parent, 10, 0);
    escrow.registerChildSubAgent(parent, c1);
    escrow.registerChildSubAgent(parent, c2);

    // c1 fails and aborts parent -> parent enters DEAD_LETTER
    escrow.failOrCancelChildSubAgent(parent, "dag-c1-m69", true);
    assert.equal(parent.state, TaskState.DEAD_LETTER);

    // Late completion of sibling c2 must NOT resurrect parent from DEAD_LETTER to RUNNING_THINK!
    escrow.completeChildSubAgent(parent, "dag-c2-m69");
    assert.equal(parent.state, TaskState.DEAD_LETTER);

    // Attempting to register a new child under dead-lettered parent must throw
    const c3 = new TaskSpec({
      task_id: "dag-c3-m69",
      tenant_id: "Northstar",
      parent_task_id: "dag-p-m69",
    });
    assert.throws(() => escrow.registerChildSubAgent(parent, c3), /terminal state/);
  });

  it("[M69-4] SyntacticAndVelocityBreaker sanitizes NaN/negative tokensConsumed and invalid constructor thresholds", () => {
    const breaker = new SyntacticAndVelocityBreaker(-2, Number.NaN);
    assert.equal(breaker.maxIdenticalConsecutiveCalls, 3);
    assert.equal(breaker.maxTokensPerMinutePerTask, 120_000);

    // Inject negative & NaN token diagnostics followed by a 125,000 token burst: negative/NaN must be clamped to 0 so velocity breaker still trips!
    breaker.recordAndCheckTurn("t-vel-m69", {
      turnIndex: 1,
      toolName: "t1",
      argsHash: "h1",
      tokensConsumed: -999_999,
      timestampMs: 10_000,
    });
    breaker.recordAndCheckTurn("t-vel-m69", {
      turnIndex: 2,
      toolName: "t2",
      argsHash: "h2",
      tokensConsumed: Number.NaN,
      timestampMs: 11_000,
    });
    const check = breaker.recordAndCheckTurn("t-vel-m69", {
      turnIndex: 3,
      toolName: "t3",
      argsHash: "h3",
      tokensConsumed: 125_000,
      timestampMs: 12_000,
    });
    assert.equal(check.tripped, true);
    assert.equal(check.reason, "TOKEN_VELOCITY_EXCEEDED");
  });

  it("[M69-5] AgentTaskHarnessCell rejects foreign taskId R2 snapshots, validates positive turn/fence, supports truncateTurnsFrom, and HaSchedulerShardCoordinator persists shardEpochs in SQLite", async () => {
    const r2 = new InMemoryR2Bucket();
    const hSql = new InMemoryCellSqlStorage();
    const hState = {
      id: { toString: () => "hcell-m69" },
      storage: {
        sql: hSql,
        transactionSync: <T>(fn: () => T) => fn(),
        getAlarm: async () => null,
        setAlarm: async () => {},
      },
      blockConcurrencyWhile: async <T>(fn: () => Promise<T>) => fn(),
    };
    const hCell = new AgentTaskHarnessCell(hState, r2, "task-m69-owner");
    await assert.rejects(
      hCell.executeAndCheckpointTurn({
        turnIndex: 0,
        fencingToken: 1,
        toolName: "bash",
        toolOutput: {},
      }),
      /Invalid turnIndex/
    );

    await hCell.executeAndCheckpointTurn({
      turnIndex: 1,
      fencingToken: 2,
      toolName: "bash",
      toolOutput: { step: 1 },
    });
    await hCell.executeAndCheckpointTurn({
      turnIndex: 2,
      fencingToken: 2,
      toolName: "bash",
      toolOutput: { step: 2 },
    });

    // Inject a polluted R2 object under cells/task-m69-owner/ belonging to a foreign taskId
    await r2.put(
      "cells/task-m69-owner/turn-99-f99.ltx",
      JSON.stringify({
        taskId: "foreign-task-id",
        turnIndex: 99,
        fencingToken: 99,
        toolName: "spoof",
        toolOutput: {},
      })
    );
    const rec = await hCell.reconcileBreakpointFromR2AndSql();
    assert.equal(rec.reconciledTurnsCount, 0);
    assert.equal(rec.highestTurnIndex, 2);

    // Rollback/truncate turn >= 2 under fencingToken 3
    const truncated = hCell.truncateTurnsFrom(2, 3);
    assert.equal(truncated, 1);
    assert.equal(hCell.getTurnHistory().length, 1);
    assert.equal(hCell.getHighestFencingToken(), 3);

    // HaSchedulerShardCoordinator persists per-Brain shardEpochs across restarts
    const shardSql = new InMemoryCellSqlStorage();
    const coord1 = new HaSchedulerShardCoordinator(256, 8, shardSql);
    coord1.registerBrainShardNode("brain-01");
    coord1.registerBrainShardNode("brain-01"); // bumps brain-01 epoch to 2
    coord1.evictUnhealthyBrainShardNode("brain-01"); // Last-Brain guard bumps brain-01 epoch to 3
    assert.equal(coord1.resolveBrainForTask("Northstar", "t1").epoch, 3);

    const coord2 = new HaSchedulerShardCoordinator(256, 8, shardSql);
    assert.equal(coord2.resolveBrainForTask("Northstar", "t1").epoch, 3);
  });

  it("[M69-6] Live server exposes POST /v1beta1/management/system/final-closure-audit & CLI anstsctl mgmt-final-closure-audit", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m69-live-"));
    const server = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
      tickIntervalMs: 60_000,
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await runAnstsCtlRemote(["mgmt-final-closure-audit"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout) as Record<string, unknown>;
      assert.equal(body.allM69FinalClosureChecksPassed, true);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
