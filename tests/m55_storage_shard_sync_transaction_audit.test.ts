/**
 * @fileoverview M55 Deep Persistence, Shard Last-Brain Survival Guard, LayerFS Rule D9 Pull-State Sync,
 * Time-Travel CRC32 Verification, Velocity Breaker Recovery & RPO=0 Atomic Rollback Verification Suite.
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { InMemoryCellSqlStorage, InMemoryR2Bucket } from "../src/cloudflare/env.js";
import { SchedulerV25SingleWritePathStore } from "../src/cloudflare/scheduler_v25_persistence.js";
import { HaSchedulerShardCoordinator } from "../src/cloudflare/ha_scheduler_shard_coordinator.js";
import { LayerFsRemoteStoreSynchronizer } from "../src/checkpoint/layerfs_remote_sync.js";
import { SyntacticAndVelocityBreaker } from "../src/harness/velocity_breaker.js";
import { TurnWalEngine } from "../src/checkpoint/wal_engine.js";
import { DeterministicTimeTravelReplayer } from "../src/checkpoint/time_travel.js";
import { RpoZeroDurabilityEngine, DurableWorkflowContext } from "../src/cloudflare/celld_ha_engine.js";
import { TaskPriority, TaskSpec } from "../src/models.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M55: Deep Persistence, Shard Last-Brain Survival Guard, LayerFS Pull-to-Push Sync, Time-Travel CRC32 Guard, Velocity Reset & RPO=0 Atomic Rollback", () => {
  it("validates all 6 storage/HA/transactional invariants & live HTTP/CLI storage-ha-audit", async () => {
    // 1. [V25Persistence-CrossTaskIdem-1] Tenant-scoped cross-task idempotency lookup & full spec_json fidelity
    const store = new SchedulerV25SingleWritePathStore();
    const t1 = new TaskSpec({
      task_id: "task-m55-1",
      tenant_id: "Northstar",
      priority: TaskPriority.P0_EMERGENCY,
      prompt: "deploy-v25",
      idempotency_key: "idem-m55-deploy",
      dependencies: ["dep-precheck"],
      expected_SubAgent_count: 3,
    });
    const res1 = store.commitTaskSingleWritePath(t1, "body-host-01");
    assert.equal(res1.deduplicated, false);

    // Retry with same tenant & idempotency_key, but different auto-generated task_id
    const tRetry = new TaskSpec({
      task_id: "task-m55-retry-999",
      tenant_id: "Northstar",
      priority: TaskPriority.P0_EMERGENCY,
      prompt: "deploy-v25",
      idempotency_key: "idem-m55-deploy",
    });
    const resRetry = store.commitTaskSingleWritePath(tRetry, "body-host-02");
    assert.equal(resRetry.deduplicated, true);
    assert.equal(resRetry.task.task_id, "task-m55-1", "Must return original persisted task_id on cross-task idempotency hit");
    assert.deepEqual(resRetry.task.dependencies, ["dep-precheck"]);
    assert.equal(resRetry.task.expected_SubAgent_count, 3);

    // Different tenant using same idempotency_key string must not collide
    const tOtherTenant = new TaskSpec({
      task_id: "task-m55-helios",
      tenant_id: "Helios",
      priority: TaskPriority.P1_HIGH,
      prompt: "deploy-helios",
      idempotency_key: "idem-m55-deploy",
    });
    const resOther = store.commitTaskSingleWritePath(tOtherTenant, "body-host-01");
    assert.equal(resOther.deduplicated, false);

    // 2. [ShardCoordinator-LastBrainSurvival-2] Last surviving Brain shard is protected from eviction deadlock
    const shard = new HaSchedulerShardCoordinator(256, 8);
    shard.registerBrainShardNode("brain-01");
    shard.registerBrainShardNode("brain-02");
    const ev1 = shard.evictUnhealthyBrainShardNode("brain-02");
    assert.equal(ev1.lastBrainSurvivalGuardTriggered, false);
    assert.deepEqual(ev1.survivingBrains, ["brain-01"]);

    const evLast = shard.evictUnhealthyBrainShardNode("brain-01");
    assert.equal(evLast.lastBrainSurvivalGuardTriggered, true);
    assert.deepEqual(evLast.survivingBrains, ["brain-01"]);
    const resolved = shard.resolveBrainForTask("Northstar", "task-m55-1");
    assert.equal(resolved.brainNodeId, "brain-01");
    assert.ok(resolved.epoch >= 3);

    // 3. [LayerFsSync-PullToPush-3] pullStoreSnapshot updates remoteGeneration and lastKnownEtag for subsequent push
    const r2 = new InMemoryR2Bucket();
    const nodeA = new LayerFsRemoteStoreSynchronizer(r2, "proj-m55-unit");
    const nodeB = new LayerFsRemoteStoreSynchronizer(r2, "proj-m55-unit");

    await nodeA.pushStoreSnapshot({
      machineId: "machine-a",
      basedOnGeneration: 0,
      storeSqliteBytes: new Uint8Array([10, 20]),
      mainHeadCommitId: "C1",
    });

    const pulled = await nodeB.pullStoreSnapshot();
    assert.equal(pulled.remoteGeneration, 1);

    const pushed = await nodeB.pushStoreSnapshot({
      machineId: "machine-b",
      basedOnGeneration: pulled.remoteGeneration,
      storeSqliteBytes: new Uint8Array([10, 20, 30]),
      mainHeadCommitId: "C2",
    });
    assert.equal(pushed.remoteGeneration, 2);

    // 4. [VelocityBreaker-Reset-4] resetTaskBreaker clears tripped loop state
    const vb = new SyntacticAndVelocityBreaker(3, 120_000);
    const t0 = 9_000_000;
    vb.recordAndCheckTurn("task-vb", { turnIndex: 1, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 });
    vb.recordAndCheckTurn("task-vb", { turnIndex: 2, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 + 1 });
    const third = vb.recordAndCheckTurn("task-vb", { turnIndex: 3, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 + 2 });
    assert.equal(third.tripped, true);

    vb.resetTaskBreaker("task-vb", 3);
    const resumed = vb.recordAndCheckTurn("task-vb", { turnIndex: 3, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 + 3 });
    assert.equal(resumed.tripped, false);

    // 5. [TimeTravel-Crc32AndEpochDedup-5] Rejects corrupted WAL frame and deduplicates superseded epoch turns
    const wal = new TurnWalEngine();
    wal.appendTurnDelta({
      taskId: "task-tt-m55",
      turnIndex: 1,
      fencingToken: 1,
      changedFiles: ["old_attempt.ts"],
      sqliteWalPages: 2,
      highestSubmittedFencingToken: 1,
    });
    const latestEntry = wal.appendTurnDelta({
      taskId: "task-tt-m55",
      turnIndex: 1,
      fencingToken: 3,
      changedFiles: ["new_attempt.ts"],
      sqliteWalPages: 4,
      highestSubmittedFencingToken: 3,
    });

    const replayer = new DeterministicTimeTravelReplayer();
    const branch = replayer.forkTaskAtTurn(t1, wal.getTaskWalEntries("task-tt-m55"), 1, "task-tt-fork");
    assert.deepEqual(branch.inheritedFiles, ["new_attempt.ts"]);
    assert.equal(branch.inheritedFencingToken, 3);

    latestEntry.frame.crc32Hex = "deadbeef";
    assert.throws(
      () => replayer.forkTaskAtTurn(t1, [latestEntry], 1, "task-tt-corrupt"),
      /corrupted WAL frame/
    );

    // 6. [RpoZero-AtomicRollbackAndSingleFlight-6] Rolls back SQLite on LTX CAS conflict & coalesces concurrent workflow steps
    const sql = new InMemoryCellSqlStorage();
    sql.exec("CREATE TABLE IF NOT EXISTS test_rpo (id TEXT PRIMARY KEY, val TEXT)");
    const rpo = new RpoZeroDurabilityEngine(r2, sql);
    await r2.put("ltx/cell-rpo/tx-00000001-f2.ltx", new Uint8Array([99]));

    await assert.rejects(
      async () => {
        await rpo.commitWithSyncLtxFlush(
          "cell-rpo",
          2,
          () => {
            sql.exec("INSERT INTO test_rpo (id, val) VALUES (?, ?)", "row-1", "should-rollback");
          },
          new Uint8Array([1, 2]),
          { explicitTxId: 1, sql }
        );
      },
      /split-brain fenced/
    );
    assert.equal(sql.exec("SELECT * FROM test_rpo WHERE id = ?", "row-1").one(), null);

    const wf = new DurableWorkflowContext(sql, "wf-m55");
    let runs = 0;
    const [a, b] = await Promise.all([
      wf.stepDo("step-1", async () => {
        runs += 1;
        await new Promise((r) => setTimeout(r, 10));
        return { v: 42 };
      }),
      wf.stepDo("step-1", async () => {
        runs += 1;
        return { v: 99 };
      }),
    ]);
    assert.equal(runs, 1);
    assert.equal(a.result.v, 42);
    assert.equal(b.result.v, 42);

    // 7. Verify live HTTP endpoint & CLI `mgmt-storage-ha-audit`
    const server = new AnstsLocalAndComposeServer({ nodeId: "gw-m55-test" });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await runAnstsCtlRemote(["mgmt-storage-ha-audit"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);
      assert.equal(body.allM55RisksEliminated, true);
      assert.equal(body.v25PersistenceIdem1.crossTaskIdemAndTenantIsolationVerified, true);
      assert.equal(body.shardLastBrainSurvival2.lastBrainSurvivalVerified, true);
      assert.equal(body.layerfsPullToPushSync3.layerfsPullThenPushVerified, true);
      assert.equal(body.velocityBreakerReset4.velocityResetVerified, true);
      assert.equal(body.timeTravelCrcAndDedup5.timeTravelIntegrityAndDedupVerified, true);
      assert.equal(body.rpoZeroAndWorkflow6.rpoRollbackAndSingleFlightVerified, true);
    } finally {
      await server.stop();
    }
  });
});
