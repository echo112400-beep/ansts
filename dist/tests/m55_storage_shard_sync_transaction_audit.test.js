"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview M55 Deep Persistence, Shard Last-Brain Survival Guard, LayerFS Rule D9 Pull-State Sync,
 * Time-Travel CRC32 Verification, Velocity Breaker Recovery & RPO=0 Atomic Rollback Verification Suite.
 */
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const env_js_1 = require("../src/cloudflare/env.js");
const scheduler_v25_persistence_js_1 = require("../src/cloudflare/scheduler_v25_persistence.js");
const ha_scheduler_shard_coordinator_js_1 = require("../src/cloudflare/ha_scheduler_shard_coordinator.js");
const layerfs_remote_sync_js_1 = require("../src/checkpoint/layerfs_remote_sync.js");
const velocity_breaker_js_1 = require("../src/harness/velocity_breaker.js");
const wal_engine_js_1 = require("../src/checkpoint/wal_engine.js");
const time_travel_js_1 = require("../src/checkpoint/time_travel.js");
const celld_ha_engine_js_1 = require("../src/cloudflare/celld_ha_engine.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M55: Deep Persistence, Shard Last-Brain Survival Guard, LayerFS Pull-to-Push Sync, Time-Travel CRC32 Guard, Velocity Reset & RPO=0 Atomic Rollback", () => {
    (0, node_test_1.it)("validates all 6 storage/HA/transactional invariants & live HTTP/CLI storage-ha-audit", async () => {
        // 1. [V25Persistence-CrossTaskIdem-1] Tenant-scoped cross-task idempotency lookup & full spec_json fidelity
        const store = new scheduler_v25_persistence_js_1.SchedulerV25SingleWritePathStore();
        const t1 = new models_js_1.TaskSpec({
            task_id: "task-m55-1",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P0_EMERGENCY,
            prompt: "deploy-v25",
            idempotency_key: "idem-m55-deploy",
            dependencies: ["dep-precheck"],
            expected_SubAgent_count: 3,
        });
        const res1 = store.commitTaskSingleWritePath(t1, "body-host-01");
        strict_1.default.equal(res1.deduplicated, false);
        // Retry with same tenant & idempotency_key, but different auto-generated task_id
        const tRetry = new models_js_1.TaskSpec({
            task_id: "task-m55-retry-999",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P0_EMERGENCY,
            prompt: "deploy-v25",
            idempotency_key: "idem-m55-deploy",
        });
        const resRetry = store.commitTaskSingleWritePath(tRetry, "body-host-02");
        strict_1.default.equal(resRetry.deduplicated, true);
        strict_1.default.equal(resRetry.task.task_id, "task-m55-1", "Must return original persisted task_id on cross-task idempotency hit");
        strict_1.default.deepEqual(resRetry.task.dependencies, ["dep-precheck"]);
        strict_1.default.equal(resRetry.task.expected_SubAgent_count, 3);
        // Different tenant using same idempotency_key string must not collide
        const tOtherTenant = new models_js_1.TaskSpec({
            task_id: "task-m55-helios",
            tenant_id: "Helios",
            priority: models_js_1.TaskPriority.P1_HIGH,
            prompt: "deploy-helios",
            idempotency_key: "idem-m55-deploy",
        });
        const resOther = store.commitTaskSingleWritePath(tOtherTenant, "body-host-01");
        strict_1.default.equal(resOther.deduplicated, false);
        // 2. [ShardCoordinator-LastBrainSurvival-2] Last surviving Brain shard is protected from eviction deadlock
        const shard = new ha_scheduler_shard_coordinator_js_1.HaSchedulerShardCoordinator(256, 8);
        shard.registerBrainShardNode("brain-01");
        shard.registerBrainShardNode("brain-02");
        const ev1 = shard.evictUnhealthyBrainShardNode("brain-02");
        strict_1.default.equal(ev1.lastBrainSurvivalGuardTriggered, false);
        strict_1.default.deepEqual(ev1.survivingBrains, ["brain-01"]);
        const evLast = shard.evictUnhealthyBrainShardNode("brain-01");
        strict_1.default.equal(evLast.lastBrainSurvivalGuardTriggered, true);
        strict_1.default.deepEqual(evLast.survivingBrains, ["brain-01"]);
        const resolved = shard.resolveBrainForTask("Northstar", "task-m55-1");
        strict_1.default.equal(resolved.brainNodeId, "brain-01");
        strict_1.default.ok(resolved.epoch >= 3);
        // 3. [LayerFsSync-PullToPush-3] pullStoreSnapshot updates remoteGeneration and lastKnownEtag for subsequent push
        const r2 = new env_js_1.InMemoryR2Bucket();
        const nodeA = new layerfs_remote_sync_js_1.LayerFsRemoteStoreSynchronizer(r2, "proj-m55-unit");
        const nodeB = new layerfs_remote_sync_js_1.LayerFsRemoteStoreSynchronizer(r2, "proj-m55-unit");
        await nodeA.pushStoreSnapshot({
            machineId: "machine-a",
            basedOnGeneration: 0,
            storeSqliteBytes: new Uint8Array([10, 20]),
            mainHeadCommitId: "C1",
        });
        const pulled = await nodeB.pullStoreSnapshot();
        strict_1.default.equal(pulled.remoteGeneration, 1);
        const pushed = await nodeB.pushStoreSnapshot({
            machineId: "machine-b",
            basedOnGeneration: pulled.remoteGeneration,
            storeSqliteBytes: new Uint8Array([10, 20, 30]),
            mainHeadCommitId: "C2",
        });
        strict_1.default.equal(pushed.remoteGeneration, 2);
        // 4. [VelocityBreaker-Reset-4] resetTaskBreaker clears tripped loop state
        const vb = new velocity_breaker_js_1.SyntacticAndVelocityBreaker(3, 120_000);
        const t0 = 9_000_000;
        vb.recordAndCheckTurn("task-vb", { turnIndex: 1, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 });
        vb.recordAndCheckTurn("task-vb", { turnIndex: 2, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 + 1 });
        const third = vb.recordAndCheckTurn("task-vb", { turnIndex: 3, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 + 2 });
        strict_1.default.equal(third.tripped, true);
        vb.resetTaskBreaker("task-vb", 3);
        const resumed = vb.recordAndCheckTurn("task-vb", { turnIndex: 3, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 + 3 });
        strict_1.default.equal(resumed.tripped, false);
        // 5. [TimeTravel-Crc32AndEpochDedup-5] Rejects corrupted WAL frame and deduplicates superseded epoch turns
        const wal = new wal_engine_js_1.TurnWalEngine();
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
        const replayer = new time_travel_js_1.DeterministicTimeTravelReplayer();
        const branch = replayer.forkTaskAtTurn(t1, wal.getTaskWalEntries("task-tt-m55"), 1, "task-tt-fork");
        strict_1.default.deepEqual(branch.inheritedFiles, ["new_attempt.ts"]);
        strict_1.default.equal(branch.inheritedFencingToken, 3);
        latestEntry.frame.crc32Hex = "deadbeef";
        strict_1.default.throws(() => replayer.forkTaskAtTurn(t1, [latestEntry], 1, "task-tt-corrupt"), /corrupted WAL frame/);
        // 6. [RpoZero-AtomicRollbackAndSingleFlight-6] Rolls back SQLite on LTX CAS conflict & coalesces concurrent workflow steps
        const sql = new env_js_1.InMemoryCellSqlStorage();
        sql.exec("CREATE TABLE IF NOT EXISTS test_rpo (id TEXT PRIMARY KEY, val TEXT)");
        const rpo = new celld_ha_engine_js_1.RpoZeroDurabilityEngine(r2, sql);
        await r2.put("ltx/cell-rpo/tx-00000001-f2.ltx", new Uint8Array([99]));
        await strict_1.default.rejects(async () => {
            await rpo.commitWithSyncLtxFlush("cell-rpo", 2, () => {
                sql.exec("INSERT INTO test_rpo (id, val) VALUES (?, ?)", "row-1", "should-rollback");
            }, new Uint8Array([1, 2]), { explicitTxId: 1, sql });
        }, /split-brain fenced/);
        strict_1.default.equal(sql.exec("SELECT * FROM test_rpo WHERE id = ?", "row-1").one(), null);
        const wf = new celld_ha_engine_js_1.DurableWorkflowContext(sql, "wf-m55");
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
        strict_1.default.equal(runs, 1);
        strict_1.default.equal(a.result.v, 42);
        strict_1.default.equal(b.result.v, 42);
        // 7. Verify live HTTP endpoint & CLI `mgmt-storage-ha-audit`
        const server = new server_main_js_1.AnstsLocalAndComposeServer({ nodeId: "gw-m55-test" });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-storage-ha-audit"], baseUrl);
            strict_1.default.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            strict_1.default.equal(body.allM55RisksEliminated, true);
            strict_1.default.equal(body.v25PersistenceIdem1.crossTaskIdemAndTenantIsolationVerified, true);
            strict_1.default.equal(body.shardLastBrainSurvival2.lastBrainSurvivalVerified, true);
            strict_1.default.equal(body.layerfsPullToPushSync3.layerfsPullThenPushVerified, true);
            strict_1.default.equal(body.velocityBreakerReset4.velocityResetVerified, true);
            strict_1.default.equal(body.timeTravelCrcAndDedup5.timeTravelIntegrityAndDedupVerified, true);
            strict_1.default.equal(body.rpoZeroAndWorkflow6.rpoRollbackAndSingleFlightVerified, true);
        }
        finally {
            await server.stop();
        }
    });
});
