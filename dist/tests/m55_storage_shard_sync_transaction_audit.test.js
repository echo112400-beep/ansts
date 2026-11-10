"use strict";



var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var _env = require("../src/cloudflare/env.js");
var _scheduler_v25_persistence = require("../src/cloudflare/scheduler_v25_persistence.js");
var _ha_scheduler_shard_coordinator = require("../src/cloudflare/ha_scheduler_shard_coordinator.js");
var _layerfs_remote_sync = require("../src/checkpoint/layerfs_remote_sync.js");
var _velocity_breaker = require("../src/harness/velocity_breaker.js");
var _wal_engine = require("../src/checkpoint/wal_engine.js");
var _time_travel = require("../src/checkpoint/time_travel.js");
var _celld_ha_engine = require("../src/cloudflare/celld_ha_engine.js");
var _models = require("../src/models.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M55 Deep Persistence, Shard Last-Brain Survival Guard, LayerFS Rule D9 Pull-State Sync,
 * Time-Travel CRC32 Verification, Velocity Breaker Recovery & RPO=0 Atomic Rollback Verification Suite.
 */(0, _nodeTest.describe)("M55: Deep Persistence, Shard Last-Brain Survival Guard, LayerFS Pull-to-Push Sync, Time-Travel CRC32 Guard, Velocity Reset & RPO=0 Atomic Rollback", () => {(0, _nodeTest.it)("validates all 6 storage/HA/transactional invariants & live HTTP/CLI storage-ha-audit", async () => {
    // 1. [V25Persistence-CrossTaskIdem-1] Tenant-scoped cross-task idempotency lookup & full spec_json fidelity
    const store = new _scheduler_v25_persistence.SchedulerV25SingleWritePathStore();
    const t1 = new _models.TaskSpec({
      task_id: "task-m55-1",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P0_EMERGENCY,
      prompt: "deploy-v25",
      idempotency_key: "idem-m55-deploy",
      dependencies: ["dep-precheck"],
      expected_SubAgent_count: 3
    });
    const res1 = store.commitTaskSingleWritePath(t1, "body-host-01");
    _strict.default.equal(res1.deduplicated, false);

    // Retry with same tenant & idempotency_key, but different auto-generated task_id
    const tRetry = new _models.TaskSpec({
      task_id: "task-m55-retry-999",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P0_EMERGENCY,
      prompt: "deploy-v25",
      idempotency_key: "idem-m55-deploy"
    });
    const resRetry = store.commitTaskSingleWritePath(tRetry, "body-host-02");
    _strict.default.equal(resRetry.deduplicated, true);
    _strict.default.equal(resRetry.task.task_id, "task-m55-1", "Must return original persisted task_id on cross-task idempotency hit");
    _strict.default.deepEqual(resRetry.task.dependencies, ["dep-precheck"]);
    _strict.default.equal(resRetry.task.expected_SubAgent_count, 3);

    // Different tenant using same idempotency_key string must not collide
    const tOtherTenant = new _models.TaskSpec({
      task_id: "task-m55-helios",
      tenant_id: "Helios",
      priority: _models.TaskPriority.P1_HIGH,
      prompt: "deploy-helios",
      idempotency_key: "idem-m55-deploy"
    });
    const resOther = store.commitTaskSingleWritePath(tOtherTenant, "body-host-01");
    _strict.default.equal(resOther.deduplicated, false);

    // 2. [ShardCoordinator-LastBrainSurvival-2] Last surviving Brain shard is protected from eviction deadlock
    const shard = new _ha_scheduler_shard_coordinator.HaSchedulerShardCoordinator(256, 8);
    shard.registerBrainShardNode("brain-01");
    shard.registerBrainShardNode("brain-02");
    const ev1 = shard.evictUnhealthyBrainShardNode("brain-02");
    _strict.default.equal(ev1.lastBrainSurvivalGuardTriggered, false);
    _strict.default.deepEqual(ev1.survivingBrains, ["brain-01"]);

    const evLast = shard.evictUnhealthyBrainShardNode("brain-01");
    _strict.default.equal(evLast.lastBrainSurvivalGuardTriggered, true);
    _strict.default.deepEqual(evLast.survivingBrains, ["brain-01"]);
    const resolved = shard.resolveBrainForTask("Northstar", "task-m55-1");
    _strict.default.equal(resolved.brainNodeId, "brain-01");
    _strict.default.ok(resolved.epoch >= 3);

    // 3. [LayerFsSync-PullToPush-3] pullStoreSnapshot updates remoteGeneration and lastKnownEtag for subsequent push
    const r2 = new _env.InMemoryR2Bucket();
    const nodeA = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(r2, "proj-m55-unit");
    const nodeB = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(r2, "proj-m55-unit");

    await nodeA.pushStoreSnapshot({
      machineId: "machine-a",
      basedOnGeneration: 0,
      storeSqliteBytes: new Uint8Array([10, 20]),
      mainHeadCommitId: "C1"
    });

    const pulled = await nodeB.pullStoreSnapshot();
    _strict.default.equal(pulled.remoteGeneration, 1);

    const pushed = await nodeB.pushStoreSnapshot({
      machineId: "machine-b",
      basedOnGeneration: pulled.remoteGeneration,
      storeSqliteBytes: new Uint8Array([10, 20, 30]),
      mainHeadCommitId: "C2"
    });
    _strict.default.equal(pushed.remoteGeneration, 2);

    // 4. [VelocityBreaker-Reset-4] resetTaskBreaker clears tripped loop state
    const vb = new _velocity_breaker.SyntacticAndVelocityBreaker(3, 120_000);
    const t0 = 9_000_000;
    vb.recordAndCheckTurn("task-vb", { turnIndex: 1, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 });
    vb.recordAndCheckTurn("task-vb", { turnIndex: 2, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 + 1 });
    const third = vb.recordAndCheckTurn("task-vb", { turnIndex: 3, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 + 2 });
    _strict.default.equal(third.tripped, true);

    vb.resetTaskBreaker("task-vb", 3);
    const resumed = vb.recordAndCheckTurn("task-vb", { turnIndex: 3, toolName: "bash", argsHash: "same", tokensConsumed: 100, timestampMs: t0 + 3 });
    _strict.default.equal(resumed.tripped, false);

    // 5. [TimeTravel-Crc32AndEpochDedup-5] Rejects corrupted WAL frame and deduplicates superseded epoch turns
    const wal = new _wal_engine.TurnWalEngine();
    wal.appendTurnDelta({
      taskId: "task-tt-m55",
      turnIndex: 1,
      fencingToken: 1,
      changedFiles: ["old_attempt.ts"],
      sqliteWalPages: 2,
      highestSubmittedFencingToken: 1
    });
    const latestEntry = wal.appendTurnDelta({
      taskId: "task-tt-m55",
      turnIndex: 1,
      fencingToken: 3,
      changedFiles: ["new_attempt.ts"],
      sqliteWalPages: 4,
      highestSubmittedFencingToken: 3
    });

    const replayer = new _time_travel.DeterministicTimeTravelReplayer();
    const branch = replayer.forkTaskAtTurn(t1, wal.getTaskWalEntries("task-tt-m55"), 1, "task-tt-fork");
    _strict.default.deepEqual(branch.inheritedFiles, ["new_attempt.ts"]);
    _strict.default.equal(branch.inheritedFencingToken, 3);

    latestEntry.frame.crc32Hex = "deadbeef";
    _strict.default.throws(
    () => replayer.forkTaskAtTurn(t1, [latestEntry], 1, "task-tt-corrupt"),
    /corrupted WAL frame/);


    // 6. [RpoZero-AtomicRollbackAndSingleFlight-6] Rolls back SQLite on LTX CAS conflict & coalesces concurrent workflow steps
    const sql = new _env.InMemoryCellSqlStorage();
    sql.exec("CREATE TABLE IF NOT EXISTS test_rpo (id TEXT PRIMARY KEY, val TEXT)");
    const rpo = new _celld_ha_engine.RpoZeroDurabilityEngine(r2, sql);
    await r2.put("ltx/cell-rpo/tx-00000001-f2.ltx", new Uint8Array([99]));

    await _strict.default.rejects(
    async () => {
      await rpo.commitWithSyncLtxFlush(
      "cell-rpo",
      2,
      () => {
        sql.exec("INSERT INTO test_rpo (id, val) VALUES (?, ?)", "row-1", "should-rollback");
      },
      new Uint8Array([1, 2]),
      { explicitTxId: 1, sql });

    },
    /split-brain fenced/);

    _strict.default.equal(sql.exec("SELECT * FROM test_rpo WHERE id = ?", "row-1").one(), null);

    const wf = new _celld_ha_engine.DurableWorkflowContext(sql, "wf-m55");
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
    })]);

    _strict.default.equal(runs, 1);
    _strict.default.equal(a.result.v, 42);
    _strict.default.equal(b.result.v, 42);

    // 7. Verify live HTTP endpoint & CLI `mgmt-storage-ha-audit`
    const server = new _server_main.AnstsLocalAndComposeServer({ nodeId: "gw-m55-test" });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-storage-ha-audit"], baseUrl);
      _strict.default.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);
      _strict.default.equal(body.allM55RisksEliminated, true);
      _strict.default.equal(body.v25PersistenceIdem1.crossTaskIdemAndTenantIsolationVerified, true);
      _strict.default.equal(body.shardLastBrainSurvival2.lastBrainSurvivalVerified, true);
      _strict.default.equal(body.layerfsPullToPushSync3.layerfsPullThenPushVerified, true);
      _strict.default.equal(body.velocityBreakerReset4.velocityResetVerified, true);
      _strict.default.equal(body.timeTravelCrcAndDedup5.timeTravelIntegrityAndDedupVerified, true);
      _strict.default.equal(body.rpoZeroAndWorkflow6.rpoRollbackAndSingleFlightVerified, true);
    } finally {
      await server.stop();
    }
  });
});