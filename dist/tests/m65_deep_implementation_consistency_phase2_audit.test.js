"use strict";








var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _worker = _interopRequireWildcard(require("../src/cloudflare/worker.js"));




var _env = require("../src/cloudflare/env.js");







var _wal_engine = require("../src/checkpoint/wal_engine.js");
var _compactor = require("../src/checkpoint/compactor.js");
var _layerfs_remote_sync = require("../src/checkpoint/layerfs_remote_sync.js");
var _canary_rollout_controller = require("../src/cluster/canary_rollout_controller.js");
var _predictive_prewarmer = require("../src/runtime/predictive_prewarmer.js");
var _profiler = require("../src/profiler.js");
var _models = require("../src/models.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M65 Deep Implementation Consistency Phase II Verification Suite.
 * Verifies:
 * 1. [M65-1] `WalCompactorAndPagedRestorer` & `LayerFsRemoteStoreSynchronizer` constructor contract & SQLite persistence across restarts.
 * 2. [M65-2] `CanaryRolloutController` (post-rollback lock), `PredictiveSandboxPrewarmer` & `TaskResourceProfiler` SQLite rehydration.
 * 3. [M65-3] `AnstsLocalAndComposeServer` live route state-mutation persistence sync across `/v1beta1/harness/run-turn`, `/v1beta1/saga/*` & `/v1beta1/dag/escrow`.
 * 4. [M65-4] `TunnelGatewayCell` DO `fetch` API & Cloudflare Worker `/v1/tunnel/*` routing.
 * 5. [M65-5 & M65-6] Live HTTP `POST /v1beta1/management/system/deep-consistency-phase2-audit` & CLI `anstsctl mgmt-consistency-phase2-audit`.
 */(0, _nodeTest.describe)("M65 Deep Implementation Consistency Phase II Suite", () => {(0, _nodeTest.it)("[M65-1] Persists WalCompactorAndPagedRestorer snapshots & LayerFsRemoteStoreSynchronizer Rule D9 state in SQLite across restarts", async () => {const sql = new _env.InMemoryCellSqlStorage();const wal = new _wal_engine.TurnWalEngine(sql);wal.appendTurnDelta({ taskId: "t-comp-m65", turnIndex: 1,
      fencingToken: 2,
      changedFiles: ["a.ts", "b.ts", "c.ts"],
      sqliteWalPages: 40,
      highestSubmittedFencingToken: 2
    });
    const comp1 = new _compactor.WalCompactorAndPagedRestorer(4096, sql);
    const snap = comp1.compactLayers("t-comp-m65", wal.getTaskWalEntries("t-comp-m65"));
    assert.equal(snap.totalSqlitePages, 40);

    // Cold-start second compactor over the same SQLite
    const comp2 = new _compactor.WalCompactorAndPagedRestorer(4096, sql);
    const uffd = comp2.restoreViaUserfaultfd("t-comp-m65", 0.25);
    assert.equal(uffd.eagerPagesLoaded, 10);
    assert.equal(uffd.lazyDeferredPages, 30);

    // LayerFsRemoteStoreSynchronizer SQLite persistence
    const bucket = new _env.InMemoryR2Bucket();
    const sync1 = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(bucket, "proj-m65-test", sql);
    const payload = new TextEncoder().encode("layerfs-sqlite-bytes");
    const push1 = await sync1.pushStoreSnapshot({
      machineId: "host-01",
      basedOnGeneration: 0,
      storeSqliteBytes: payload,
      mainHeadCommitId: "C10",
      idempotencyKey: "idem-push-m65"
    });
    assert.equal(push1.remoteGeneration, 1);

    // Cold-start second synchronizer over the same SQLite and verify idempotent retry + generation preservation
    const sync2 = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(bucket, "proj-m65-test", sql);
    assert.equal(sync2.getRemoteGeneration(), 1);
    const retryPush = await sync2.pushStoreSnapshot({
      machineId: "host-01",
      basedOnGeneration: 0,
      storeSqliteBytes: payload,
      mainHeadCommitId: "C10",
      idempotencyKey: "idem-push-m65"
    });
    assert.equal(retryPush.deduplicated, true);
    assert.equal(retryPush.remoteGeneration, 1);
  });

  (0, _nodeTest.it)("[M65-2] Persists CanaryRolloutController post-rollback lock, PredictiveSandboxPrewarmer EWMA & TaskResourceProfiler history in SQLite", () => {
    const sql = new _env.InMemoryCellSqlStorage();
    const can1 = new _canary_rollout_controller.CanaryRolloutController(sql);
    const rolledBack = can1.evaluateCanaryStep({ errorRatePct: 4.5, p99LatencyMs: 250 });
    assert.equal(rolledBack.state, "ROLLED_BACK");

    // Cold-start new CanaryRolloutController over same SQLite: healthy step must NOT promote ROLLED_BACK canary!
    const can2 = new _canary_rollout_controller.CanaryRolloutController(sql);
    const afterRestart = can2.evaluateCanaryStep({ errorRatePct: 0.05, p99LatencyMs: 90 });
    assert.equal(afterRestart.state, "ROLLED_BACK");
    assert.equal(afterRestart.trafficWeightPct, 0);

    const pre1 = new _predictive_prewarmer.PredictiveSandboxPrewarmer(0.5, sql);
    pre1.recordArrivalSampleAndPredict(35.0, 4.0);
    const pre2 = new _predictive_prewarmer.PredictiveSandboxPrewarmer(0.5, sql);
    assert.equal(pre2.getEwmaArrivalRatePerSec(), 20.0);

    const prof1 = new _profiler.TaskResourceProfiler(0.5, sql);
    const t = new _models.TaskSpec({ task_id: "t-p", tenant_id: "Helios" });
    prof1.recordExecutionMetrics(t, 10.0, 16384);
    prof1.recordExecutionMetrics(t, 10.0, 16384);
    prof1.recordExecutionMetrics(t, 10.0, 16384);
    const prof2 = new _profiler.TaskResourceProfiler(0.5, sql);
    const rec = prof2.recommendProfile(t);
    assert.equal(rec.act_cpu_cores, 12.0);
    assert.equal(rec.act_memory_mb, 20480);
  });

  (0, _nodeTest.it)("[M65-4] Routes /v1/tunnel/* through Cloudflare Worker to TunnelGatewayCell fetch()", async () => {
    const schedCell = new _worker.AnstsSchedulerCell(new _env.MockDurableObjectState("sched-0"));
    const harnessCell = new _worker.AgentTaskHarnessCell(
    new _env.MockDurableObjectState("h-0"),
    new _env.InMemoryR2Bucket(),
    "h-0");

    const tunnelCell = new _worker.TunnelGatewayCell(new _env.MockDurableObjectState("body-host-01"));

    const env = {
      CELLD_REGION: "eu-central-1",
      ANSTS_SCHEDULER_CELL: {
        idFromName: (n) => ({ toString: () => n }),
        get: () => ({ fetch: (req) => schedCell.fetch(req) })
      },
      ANSTS_HARNESS_CELL: {
        idFromName: (n) => ({ toString: () => n }),
        get: () => ({ fetch: (req) => harnessCell.fetch(req) })
      },
      ANSTS_TUNNEL_CELL: {
        idFromName: (n) => ({ toString: () => n }),
        get: () => ({ fetch: (req) => tunnelCell.fetch(req) })
      },
      ANSTS_IDEMPOTENCY_KV: new _env.InMemoryKvNamespace(),
      ANSTS_CHECKPOINT_BUCKET: new _env.InMemoryR2Bucket(),
      ANSTS_TASK_DISPATCH_QUEUE: new _env.InMemoryQueueProducer(),
      ANSTS_WEBHOOK_OUTBOX_QUEUE: new _env.InMemoryQueueProducer()
    };

    const regResp = await _worker.default.fetch(
    new Request("https://edge.ansts.io/v1/tunnel/register?hostId=body-host-01", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        hostId: "body-host-01",
        gatewayId: "celld-gw-01",
        role: "PRIMARY",
        connGeneration: 1,
        epoch: 1
      })
    }),
    env);

    assert.equal(regResp.status, 200);

    const enq = tunnelCell.enqueueCommandForHost("body-host-01", { action: "ping" }, 5000);
    const pollResp = await _worker.default.fetch(
    new Request("https://edge.ansts.io/v1/tunnel/poll?hostId=body-host-01"),
    env);

    const pollData = await pollResp.json();
    assert.equal(pollData.commands.length, 1);

    const ackResp = await _worker.default.fetch(
    new Request("https://edge.ansts.io/v1/tunnel/ack?hostId=body-host-01", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        commandId: pollData.commands[0].commandId,
        result: { pong: true }
      })
    }),
    env);

    assert.equal(ackResp.status, 200);
    const res = await enq.completionPromise;
    assert.equal(res.pong, true);
  });

  (0, _nodeTest.it)("[M65-3, M65-5 & M65-6] Verifies live route state-mutation persistence across server restart & CLI mgmt-consistency-phase2-audit", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m65-srv-"));
    const srv1 = new _server_main.AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      storageDir: tmpDir,
      tickIntervalMs: 60_000
    });
    const { apiPort: port1 } = await srv1.start(0, 0);
    const baseUrl1 = `http://127.0.0.1:${port1}`;

    try {
      // 1. Run 2 turns via POST /v1beta1/harness/run-turn and verify v25Store & walEngine are updated
      await fetch(`${baseUrl1}/v1beta1/harness/run-turn`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "t-m65-live-harness",
          tenantId: "Northstar",
          toolName: "shell_exec",
          toolArgs: { cmd: "echo step1" },
          estimatedTokens: 900
        })
      });
      await fetch(`${baseUrl1}/v1beta1/harness/run-turn`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "t-m65-live-harness",
          tenantId: "Northstar",
          toolName: "shell_exec",
          toolArgs: { cmd: "echo step2" },
          estimatedTokens: 1100
        })
      });

      // 2. Register and fail a DAG child via POST /v1beta1/dag/escrow
      await fetch(`${baseUrl1}/v1beta1/dag/escrow`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "reserveParent",
          parentTaskId: "t-m65-dag-parent",
          tenantId: "Northstar",
          expectedSubAgentCount: 2
        })
      });
      await fetch(`${baseUrl1}/v1beta1/dag/escrow`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "registerChild",
          parentTaskId: "t-m65-dag-parent",
          childTaskId: "t-m65-dag-child-1",
          tenantId: "Northstar"
        })
      });
      const failChildResp = await fetch(`${baseUrl1}/v1beta1/dag/escrow`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "failChild",
          parentTaskId: "t-m65-dag-parent",
          childTaskId: "t-m65-dag-child-1",
          abortParentOnChildFailure: true
        })
      });
      assert.equal(failChildResp.status, 200);

      // 3. Execute CLI mgmt-consistency-phase2-audit
      const cliOut = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-consistency-phase2-audit"], baseUrl1);
      assert.equal(cliOut.exitCode, 0);
      const parsed = JSON.parse(cliOut.stdout);
      assert.equal(parsed.allM65Phase2ChecksPassed, true);
    } finally {
      await srv1.stop();
    }

    // 4. Cold-start second server over same storageDir and verify harness turn progress, WAL entries & DAG parent state persisted!
    const srv2 = new _server_main.AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      storageDir: tmpDir,
      tickIntervalMs: 60_000
    });
    try {
      const persistedHarnessTask = srv2.scheduler.allTasks.get("t-m65-live-harness");
      assert.equal(persistedHarnessTask?.current_turn, 2);
      assert.equal(srv2.walEngine.getTaskWalEntries("t-m65-live-harness").length, 2);

      const persistedDagParent = srv2.scheduler.allTasks.get("t-m65-dag-parent");
      assert.equal(persistedDagParent?.state, _models.TaskState.DEAD_LETTER);
      assert.equal(persistedDagParent?.dead_letter_reason, "CHILD_SUB_AGENT_FAILED:t-m65-dag-child-1");
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});