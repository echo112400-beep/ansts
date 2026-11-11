"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview M65 Deep Implementation Consistency Phase II Verification Suite.
 * Verifies:
 * 1. [M65-1] `WalCompactorAndPagedRestorer` & `LayerFsRemoteStoreSynchronizer` constructor contract & SQLite persistence across restarts.
 * 2. [M65-2] `CanaryRolloutController` (post-rollback lock), `PredictiveSandboxPrewarmer` & `TaskResourceProfiler` SQLite rehydration.
 * 3. [M65-3] `AnstsLocalAndComposeServer` live route state-mutation persistence sync across `/v1beta1/harness/run-turn`, `/v1beta1/saga/*` & `/v1beta1/dag/escrow`.
 * 4. [M65-4] `TunnelGatewayCell` DO `fetch` API & Cloudflare Worker `/v1/tunnel/*` routing.
 * 5. [M65-5 & M65-6] Live HTTP `POST /v1beta1/management/system/deep-consistency-phase2-audit` & CLI `anstsctl mgmt-consistency-phase2-audit`.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const worker_js_1 = __importStar(require("../src/cloudflare/worker.js"));
const env_js_1 = require("../src/cloudflare/env.js");
const wal_engine_js_1 = require("../src/checkpoint/wal_engine.js");
const compactor_js_1 = require("../src/checkpoint/compactor.js");
const layerfs_remote_sync_js_1 = require("../src/checkpoint/layerfs_remote_sync.js");
const canary_rollout_controller_js_1 = require("../src/cluster/canary_rollout_controller.js");
const predictive_prewarmer_js_1 = require("../src/runtime/predictive_prewarmer.js");
const profiler_js_1 = require("../src/profiler.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M65 Deep Implementation Consistency Phase II Suite", () => {
    (0, node_test_1.it)("[M65-1] Persists WalCompactorAndPagedRestorer snapshots & LayerFsRemoteStoreSynchronizer Rule D9 state in SQLite across restarts", async () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const wal = new wal_engine_js_1.TurnWalEngine(sql);
        wal.appendTurnDelta({
            taskId: "t-comp-m65",
            turnIndex: 1,
            fencingToken: 2,
            changedFiles: ["a.ts", "b.ts", "c.ts"],
            sqliteWalPages: 40,
            highestSubmittedFencingToken: 2,
        });
        const comp1 = new compactor_js_1.WalCompactorAndPagedRestorer(4096, sql);
        const snap = comp1.compactLayers("t-comp-m65", wal.getTaskWalEntries("t-comp-m65"));
        assert.equal(snap.totalSqlitePages, 40);
        // Cold-start second compactor over the same SQLite
        const comp2 = new compactor_js_1.WalCompactorAndPagedRestorer(4096, sql);
        const uffd = comp2.restoreViaUserfaultfd("t-comp-m65", 0.25);
        assert.equal(uffd.eagerPagesLoaded, 10);
        assert.equal(uffd.lazyDeferredPages, 30);
        // LayerFsRemoteStoreSynchronizer SQLite persistence
        const bucket = new env_js_1.InMemoryR2Bucket();
        const sync1 = new layerfs_remote_sync_js_1.LayerFsRemoteStoreSynchronizer(bucket, "proj-m65-test", sql);
        const payload = new TextEncoder().encode("layerfs-sqlite-bytes");
        const push1 = await sync1.pushStoreSnapshot({
            machineId: "host-01",
            basedOnGeneration: 0,
            storeSqliteBytes: payload,
            mainHeadCommitId: "C10",
            idempotencyKey: "idem-push-m65",
        });
        assert.equal(push1.remoteGeneration, 1);
        // Cold-start second synchronizer over the same SQLite and verify idempotent retry + generation preservation
        const sync2 = new layerfs_remote_sync_js_1.LayerFsRemoteStoreSynchronizer(bucket, "proj-m65-test", sql);
        assert.equal(sync2.getRemoteGeneration(), 1);
        const retryPush = await sync2.pushStoreSnapshot({
            machineId: "host-01",
            basedOnGeneration: 0,
            storeSqliteBytes: payload,
            mainHeadCommitId: "C10",
            idempotencyKey: "idem-push-m65",
        });
        assert.equal(retryPush.deduplicated, true);
        assert.equal(retryPush.remoteGeneration, 1);
    });
    (0, node_test_1.it)("[M65-2] Persists CanaryRolloutController post-rollback lock, PredictiveSandboxPrewarmer EWMA & TaskResourceProfiler history in SQLite", () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const can1 = new canary_rollout_controller_js_1.CanaryRolloutController(sql);
        const rolledBack = can1.evaluateCanaryStep({ errorRatePct: 4.5, p99LatencyMs: 250 });
        assert.equal(rolledBack.state, "ROLLED_BACK");
        // Cold-start new CanaryRolloutController over same SQLite: healthy step must NOT promote ROLLED_BACK canary!
        const can2 = new canary_rollout_controller_js_1.CanaryRolloutController(sql);
        const afterRestart = can2.evaluateCanaryStep({ errorRatePct: 0.05, p99LatencyMs: 90 });
        assert.equal(afterRestart.state, "ROLLED_BACK");
        assert.equal(afterRestart.trafficWeightPct, 0);
        const pre1 = new predictive_prewarmer_js_1.PredictiveSandboxPrewarmer(0.5, sql);
        pre1.recordArrivalSampleAndPredict(35.0, 4.0);
        const pre2 = new predictive_prewarmer_js_1.PredictiveSandboxPrewarmer(0.5, sql);
        assert.equal(pre2.getEwmaArrivalRatePerSec(), 20.0);
        const prof1 = new profiler_js_1.TaskResourceProfiler(0.5, sql);
        const t = new models_js_1.TaskSpec({ task_id: "t-p", tenant_id: "Helios" });
        prof1.recordExecutionMetrics(t, 10.0, 16384);
        prof1.recordExecutionMetrics(t, 10.0, 16384);
        prof1.recordExecutionMetrics(t, 10.0, 16384);
        const prof2 = new profiler_js_1.TaskResourceProfiler(0.5, sql);
        const rec = prof2.recommendProfile(t);
        assert.equal(rec.act_cpu_cores, 12.0);
        assert.equal(rec.act_memory_mb, 20480);
    });
    (0, node_test_1.it)("[M65-4] Routes /v1/tunnel/* through Cloudflare Worker to TunnelGatewayCell fetch()", async () => {
        const schedCell = new worker_js_1.AnstsSchedulerCell(new env_js_1.MockDurableObjectState("sched-0"));
        const harnessCell = new worker_js_1.AgentTaskHarnessCell(new env_js_1.MockDurableObjectState("h-0"), new env_js_1.InMemoryR2Bucket(), "h-0");
        const tunnelCell = new worker_js_1.TunnelGatewayCell(new env_js_1.MockDurableObjectState("body-host-01"));
        const env = {
            CELLD_REGION: "eu-central-1",
            ANSTS_SCHEDULER_CELL: {
                idFromName: (n) => ({ toString: () => n }),
                get: () => ({ fetch: (req) => schedCell.fetch(req) }),
            },
            ANSTS_HARNESS_CELL: {
                idFromName: (n) => ({ toString: () => n }),
                get: () => ({ fetch: (req) => harnessCell.fetch(req) }),
            },
            ANSTS_TUNNEL_CELL: {
                idFromName: (n) => ({ toString: () => n }),
                get: () => ({ fetch: (req) => tunnelCell.fetch(req) }),
            },
            ANSTS_IDEMPOTENCY_KV: new env_js_1.InMemoryKvNamespace(),
            ANSTS_CHECKPOINT_BUCKET: new env_js_1.InMemoryR2Bucket(),
            ANSTS_TASK_DISPATCH_QUEUE: new env_js_1.InMemoryQueueProducer(),
            ANSTS_WEBHOOK_OUTBOX_QUEUE: new env_js_1.InMemoryQueueProducer(),
        };
        const regResp = await worker_js_1.default.fetch(new Request("https://edge.ansts.io/v1/tunnel/register?hostId=body-host-01", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                hostId: "body-host-01",
                gatewayId: "celld-gw-01",
                role: "PRIMARY",
                connGeneration: 1,
                epoch: 1,
            }),
        }), env);
        assert.equal(regResp.status, 200);
        const enq = tunnelCell.enqueueCommandForHost("body-host-01", { action: "ping" }, 5000);
        const pollResp = await worker_js_1.default.fetch(new Request("https://edge.ansts.io/v1/tunnel/poll?hostId=body-host-01"), env);
        const pollData = (await pollResp.json());
        assert.equal(pollData.commands.length, 1);
        const ackResp = await worker_js_1.default.fetch(new Request("https://edge.ansts.io/v1/tunnel/ack?hostId=body-host-01", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
                commandId: pollData.commands[0].commandId,
                result: { pong: true },
            }),
        }), env);
        assert.equal(ackResp.status, 200);
        const res = await enq.completionPromise;
        assert.equal(res.pong, true);
    });
    (0, node_test_1.it)("[M65-3, M65-5 & M65-6] Verifies live route state-mutation persistence across server restart & CLI mgmt-consistency-phase2-audit", async () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m65-srv-"));
        const srv1 = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            storageDir: tmpDir,
            tickIntervalMs: 60_000,
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
                    estimatedTokens: 900,
                }),
            });
            await fetch(`${baseUrl1}/v1beta1/harness/run-turn`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    taskId: "t-m65-live-harness",
                    tenantId: "Northstar",
                    toolName: "shell_exec",
                    toolArgs: { cmd: "echo step2" },
                    estimatedTokens: 1100,
                }),
            });
            // 2. Register and fail a DAG child via POST /v1beta1/dag/escrow
            await fetch(`${baseUrl1}/v1beta1/dag/escrow`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "reserveParent",
                    parentTaskId: "t-m65-dag-parent",
                    tenantId: "Northstar",
                    expectedSubAgentCount: 2,
                }),
            });
            await fetch(`${baseUrl1}/v1beta1/dag/escrow`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "registerChild",
                    parentTaskId: "t-m65-dag-parent",
                    childTaskId: "t-m65-dag-child-1",
                    tenantId: "Northstar",
                }),
            });
            const failChildResp = await fetch(`${baseUrl1}/v1beta1/dag/escrow`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "failChild",
                    parentTaskId: "t-m65-dag-parent",
                    childTaskId: "t-m65-dag-child-1",
                    abortParentOnChildFailure: true,
                }),
            });
            assert.equal(failChildResp.status, 200);
            // 3. Execute CLI mgmt-consistency-phase2-audit
            const cliOut = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-consistency-phase2-audit"], baseUrl1);
            assert.equal(cliOut.exitCode, 0);
            const parsed = JSON.parse(cliOut.stdout);
            assert.equal(parsed.allM65Phase2ChecksPassed, true);
        }
        finally {
            await srv1.stop();
        }
        // 4. Cold-start second server over same storageDir and verify harness turn progress, WAL entries & DAG parent state persisted!
        const srv2 = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            storageDir: tmpDir,
            tickIntervalMs: 60_000,
        });
        try {
            const persistedHarnessTask = srv2.scheduler.allTasks.get("t-m65-live-harness");
            assert.equal(persistedHarnessTask?.current_turn, 2);
            assert.equal(srv2.walEngine.getTaskWalEntries("t-m65-live-harness").length, 2);
            const persistedDagParent = srv2.scheduler.allTasks.get("t-m65-dag-parent");
            assert.equal(persistedDagParent?.state, models_js_1.TaskState.DEAD_LETTER);
            assert.equal(persistedDagParent?.dead_letter_reason, "CHILD_SUB_AGENT_FAILED:t-m65-dag-child-1");
        }
        finally {
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
