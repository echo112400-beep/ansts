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
 * @fileoverview M50 End-to-End Verification: Full-Stack Architecture Design, Versioned Configuration Center,
 * Runtime Engine & SRE Observability/Ops Deep Optimization Suite.
 * Verifies:
 * 1. [M50-1] Architecture Design Blueprint & Contract Compliance Matrix (`GET /v1beta1/management/design/blueprint`).
 * 2. [M50-2] Versioned Dynamic Configuration Center with Schema Validation, CAS Conflict Detection, Hot-Reload & Revision Rollback (`POST /v1beta1/management/config/center`).
 * 3. [M50-3] Deep Runtime Engine Optimization (`AnstsSchedulerCell` `js_rpc` + `FleetReplicatedDurabilityEngine` + `HotCellOverloadAndQueueGuard` + `WalCompactorAndPagedRestorer` + `LayerFsRemoteStoreSynchronizer`) (`POST /v1beta1/management/runtime/celld-engine`).
 * 4. [M50-4] Closed-Loop SRE Observability, Histogram Quantiles (`p50`/`p95`/`p99`), W3C Distributed Trace Waterfall, SLO Burn-Rate Alerting & Automated Self-Healing Runbook (`POST /v1beta1/management/ops/observability`).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M50 Full-Stack Architecture Design, Versioned Configuration Center, Runtime Engine & SRE Observability/Ops Deep Optimization Suite", () => {
    (0, node_test_1.it)("executes design blueprint audit, versioned config center CAS hot-reload & rollback, celld/LayerFS runtime engine tuning, and W3C trace waterfall + SLO burn-rate + self-healing runbook", async () => {
        const tmpDir = `/tmp/ansts-m50-${process.pid}-${Date.now()}`;
        fs.mkdirSync(tmpDir, { recursive: true });
        const server = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-01",
            nodeRole: "gateway",
            storageDir: tmpDir,
        });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            // 1. [M50-1] Architecture Design Blueprint & Contract Compliance Matrix
            const designCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-design"], baseUrl);
            assert.equal(designCli.exitCode, 0);
            const designData = JSON.parse(designCli.stdout);
            assert.equal(designData.designVersion, "v2.5-M50");
            assert.deepEqual(designData.pillars, ["DESIGN", "CONFIGURATION", "RUNTIME", "OBSERVABILITY_OPS"]);
            assert.equal(designData.sourceModulesCount, 86);
            assert.equal(designData.invariantsVerified, true);
            assert.ok(designData.designMarkdownLength > 500);
            // 2. [M50-2] Versioned Dynamic Configuration Center (Inspect -> Validate & Apply -> CAS 409 -> Schema 400 -> Rollback)
            const cfgInspectCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-config", "inspect_active_config"], baseUrl);
            assert.equal(cfgInspectCli.exitCode, 0);
            const cfgInitial = JSON.parse(cfgInspectCli.stdout);
            assert.equal(cfgInitial.activeRevision, 1);
            assert.equal(cfgInitial.activeConfig.maxCellConcurrentRequests, 64);
            const cfgApplyCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-config", "validate_and_apply_config", "96"], baseUrl);
            assert.equal(cfgApplyCli.exitCode, 0);
            const cfgApplied = JSON.parse(cfgApplyCli.stdout);
            assert.equal(cfgApplied.previousRevision, 1);
            assert.equal(cfgApplied.activeRevision, 2);
            assert.ok(cfgApplied.changedKeys.includes("maxCellConcurrentRequests"));
            assert.equal(cfgApplied.hotReloaded, true);
            assert.equal(cfgApplied.activeConfig.maxCellConcurrentRequests, 96);
            // Verify CAS conflict (409) when passing stale expectedRevision=1
            const casConflictResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "validate_and_apply_config",
                    expectedRevision: 1,
                    runtimeOverrides: { maxCellConcurrentRequests: 128 },
                    actor: "chen",
                    role: "SRE_ADMIN",
                }),
            });
            assert.equal(casConflictResp.status, 409);
            // Verify Schema validation error (400) when actCpuCores < thinkCpuCores
            const invalidSchemaResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                    action: "validate_and_apply_config",
                    runtimeOverrides: { thinkCpuCores: 8.0, actCpuCores: 1.0 },
                    actor: "chen",
                    role: "SRE_ADMIN",
                }),
            });
            assert.equal(invalidSchemaResp.status, 400);
            // Rollback to revision 1
            const cfgRollbackCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-config", "rollback_revision", "1"], baseUrl);
            assert.equal(cfgRollbackCli.exitCode, 0);
            const cfgRolledBack = JSON.parse(cfgRollbackCli.stdout);
            assert.equal(cfgRolledBack.rolledBackToRevision, 1);
            assert.equal(cfgRolledBack.activeRevision, 3);
            assert.equal(cfgRolledBack.activeConfig.maxCellConcurrentRequests, 64);
            // 3. [M50-3] Deep Runtime Engine Optimization (SchedulerCell RPC/Alarm, Fleet Durability & Overload Guard, LTX Compaction & LayerFS D9)
            const cellRpcCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-runtime-engine", "scheduler_cell_rpc_and_alarm"], baseUrl);
            assert.equal(cellRpcCli.exitCode, 0);
            const cellRpcData = JSON.parse(cellRpcCli.stdout);
            assert.ok(cellRpcData.submittedTaskId.startsWith("cell-rpc-"));
            assert.ok(cellRpcData.placements.length >= 1);
            assert.ok(cellRpcData.rehydratedTasksCount >= 1);
            const fleetCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-runtime-engine", "fleet_durability_and_overload_guard"], baseUrl);
            assert.equal(fleetCli.exitCode, 0);
            const fleetData = JSON.parse(fleetCli.stdout);
            assert.equal(fleetData.fastPeerCommit.acknowledgedVia, "PEER_FSYNC");
            assert.equal(fleetData.hedgedPeerCommit.acknowledgedVia, "HEDGED_PEER_COPY");
            assert.equal(fleetData.hedgedPeerCommit.hedgedCopyTriggered, true);
            assert.equal(fleetData.singleNodeFallback.effectiveDurabilityMode, "bucket_fallback");
            assert.equal(fleetData.overloadGuardProbe.admitted, false);
            assert.equal(fleetData.overloadGuardProbe.statusCode, 503);
            assert.equal(fleetData.queueGroupCommit.admitted, true);
            assert.equal(fleetData.queueGroupCommit.committedTransactions, 2);
            const ltxCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-runtime-engine", "ltx_compaction_and_layerfs_d9_sync", "task-ns-8841"], baseUrl);
            assert.equal(ltxCli.exitCode, 0);
            const ltxData = JSON.parse(ltxCli.stdout);
            assert.equal(ltxData.l1Compaction.compactionTriggered, true);
            assert.equal(ltxData.l1Compaction.batchesMerged, 2);
            assert.equal(ltxData.walTruncation.shouldTruncateWalAtCheckpoint, true);
            assert.equal(ltxData.walTruncation.usePagedRestore, true);
            assert.ok(ltxData.uffdRestore.eagerPagesLoaded >= 1 && ltxData.uffdRestore.lazyDeferredPages >= 1);
            assert.equal(ltxData.layerfsSync.remoteGeneration, 1);
            // 4. [M50-4] Closed-Loop SRE Observability, W3C Trace Waterfall, SLO Burn-Rate & Automated Self-Healing Runbook
            const obsCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-ops", "probe_slo_traces_and_metrics"], baseUrl);
            assert.equal(obsCli.exitCode, 0);
            const obsData = JSON.parse(obsCli.stdout);
            assert.ok(obsData.traceWaterfall.traceparent.startsWith("00-"));
            assert.equal(obsData.traceWaterfall.spans.length, 5);
            assert.ok(obsData.latencyQuantiles.count >= 9);
            assert.ok(obsData.latencyQuantiles.p99 >= obsData.latencyQuantiles.p50);
            assert.ok(obsData.sloEvaluation.burnRate1h > 0);
            assert.ok(obsData.prometheusMetrics.includes("ansts_task_dispatch_latency_ms"));
            const rbCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-ops", "execute_auto_remediation_runbook", "RB-SLO-BURN-AND-DLQ-HEAL"], baseUrl);
            assert.equal(rbCli.exitCode, 0);
            const rbData = JSON.parse(rbCli.stdout);
            assert.equal(rbData.runbookId, "RB-SLO-BURN-AND-DLQ-HEAL");
            assert.deepEqual(rbData.circuitBreakersReset, ["ep-deepseek-r1", "ep-qwen-38"]);
            assert.equal(rbData.cloudEvent.type, "io.ansts.sre.runbook.executed");
            assert.ok(rbData.webhookOutbox.deliveredCount >= 1);
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
