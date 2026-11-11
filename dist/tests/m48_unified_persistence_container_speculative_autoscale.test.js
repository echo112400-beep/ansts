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
 * @fileoverview M48 End-to-End Verification: Unified 8-Table Full-Cluster State Snapshot & Cold-Start Rehydration,
 * Native `denoland/celld` `ctx.container` (`kata` OCI + `celld-fence` IMDSv2/RFC1918 Firewall) + `DurableWorkflowContext`,
 * Speculative Deterministic Multi-Branch CoW Execution (`CoWSandboxBranchForker` + `DeterministicExecutionEnvelope`),
 * and Closed-Loop Linux `/proc/pressure/memory` PSI Guard + Dynamic Queue/Container Auto-Scaler.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M48 Unified 8-Table Full-Cluster Persistence, Native ctx.container + celld-fence Bridge, Speculative Deterministic Branching & Closed-Loop PSI/Queue Auto-Scaler", () => {
    (0, node_test_1.it)("executes 8-table cluster snapshot & cold-start rehydration, native kata ctx.container + celld-fence IMDS block + DurableWorkflowContext, deterministic speculative CoW branches, and closed-loop PSI/Queue auto-scaling", async () => {
        const tmpDir = `/tmp/ansts-m48-${process.pid}-${Date.now()}`;
        fs.mkdirSync(tmpDir, { recursive: true });
        const server = new server_main_js_1.AnstsLocalAndComposeServer({
            host: "127.0.0.1",
            nodeId: "celld-gw-01",
            nodeRole: "gateway",
            storageDir: `${tmpDir}/gw-01`,
        });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            // 1. [M48-1] Unified 8-Table Full-Cluster State Snapshot & Cold-Start Rehydration (`POST /v1beta1/management/persistence/snapshot`)
            const snapCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-snapshot", "verify_cold_start_rehydration"], baseUrl);
            assert.equal(snapCli.exitCode, 0);
            const snapData = JSON.parse(snapCli.stdout);
            assert.ok(snapData.restoredTasksCount >= 4);
            assert.equal(snapData.restoredFinopsCount, 4);
            assert.ok(snapData.restoredApprovalsCount >= 3);
            assert.ok(snapData.restoredMcpCount >= 1);
            assert.equal(snapData.rpoZeroVerified, true);
            assert.ok(snapData.tableCounts.persisted_tasks >= 4);
            assert.ok(snapData.tableCounts.persisted_leases >= 4);
            // 2. [M48-2] Native `denoland/celld` `ctx.container` (`kata`) + `celld-fence` IMDSv2 Block & `DurableWorkflowContext`
            const contCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-container", "start_and_probe_container"], baseUrl);
            assert.equal(contCli.exitCode, 0);
            const contData = JSON.parse(contCli.stdout);
            assert.equal(contData.started.running, true);
            assert.equal(contData.started.ociRuntime, "kata");
            assert.deepEqual(contData.started.resolvConf, ["1.1.1.1", "1.0.0.1"]);
            assert.equal(contData.execResult.exitCode, 0);
            assert.ok(contData.execResult.rawStdout.includes("nameserver 1.1.1.1"));
            assert.equal(contData.webSocketProbe.status, 101);
            assert.equal(contData.webSocketProbe.webSocketUpgraded, true);
            assert.equal(contData.celldFenceImdsBlocked, true);
            const wfCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-container", "run_durable_workflow"], baseUrl);
            assert.equal(wfCli.exitCode, 0);
            const wfData = JSON.parse(wfCli.stdout);
            assert.equal(wfData.firstExecution.replayedFromJournal, false);
            assert.equal(wfData.crashReplayExecution.replayedFromJournal, true);
            assert.equal(wfData.firstExecution.result.sha256, wfData.crashReplayExecution.result.sha256);
            // 3. [M48-3] Speculative Deterministic Multi-Branch CoW Execution & Winning Branch Merge (`POST /v1beta1/management/layerfs/speculative`)
            const specCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-speculative", "feat-ns-auth"], baseUrl);
            assert.equal(specCli.exitCode, 0);
            const specData = JSON.parse(specCli.stdout);
            assert.equal(specData.deterministicReproducible, true);
            assert.equal(specData.branches.length, 2);
            assert.ok(specData.mergeResult.mergedCommitId);
            // 4. [M48-4] Closed-Loop Linux `/proc/pressure/memory` PSI Guard, Bidirectional Backpressure & Dynamic Queue/Container Auto-Scaler
            const autoCli = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-autoscale", "simulate_psi_spike"], baseUrl);
            assert.equal(autoCli.exitCode, 0);
            const autoData = JSON.parse(autoCli.stdout);
            assert.ok(autoData.liveKernelPsi.freeMemMb > 0);
            assert.equal(autoData.psiDefense.level, "FREEZE_AND_CHECKPOINT");
            assert.equal(autoData.backpressure.mode, "RED_SHEDDING");
            assert.equal(autoData.queueSharding.newShards, 32);
            assert.ok(autoData.poolScaling.totalContainerPoolSize >= 4);
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
