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
 * @fileoverview M52 End-to-End & Subsystem Verification: System-Wide Stale, Deadlock, Starvation & State-Consistency Rectification Suite.
 * Verifies:
 * 1. [Cell-Stale-1] `AnstsSchedulerCell` & `LeaseCoordinator` cold-start fencing watermark rehydration, interrupted `LEASED` -> `QUEUED` recovery, and active-lease DO alarm renewal.
 * 2. [Router-Stale-2] `CognitiveRouter` 60-second sliding-window quota decay, half-open circuit-breaker auto-recovery from rate-limit degradation, and fallback `active_requests` concurrency accounting.
 * 3. [Escrow-Deadlock-3] `DagEscrowCoordinator` child sub-agent failure parent unblocking & escrow slot reclamation + `A2aWaitForGraphDeadlockDetector` priority-aware victim selection.
 * 4. [Tunnel-Deadlock-4] `TunnelGatewayCell` stale-heartbeat tunnel auto-demotion and unacknowledged in-flight command re-queueing onto secondary failover gateway.
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M52 Comprehensive System-Wide Stale, Deadlock, Starvation & State-Consistency Rectification Suite", () => {
    (0, node_test_1.it)("verifies Cell cold-start lease/fencing rehydration & alarm renewal, CognitiveRouter window decay & half-open recovery, DagEscrow child-failure unblocking & WFG priority victim, and Tunnel in-flight command re-queueing", async () => {
        const tmpDir = `/tmp/ansts-m52-${process.pid}-${Date.now()}`;
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
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-rectify-audit"], baseUrl);
            assert.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            assert.equal(body.cellStale1.activeLeaseAlarmRenewed, true);
            assert.equal(body.cellStale1.interruptedLeaseConvertedToQueued, true);
            assert.equal(body.cellStale1.monotonicFencePreservedAcrossColdStart, true);
            assert.ok(body.cellStale1.fenceAfterRestart > body.cellStale1.fenceBeforeRestart);
            assert.equal(body.routerStale2.fallbackIncrementedActiveRequests, true);
            assert.equal(body.routerStale2.fallbackReleasedCleanly, true);
            assert.equal(body.routerStale2.markedUnhealthyOnRateLimit, true);
            assert.equal(body.routerStale2.routerWindowAndHalfOpenRecovered, true);
            assert.equal(body.escrowAndWfgDeadlock3.escrowUnblockedAndReclaimed, true);
            assert.equal(body.escrowAndWfgDeadlock3.wfgLowestPriorityVictimChosen, true);
            assert.equal(body.escrowAndWfgDeadlock3.wfgVictimAgentId, "agent-p3-helper");
            assert.equal(body.tunnelDeadlock4.demotedGateways.length, 1);
            assert.equal(body.tunnelDeadlock4.demotedGateways[0].gatewayId, "celld-gw-01");
            assert.equal(body.tunnelDeadlock4.requeuedCommandIds.length, 1);
            assert.equal(body.tunnelDeadlock4.tunnelInFlightRequeuedAndAcked, true);
            assert.equal(body.allM52RectificationsVerified, true);
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
