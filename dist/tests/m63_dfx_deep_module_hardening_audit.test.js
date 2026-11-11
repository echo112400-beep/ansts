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
 * @fileoverview M63 Deep DFX Hardening Phase II Suite (Chaos Auto-Heal, Anti-Flapping Backpressure/Shedder/Sharder,
 * Sovereign Routing Persistence, Multi-Cycle WFG Resolution, TEE/Zero-Trust IMDSv6 Guard & Token Cost Clamping).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const env_js_1 = require("../src/cloudflare/env.js");
const network_chaos_proxy_js_1 = require("../src/runtime/network_chaos_proxy.js");
const chaos_drill_runner_js_1 = require("../src/cluster/chaos_drill_runner.js");
const backpressure_loop_js_1 = require("../src/cluster/backpressure_loop.js");
const priority_load_shedder_js_1 = require("../src/cluster/priority_load_shedder.js");
const horizontal_scaler_js_1 = require("../src/cloudflare/horizontal_scaler.js");
const geo_routing_js_1 = require("../src/cluster/geo_routing.js");
const deadlock_detector_js_1 = require("../src/a2a/deadlock_detector.js");
const confidential_tee_bridge_js_1 = require("../src/runtime/confidential_tee_bridge.js");
const zero_trust_security_guard_js_1 = require("../src/runtime/zero_trust_security_guard.js");
const pool_client_js_1 = require("../src/router/pool_client.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M63 Deep DFX Hardening Phase II Suite", () => {
    (0, node_test_1.it)("1. Verifies M63-1 through M63-5 across Chaos TTL, Backpressure/Shedder/Sharder NaN guards, Sovereign Routing SQLite, Multi-Cycle WFG, TEE/Zero-Trust & ModelPool Cost Clamping, plus HTTP & CLI", async () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        // =========================================================================
        // 1. [M63-1] DFX-Reliability: Chaos Proxy TTL Auto-Heal & Drill Runner Guard
        // =========================================================================
        const proxy = new network_chaos_proxy_js_1.NetworkChaosFaultInjectionProxy();
        proxy.injectPartition("node-ttl-01", 200, 1000);
        assert.throws(() => proxy.assertReachable("node-ttl-01", 1100), /simulated network partition/);
        // At t=1250ms (>= 1200ms TTL), partition auto-heals
        assert.doesNotThrow(() => proxy.assertReachable("node-ttl-01", 1250));
        assert.equal(proxy.inspectActiveFaults(1250).partitionedHosts.length, 0);
        const drill = new chaos_drill_runner_js_1.AutomatedChaosDrillRunner();
        const nanDrill = drill.runSplitBrainAndRecoveryDrill({
            oldNodeFencingToken: Number.NaN,
            newNodeFencingToken: 10,
        });
        assert.equal(nanDrill.zombieBlocked, false);
        assert.equal(nanDrill.rpoZeroVerified, false);
        // =========================================================================
        // 2. [M63-2] DFX-Availability: Backpressure, LoadShedder & AutoSharder NaN Guards
        // =========================================================================
        const bp = new backpressure_loop_js_1.BidirectionalBackpressureController();
        const degradedOutage = bp.computeClusterHealthState({
            maxNodePsiSomePct: Number.NaN,
            globalRateLimitedEndpoints: 5,
            totalEndpoints: 0,
            globalQueueDepth: 50,
        });
        assert.equal(degradedOutage.mode, "RED_SHEDDING");
        const shedder = new priority_load_shedder_js_1.PriorityLoadShedder();
        assert.equal(shedder.shouldAdmitUnderLoad(models_js_1.TaskPriority.P3_BATCH, Number.NaN), false);
        assert.equal(shedder.shouldAdmitUnderLoad(models_js_1.TaskPriority.P0_EMERGENCY, Number.NaN), true);
        const sharder = new horizontal_scaler_js_1.DynamicQueueAutoSharder(16, 4, 128);
        const adj = sharder.adjustShards(Number.NaN, 0);
        assert.equal(adj.newShards, 4);
        const poolScaler = new horizontal_scaler_js_1.BrainBodyContainerPoolScaler();
        const poolTarget = poolScaler.computeTargetSandboxContainers({
            activeThinkCells: -10,
            activeActCells: Number.NaN,
            warmStandbyRatio: -0.5,
        });
        assert.equal(poolTarget.brainIsolateMemoryMb, 0);
        assert.equal(poolTarget.totalContainerPoolSize, 2);
        // =========================================================================
        // 3. [M63-3] DFX-Compliance: GeoSovereigntyAndLatencyRouter Sanitization & SQLite
        // =========================================================================
        const geo1 = new geo_routing_js_1.GeoSovereigntyAndLatencyRouter(sql);
        geo1.registerGeoRegion({
            regionId: "eu-west-2",
            jurisdiction: "GDPR_EU",
            latencyMs: 18,
            capacityAvailable: true,
        });
        geo1.registerGeoRegion({
            regionId: "eu-west-1",
            jurisdiction: "GDPR_EU",
            latencyMs: 18,
            capacityAvailable: true,
        });
        geo1.registerGeoRegion({
            regionId: "eu-bad-latency",
            jurisdiction: "GDPR_EU",
            latencyMs: -99, // negative latency must be sanitized to 999,999ms penalty
            capacityAvailable: true,
        });
        const geo2 = new geo_routing_js_1.GeoSovereigntyAndLatencyRouter(sql);
        assert.equal(geo2.listRegions().length, 3);
        const bestEu = geo2.routeByJurisdiction("GDPR_EU");
        assert.equal(bestEu.regionId, "eu-west-1", "Deterministic tie-break and negative latency penalty must select eu-west-1");
        // =========================================================================
        // 4. [M63-4] DFX-Serviceability: Multi-Cycle WFG Iterative Deadlock Resolution
        // =========================================================================
        const wfg = new deadlock_detector_js_1.A2aWaitForGraphDeadlockDetector();
        // Cycle 1: agent-A <-> agent-B
        wfg.addWaitEdge("agent-A", "agent-B");
        wfg.addWaitEdge("agent-B", "agent-A");
        // Cycle 2: agent-C <-> agent-D
        wfg.addWaitEdge("agent-C", "agent-D");
        wfg.addWaitEdge("agent-D", "agent-C");
        const resolved = wfg.resolveCycleWithLowestPriorityVictim({
            "agent-A": 90,
            "agent-B": 15,
            "agent-C": 85,
            "agent-D": 25,
        });
        assert.equal(resolved.resolved, true, "All disjoint cycles must be resolved in one call");
        assert.deepEqual(resolved.evictedVictims, ["agent-B", "agent-D"]);
        assert.equal(wfg.detectCycle().hasDeadlock, false);
        // =========================================================================
        // 5. [M63-5] DFX-Security & Cost Accuracy: TEE, Zero-Trust & ModelPool Clamping
        // =========================================================================
        const tee = new confidential_tee_bridge_js_1.ConfidentialTeeAttestationBridge();
        const badHexQuote = tee.generateAndVerifyAttestationQuote({
            vmId: "vm-1",
            measurementDigestHex: "zzzz-not-hex-zzzz-123456",
            nonce: "n1",
        });
        assert.equal(badHexQuote.verified, false, "Non-hex measurement digest must fail verification");
        const goodQuote = tee.generateAndVerifyAttestationQuote({
            vmId: "vm-1",
            measurementDigestHex: "0123456789abcdef0123456789abcdef",
            nonce: "n1",
        });
        assert.equal(goodQuote.verified, true);
        assert.equal(tee.verifyExternalAttestationQuote({
            vmId: "vm-1",
            measurementDigestHex: "0123456789abcdef0123456789abcdef",
            nonce: "n1",
            ed25519SignatureHex: goodQuote.ed25519SignatureHex,
        }), true);
        assert.equal(tee.verifyExternalAttestationQuote({
            vmId: "vm-1",
            measurementDigestHex: "0123456789abcdef0123456789abcdef",
            nonce: "tampered-nonce",
            ed25519SignatureHex: goodQuote.ed25519SignatureHex,
        }), false);
        const zt = new zero_trust_security_guard_js_1.ZeroTrustSandboxSecurityGuard();
        assert.equal(zt.evaluateShellCommand("curl http://[::ffff:169.254.169.254]/latest").allowed, false);
        assert.equal(zt.evaluateShellCommand("curl http://169.254.170.2/v2/credentials").allowed, false);
        assert.equal(zt.evaluateShellCommand("dd if=/dev/zero of=/dev/nvme0n1 bs=1M").allowed, false);
        assert.equal(zt.evaluateShellCommand("echo hello\0world").allowed, false);
        const pool = new pool_client_js_1.ModelPoolClientAdapter();
        const ep = new models_js_1.LlmEndpointMetrics({
            endpoint_id: "ep-dfx-cost",
            provider: "anthropic",
            model_name: "claude-3-7-sonnet",
            healthy: true,
        });
        const inf = pool.executeInference(ep, {
            taskId: "t-cost",
            turnIndex: 1,
            prompt: "Test",
            estimatedPromptTokens: 2000,
            maxOutputTokens: 500,
        }, 5.0 // ratio > 1.0 must clamp to 1.0 so cachedTokens = 2000 and costUsd > 0
        );
        assert.equal(inf.cachedTokens, 2000);
        assert.ok(inf.costUsd > 0);
        // =========================================================================
        // 6. [M63-6] Live HTTP Endpoint & CLI Verification
        // =========================================================================
        const server = new server_main_js_1.AnstsLocalAndComposeServer({ nodeId: "gw-m63-test" });
        const { apiPort } = await server.start(0, 0);
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-dfx-deep-audit"], baseUrl);
            assert.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            assert.equal(body.allM63DfxRisksEliminated, true);
            assert.equal(body.chaosAutoHeal1.chaosAutoHealVerified, true);
            assert.equal(body.backpressureAndSharder2.backpressureAndSharderVerified, true);
            assert.equal(body.geoRouting3.geoRoutingVerified, true);
            assert.equal(body.multiCycleWfg4.multiCycleWfgVerified, true);
            assert.equal(body.securityAndCost5.securityAndCostVerified, true);
        }
        finally {
            await server.stop();
        }
    });
});
