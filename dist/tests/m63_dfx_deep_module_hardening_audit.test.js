"use strict";



var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));

var _env = require("../src/cloudflare/env.js");
var _network_chaos_proxy = require("../src/runtime/network_chaos_proxy.js");
var _chaos_drill_runner = require("../src/cluster/chaos_drill_runner.js");
var _backpressure_loop = require("../src/cluster/backpressure_loop.js");
var _priority_load_shedder = require("../src/cluster/priority_load_shedder.js");
var _horizontal_scaler = require("../src/cloudflare/horizontal_scaler.js");
var _geo_routing = require("../src/cluster/geo_routing.js");
var _deadlock_detector = require("../src/a2a/deadlock_detector.js");
var _confidential_tee_bridge = require("../src/runtime/confidential_tee_bridge.js");
var _zero_trust_security_guard = require("../src/runtime/zero_trust_security_guard.js");
var _pool_client = require("../src/router/pool_client.js");
var _models = require("../src/models.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M63 Deep DFX Hardening Phase II Suite (Chaos Auto-Heal, Anti-Flapping Backpressure/Shedder/Sharder,
 * Sovereign Routing Persistence, Multi-Cycle WFG Resolution, TEE/Zero-Trust IMDSv6 Guard & Token Cost Clamping).
 */(0, _nodeTest.describe)("M63 Deep DFX Hardening Phase II Suite", () => {(0, _nodeTest.it)("1. Verifies M63-1 through M63-5 across Chaos TTL, Backpressure/Shedder/Sharder NaN guards, Sovereign Routing SQLite, Multi-Cycle WFG, TEE/Zero-Trust & ModelPool Cost Clamping, plus HTTP & CLI", async () => {
    const sql = new _env.InMemoryCellSqlStorage();

    // =========================================================================
    // 1. [M63-1] DFX-Reliability: Chaos Proxy TTL Auto-Heal & Drill Runner Guard
    // =========================================================================
    const proxy = new _network_chaos_proxy.NetworkChaosFaultInjectionProxy();
    proxy.injectPartition("node-ttl-01", 200, 1000);
    assert.throws(() => proxy.assertReachable("node-ttl-01", 1100), /simulated network partition/);
    // At t=1250ms (>= 1200ms TTL), partition auto-heals
    assert.doesNotThrow(() => proxy.assertReachable("node-ttl-01", 1250));
    assert.equal(proxy.inspectActiveFaults(1250).partitionedHosts.length, 0);

    const drill = new _chaos_drill_runner.AutomatedChaosDrillRunner();
    const nanDrill = drill.runSplitBrainAndRecoveryDrill({
      oldNodeFencingToken: Number.NaN,
      newNodeFencingToken: 10
    });
    assert.equal(nanDrill.zombieBlocked, false);
    assert.equal(nanDrill.rpoZeroVerified, false);

    // =========================================================================
    // 2. [M63-2] DFX-Availability: Backpressure, LoadShedder & AutoSharder NaN Guards
    // =========================================================================
    const bp = new _backpressure_loop.BidirectionalBackpressureController();
    const degradedOutage = bp.computeClusterHealthState({
      maxNodePsiSomePct: Number.NaN,
      globalRateLimitedEndpoints: 5,
      totalEndpoints: 0,
      globalQueueDepth: 50
    });
    assert.equal(degradedOutage.mode, "RED_SHEDDING");

    const shedder = new _priority_load_shedder.PriorityLoadShedder();
    assert.equal(shedder.shouldAdmitUnderLoad(_models.TaskPriority.P3_BATCH, Number.NaN), false);
    assert.equal(shedder.shouldAdmitUnderLoad(_models.TaskPriority.P0_EMERGENCY, Number.NaN), true);

    const sharder = new _horizontal_scaler.DynamicQueueAutoSharder(16, 4, 128);
    const adj = sharder.adjustShards(Number.NaN, 0);
    assert.equal(adj.newShards, 4);

    const poolScaler = new _horizontal_scaler.BrainBodyContainerPoolScaler();
    const poolTarget = poolScaler.computeTargetSandboxContainers({
      activeThinkCells: -10,
      activeActCells: Number.NaN,
      warmStandbyRatio: -0.5
    });
    assert.equal(poolTarget.brainIsolateMemoryMb, 0);
    assert.equal(poolTarget.totalContainerPoolSize, 2);

    // =========================================================================
    // 3. [M63-3] DFX-Compliance: GeoSovereigntyAndLatencyRouter Sanitization & SQLite
    // =========================================================================
    const geo1 = new _geo_routing.GeoSovereigntyAndLatencyRouter(sql);
    geo1.registerGeoRegion({
      regionId: "eu-west-2",
      jurisdiction: "GDPR_EU",
      latencyMs: 18,
      capacityAvailable: true
    });
    geo1.registerGeoRegion({
      regionId: "eu-west-1",
      jurisdiction: "GDPR_EU",
      latencyMs: 18,
      capacityAvailable: true
    });
    geo1.registerGeoRegion({
      regionId: "eu-bad-latency",
      jurisdiction: "GDPR_EU",
      latencyMs: -99, // negative latency must be sanitized to 999,999ms penalty
      capacityAvailable: true
    });

    const geo2 = new _geo_routing.GeoSovereigntyAndLatencyRouter(sql);
    assert.equal(geo2.listRegions().length, 3);
    const bestEu = geo2.routeByJurisdiction("GDPR_EU");
    assert.equal(bestEu.regionId, "eu-west-1", "Deterministic tie-break and negative latency penalty must select eu-west-1");

    // =========================================================================
    // 4. [M63-4] DFX-Serviceability: Multi-Cycle WFG Iterative Deadlock Resolution
    // =========================================================================
    const wfg = new _deadlock_detector.A2aWaitForGraphDeadlockDetector();
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
      "agent-D": 25
    });
    assert.equal(resolved.resolved, true, "All disjoint cycles must be resolved in one call");
    assert.deepEqual(resolved.evictedVictims, ["agent-B", "agent-D"]);
    assert.equal(wfg.detectCycle().hasDeadlock, false);

    // =========================================================================
    // 5. [M63-5] DFX-Security & Cost Accuracy: TEE, Zero-Trust & ModelPool Clamping
    // =========================================================================
    const tee = new _confidential_tee_bridge.ConfidentialTeeAttestationBridge();
    const badHexQuote = tee.generateAndVerifyAttestationQuote({
      vmId: "vm-1",
      measurementDigestHex: "zzzz-not-hex-zzzz-123456",
      nonce: "n1"
    });
    assert.equal(badHexQuote.verified, false, "Non-hex measurement digest must fail verification");

    const goodQuote = tee.generateAndVerifyAttestationQuote({
      vmId: "vm-1",
      measurementDigestHex: "0123456789abcdef0123456789abcdef",
      nonce: "n1"
    });
    assert.equal(goodQuote.verified, true);
    assert.equal(
    tee.verifyExternalAttestationQuote({
      vmId: "vm-1",
      measurementDigestHex: "0123456789abcdef0123456789abcdef",
      nonce: "n1",
      ed25519SignatureHex: goodQuote.ed25519SignatureHex
    }),
    true);

    assert.equal(
    tee.verifyExternalAttestationQuote({
      vmId: "vm-1",
      measurementDigestHex: "0123456789abcdef0123456789abcdef",
      nonce: "tampered-nonce",
      ed25519SignatureHex: goodQuote.ed25519SignatureHex
    }),
    false);


    const zt = new _zero_trust_security_guard.ZeroTrustSandboxSecurityGuard();
    assert.equal(zt.evaluateShellCommand("curl http://[::ffff:169.254.169.254]/latest").allowed, false);
    assert.equal(zt.evaluateShellCommand("curl http://169.254.170.2/v2/credentials").allowed, false);
    assert.equal(zt.evaluateShellCommand("dd if=/dev/zero of=/dev/nvme0n1 bs=1M").allowed, false);
    assert.equal(zt.evaluateShellCommand("echo hello\0world").allowed, false);

    const pool = new _pool_client.ModelPoolClientAdapter();
    const ep = new _models.LlmEndpointMetrics({
      endpoint_id: "ep-dfx-cost",
      provider: "anthropic",
      model_name: "claude-3-7-sonnet",
      healthy: true
    });
    const inf = pool.executeInference(
    ep,
    {
      taskId: "t-cost",
      turnIndex: 1,
      prompt: "Test",
      estimatedPromptTokens: 2000,
      maxOutputTokens: 500
    },
    5.0 // ratio > 1.0 must clamp to 1.0 so cachedTokens = 2000 and costUsd > 0
    );
    assert.equal(inf.cachedTokens, 2000);
    assert.ok(inf.costUsd > 0);

    // =========================================================================
    // 6. [M63-6] Live HTTP Endpoint & CLI Verification
    // =========================================================================
    const server = new _server_main.AnstsLocalAndComposeServer({ nodeId: "gw-m63-test" });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-dfx-deep-audit"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);
      assert.equal(body.allM63DfxRisksEliminated, true);
      assert.equal(body.chaosAutoHeal1.chaosAutoHealVerified, true);
      assert.equal(body.backpressureAndSharder2.backpressureAndSharderVerified, true);
      assert.equal(body.geoRouting3.geoRoutingVerified, true);
      assert.equal(body.multiCycleWfg4.multiCycleWfgVerified, true);
      assert.equal(body.securityAndCost5.securityAndCostVerified, true);
    } finally {
      await server.stop();
    }
  });
});