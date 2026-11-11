"use strict";














var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _ebpf_network_tracker = require("../src/runtime/ebpf_network_tracker.js");
var _blackboard_and_git_arbiter = require("../src/a2a/blackboard_and_git_arbiter.js");
var _erasure_coder = require("../src/checkpoint/erasure_coder.js");
var _env = require("../src/cloudflare/env.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M75: Phase 2 eBPF Network Tracing, Byzantine Slashing & Erasure Coding Suite
 * Verifies:
 * - [M75-1 / N4] Cross-Node eBPF Dynamic Network Tracing & Microsecond Circuit Breaker
 *   (`EbpfSocketNetworkTrackerAndBreaker`, RFC 6298 EWMA SRTT, RTTVAR, sliding packet loss,
 *    socket buffer pressure, `TRIPPED_MICROS_OPEN` packet shedding, cooldown half-open & SQLite persistence).
 * - [M75-2 / N5] Byzantine Behavioral Fingerprinting, Progressive Credit Slashing & Quarantine Jail
 *   (`SharedBlackboardAndGitArbiter.recordByzantineViolation`, reputation slashing,
 *    `QUARANTINE_JAIL` confinement, swarm bidding disqualification, operator pardon & SQLite persistence).
 * - [M75-3 / N6] Cross-Region Reed-Solomon Erasure-Coded WAL & Lossless Rebuild
 *   (`CrossRegionErasureCodedWalManager`, Galois Field GF(2^8) (K=4, M=2) erasure coding,
 *    lossless mathematical reconstruction under any 2 lost shards, CRC32 verification & SQLite persistence).
 * - [M75-4] Live Console BFF (`POST /v1beta1/management/ops/phase2-network-slashing-erasure`),
 *   Console UI bindings, and CLI (`anstsctl mgmt-phase2-swarm`).
 */(0, _nodeTest.describe)("M75 Phase 2 eBPF Network Tracing, Byzantine Slashing & Erasure Coding Suite", () => {(0, _nodeTest.it)("[M75-1 / N4] eBPF socket network tracker calculates RFC 6298 SRTT/RTTVAR, trips microsecond breaker, and sheds packets", () => {const sql = new _env.InMemoryCellSqlStorage();const tracker = new _ebpf_network_tracker.EbpfSocketNetworkTrackerAndBreaker(sql, { p99LatencyThresholdUs: 25_000, packetLossThresholdRatio: 0.15, bufferPressureThresholdPct: 80.0, cooldownDurationUs: 100_000 // 100ms
    });const peer = "10.244.3.42:9443"; // 1. Send normal healthy samples
    for (let i = 0; i < 5; i++) {tracker.recordSocketSample({
        peerEndpoint: peer,
        rttUs: 1500 + i * 100,
        retransmitted: false,
        bufferBytes: 8192,
        maxBufferBytes: 65536
      });
    }

    const healthyTel = tracker.getSocketTelemetry(peer);
    _strict.default.ok(healthyTel);
    _strict.default.equal(healthyTel.circuitBreakerState, "CLOSED");
    _strict.default.ok(healthyTel.srttUs > 1400 && healthyTel.srttUs < 2500);

    const normalIntercept = tracker.interceptOutboundSocketTransfer(peer);
    _strict.default.equal(normalIntercept.allowed, true);
    _strict.default.equal(normalIntercept.circuitBreakerState, "CLOSED");

    // 2. High loss and latency spike -> trip breaker
    for (let i = 0; i < 5; i++) {
      tracker.recordSocketSample({
        peerEndpoint: peer,
        rttUs: 45_000,
        retransmitted: true,
        bufferBytes: 60000,
        maxBufferBytes: 65536
      });
    }

    const trippedTel = tracker.getSocketTelemetry(peer);
    _strict.default.equal(trippedTel?.circuitBreakerState, "TRIPPED_MICROS_OPEN");
    _strict.default.ok((trippedTel?.packetLossRatio ?? 0) >= 0.15);

    // Egress should now be immediately intercepted & shed
    const trippedIntercept = tracker.interceptOutboundSocketTransfer(peer);
    _strict.default.equal(trippedIntercept.allowed, false);
    _strict.default.equal(trippedIntercept.circuitBreakerState, "TRIPPED_MICROS_OPEN");
    _strict.default.ok(trippedIntercept.reason.includes("eBPF_TC_EGRESS_CIRCUIT_TRIPPED"));

    // 3. Test cooldown -> HALF_OPEN probe allowed
    const futureUs = Date.now() * 1000 + 150_000; // 150ms > 100ms cooldown
    const probeIntercept = tracker.interceptOutboundSocketTransfer(peer, futureUs);
    _strict.default.equal(probeIntercept.allowed, true);
    _strict.default.equal(probeIntercept.circuitBreakerState, "HALF_OPEN");

    // 4. Persistence across tracker reconstruction
    const tracker2 = new _ebpf_network_tracker.EbpfSocketNetworkTrackerAndBreaker(sql);
    const rehydratedTel = tracker2.getSocketTelemetry(peer);
    _strict.default.equal(rehydratedTel?.peerEndpoint, peer);
    _strict.default.ok(rehydratedTel?.totalTripsCount ?? 0 >= 1);
  });

  (0, _nodeTest.it)("[M75-2 / N5] Byzantine behavioral fingerprinting executes progressive reputation slashing and confines rogue agents to Quarantine Jail", () => {
    const sql = new _env.InMemoryCellSqlStorage();
    const arbiter = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter(sql);

    const agentId = "agent-malicious-009";
    arbiter.registerSwarmExpertAgent({
      agentId,
      specialtyRole: "DEVELOPER",
      supportedCapabilities: ["code_gen", "test_runner"],
      historicalSuccessRate: 0.9,
      costPer1kTokensUsd: 0.002,
      warmSandboxReady: true,
      activeTaskCount: 0,
      reputationScore: 90
    });

    // Violation 1: SLA_DEFECT_SPAM (-15 pts) -> 90 - 15 = 75
    const rec1 = arbiter.recordByzantineViolation({
      agentId,
      tenantId: "tenant-ares",
      violationType: "SLA_DEFECT_SPAM",
      evidence: { failedCfp: "cfp-spam-1" }
    });
    _strict.default.equal(rec1.slashedPoints, 15);
    _strict.default.equal(rec1.reputationAfter, 75);
    _strict.default.equal(rec1.inQuarantineJail, false);

    // Violation 2: DOUBLE_SIGN_CONTRADICTORY_VOTES (-50 pts) -> 75 - 50 = 25 (Quarantine Jail!)
    const rec2 = arbiter.recordByzantineViolation({
      agentId,
      tenantId: "tenant-ares",
      violationType: "DOUBLE_SIGN_CONTRADICTORY_VOTES",
      evidence: { proposalKey: "arch/zero-trust", signatures: ["sig1", "sig2"] }
    });
    _strict.default.equal(rec2.slashedPoints, 50);
    _strict.default.equal(rec2.reputationAfter, 25);
    _strict.default.equal(rec2.inQuarantineJail, true);

    const agentProfile = arbiter.getSwarmExpertAgent(agentId);
    _strict.default.equal(agentProfile?.isQuarantined, true);
    _strict.default.ok((agentProfile?.quarantineUntilMs ?? 0) > Date.now());

    arbiter.registerSwarmExpertAgent({
      agentId: "agent-healthy-peer",
      specialtyRole: "DEVELOPER",
      supportedCapabilities: ["code_gen"],
      historicalSuccessRate: 0.95,
      costPer1kTokensUsd: 0.002,
      warmSandboxReady: true,
      activeTaskCount: 0,
      reputationScore: 88
    });

    // Quarantined agent must fail bidding, while conforming peer is awarded
    const biddingRes = arbiter.matchContractNetBidding({
      task: {
        cfpId: "cfp-critical-fix",
        requiredRole: "DEVELOPER",
        requiredCapabilities: ["code_gen"],
        maxBudgetUsd: 1.0,
        slaDeadlineMs: 5000
      },
      bids: [
      {
        bidId: "bid-rogue",
        callForProposalId: "cfp-critical-fix",
        agentId,
        estimatedDurationMs: 2000,
        bidPriceUsd: 0.2,
        confidenceScore: 0.99
      },
      {
        bidId: "bid-healthy",
        callForProposalId: "cfp-critical-fix",
        agentId: "agent-healthy-peer",
        estimatedDurationMs: 3000,
        bidPriceUsd: 0.3,
        confidenceScore: 0.95
      }]

    });
    _strict.default.equal(biddingRes.awardedAgentId, "agent-healthy-peer");
    const rogueRanking = biddingRes.bidRankings.find((b) => b.agentId === agentId);
    _strict.default.equal(rogueRanking?.passedSlaAndBudget, false);
    _strict.default.equal(rogueRanking?.utilityScore, -100.0);

    // Pardon agent
    const pardonRes = arbiter.pardonQuarantinedAgent(agentId, "Security audited and sandbox cleaned", 70);
    _strict.default.equal(pardonRes.pardoned, true);
    _strict.default.equal(pardonRes.restoredReputation, 70);
    _strict.default.equal(arbiter.getSwarmExpertAgent(agentId)?.isQuarantined, false);

    // Persistence across arbiter reconstruction
    const arbiter2 = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter(sql);
    _strict.default.equal(arbiter2.getSwarmExpertAgent(agentId)?.reputationScore, 70);
    _strict.default.equal(arbiter2.listByzantineSlashingRecords().length, 2);
  });

  (0, _nodeTest.it)("[M75-3 / N6] Cross-Region Reed-Solomon Erasure Coding (K=4, M=2) reconstructs losslessly under 2 destroyed regions", () => {
    const sql = new _env.InMemoryCellSqlStorage();
    const manager = new _erasure_coder.CrossRegionErasureCodedWalManager(sql);

    const walId = "wal-cluster-audit-log-2026";
    const samplePayload = "HOT_PATH_TURN_WAL_TRANSACTION_PAYLOAD_WITH_CRC32_SHA256_INTEGRITY_CHECK_REED_SOLOMON_TEST_XYZ_1234567890";

    const manifest = manager.encodeWal(walId, samplePayload);
    _strict.default.equal(manifest.shards.length, 6);
    _strict.default.equal(manifest.dataShardsCount, 4);
    _strict.default.equal(manifest.parityShardsCount, 2);

    // Case 1: Destroy shards 0 and 2 (both data shards)
    const survivingCase1 = manifest.shards.filter((s) => s.shardIndex !== 0 && s.shardIndex !== 2);
    const rebuild1 = manager.reconstructWal(walId, survivingCase1);
    _strict.default.equal(rebuild1.rebuiltSuccessfully, true);
    _strict.default.equal(rebuild1.crc32Match, true);
    _strict.default.equal(rebuild1.reconstructedDataText, samplePayload);
    _strict.default.deepEqual(rebuild1.missingShardIndices, [0, 2]);

    // Case 2: Destroy shard 1 (data) and shard 5 (parity)
    const survivingCase2 = manifest.shards.filter((s) => s.shardIndex !== 1 && s.shardIndex !== 5);
    const rebuild2 = manager.reconstructWal(walId, survivingCase2);
    _strict.default.equal(rebuild2.rebuiltSuccessfully, true);
    _strict.default.equal(rebuild2.crc32Match, true);
    _strict.default.equal(rebuild2.reconstructedDataText, samplePayload);

    // Case 3: Destroy 3 shards (exceeds parity capacity M=2)
    const survivingCase3 = manifest.shards.filter((s) => s.shardIndex === 0 || s.shardIndex === 1 || s.shardIndex === 3);
    const rebuild3 = manager.reconstructWal(walId, survivingCase3);
    _strict.default.equal(rebuild3.rebuiltSuccessfully, false);
    _strict.default.equal(rebuild3.missingShardsCount, 3);

    // Persistence check
    const manager2 = new _erasure_coder.CrossRegionErasureCodedWalManager(sql);
    const rehydratedManifest = manager2.getManifest(walId);
    _strict.default.equal(rehydratedManifest?.walId, walId);
    _strict.default.equal(rehydratedManifest?.originalCrc32Hex, manifest.originalCrc32Hex);
  });

  (0, _nodeTest.it)("[M75-4] Live BFF endpoint POST /v1beta1/management/ops/phase2-network-slashing-erasure, Console UI & CLI mgmt-phase2-swarm", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m75-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      dataDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // Test CLI invoking the endpoint
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-phase2-swarm", "run_all_phase2_suite"], baseUrl);
      _strict.default.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);





      _strict.default.equal(body.allM75Phase2ChecksPassed, true);
      _strict.default.equal(body.n4EbpfBreaker.n4Verified, true);
      _strict.default.equal(body.n5ByzantineSlashing.n5Verified, true);
      _strict.default.equal(body.n6ErasureCoding.n6Verified, true);

      // Verify Console UI prototype JS
      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      _strict.default.equal(jsResp.status, 200);
      const jsText = await jsResp.text();
      _strict.default.equal(jsText.includes("open-ebpf-breaker"), true);
      _strict.default.equal(jsText.includes("open-byzantine-slashing"), true);
      _strict.default.equal(jsText.includes("open-erasure-coding"), true);

      // Verify HTML file bindings
      const htmlPath = "/working_dir/c_d9cff1d4633c1323/artifacts/file_generation/ttl=63d/output/ansts_console_management.html";
      if (fs.existsSync(htmlPath)) {
        const htmlContent = fs.readFileSync(htmlPath, "utf-8");
        _strict.default.equal(htmlContent.includes("open-ebpf-breaker"), true);
        _strict.default.equal(htmlContent.includes("open-byzantine-slashing"), true);
        _strict.default.equal(htmlContent.includes("open-erasure-coding"), true);
      }
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});