"use strict";














var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _blackboard_and_git_arbiter = require("../src/a2a/blackboard_and_git_arbiter.js");
var _deadlock_detector = require("../src/a2a/deadlock_detector.js");
var _env = require("../src/cloudflare/env.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M74: Phase 1 Multi-Agent Swarm Governance & Causal Consistency Suite
 * Verifies:
 * - [M74-1 / N1] Contract-Net Protocol (CNP) Multi-Agent Bidding & Task Allocation
 *   (`SharedBlackboardAndGitArbiter.registerSwarmExpertAgent`, `matchContractNetBidding`,
 *    SQLite persistence of `a2a_swarm_agents`, SLA/budget conformance filter, multi-attribute utility scoring).
 * - [M74-2 / N2] Byzantine-Fault-Tolerant (BFT) Reputation-Weighted Blackboard Consensus & Semantic Export Conflict Analyzer
 *   (`SharedBlackboardAndGitArbiter.putBftConsensusCas`, zero-veto from core authorities,
 *    `SharedBlackboardAndGitArbiter.analyzeSemanticExportConflicts` with AST regex export-table conflict detection).
 * - [M74-3 / N3] Lamport / Vector Clock Causal Barrier & Distributed Consistent Cut Verification
 *   (`wfg.advanceAgentVectorClock`, `wfg.recordCausalMessage`, `A2aWaitForGraphDeadlockDetector.compareVectorClocks`,
 *    `wfg.captureDistributedConsistentCut` with orphan message causal inversion detection).
 * - [M74-4] Live Console BFF (`POST /v1beta1/management/ops/phase1-swarm-governance`),
 *   Console UI bindings, and CLI (`anstsctl mgmt-phase1-swarm`).
 */(0, _nodeTest.describe)("M74 Phase 1 Multi-Agent Swarm Governance & Causal Consistency Suite", () => {(0, _nodeTest.it)("[M74-1 / N1] Contract-Net Protocol (CNP) matches expert agent bids via multi-attribute utility and filters SLA non-conformance", () => {const sql = new _env.InMemoryCellSqlStorage();const arbiter = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter(sql); // Register 3 expert agents
    arbiter.registerSwarmExpertAgent({ agentId: "agent-security-auditor", specialtyRole: "SECURITY_AUDITOR", supportedCapabilities: ["vulnerability_scan", "cve_patching", "threat_modeling"], historicalSuccessRate: 0.98, costPer1kTokensUsd: 0.003, warmSandboxReady: true, activeTaskCount: 1,
      reputationScore: 95
    });
    arbiter.registerSwarmExpertAgent({
      agentId: "agent-db-dba",
      specialtyRole: "DBA",
      supportedCapabilities: ["vulnerability_scan"],
      historicalSuccessRate: 0.85,
      costPer1kTokensUsd: 0.002,
      warmSandboxReady: true,
      activeTaskCount: 3,
      reputationScore: 88
    });
    arbiter.registerSwarmExpertAgent({
      agentId: "agent-junior-coder",
      specialtyRole: "DEVELOPER",
      supportedCapabilities: ["unit_tests"],
      historicalSuccessRate: 0.72,
      costPer1kTokensUsd: 0.0005,
      warmSandboxReady: false,
      activeTaskCount: 0,
      reputationScore: 55
    });

    // Task spec with strict SLA: max $0.50 budget, max 15000ms latency
    const matchRes = arbiter.matchContractNetBidding({
      task: {
        cfpId: "cfp-zero-day-audit-202611",
        requiredRole: "SECURITY_AUDITOR",
        requiredCapabilities: ["vulnerability_scan", "cve_patching"],
        maxBudgetUsd: 0.5,
        slaDeadlineMs: 15000
      },
      bids: [
      {
        bidId: "bid-sec-01",
        callForProposalId: "cfp-zero-day-audit-202611",
        agentId: "agent-security-auditor",
        estimatedDurationMs: 8500,
        bidPriceUsd: 0.35,
        confidenceScore: 0.97
      },
      {
        bidId: "bid-dba-02",
        callForProposalId: "cfp-zero-day-audit-202611",
        agentId: "agent-db-dba",
        estimatedDurationMs: 12000,
        bidPriceUsd: 0.45,
        confidenceScore: 0.65
      },
      {
        bidId: "bid-jr-03",
        callForProposalId: "cfp-zero-day-audit-202611",
        agentId: "agent-junior-coder",
        estimatedDurationMs: 25000, // SLA breached (> 15000ms)
        bidPriceUsd: 0.1,
        confidenceScore: 0.5
      }]

    });

    _strict.default.equal(matchRes.awardedAgentId, "agent-security-auditor");
    _strict.default.equal(matchRes.winningBidId, "bid-sec-01");
    _strict.default.ok(matchRes.utilityScore >= 80);
    _strict.default.equal(matchRes.bidsEvaluated, 3);

    // Verify SLA non-conforming bids
    const juniorRanking = matchRes.bidRankings.find((b) => b.agentId === "agent-junior-coder");
    _strict.default.equal(juniorRanking?.passedSlaAndBudget, false);
    _strict.default.equal(juniorRanking?.utilityScore, -100.0);

    // Verify persistence across arbiter reconstruction
    const arbiter2 = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter(sql);
    const agentSec = arbiter2.getSwarmExpertAgent("agent-security-auditor");
    _strict.default.equal(agentSec?.reputationScore, 95);
    _strict.default.equal(agentSec?.warmSandboxReady, true);
    _strict.default.equal(arbiter2.listSwarmAgents().length, 3);
  });

  (0, _nodeTest.it)("[M74-2 / N2] BFT reputation-weighted blackboard consensus respects quorum and core authority vetoes; semantic export analyzer detects conflicts", () => {
    const sql = new _env.InMemoryCellSqlStorage();
    const arbiter = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter(sql);

    arbiter.registerSwarmExpertAgent({
      agentId: "agent-core-sec",
      specialtyRole: "SECURITY_LEAD",
      supportedCapabilities: ["security"],
      historicalSuccessRate: 1.0,
      costPer1kTokensUsd: 0.003,
      warmSandboxReady: true,
      activeTaskCount: 0,
      reputationScore: 98 // Core authority (>= 95)
    });
    arbiter.registerSwarmExpertAgent({
      agentId: "agent-sre",
      specialtyRole: "SRE",
      supportedCapabilities: ["ops"],
      historicalSuccessRate: 0.95,
      costPer1kTokensUsd: 0.002,
      warmSandboxReady: true,
      activeTaskCount: 1,
      reputationScore: 90
    });
    arbiter.registerSwarmExpertAgent({
      agentId: "agent-qa",
      specialtyRole: "QA",
      supportedCapabilities: ["test"],
      historicalSuccessRate: 0.9,
      costPer1kTokensUsd: 0.001,
      warmSandboxReady: false,
      activeTaskCount: 0,
      reputationScore: 80
    });

    // Case A: 2/3 quorum achieved, no core veto -> accepted
    const approvedRes = arbiter.putBftConsensusCas({
      key: "arch/zero-trust-fence",
      candidateValue: { mode: "ENFORCING", egressAllowlist: ["api.openai.com", "api.anthropic.com"] },
      expectedVersion: 0,
      initiatorAgentId: "agent-sre",
      votes: [
      { agentId: "agent-core-sec", approve: true, rationale: "Strict egress allowed" },
      { agentId: "agent-sre", approve: true, rationale: "SRE compliant" },
      { agentId: "agent-qa", approve: false, rationale: "Needs mock suite" }]

    });
    _strict.default.equal(approvedRes.committed, true);
    _strict.default.equal(approvedRes.quorumPassed, true);
    _strict.default.equal(approvedRes.newVersion, 1);
    _strict.default.equal(arbiter.get("arch/zero-trust-fence")?.version, 1);

    // Case B: Core authority vetoes -> rejected even if other votes are positive
    const vetoedRes = arbiter.putBftConsensusCas({
      key: "arch/zero-trust-fence",
      candidateValue: { mode: "PERMISSIVE", egressAllowlist: ["*"] },
      expectedVersion: 1,
      initiatorAgentId: "agent-qa",
      votes: [
      { agentId: "agent-qa", approve: true },
      { agentId: "agent-sre", approve: true },
      { agentId: "agent-core-sec", approve: false, rationale: "Wildcard egress prohibited" }]

    });
    _strict.default.equal(vetoedRes.committed, false);
    _strict.default.equal(vetoedRes.vetoedByCoreAuthority, true);
    _strict.default.ok(vetoedRes.dissentingAgents.includes("agent-core-sec"));

    // Case C: Semantic export conflict analysis
    const semanticResClean = arbiter.analyzeSemanticExportConflicts({
      filePath: "src/utils/math.ts",
      agentPatches: [
      { agentId: "agent-qa", patchText: "export const clamp = (v: number) => Math.min(1, v);" },
      { agentId: "agent-sre", patchText: "export const movingAvg = (arr: number[]) => arr.reduce((a,b)=>a+b)/arr.length;" }]

    });
    _strict.default.equal(semanticResClean.semanticClean, true);
    _strict.default.equal(semanticResClean.conflictingSymbols.length, 0);

    const semanticResConflict = arbiter.analyzeSemanticExportConflicts({
      filePath: "src/utils/math.ts",
      agentPatches: [
      { agentId: "agent-qa", patchText: "export const calculateScore = (a: number) => a * 2;" },
      { agentId: "agent-sre", patchText: "export const calculateScore = (a: string) => Number(a);" }]

    });
    _strict.default.equal(semanticResConflict.semanticClean, false);
    _strict.default.deepEqual(semanticResConflict.conflictingSymbols, ["calculateScore"]);

    // Rehydration check
    const arbiterRehydrated = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter(sql);
    _strict.default.equal(arbiterRehydrated.get("arch/zero-trust-fence")?.version, 1);
  });

  (0, _nodeTest.it)("[M74-3 / N3] Vector clock tracks causal progression, compares happens-before, and validates distributed consistent cuts", () => {
    const wfg = new _deadlock_detector.A2aWaitForGraphDeadlockDetector();
    const agA = "agent-alpha";
    const agB = "agent-beta";

    // Agent A advances clock and sends msg-1 to Agent B
    wfg.advanceAgentVectorClock(agA); // A: { alpha: 1 }
    wfg.recordCausalMessage({
      eventId: "msg-1",
      senderAgentId: agA,
      recipientAgentId: agB,
      vectorClock: wfg.getAgentVectorClock(agA),
      payloadType: "TASK_DELEGATION",
      logicalTurn: 1,
      payloadDigest: "dig-1",
      timestampMs: 1700000000000
    });

    // Agent B receives msg-1 and sends msg-2 to Agent A
    wfg.recordCausalMessage({
      eventId: "msg-2",
      senderAgentId: agB,
      recipientAgentId: agA,
      vectorClock: wfg.getAgentVectorClock(agB),
      payloadType: "INTERMEDIATE_RESULT",
      logicalTurn: 2,
      payloadDigest: "dig-2",
      timestampMs: 1700000001000
    });

    // Compare vector clocks
    const cmpOrder = _deadlock_detector.A2aWaitForGraphDeadlockDetector.compareVectorClocks(
    { [agA]: 1 },
    { [agA]: 1, [agB]: 1 });

    _strict.default.equal(cmpOrder, -1); // happens-before

    const cmpAfter = _deadlock_detector.A2aWaitForGraphDeadlockDetector.compareVectorClocks(
    { [agA]: 1, [agB]: 1 },
    { [agA]: 1 });

    _strict.default.equal(cmpAfter, 1); // happens-after

    const cmpIdentical = _deadlock_detector.A2aWaitForGraphDeadlockDetector.compareVectorClocks(
    { [agA]: 1, [agB]: 1 },
    { [agA]: 1, [agB]: 1 });

    _strict.default.equal(cmpIdentical, 0); // identical

    const cmpConcurrent = _deadlock_detector.A2aWaitForGraphDeadlockDetector.compareVectorClocks(
    { [agA]: 2, [agB]: 0 },
    { [agA]: 0, [agB]: 2 });

    _strict.default.equal(cmpConcurrent, null); // concurrent (incomparable)

    // Consistent cut: both agents at turn 2 (both sends and receives included)
    const cutConsistent = wfg.captureDistributedConsistentCut({
      cutId: "cut-consistent-t2",
      agentCutTurns: { [agA]: 2, [agB]: 2 }
    });
    _strict.default.equal(cutConsistent.isCausallyConsistent, true);
    _strict.default.equal(cutConsistent.violatedCausalInversions.length, 0);

    // Inconsistent cut: agent A at turn 0, agent B at turn 2 (orphan receive of msg-1 at B)
    const cutInconsistent = wfg.captureDistributedConsistentCut({
      cutId: "cut-inconsistent-t0",
      agentCutTurns: { [agA]: 0, [agB]: 2 }
    });
    _strict.default.equal(cutInconsistent.isCausallyConsistent, false);
    _strict.default.ok(cutInconsistent.violatedCausalInversions.length >= 1);
  });

  (0, _nodeTest.it)("[M74-4] Live BFF endpoint POST /v1beta1/management/ops/phase1-swarm-governance, Console UI & CLI mgmt-phase1-swarm", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m74-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      dataDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // Test CLI invoking the endpoint
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-phase1-swarm", "run_all_phase1_swarm_suite"], baseUrl);
      _strict.default.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);





      _strict.default.equal(body.allM74Phase1SwarmChecksPassed, true);
      _strict.default.equal(body.n1ContractNet.n1Verified, true);
      _strict.default.equal(body.n1ContractNet.matchRes.awardedAgentId, "agent-security-auditor");
      _strict.default.equal(body.n2BftAndSemantic.n2Verified, true);
      _strict.default.equal(body.n2BftAndSemantic.approvedRes.committed, true);
      _strict.default.equal(body.n3VectorClockCut.n3Verified, true);
      _strict.default.equal(body.n3VectorClockCut.cutConsistent.isCausallyConsistent, true);

      // Verify Console UI prototype JS
      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      _strict.default.equal(jsResp.status, 200);
      const jsText = await jsResp.text();
      _strict.default.equal(jsText.includes("open-contract-net"), true);
      _strict.default.equal(jsText.includes("open-bft-consensus"), true);
      _strict.default.equal(jsText.includes("open-causal-vector-cut"), true);

      // Verify HTML file bindings
      const htmlPath = "/working_dir/c_d9cff1d4633c1323/artifacts/file_generation/ttl=63d/output/ansts_console_management.html";
      if (fs.existsSync(htmlPath)) {
        const htmlContent = fs.readFileSync(htmlPath, "utf-8");
        _strict.default.equal(htmlContent.includes("open-contract-net"), true);
        _strict.default.equal(htmlContent.includes("open-bft-consensus"), true);
        _strict.default.equal(htmlContent.includes("open-causal-vector-cut"), true);
      }
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});