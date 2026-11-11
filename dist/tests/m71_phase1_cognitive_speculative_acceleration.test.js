"use strict";














var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _escrow = require("../src/dag/escrow.js");
var _scheduler = require("../src/scheduler.js");
var _lease_coordinator = require("../src/lease_coordinator.js");
var _models = require("../src/models.js");
var _env = require("../src/cloudflare/env.js");
var _sandbox_branch_forker = require("../src/runtime/sandbox_branch_forker.js");
var _layerfs_workspace_driver = require("../src/runtime/layerfs_workspace_driver.js");
var _cognitive_router = require("../src/cognitive_router.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M71: Phase 1 Cognitive & Speculative Execution Acceleration Suite
 * Verifies:
 * - [M71-1 / F1] Adaptive DAG Subgraph Re-planner (`DagEscrowCoordinator.replanSubgraph` & `CoreScheduler5D.replanDagSubgraph`):
 *   subgraph pruning, dynamic fan-out, fallback replacement for failed child sub-agents, downstream dependency rewiring,
 *   dead-letter revival, and SQLite `replanRevision` persistence.
 * - [M71-2 / F2] Best-of-N Parallel LayerFS CoW Sandbox Branching & Verifier (`CoWSandboxBranchForker.executeBestOfNSpeculativeVerifier`):
 *   zero-copy multi-candidate branching, deterministic execution & test/lint scoring, fast-forward promotion of winning branch,
 *   and automatic GC of losing branches & unreachable commits.
 * - [M71-3 / F3] Cascade Confidence Router, Semantic Intent Cache & Heterogeneous Multi-Model Jury (`CognitiveRouter`):
 *   logprob confidence thresholding (`TIER1_FAST_DRAFT` vs `TIER2_FLAGSHIP_REASONER`), normalized semantic intent cache (`TIER0_SEMANTIC_CACHE`),
 *   SQLite cold-start persistence, and multi-model quorum consensus (`APPROVED` vs `REJECTED_BY_JURY`).
 * - [M71-4] Live Console BFF (`POST /v1beta1/management/ops/phase1-cognitive-acceleration`), Console UI bindings,
 *   and CLI (`anstsctl mgmt-phase1-accel`).
 */(0, _nodeTest.describe)("M71 Phase 1 Cognitive & Speculative Execution Acceleration Suite", () => {(0, _nodeTest.it)("[M71-1 / F1] DagEscrowCoordinator & CoreScheduler5D execute adaptive DAG subgraph replanning, dependency rewiring, and dead-letter revival", () => {const sql = new _env.InMemoryCellSqlStorage();const escrow = new _escrow.DagEscrowCoordinator(sql);const sched = new _scheduler.CoreScheduler5D(new _lease_coordinator.LeaseCoordinator(15));const parent = new _models.TaskSpec({ task_id: "p-m71-1", tenant_id: "Northstar", priority: _models.TaskPriority.P1_HIGH, expected_SubAgent_count: 2 });sched.submitTask(parent);
    escrow.reserveParentEscrow(parent, 10, 0);

    const cObsolete = new _models.TaskSpec({
      task_id: "c-obsolete",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P2_STANDARD,
      dependencies: [parent.task_id]
    });
    const cFailed = new _models.TaskSpec({
      task_id: "c-failed",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P2_STANDARD,
      dependencies: [parent.task_id]
    });
    const downstream = new _models.TaskSpec({
      task_id: "t-downstream-agg",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P1_HIGH,
      dependencies: [cFailed.task_id]
    });

    sched.submitTask(cObsolete);
    sched.submitTask(cFailed);
    sched.submitTask(downstream);
    escrow.registerChildSubAgent(parent, cObsolete);
    escrow.registerChildSubAgent(parent, cFailed);

    // Fail cFailed and cascade dead-letter to parent and downstream
    escrow.failOrCancelChildSubAgent(parent, cFailed.task_id, true);
    sched.cascadeDeadLetterToDependents(cFailed.task_id);
    _strict.default.equal(parent.state, _models.TaskState.DEAD_LETTER);
    _strict.default.equal(downstream.state, _models.TaskState.DEAD_LETTER);

    // Adaptive DAG replan: prune cObsolete, replace cFailed with 2 fallback sub-agents
    const fb1 = new _models.TaskSpec({
      task_id: "c-fallback-1",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P1_HIGH,
      dependencies: [parent.task_id]
    });
    const fb2 = new _models.TaskSpec({
      task_id: "c-fallback-2",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P1_HIGH,
      dependencies: [parent.task_id]
    });

    const replan = sched.replanDagSubgraph({
      parentTaskId: parent.task_id,
      pruneChildIds: [cObsolete.task_id],
      fallbackForFailedChildId: cFailed.task_id,
      injectChildren: [fb1, fb2],
      escrowCoordinator: escrow,
      maxTenantQuota: 10,
      currentTenantUsed: 1
    });

    _strict.default.equal(replan.replanRevision, 1);
    _strict.default.equal(parent.state, _models.TaskState.WAITING_SUB_AGENT);
    _strict.default.equal(cObsolete.state, _models.TaskState.CANCELLED);
    _strict.default.equal(downstream.state, _models.TaskState.SUBMITTED);
    _strict.default.deepEqual(downstream.dependencies.sort(), ["c-fallback-1", "c-fallback-2"]);

    // Verify SQLite persistence of replanRevision
    const escrow2 = new _escrow.DagEscrowCoordinator(sql);
    _strict.default.equal(escrow2.getEscrowRecord(parent.task_id)?.replanRevision, 1);
  });

  (0, _nodeTest.it)("[M71-2 / F2] CoWSandboxBranchForker executes Best-of-N parallel speculative verification, promotes winner, and GCs losers", () => {
    const driver = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    const forker = new _sandbox_branch_forker.CoWSandboxBranchForker(driver);

    const res = forker.executeBestOfNSpeculativeVerifier({
      sourceBranch: "main",
      targetPromoteBranch: "release-best",
      candidates: [
      {
        branchName: "cand-1-fail",
        modelTier: "draft-mini",
        patchCommand: "echo 'bad' > app.js",
        verifyCommand: "echo 'TESTS_PASSED=1 LINT_WARNINGS=5' && exit 1"
      },
      {
        branchName: "cand-2-ok-warn",
        modelTier: "sonnet-std",
        patchCommand: "echo 'ok' > app.js",
        verifyCommand: "echo 'TESTS_PASSED=8 LINT_WARNINGS=3'"
      },
      {
        branchName: "cand-3-winner",
        modelTier: "sonnet-thinking",
        patchCommand: "echo 'clean' > app.js",
        verifyCommand: "echo 'TESTS_PASSED=12 LINT_WARNINGS=0'"
      }]

    });

    _strict.default.equal(res.winnerBranchName, "cand-3-winner");
    _strict.default.equal(res.promotedBranchName, "release-best");
    _strict.default.equal(res.winnerScore >= 90, true);
    _strict.default.deepEqual(res.prunedLoserBranches.sort(), ["cand-1-fail", "cand-2-ok-warn"]);
    _strict.default.equal(driver.getBranchHeadCommit("release-best").commitId, res.winnerCommitId);
  });

  (0, _nodeTest.it)("[M71-3 / F3] CognitiveRouter routes with cascade confidence threshold, semantic intent cache, and heterogeneous multi-model jury quorum", () => {
    const sql = new _env.InMemoryCellSqlStorage();
    const router = new _cognitive_router.CognitiveRouter(0.3, sql, 64);
    router.registerEndpoint(
    new _models.LlmEndpointMetrics({
      endpoint_id: "ep-mini",
      provider: "openai",
      model_name: "gpt-4o-mini-fast",
      ttft_ewma_ms: 80
    }));

    router.registerEndpoint(
    new _models.LlmEndpointMetrics({
      endpoint_id: "ep-opus",
      provider: "anthropic",
      model_name: "claude-3-7-sonnet-reasoner",
      ttft_ewma_ms: 320
    }));


    const r1 = router.routeWithCascadeAndSemanticCache({
      promptText: "Fix Redis cluster slot migration timeout",
      systemPrompt: "SRE",
      toolDefinitions: "[]",
      draftLogprobs: [-0.03, -0.05, -0.04],
      confidenceThreshold: 0.85,
      riskLevel: "LOW"
    });
    _strict.default.equal(r1.cascadeTier, "TIER1_FAST_DRAFT");
    _strict.default.equal(r1.escalatedToFlagship, false);

    // Normalized semantic cache hit
    const r2 = router.routeWithCascadeAndSemanticCache({
      promptText: "  fix REDIS cluster slot migration timeout!! ",
      systemPrompt: "SRE",
      toolDefinitions: "[]",
      draftLogprobs: [-0.9],
      confidenceThreshold: 0.85,
      riskLevel: "LOW"
    });
    _strict.default.equal(r2.semanticCacheHit, true);
    _strict.default.equal(r2.cascadeTier, "TIER0_SEMANTIC_CACHE");

    // Low confidence escalates to flagship
    const r3 = router.routeWithCascadeAndSemanticCache({
      promptText: "Synthesize formal TLA+ invariant for Paxos epoch change",
      systemPrompt: "Architect",
      toolDefinitions: "[]",
      draftLogprobs: [-0.95, -1.2, -0.88],
      confidenceThreshold: 0.85,
      riskLevel: "MEDIUM"
    });
    _strict.default.equal(r3.escalatedToFlagship, true);
    _strict.default.equal(r3.cascadeTier, "TIER2_FLAGSHIP_REASONER");
    _strict.default.equal(r3.selectedModel, "claude-3-7-sonnet-reasoner");

    // Multi-model jury quorum
    const juryOk = router.evaluateMultiModelJuryQuorum({
      actionId: "act-1",
      riskLevel: "HIGH",
      votes: [
      { endpointId: "ep-opus", modelName: "claude-3-7-sonnet", approve: true, safetyScore: 95, rationale: "ok" },
      { endpointId: "ep-gpt4o", modelName: "gpt-4o", approve: true, safetyScore: 90, rationale: "ok" },
      { endpointId: "ep-r1", modelName: "deepseek-r1", approve: true, safetyScore: 88, rationale: "ok" }]

    });
    _strict.default.equal(juryOk.quorumReached, true);
    _strict.default.equal(juryOk.consensusDecision, "APPROVED");
  });

  (0, _nodeTest.it)("[M71-4] Live BFF endpoint POST /v1beta1/management/ops/phase1-cognitive-acceleration, Console UI & CLI mgmt-phase1-accel", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m71-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      dataDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const cliAll = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-phase1-accel", "run_all_phase1_suite"], baseUrl);
      _strict.default.equal(cliAll.exitCode, 0);
      const body = JSON.parse(cliAll.stdout);





      _strict.default.equal(body.allM71Phase1ChecksPassed, true);
      _strict.default.equal(body.f1AdaptiveDagReplan.f1Verified, true);
      _strict.default.equal(body.f2BestOfNSpeculative.f2Verified, true);
      _strict.default.equal(body.f3CascadeAndJury.f3Verified, true);

      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      _strict.default.equal(jsResp.status, 200);
      const jsText = await jsResp.text();
      _strict.default.equal(jsText.includes("open-adaptive-dag-replan"), true);
      _strict.default.equal(jsText.includes("open-best-of-n-verifier"), true);
      _strict.default.equal(jsText.includes("open-cascade-jury-router"), true);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});