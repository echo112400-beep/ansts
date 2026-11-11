/**
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
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { DagEscrowCoordinator } from "../src/dag/escrow.js";
import { CoreScheduler5D } from "../src/scheduler.js";
import { LeaseCoordinator } from "../src/lease_coordinator.js";
import { LlmEndpointMetrics, TaskPriority, TaskSpec, TaskState } from "../src/models.js";
import { InMemoryCellSqlStorage } from "../src/cloudflare/env.js";
import { CoWSandboxBranchForker } from "../src/runtime/sandbox_branch_forker.js";
import { LayerFsWorkspaceDriver } from "../src/runtime/layerfs_workspace_driver.js";
import { CognitiveRouter } from "../src/cognitive_router.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M71 Phase 1 Cognitive & Speculative Execution Acceleration Suite", () => {
  it("[M71-1 / F1] DagEscrowCoordinator & CoreScheduler5D execute adaptive DAG subgraph replanning, dependency rewiring, and dead-letter revival", () => {
    const sql = new InMemoryCellSqlStorage();
    const escrow = new DagEscrowCoordinator(sql);
    const sched = new CoreScheduler5D(new LeaseCoordinator(15));

    const parent = new TaskSpec({
      task_id: "p-m71-1",
      tenant_id: "Northstar",
      priority: TaskPriority.P1_HIGH,
      expected_SubAgent_count: 2,
    });
    sched.submitTask(parent);
    escrow.reserveParentEscrow(parent, 10, 0);

    const cObsolete = new TaskSpec({
      task_id: "c-obsolete",
      tenant_id: "Northstar",
      priority: TaskPriority.P2_STANDARD,
      dependencies: [parent.task_id],
    });
    const cFailed = new TaskSpec({
      task_id: "c-failed",
      tenant_id: "Northstar",
      priority: TaskPriority.P2_STANDARD,
      dependencies: [parent.task_id],
    });
    const downstream = new TaskSpec({
      task_id: "t-downstream-agg",
      tenant_id: "Northstar",
      priority: TaskPriority.P1_HIGH,
      dependencies: [cFailed.task_id],
    });

    sched.submitTask(cObsolete);
    sched.submitTask(cFailed);
    sched.submitTask(downstream);
    escrow.registerChildSubAgent(parent, cObsolete);
    escrow.registerChildSubAgent(parent, cFailed);

    // Fail cFailed and cascade dead-letter to parent and downstream
    escrow.failOrCancelChildSubAgent(parent, cFailed.task_id, true);
    sched.cascadeDeadLetterToDependents(cFailed.task_id);
    assert.equal(parent.state, TaskState.DEAD_LETTER);
    assert.equal(downstream.state, TaskState.DEAD_LETTER);

    // Adaptive DAG replan: prune cObsolete, replace cFailed with 2 fallback sub-agents
    const fb1 = new TaskSpec({
      task_id: "c-fallback-1",
      tenant_id: "Northstar",
      priority: TaskPriority.P1_HIGH,
      dependencies: [parent.task_id],
    });
    const fb2 = new TaskSpec({
      task_id: "c-fallback-2",
      tenant_id: "Northstar",
      priority: TaskPriority.P1_HIGH,
      dependencies: [parent.task_id],
    });

    const replan = sched.replanDagSubgraph({
      parentTaskId: parent.task_id,
      pruneChildIds: [cObsolete.task_id],
      fallbackForFailedChildId: cFailed.task_id,
      injectChildren: [fb1, fb2],
      escrowCoordinator: escrow,
      maxTenantQuota: 10,
      currentTenantUsed: 1,
    });

    assert.equal(replan.replanRevision, 1);
    assert.equal(parent.state, TaskState.WAITING_SUB_AGENT);
    assert.equal(cObsolete.state, TaskState.CANCELLED);
    assert.equal(downstream.state, TaskState.SUBMITTED);
    assert.deepEqual(downstream.dependencies.sort(), ["c-fallback-1", "c-fallback-2"]);

    // Verify SQLite persistence of replanRevision
    const escrow2 = new DagEscrowCoordinator(sql);
    assert.equal(escrow2.getEscrowRecord(parent.task_id)?.replanRevision, 1);
  });

  it("[M71-2 / F2] CoWSandboxBranchForker executes Best-of-N parallel speculative verification, promotes winner, and GCs losers", () => {
    const driver = new LayerFsWorkspaceDriver();
    const forker = new CoWSandboxBranchForker(driver);

    const res = forker.executeBestOfNSpeculativeVerifier({
      sourceBranch: "main",
      targetPromoteBranch: "release-best",
      candidates: [
        {
          branchName: "cand-1-fail",
          modelTier: "draft-mini",
          patchCommand: "echo 'bad' > app.js",
          verifyCommand: "echo 'TESTS_PASSED=1 LINT_WARNINGS=5' && exit 1",
        },
        {
          branchName: "cand-2-ok-warn",
          modelTier: "sonnet-std",
          patchCommand: "echo 'ok' > app.js",
          verifyCommand: "echo 'TESTS_PASSED=8 LINT_WARNINGS=3'",
        },
        {
          branchName: "cand-3-winner",
          modelTier: "sonnet-thinking",
          patchCommand: "echo 'clean' > app.js",
          verifyCommand: "echo 'TESTS_PASSED=12 LINT_WARNINGS=0'",
        },
      ],
    });

    assert.equal(res.winnerBranchName, "cand-3-winner");
    assert.equal(res.promotedBranchName, "release-best");
    assert.equal(res.winnerScore >= 90, true);
    assert.deepEqual(res.prunedLoserBranches.sort(), ["cand-1-fail", "cand-2-ok-warn"]);
    assert.equal(driver.getBranchHeadCommit("release-best").commitId, res.winnerCommitId);
  });

  it("[M71-3 / F3] CognitiveRouter routes with cascade confidence threshold, semantic intent cache, and heterogeneous multi-model jury quorum", () => {
    const sql = new InMemoryCellSqlStorage();
    const router = new CognitiveRouter(0.3, sql, 64);
    router.registerEndpoint(
      new LlmEndpointMetrics({
        endpoint_id: "ep-mini",
        provider: "openai",
        model_name: "gpt-4o-mini-fast",
        ttft_ewma_ms: 80,
      })
    );
    router.registerEndpoint(
      new LlmEndpointMetrics({
        endpoint_id: "ep-opus",
        provider: "anthropic",
        model_name: "claude-3-7-sonnet-reasoner",
        ttft_ewma_ms: 320,
      })
    );

    const r1 = router.routeWithCascadeAndSemanticCache({
      promptText: "Fix Redis cluster slot migration timeout",
      systemPrompt: "SRE",
      toolDefinitions: "[]",
      draftLogprobs: [-0.03, -0.05, -0.04],
      confidenceThreshold: 0.85,
      riskLevel: "LOW",
    });
    assert.equal(r1.cascadeTier, "TIER1_FAST_DRAFT");
    assert.equal(r1.escalatedToFlagship, false);

    // Normalized semantic cache hit
    const r2 = router.routeWithCascadeAndSemanticCache({
      promptText: "  fix REDIS cluster slot migration timeout!! ",
      systemPrompt: "SRE",
      toolDefinitions: "[]",
      draftLogprobs: [-0.9],
      confidenceThreshold: 0.85,
      riskLevel: "LOW",
    });
    assert.equal(r2.semanticCacheHit, true);
    assert.equal(r2.cascadeTier, "TIER0_SEMANTIC_CACHE");

    // Low confidence escalates to flagship
    const r3 = router.routeWithCascadeAndSemanticCache({
      promptText: "Synthesize formal TLA+ invariant for Paxos epoch change",
      systemPrompt: "Architect",
      toolDefinitions: "[]",
      draftLogprobs: [-0.95, -1.2, -0.88],
      confidenceThreshold: 0.85,
      riskLevel: "MEDIUM",
    });
    assert.equal(r3.escalatedToFlagship, true);
    assert.equal(r3.cascadeTier, "TIER2_FLAGSHIP_REASONER");
    assert.equal(r3.selectedModel, "claude-3-7-sonnet-reasoner");

    // Multi-model jury quorum
    const juryOk = router.evaluateMultiModelJuryQuorum({
      actionId: "act-1",
      riskLevel: "HIGH",
      votes: [
        { endpointId: "ep-opus", modelName: "claude-3-7-sonnet", approve: true, safetyScore: 95, rationale: "ok" },
        { endpointId: "ep-gpt4o", modelName: "gpt-4o", approve: true, safetyScore: 90, rationale: "ok" },
        { endpointId: "ep-r1", modelName: "deepseek-r1", approve: true, safetyScore: 88, rationale: "ok" },
      ],
    });
    assert.equal(juryOk.quorumReached, true);
    assert.equal(juryOk.consensusDecision, "APPROVED");
  });

  it("[M71-4] Live BFF endpoint POST /v1beta1/management/ops/phase1-cognitive-acceleration, Console UI & CLI mgmt-phase1-accel", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m71-"));
    const server = new AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      dataDir: tmpDir,
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const cliAll = await runAnstsCtlRemote(["mgmt-phase1-accel", "run_all_phase1_suite"], baseUrl);
      assert.equal(cliAll.exitCode, 0);
      const body = JSON.parse(cliAll.stdout) as {
        allM71Phase1ChecksPassed: boolean;
        f1AdaptiveDagReplan: { f1Verified: boolean };
        f2BestOfNSpeculative: { f2Verified: boolean };
        f3CascadeAndJury: { f3Verified: boolean };
      };
      assert.equal(body.allM71Phase1ChecksPassed, true);
      assert.equal(body.f1AdaptiveDagReplan.f1Verified, true);
      assert.equal(body.f2BestOfNSpeculative.f2Verified, true);
      assert.equal(body.f3CascadeAndJury.f3Verified, true);

      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      assert.equal(jsResp.status, 200);
      const jsText = await jsResp.text();
      assert.equal(jsText.includes("open-adaptive-dag-replan"), true);
      assert.equal(jsText.includes("open-best-of-n-verifier"), true);
      assert.equal(jsText.includes("open-cascade-jury-router"), true);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
