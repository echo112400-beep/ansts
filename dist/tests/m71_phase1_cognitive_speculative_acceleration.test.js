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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
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
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const escrow_js_1 = require("../src/dag/escrow.js");
const scheduler_js_1 = require("../src/scheduler.js");
const lease_coordinator_js_1 = require("../src/lease_coordinator.js");
const models_js_1 = require("../src/models.js");
const env_js_1 = require("../src/cloudflare/env.js");
const sandbox_branch_forker_js_1 = require("../src/runtime/sandbox_branch_forker.js");
const layerfs_workspace_driver_js_1 = require("../src/runtime/layerfs_workspace_driver.js");
const cognitive_router_js_1 = require("../src/cognitive_router.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M71 Phase 1 Cognitive & Speculative Execution Acceleration Suite", () => {
    (0, node_test_1.it)("[M71-1 / F1] DagEscrowCoordinator & CoreScheduler5D execute adaptive DAG subgraph replanning, dependency rewiring, and dead-letter revival", () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const escrow = new escrow_js_1.DagEscrowCoordinator(sql);
        const sched = new scheduler_js_1.CoreScheduler5D(new lease_coordinator_js_1.LeaseCoordinator(15));
        const parent = new models_js_1.TaskSpec({
            task_id: "p-m71-1",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P1_HIGH,
            expected_SubAgent_count: 2,
        });
        sched.submitTask(parent);
        escrow.reserveParentEscrow(parent, 10, 0);
        const cObsolete = new models_js_1.TaskSpec({
            task_id: "c-obsolete",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P2_STANDARD,
            dependencies: [parent.task_id],
        });
        const cFailed = new models_js_1.TaskSpec({
            task_id: "c-failed",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P2_STANDARD,
            dependencies: [parent.task_id],
        });
        const downstream = new models_js_1.TaskSpec({
            task_id: "t-downstream-agg",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P1_HIGH,
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
        strict_1.default.equal(parent.state, models_js_1.TaskState.DEAD_LETTER);
        strict_1.default.equal(downstream.state, models_js_1.TaskState.DEAD_LETTER);
        // Adaptive DAG replan: prune cObsolete, replace cFailed with 2 fallback sub-agents
        const fb1 = new models_js_1.TaskSpec({
            task_id: "c-fallback-1",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P1_HIGH,
            dependencies: [parent.task_id],
        });
        const fb2 = new models_js_1.TaskSpec({
            task_id: "c-fallback-2",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P1_HIGH,
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
        strict_1.default.equal(replan.replanRevision, 1);
        strict_1.default.equal(parent.state, models_js_1.TaskState.WAITING_SUB_AGENT);
        strict_1.default.equal(cObsolete.state, models_js_1.TaskState.CANCELLED);
        strict_1.default.equal(downstream.state, models_js_1.TaskState.SUBMITTED);
        strict_1.default.deepEqual(downstream.dependencies.sort(), ["c-fallback-1", "c-fallback-2"]);
        // Verify SQLite persistence of replanRevision
        const escrow2 = new escrow_js_1.DagEscrowCoordinator(sql);
        strict_1.default.equal(escrow2.getEscrowRecord(parent.task_id)?.replanRevision, 1);
    });
    (0, node_test_1.it)("[M71-2 / F2] CoWSandboxBranchForker executes Best-of-N parallel speculative verification, promotes winner, and GCs losers", () => {
        const driver = new layerfs_workspace_driver_js_1.LayerFsWorkspaceDriver();
        const forker = new sandbox_branch_forker_js_1.CoWSandboxBranchForker(driver);
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
        strict_1.default.equal(res.winnerBranchName, "cand-3-winner");
        strict_1.default.equal(res.promotedBranchName, "release-best");
        strict_1.default.equal(res.winnerScore >= 90, true);
        strict_1.default.deepEqual(res.prunedLoserBranches.sort(), ["cand-1-fail", "cand-2-ok-warn"]);
        strict_1.default.equal(driver.getBranchHeadCommit("release-best").commitId, res.winnerCommitId);
    });
    (0, node_test_1.it)("[M71-3 / F3] CognitiveRouter routes with cascade confidence threshold, semantic intent cache, and heterogeneous multi-model jury quorum", () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const router = new cognitive_router_js_1.CognitiveRouter(0.3, sql, 64);
        router.registerEndpoint(new models_js_1.LlmEndpointMetrics({
            endpoint_id: "ep-mini",
            provider: "openai",
            model_name: "gpt-4o-mini-fast",
            ttft_ewma_ms: 80,
        }));
        router.registerEndpoint(new models_js_1.LlmEndpointMetrics({
            endpoint_id: "ep-opus",
            provider: "anthropic",
            model_name: "claude-3-7-sonnet-reasoner",
            ttft_ewma_ms: 320,
        }));
        const r1 = router.routeWithCascadeAndSemanticCache({
            promptText: "Fix Redis cluster slot migration timeout",
            systemPrompt: "SRE",
            toolDefinitions: "[]",
            draftLogprobs: [-0.03, -0.05, -0.04],
            confidenceThreshold: 0.85,
            riskLevel: "LOW",
        });
        strict_1.default.equal(r1.cascadeTier, "TIER1_FAST_DRAFT");
        strict_1.default.equal(r1.escalatedToFlagship, false);
        // Normalized semantic cache hit
        const r2 = router.routeWithCascadeAndSemanticCache({
            promptText: "  fix REDIS cluster slot migration timeout!! ",
            systemPrompt: "SRE",
            toolDefinitions: "[]",
            draftLogprobs: [-0.9],
            confidenceThreshold: 0.85,
            riskLevel: "LOW",
        });
        strict_1.default.equal(r2.semanticCacheHit, true);
        strict_1.default.equal(r2.cascadeTier, "TIER0_SEMANTIC_CACHE");
        // Low confidence escalates to flagship
        const r3 = router.routeWithCascadeAndSemanticCache({
            promptText: "Synthesize formal TLA+ invariant for Paxos epoch change",
            systemPrompt: "Architect",
            toolDefinitions: "[]",
            draftLogprobs: [-0.95, -1.2, -0.88],
            confidenceThreshold: 0.85,
            riskLevel: "MEDIUM",
        });
        strict_1.default.equal(r3.escalatedToFlagship, true);
        strict_1.default.equal(r3.cascadeTier, "TIER2_FLAGSHIP_REASONER");
        strict_1.default.equal(r3.selectedModel, "claude-3-7-sonnet-reasoner");
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
        strict_1.default.equal(juryOk.quorumReached, true);
        strict_1.default.equal(juryOk.consensusDecision, "APPROVED");
    });
    (0, node_test_1.it)("[M71-4] Live BFF endpoint POST /v1beta1/management/ops/phase1-cognitive-acceleration, Console UI & CLI mgmt-phase1-accel", async () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m71-"));
        const server = new server_main_js_1.AnstsLocalAndComposeServer({
            apiPort: 0,
            dashboardPort: 0,
            dataDir: tmpDir,
        });
        const { apiPort } = await server.start();
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliAll = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-phase1-accel", "run_all_phase1_suite"], baseUrl);
            strict_1.default.equal(cliAll.exitCode, 0);
            const body = JSON.parse(cliAll.stdout);
            strict_1.default.equal(body.allM71Phase1ChecksPassed, true);
            strict_1.default.equal(body.f1AdaptiveDagReplan.f1Verified, true);
            strict_1.default.equal(body.f2BestOfNSpeculative.f2Verified, true);
            strict_1.default.equal(body.f3CascadeAndJury.f3Verified, true);
            const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
            strict_1.default.equal(jsResp.status, 200);
            const jsText = await jsResp.text();
            strict_1.default.equal(jsText.includes("open-adaptive-dag-replan"), true);
            strict_1.default.equal(jsText.includes("open-best-of-n-verifier"), true);
            strict_1.default.equal(jsText.includes("open-cascade-jury-router"), true);
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
