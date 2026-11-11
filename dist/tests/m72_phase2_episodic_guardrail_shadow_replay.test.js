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
 * @fileoverview M72: Phase 2 Episodic Memory, Formal Guardrails & Shadow WAL Replay Suite
 * Verifies:
 * - [M72-1 / F4] Cross-Task Semantic Episodic Memory & LayerFS Golden Base Image Hatchery
 *   (`LayerFsContentStoreEngine.recordEpisodicTrajectory`, `retrieveSimilarEpisodesAndGoldenBase`,
 *    `LayerFsWorkspaceDriver.hatchGoldenBaseBranch`, and `mountWorkspaceFromEpisodicMemory`).
 * - [M72-2 / F5] Declarative Policy-as-Code Formal Guardrail Engine
 *   (`StagingCommitAndApprovalGate.registerPolicyRule` & `evaluateFormalGuardrailPolicy` with SQL AST proof,
 *    path/secret boundary proof, blast-radius scoring, and SQLite policy rule persistence).
 * - [M72-3 / F6] Production Shadow WAL Replay & Regression Eval Harness
 *   (`DeterministicTimeTravelReplayer.redactSensitivePromptTokens` & `executeShadowWalRegressionSuite` with
 *    CRC32 WAL verification, PII/secret redaction, and canary promotion/rollback gating).
 * - [M72-4] Live Console BFF (`POST /v1beta1/management/ops/phase2-memory-guardrail-replay`),
 *   Console UI bindings, and CLI (`anstsctl mgmt-phase2-gov`).
 */
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const layerfs_workspace_driver_js_1 = require("../src/runtime/layerfs_workspace_driver.js");
const staging_js_1 = require("../src/saga/staging.js");
const time_travel_js_1 = require("../src/checkpoint/time_travel.js");
const wal_engine_js_1 = require("../src/checkpoint/wal_engine.js");
const env_js_1 = require("../src/cloudflare/env.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M72 Phase 2 Episodic Memory, Formal Guardrails & Shadow WAL Replay Suite", () => {
    (0, node_test_1.it)("[M72-1 / F4] LayerFsWorkspaceDriver & LayerFsContentStoreEngine hatch Golden Base Images, preserve pinned commits across GC, and mount in 0ms", () => {
        const drv = new layerfs_workspace_driver_js_1.LayerFsWorkspaceDriver();
        drv.forkBranch("main", "feat-prisma-fix");
        drv.executeInProcessFileTool({
            branchName: "feat-prisma-fix",
            agentId: "agent-f4",
            stepId: "step-f4-1",
            operation: "write",
            filePath: "prisma/pool.json",
            newContent: '{"pool_size":48}',
        });
        const hatched = drv.hatchGoldenBaseBranch({
            episodeId: "ep-prisma-pool-48",
            taskId: "t-f4-1",
            sourceBranchOrCommit: "feat-prisma-fix",
            repoSignature: "northstar/order-api",
            problemPattern: "Prisma P2024 connection pool timeout under high concurrency",
            resolutionSummary: "Increased pool_size to 48 and enabled transaction pooling",
            tags: ["prisma", "postgres", "pool"],
            savedColdStartMs: 4600,
        });
        strict_1.default.equal(hatched.goldenBaseBranch, "golden/ep-prisma-pool-48");
        // Delete temporary source branch and run GC; golden commit must remain intact
        drv.deleteBranch("feat-prisma-fix");
        drv.gcUnreachableCommits();
        const mounted = drv.mountWorkspaceFromEpisodicMemory({
            queryText: "Resolve Prisma P2024 connection pool timeout in order-api",
            repoSignature: "northstar/order-api",
            targetBranchName: "ws-new-task",
        });
        strict_1.default.equal(mounted.matchedEpisodeId, "ep-prisma-pool-48");
        strict_1.default.equal(mounted.copiedFilesCount, 0);
        strict_1.default.equal(mounted.savedColdStartMs, 4600);
        strict_1.default.equal(mounted.similarityScore >= 0.45, true);
        const readBack = drv.executeInProcessFileTool({
            branchName: "ws-new-task",
            agentId: "agent-f4",
            stepId: "step-f4-read",
            operation: "read",
            filePath: "prisma/pool.json",
        });
        strict_1.default.equal(readBack.readResult, '{"pool_size":48}');
    });
    (0, node_test_1.it)("[M72-2 / F5] StagingCommitAndApprovalGate enforces Policy-as-Code formal proofs (ALLOW, REQUIRE_4EYES_APPROVAL, DENY_FORMAL_VIOLATION) and persists rules in SQLite", () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const gate = new staging_js_1.StagingCommitAndApprovalGate(sql);
        gate.registerPolicyRule({
            ruleId: "POL-PROD-BULK-ROWS",
            effect: "REQUIRE_4EYES_APPROVAL",
            toolNamePattern: "sql_migrate|db_update",
            maxAffectedRows: 1000,
            description: "Require 4-eyes approval for >1000 rows",
        });
        const task = new models_js_1.TaskSpec({
            task_id: "t-f5-guard",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P1_HIGH,
        });
        const allowRes = gate.evaluateFormalGuardrailPolicy({
            task,
            turnIndex: 1,
            toolName: "db_select",
            args: { sql: "SELECT count(*) FROM invoices WHERE tenant_id = 'Northstar'" },
            environment: "prod",
        });
        strict_1.default.equal(allowRes.decision, "ALLOW");
        const approvalRes = gate.evaluateFormalGuardrailPolicy({
            task,
            turnIndex: 2,
            toolName: "sql_migrate",
            args: {
                sql: "UPDATE invoices SET status = 'SETTLED' WHERE batch_id = 42",
                affectedRowsEstimate: 5000,
            },
            environment: "prod",
        });
        strict_1.default.equal(approvalRes.decision, "REQUIRE_4EYES_APPROVAL");
        strict_1.default.equal(Boolean(approvalRes.approvalTicket?.ticketId), true);
        const denyRes = gate.evaluateFormalGuardrailPolicy({
            task,
            turnIndex: 3,
            toolName: "sql_exec",
            args: {
                sql: "TRUNCATE TABLE invoices; DELETE FROM accounts WHERE 1=1",
                targetPath: "../../etc/passwd",
            },
            environment: "prod",
        });
        strict_1.default.equal(denyRes.decision, "DENY_FORMAL_VIOLATION");
        strict_1.default.equal(denyRes.violatedRuleIds.includes("BUILTIN_SQL_DESTRUCTIVE_DDL"), true);
        strict_1.default.equal(denyRes.violatedRuleIds.includes("BUILTIN_SQL_UNSCOPED_MUTATION"), true);
        strict_1.default.equal(denyRes.violatedRuleIds.includes("BUILTIN_PATH_SECRET_BOUNDARY"), true);
        const gate2 = new staging_js_1.StagingCommitAndApprovalGate(sql);
        strict_1.default.equal(gate2.listPolicyRules().length, 1);
    });
    (0, node_test_1.it)("[M72-3 / F6] DeterministicTimeTravelReplayer redacts secrets/PII, verifies WAL CRC32, and gates canary rollout on shadow WAL replay", () => {
        const wal = new wal_engine_js_1.TurnWalEngine();
        const frameEntry = wal.appendTurnDelta({
            taskId: "t-shadow-1",
            turnIndex: 1,
            fencingToken: 4,
            thoughtSummary: "Analyze index",
            toolName: "db_explain",
            changedFiles: ["sql/idx.sql"],
        });
        const replayer = new time_travel_js_1.DeterministicTimeTravelReplayer();
        const passSuite = replayer.executeShadowWalRegressionSuite({
            suiteId: "s-pass",
            baselineModel: "sonnet-3.5",
            candidateModel: "sonnet-3.7",
            traces: [
                {
                    taskId: "t-shadow-1",
                    turnIndex: 1,
                    prompt: "User admin@northstar.io key sk-prod-1234567890abcdef Bearer eyJ0eXAiOiJKV1QiLCJhbGci",
                    expectedToolName: "db_explain",
                    expectedPathPrefix: "sql/",
                    baselineLatencyMs: 400,
                    baselineTokens: 1000,
                    candidateToolName: "db_explain",
                    candidateArgs: { path: "sql/idx.sql" },
                    candidateLatencyMs: 320,
                    candidateTokens: 920,
                    walFrame: frameEntry,
                },
            ],
        });
        strict_1.default.equal(passSuite.passedGate, true);
        strict_1.default.equal(passSuite.canaryRecommendation, "PROMOTE_CANARY");
        strict_1.default.equal(passSuite.totalRedactions, 3);
        strict_1.default.equal(passSuite.traceResults[0].redactedPrompt.includes("sk-prod-"), false);
        strict_1.default.equal(passSuite.traceResults[0].redactedPrompt.includes("admin@northstar.io"), false);
    });
    (0, node_test_1.it)("[M72-4] Live BFF endpoint POST /v1beta1/management/ops/phase2-memory-guardrail-replay, Console UI & CLI mgmt-phase2-gov", async () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m72-"));
        const server = new server_main_js_1.AnstsLocalAndComposeServer({
            apiPort: 0,
            dashboardPort: 0,
            dataDir: tmpDir,
        });
        const { apiPort } = await server.start();
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-phase2-gov", "run_all_phase2_suite"], baseUrl);
            strict_1.default.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            strict_1.default.equal(body.allM72Phase2ChecksPassed, true);
            strict_1.default.equal(body.f4EpisodicHatchery.f4Verified, true);
            strict_1.default.equal(body.f5FormalGuardrail.f5Verified, true);
            strict_1.default.equal(body.f6ShadowWalReplay.f6Verified, true);
            const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
            strict_1.default.equal(jsResp.status, 200);
            const jsText = await jsResp.text();
            strict_1.default.equal(jsText.includes("open-episodic-hatchery"), true);
            strict_1.default.equal(jsText.includes("open-formal-guardrail"), true);
            strict_1.default.equal(jsText.includes("open-shadow-wal-replay"), true);
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
