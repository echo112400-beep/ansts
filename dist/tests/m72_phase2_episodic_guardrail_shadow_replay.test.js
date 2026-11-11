"use strict";














var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _layerfs_workspace_driver = require("../src/runtime/layerfs_workspace_driver.js");
var _staging = require("../src/saga/staging.js");
var _time_travel = require("../src/checkpoint/time_travel.js");
var _wal_engine = require("../src/checkpoint/wal_engine.js");
var _env = require("../src/cloudflare/env.js");
var _models = require("../src/models.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
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
 */(0, _nodeTest.describe)("M72 Phase 2 Episodic Memory, Formal Guardrails & Shadow WAL Replay Suite", () => {(0, _nodeTest.it)("[M72-1 / F4] LayerFsWorkspaceDriver & LayerFsContentStoreEngine hatch Golden Base Images, preserve pinned commits across GC, and mount in 0ms", () => {const drv = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();drv.forkBranch("main", "feat-prisma-fix");drv.executeInProcessFileTool({ branchName: "feat-prisma-fix", agentId: "agent-f4", stepId: "step-f4-1", operation: "write", filePath: "prisma/pool.json", newContent: '{"pool_size":48}' });
    const hatched = drv.hatchGoldenBaseBranch({
      episodeId: "ep-prisma-pool-48",
      taskId: "t-f4-1",
      sourceBranchOrCommit: "feat-prisma-fix",
      repoSignature: "northstar/order-api",
      problemPattern: "Prisma P2024 connection pool timeout under high concurrency",
      resolutionSummary: "Increased pool_size to 48 and enabled transaction pooling",
      tags: ["prisma", "postgres", "pool"],
      savedColdStartMs: 4600
    });

    _strict.default.equal(hatched.goldenBaseBranch, "golden/ep-prisma-pool-48");

    // Delete temporary source branch and run GC; golden commit must remain intact
    drv.deleteBranch("feat-prisma-fix");
    drv.gcUnreachableCommits();

    const mounted = drv.mountWorkspaceFromEpisodicMemory({
      queryText: "Resolve Prisma P2024 connection pool timeout in order-api",
      repoSignature: "northstar/order-api",
      targetBranchName: "ws-new-task"
    });

    _strict.default.equal(mounted.matchedEpisodeId, "ep-prisma-pool-48");
    _strict.default.equal(mounted.copiedFilesCount, 0);
    _strict.default.equal(mounted.savedColdStartMs, 4600);
    _strict.default.equal(mounted.similarityScore >= 0.45, true);

    const readBack = drv.executeInProcessFileTool({
      branchName: "ws-new-task",
      agentId: "agent-f4",
      stepId: "step-f4-read",
      operation: "read",
      filePath: "prisma/pool.json"
    });
    _strict.default.equal(readBack.readResult, '{"pool_size":48}');
  });

  (0, _nodeTest.it)("[M72-2 / F5] StagingCommitAndApprovalGate enforces Policy-as-Code formal proofs (ALLOW, REQUIRE_4EYES_APPROVAL, DENY_FORMAL_VIOLATION) and persists rules in SQLite", () => {
    const sql = new _env.InMemoryCellSqlStorage();
    const gate = new _staging.StagingCommitAndApprovalGate(sql);

    gate.registerPolicyRule({
      ruleId: "POL-PROD-BULK-ROWS",
      effect: "REQUIRE_4EYES_APPROVAL",
      toolNamePattern: "sql_migrate|db_update",
      maxAffectedRows: 1000,
      description: "Require 4-eyes approval for >1000 rows"
    });

    const task = new _models.TaskSpec({
      task_id: "t-f5-guard",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P1_HIGH
    });

    const allowRes = gate.evaluateFormalGuardrailPolicy({
      task,
      turnIndex: 1,
      toolName: "db_select",
      args: { sql: "SELECT count(*) FROM invoices WHERE tenant_id = 'Northstar'" },
      environment: "prod"
    });
    _strict.default.equal(allowRes.decision, "ALLOW");

    const approvalRes = gate.evaluateFormalGuardrailPolicy({
      task,
      turnIndex: 2,
      toolName: "sql_migrate",
      args: {
        sql: "UPDATE invoices SET status = 'SETTLED' WHERE batch_id = 42",
        affectedRowsEstimate: 5000
      },
      environment: "prod"
    });
    _strict.default.equal(approvalRes.decision, "REQUIRE_4EYES_APPROVAL");
    _strict.default.equal(Boolean(approvalRes.approvalTicket?.ticketId), true);

    const denyRes = gate.evaluateFormalGuardrailPolicy({
      task,
      turnIndex: 3,
      toolName: "sql_exec",
      args: {
        sql: "TRUNCATE TABLE invoices; DELETE FROM accounts WHERE 1=1",
        targetPath: "../../etc/passwd"
      },
      environment: "prod"
    });
    _strict.default.equal(denyRes.decision, "DENY_FORMAL_VIOLATION");
    _strict.default.equal(denyRes.violatedRuleIds.includes("BUILTIN_SQL_DESTRUCTIVE_DDL"), true);
    _strict.default.equal(denyRes.violatedRuleIds.includes("BUILTIN_SQL_UNSCOPED_MUTATION"), true);
    _strict.default.equal(denyRes.violatedRuleIds.includes("BUILTIN_PATH_SECRET_BOUNDARY"), true);

    const gate2 = new _staging.StagingCommitAndApprovalGate(sql);
    _strict.default.equal(gate2.listPolicyRules().length, 1);
  });

  (0, _nodeTest.it)("[M72-3 / F6] DeterministicTimeTravelReplayer redacts secrets/PII, verifies WAL CRC32, and gates canary rollout on shadow WAL replay", () => {
    const wal = new _wal_engine.TurnWalEngine();
    const frameEntry = wal.appendTurnDelta({
      taskId: "t-shadow-1",
      turnIndex: 1,
      fencingToken: 4,
      thoughtSummary: "Analyze index",
      toolName: "db_explain",
      changedFiles: ["sql/idx.sql"]
    });

    const replayer = new _time_travel.DeterministicTimeTravelReplayer();
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
        walFrame: frameEntry
      }]

    });

    _strict.default.equal(passSuite.passedGate, true);
    _strict.default.equal(passSuite.canaryRecommendation, "PROMOTE_CANARY");
    _strict.default.equal(passSuite.totalRedactions, 3);
    _strict.default.equal(passSuite.traceResults[0].redactedPrompt.includes("sk-prod-"), false);
    _strict.default.equal(passSuite.traceResults[0].redactedPrompt.includes("admin@northstar.io"), false);
  });

  (0, _nodeTest.it)("[M72-4] Live BFF endpoint POST /v1beta1/management/ops/phase2-memory-guardrail-replay, Console UI & CLI mgmt-phase2-gov", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m72-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      dataDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-phase2-gov", "run_all_phase2_suite"], baseUrl);
      _strict.default.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);





      _strict.default.equal(body.allM72Phase2ChecksPassed, true);
      _strict.default.equal(body.f4EpisodicHatchery.f4Verified, true);
      _strict.default.equal(body.f5FormalGuardrail.f5Verified, true);
      _strict.default.equal(body.f6ShadowWalReplay.f6Verified, true);

      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      _strict.default.equal(jsResp.status, 200);
      const jsText = await jsResp.text();
      _strict.default.equal(jsText.includes("open-episodic-hatchery"), true);
      _strict.default.equal(jsText.includes("open-formal-guardrail"), true);
      _strict.default.equal(jsText.includes("open-shadow-wal-replay"), true);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});