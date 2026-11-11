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
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { LayerFsWorkspaceDriver } from "../src/runtime/layerfs_workspace_driver.js";
import { StagingCommitAndApprovalGate } from "../src/saga/staging.js";
import { DeterministicTimeTravelReplayer } from "../src/checkpoint/time_travel.js";
import { TurnWalEngine } from "../src/checkpoint/wal_engine.js";
import { InMemoryCellSqlStorage } from "../src/cloudflare/env.js";
import { TaskPriority, TaskSpec } from "../src/models.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M72 Phase 2 Episodic Memory, Formal Guardrails & Shadow WAL Replay Suite", () => {
  it("[M72-1 / F4] LayerFsWorkspaceDriver & LayerFsContentStoreEngine hatch Golden Base Images, preserve pinned commits across GC, and mount in 0ms", () => {
    const drv = new LayerFsWorkspaceDriver();
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

    assert.equal(hatched.goldenBaseBranch, "golden/ep-prisma-pool-48");

    // Delete temporary source branch and run GC; golden commit must remain intact
    drv.deleteBranch("feat-prisma-fix");
    drv.gcUnreachableCommits();

    const mounted = drv.mountWorkspaceFromEpisodicMemory({
      queryText: "Resolve Prisma P2024 connection pool timeout in order-api",
      repoSignature: "northstar/order-api",
      targetBranchName: "ws-new-task",
    });

    assert.equal(mounted.matchedEpisodeId, "ep-prisma-pool-48");
    assert.equal(mounted.copiedFilesCount, 0);
    assert.equal(mounted.savedColdStartMs, 4600);
    assert.equal(mounted.similarityScore >= 0.45, true);

    const readBack = drv.executeInProcessFileTool({
      branchName: "ws-new-task",
      agentId: "agent-f4",
      stepId: "step-f4-read",
      operation: "read",
      filePath: "prisma/pool.json",
    });
    assert.equal(readBack.readResult, '{"pool_size":48}');
  });

  it("[M72-2 / F5] StagingCommitAndApprovalGate enforces Policy-as-Code formal proofs (ALLOW, REQUIRE_4EYES_APPROVAL, DENY_FORMAL_VIOLATION) and persists rules in SQLite", () => {
    const sql = new InMemoryCellSqlStorage();
    const gate = new StagingCommitAndApprovalGate(sql);

    gate.registerPolicyRule({
      ruleId: "POL-PROD-BULK-ROWS",
      effect: "REQUIRE_4EYES_APPROVAL",
      toolNamePattern: "sql_migrate|db_update",
      maxAffectedRows: 1000,
      description: "Require 4-eyes approval for >1000 rows",
    });

    const task = new TaskSpec({
      task_id: "t-f5-guard",
      tenant_id: "Northstar",
      priority: TaskPriority.P1_HIGH,
    });

    const allowRes = gate.evaluateFormalGuardrailPolicy({
      task,
      turnIndex: 1,
      toolName: "db_select",
      args: { sql: "SELECT count(*) FROM invoices WHERE tenant_id = 'Northstar'" },
      environment: "prod",
    });
    assert.equal(allowRes.decision, "ALLOW");

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
    assert.equal(approvalRes.decision, "REQUIRE_4EYES_APPROVAL");
    assert.equal(Boolean(approvalRes.approvalTicket?.ticketId), true);

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
    assert.equal(denyRes.decision, "DENY_FORMAL_VIOLATION");
    assert.equal(denyRes.violatedRuleIds.includes("BUILTIN_SQL_DESTRUCTIVE_DDL"), true);
    assert.equal(denyRes.violatedRuleIds.includes("BUILTIN_SQL_UNSCOPED_MUTATION"), true);
    assert.equal(denyRes.violatedRuleIds.includes("BUILTIN_PATH_SECRET_BOUNDARY"), true);

    const gate2 = new StagingCommitAndApprovalGate(sql);
    assert.equal(gate2.listPolicyRules().length, 1);
  });

  it("[M72-3 / F6] DeterministicTimeTravelReplayer redacts secrets/PII, verifies WAL CRC32, and gates canary rollout on shadow WAL replay", () => {
    const wal = new TurnWalEngine();
    const frameEntry = wal.appendTurnDelta({
      taskId: "t-shadow-1",
      turnIndex: 1,
      fencingToken: 4,
      thoughtSummary: "Analyze index",
      toolName: "db_explain",
      changedFiles: ["sql/idx.sql"],
    });

    const replayer = new DeterministicTimeTravelReplayer();
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

    assert.equal(passSuite.passedGate, true);
    assert.equal(passSuite.canaryRecommendation, "PROMOTE_CANARY");
    assert.equal(passSuite.totalRedactions, 3);
    assert.equal(passSuite.traceResults[0].redactedPrompt.includes("sk-prod-"), false);
    assert.equal(passSuite.traceResults[0].redactedPrompt.includes("admin@northstar.io"), false);
  });

  it("[M72-4] Live BFF endpoint POST /v1beta1/management/ops/phase2-memory-guardrail-replay, Console UI & CLI mgmt-phase2-gov", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m72-"));
    const server = new AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      dataDir: tmpDir,
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const cliRes = await runAnstsCtlRemote(["mgmt-phase2-gov", "run_all_phase2_suite"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout) as {
        allM72Phase2ChecksPassed: boolean;
        f4EpisodicHatchery: { f4Verified: boolean };
        f5FormalGuardrail: { f5Verified: boolean };
        f6ShadowWalReplay: { f6Verified: boolean };
      };
      assert.equal(body.allM72Phase2ChecksPassed, true);
      assert.equal(body.f4EpisodicHatchery.f4Verified, true);
      assert.equal(body.f5FormalGuardrail.f5Verified, true);
      assert.equal(body.f6ShadowWalReplay.f6Verified, true);

      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      assert.equal(jsResp.status, 200);
      const jsText = await jsResp.text();
      assert.equal(jsText.includes("open-episodic-hatchery"), true);
      assert.equal(jsText.includes("open-formal-guardrail"), true);
      assert.equal(jsText.includes("open-shadow-wal-replay"), true);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
