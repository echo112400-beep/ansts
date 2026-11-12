import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

test("M92: Batch 4-Eyes Approval Governance, Staging Hold-and-Commit Buffer & LIFO Saga Compensation Workbench", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m92-saga-"));
  const srv = new AnstsLocalAndComposeServer({
    apiPort: 0,
    dashboardPort: 0,
    storageDir: tmpDir,
  });
  const { apiPort } = await srv.start();
  const baseUrl = `http://127.0.0.1:${apiPort}`;

  try {
    const guard = new ConsoleRbacAuthGuard();
    const adminToken = guard.issuePrincipalToken("zhou");

    // 1. GET /v1beta1/console/approvals/saga returns seeded staging & saga ledgers
    const initResp = await fetch(`${baseUrl}/v1beta1/console/approvals/saga`, {
      headers: { authorization: `Bearer ${adminToken}` },
    });
    assert.equal(initResp.status, 200);
    const initBody = (await initResp.json()) as {
      approvalStats: { totalCount: number; pendingCount: number };
      stagingStats: { uncommittedCount: number };
      sagaStats: { activeForwardCount: number };
    };
    assert.ok(initBody.approvalStats.totalCount >= 1);
    assert.ok(initBody.stagingStats.uncommittedCount >= 1);
    assert.ok(initBody.sagaStats.activeForwardCount >= 2);

    // 2. Create 2 approval tickets: one by 'chen', one by 'zhou'
    const c1 = srv.managementPlane.createApprovalTicket({
      type: "QUOTA_EMERGENCY_BOOST",
      target: "Northstar 120 → 160 并发",
      region: "us-west-2",
      tenant: "Northstar",
      risk: "HIGH",
      initiator: "chen",
      role: "SRE_ADMIN",
    });
    const c2 = srv.managementPlane.createApprovalTicket({
      type: "SAGA_HIGH_RISK_TOOL",
      target: "db_truncate_temp",
      region: "us-west-2",
      tenant: "Northstar",
      risk: "CRITICAL",
      initiator: "zhou",
      role: "SRE_ADMIN",
    });
    const idChen = String(c1.body.approvalId);
    const idZhou = String(c2.body.approvalId);

    // 3. Batch approve both as 'zhou': chen's ticket is DECIDED, zhou's own ticket is SELF_APPROVAL_SKIPPED
    const batchResp = await fetch(`${baseUrl}/v1beta1/management/approvals/batch`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      },
      body: JSON.stringify({
        approvalIds: [idChen, idZhou],
        decision: "approve",
        approver: "zhou",
        role: "SRE_ADMIN",
      }),
    });
    assert.equal(batchResp.status, 200);
    const batchBody = (await batchResp.json()) as {
      decidedCount: number;
      skippedSelfApprovalCount: number;
      results: Array<{ approvalId: string; outcome: string }>;
    };
    assert.equal(batchBody.decidedCount, 1);
    assert.equal(batchBody.skippedSelfApprovalCount, 1);
    assert.equal(
      batchBody.results.find((r) => r.approvalId === idChen)?.outcome,
      "DECIDED"
    );
    assert.equal(
      batchBody.results.find((r) => r.approvalId === idZhou)?.outcome,
      "SELF_APPROVAL_SKIPPED"
    );

    // RBAC guard: OPERATOR or VIEWER blocked from batch approval
    const rbacBatch = srv.managementPlane.executeBatchApprovalDecision({
      approvalIds: [idZhou],
      decision: "approve",
      approver: "lin",
      role: "OPERATOR",
    });
    assert.equal(rbacBatch.status, 403);

    // 4. Stage, Commit, and Rollback Saga effects via /v1beta1/management/approvals/saga-action
    const stageResp = await fetch(`${baseUrl}/v1beta1/management/approvals/saga-action`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      },
      body: JSON.stringify({
        action: "stage_high_risk_effect",
        taskId: "task-ns-8842",
        toolName: "k8s_drain_shard",
        actor: "zhou",
        role: "SRE_ADMIN",
      }),
    });
    assert.equal(stageResp.status, 201);

    const commitResp = await fetch(`${baseUrl}/v1beta1/management/approvals/saga-action`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      },
      body: JSON.stringify({
        action: "commit_staged_effects",
        taskId: "task-ns-8842",
        actor: "zhou",
        role: "SRE_ADMIN",
      }),
    });
    assert.equal(commitResp.status, 200);
    const commitBody = (await commitResp.json()) as { committedCount: number };
    assert.ok(commitBody.committedCount >= 2);

    const rollbackResp = await fetch(`${baseUrl}/v1beta1/management/approvals/saga-action`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      },
      body: JSON.stringify({
        action: "abort_and_rollback_saga",
        taskId: "task-ns-8842",
        actor: "zhou",
        role: "SRE_ADMIN",
      }),
    });
    assert.equal(rollbackResp.status, 200);
    const rollbackBody = (await rollbackResp.json()) as { rolledBackSagaCount: number };
    assert.ok(rollbackBody.rolledBackSagaCount >= 4);

    // 5. CLI verification
    const cliSaga = await runAnstsCtlRemote(["mgmt-approvals-saga"], baseUrl);
    assert.equal(cliSaga.exitCode, 0);

    const cliAct = await runAnstsCtlRemote(
      ["mgmt-saga-action", "stage_high_risk_effect", "task-ns-8842", "dns_cutover"],
      baseUrl
    );
    assert.equal(cliAct.exitCode, 0);

    const cliBatch = await runAnstsCtlRemote(
      ["mgmt-approvals-batch", "approve", idZhou, "chen"],
      baseUrl
    );
    assert.equal(cliBatch.exitCode, 0);
    const cliBatchJson = JSON.parse(cliBatch.stdout) as { decidedCount: number };
    assert.equal(cliBatchJson.decidedCount, 1);

    // 6. Frontend UI markers
    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = fs.readFileSync(path.resolve(process.cwd(), rel), "utf8");
      assert.ok(content.includes("batch-decide-approvals"), `${rel} missing batch-decide-approvals`);
      assert.ok(content.includes("saga-stage-effect"), `${rel} missing saga-stage-effect`);
      assert.ok(content.includes("saga-commit-effects"), `${rel} missing saga-commit-effects`);
      assert.ok(content.includes("saga-abort-rollback"), `${rel} missing saga-abort-rollback`);
    }
  } finally {
    await srv.stop();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});
