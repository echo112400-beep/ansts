"use strict";var _strict = _interopRequireDefault(require("node:assert/strict"));
var _nodeFs = _interopRequireDefault(require("node:fs"));
var _nodeOs = _interopRequireDefault(require("node:os"));
var _nodePath = _interopRequireDefault(require("node:path"));
var _nodeTest = _interopRequireDefault(require("node:test"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.default)("M92: Batch 4-Eyes Approval Governance, Staging Hold-and-Commit Buffer & LIFO Saga Compensation Workbench", async () => {
  const tmpDir = _nodeFs.default.mkdtempSync(_nodePath.default.join(_nodeOs.default.tmpdir(), "ansts-m92-saga-"));
  const srv = new _server_main.AnstsLocalAndComposeServer({
    apiPort: 0,
    dashboardPort: 0,
    storageDir: tmpDir
  });
  const { apiPort } = await srv.start();
  const baseUrl = `http://127.0.0.1:${apiPort}`;

  try {
    const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
    const adminToken = guard.issuePrincipalToken("zhou");

    // 1. GET /v1beta1/console/approvals/saga returns seeded staging & saga ledgers
    const initResp = await fetch(`${baseUrl}/v1beta1/console/approvals/saga`, {
      headers: { authorization: `Bearer ${adminToken}` }
    });
    _strict.default.equal(initResp.status, 200);
    const initBody = await initResp.json();




    _strict.default.ok(initBody.approvalStats.totalCount >= 1);
    _strict.default.ok(initBody.stagingStats.uncommittedCount >= 1);
    _strict.default.ok(initBody.sagaStats.activeForwardCount >= 2);

    // 2. Create 2 approval tickets: one by 'chen', one by 'zhou'
    const c1 = srv.managementPlane.createApprovalTicket({
      type: "QUOTA_EMERGENCY_BOOST",
      target: "Northstar 120 → 160 并发",
      region: "us-west-2",
      tenant: "Northstar",
      risk: "HIGH",
      initiator: "chen",
      role: "SRE_ADMIN"
    });
    const c2 = srv.managementPlane.createApprovalTicket({
      type: "SAGA_HIGH_RISK_TOOL",
      target: "db_truncate_temp",
      region: "us-west-2",
      tenant: "Northstar",
      risk: "CRITICAL",
      initiator: "zhou",
      role: "SRE_ADMIN"
    });
    const idChen = String(c1.body.approvalId);
    const idZhou = String(c2.body.approvalId);

    // 3. Batch approve both as 'zhou': chen's ticket is DECIDED, zhou's own ticket is SELF_APPROVAL_SKIPPED
    const batchResp = await fetch(`${baseUrl}/v1beta1/management/approvals/batch`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      },
      body: JSON.stringify({
        approvalIds: [idChen, idZhou],
        decision: "approve",
        approver: "zhou",
        role: "SRE_ADMIN"
      })
    });
    _strict.default.equal(batchResp.status, 200);
    const batchBody = await batchResp.json();




    _strict.default.equal(batchBody.decidedCount, 1);
    _strict.default.equal(batchBody.skippedSelfApprovalCount, 1);
    _strict.default.equal(
    batchBody.results.find((r) => r.approvalId === idChen)?.outcome,
    "DECIDED");

    _strict.default.equal(
    batchBody.results.find((r) => r.approvalId === idZhou)?.outcome,
    "SELF_APPROVAL_SKIPPED");


    // RBAC guard: OPERATOR or VIEWER blocked from batch approval
    const rbacBatch = srv.managementPlane.executeBatchApprovalDecision({
      approvalIds: [idZhou],
      decision: "approve",
      approver: "lin",
      role: "OPERATOR"
    });
    _strict.default.equal(rbacBatch.status, 403);

    // 4. Stage, Commit, and Rollback Saga effects via /v1beta1/management/approvals/saga-action
    const stageResp = await fetch(`${baseUrl}/v1beta1/management/approvals/saga-action`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      },
      body: JSON.stringify({
        action: "stage_high_risk_effect",
        taskId: "task-ns-8842",
        toolName: "k8s_drain_shard",
        actor: "zhou",
        role: "SRE_ADMIN"
      })
    });
    _strict.default.equal(stageResp.status, 201);

    const commitResp = await fetch(`${baseUrl}/v1beta1/management/approvals/saga-action`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      },
      body: JSON.stringify({
        action: "commit_staged_effects",
        taskId: "task-ns-8842",
        actor: "zhou",
        role: "SRE_ADMIN"
      })
    });
    _strict.default.equal(commitResp.status, 200);
    const commitBody = await commitResp.json();
    _strict.default.ok(commitBody.committedCount >= 2);

    const rollbackResp = await fetch(`${baseUrl}/v1beta1/management/approvals/saga-action`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      },
      body: JSON.stringify({
        action: "abort_and_rollback_saga",
        taskId: "task-ns-8842",
        actor: "zhou",
        role: "SRE_ADMIN"
      })
    });
    _strict.default.equal(rollbackResp.status, 200);
    const rollbackBody = await rollbackResp.json();
    _strict.default.ok(rollbackBody.rolledBackSagaCount >= 4);

    // 5. CLI verification
    const cliSaga = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-approvals-saga"], baseUrl);
    _strict.default.equal(cliSaga.exitCode, 0);

    const cliAct = await (0, _anstsctl.runAnstsCtlRemote)(
    ["mgmt-saga-action", "stage_high_risk_effect", "task-ns-8842", "dns_cutover"],
    baseUrl);

    _strict.default.equal(cliAct.exitCode, 0);

    const cliBatch = await (0, _anstsctl.runAnstsCtlRemote)(
    ["mgmt-approvals-batch", "approve", idZhou, "chen"],
    baseUrl);

    _strict.default.equal(cliBatch.exitCode, 0);
    const cliBatchJson = JSON.parse(cliBatch.stdout);
    _strict.default.equal(cliBatchJson.decidedCount, 1);

    // 6. Frontend UI markers
    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = _nodeFs.default.readFileSync(_nodePath.default.resolve(process.cwd(), rel), "utf8");
      _strict.default.ok(content.includes("batch-decide-approvals"), `${rel} missing batch-decide-approvals`);
      _strict.default.ok(content.includes("saga-stage-effect"), `${rel} missing saga-stage-effect`);
      _strict.default.ok(content.includes("saga-commit-effects"), `${rel} missing saga-commit-effects`);
      _strict.default.ok(content.includes("saga-abort-rollback"), `${rel} missing saga-abort-rollback`);
    }
  } finally {
    await srv.stop();
    _nodeFs.default.rmSync(tmpDir, { recursive: true, force: true });
  }
});