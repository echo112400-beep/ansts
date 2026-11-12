"use strict";var _strict = _interopRequireDefault(require("node:assert/strict"));
var _nodeFs = _interopRequireDefault(require("node:fs"));
var _nodeOs = _interopRequireDefault(require("node:os"));
var _nodePath = _interopRequireDefault(require("node:path"));
var _nodeTest = _interopRequireDefault(require("node:test"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.default)("M91: Batch Task Planning, Kahn Topological Wave Derivation, DAG Cycle Guard & Atomic Escrow Dispatch", async () => {
  const tmpDir = _nodeFs.default.mkdtempSync(_nodePath.default.join(_nodeOs.default.tmpdir(), "ansts-m91-batch-"));
  const srv = new _server_main.AnstsLocalAndComposeServer({
    apiPort: 0,
    dashboardPort: 0,
    storageDir: tmpDir
  });
  const { apiPort } = await srv.start();
  const baseUrl = `http://127.0.0.1:${apiPort}`;

  try {
    const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
    const adminToken = guard.issuePrincipalToken("chen");

    // 1. GET /v1beta1/console/tasks/blueprints returns 3 preset blueprints
    const bpResp = await fetch(`${baseUrl}/v1beta1/console/tasks/blueprints`, {
      headers: { authorization: `Bearer ${adminToken}` }
    });
    _strict.default.equal(bpResp.status, 200);
    const bpBody = await bpResp.json();



    _strict.default.equal(bpBody.presets.length, 3);
    _strict.default.ok(bpBody.presets.some((p) => p.blueprintId === "fullstack-feature-pipeline"));

    // 2. Dry-Run fullstack-feature-pipeline: derives 3 topological waves with maxParallelism=2
    const dryResp = await fetch(`${baseUrl}/v1beta1/management/tasks/batch-plan`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      },
      body: JSON.stringify({
        mode: "dry_run",
        blueprintId: "fullstack-feature-pipeline",
        actor: "chen",
        role: "SRE_ADMIN"
      })
    });
    _strict.default.equal(dryResp.status, 200);
    const dryBody = await dryResp.json();







    _strict.default.equal(dryBody.valid, true);
    _strict.default.equal(dryBody.stageCount, 4);
    _strict.default.equal(dryBody.waveCount, 3);
    _strict.default.equal(dryBody.maxParallelism, 2);
    _strict.default.deepEqual(dryBody.waves[0], ["plan-arch-spec"]);
    _strict.default.deepEqual(dryBody.waves[2], ["verify-ci-canary"]);
    _strict.default.equal(dryBody.resourceSummary.requiredEscrowSlots, 4);

    // 3. Cycle detection & RBAC guard
    const cycleRes = srv.managementPlane.planAndDispatchTaskBatch({
      mode: "dry_run",
      stages: [
      { taskId: "step-a", prompt: "Step A", dependsOn: ["step-b"] },
      { taskId: "step-b", prompt: "Step B", dependsOn: ["step-a"] }],

      actor: "chen",
      role: "SRE_ADMIN"
    });
    _strict.default.equal(cycleRes.status, 400);
    _strict.default.equal(cycleRes.body.valid, false);
    _strict.default.ok(
    cycleRes.body.validationErrors.some((e) => e.includes("DAG_CYCLE_DETECTED")));


    const rbacRes = srv.managementPlane.planAndDispatchTaskBatch({
      mode: "commit_and_dispatch",
      blueprintId: "distributed-chaos-verif",
      actor: "xu",
      role: "VIEWER"
    });
    _strict.default.equal(rbacRes.status, 403);
    _strict.default.equal(rbacRes.body.error, "RBAC_FORBIDDEN");

    // 4. Commit & Dispatch batch blueprint atomically into Scheduler, Queue & DagEscrow
    const commitResp = await fetch(`${baseUrl}/v1beta1/management/tasks/batch-plan`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      },
      body: JSON.stringify({
        mode: "commit_and_dispatch",
        blueprintId: "distributed-chaos-verif",
        actor: "chen",
        role: "SRE_ADMIN"
      })
    });
    _strict.default.equal(commitResp.status, 201);
    const commitBody = await commitResp.json();






    _strict.default.equal(commitBody.valid, true);
    _strict.default.equal(commitBody.dispatchedCount, 3);
    _strict.default.ok(srv.managementPlane.deps.scheduler.allTasks.has(commitBody.coordinatorTaskId));
    for (const id of commitBody.dispatchedTaskIds) {
      _strict.default.ok(srv.managementPlane.deps.scheduler.allTasks.has(id), `Missing dispatched task ${id}`);
    }

    // 5. CLI mgmt-task-blueprints & mgmt-task-batch-plan
    const cliBp = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-task-blueprints"], baseUrl);
    _strict.default.equal(cliBp.exitCode, 0);
    const cliBpJson = JSON.parse(cliBp.stdout);
    _strict.default.equal(cliBpJson.customBlueprints.length, 1);

    const cliPlan = await (0, _anstsctl.runAnstsCtlRemote)(
    ["mgmt-task-batch-plan", "dry_run", "multi-tenant-finops-sweep"],
    baseUrl);

    _strict.default.equal(cliPlan.exitCode, 0);
    const cliPlanJson = JSON.parse(cliPlan.stdout);
    _strict.default.equal(cliPlanJson.valid, true);
    _strict.default.equal(cliPlanJson.waveCount, 2);

    // 6. Frontend UI markers
    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = _nodeFs.default.readFileSync(_nodePath.default.resolve(process.cwd(), rel), "utf8");
      _strict.default.ok(content.includes("open-batch-task-planner-dialog"), `${rel} missing open-batch-task-planner-dialog`);
      _strict.default.ok(content.includes("batch-plan-dry-run"), `${rel} missing batch-plan-dry-run`);
      _strict.default.ok(content.includes("batch-plan-commit-dispatch"), `${rel} missing batch-plan-commit-dispatch`);
      _strict.default.ok(content.includes("/v1beta1/management/tasks/batch-plan"), `${rel} missing /v1beta1/management/tasks/batch-plan`);
    }
  } finally {
    await srv.stop();
    _nodeFs.default.rmSync(tmpDir, { recursive: true, force: true });
  }
});