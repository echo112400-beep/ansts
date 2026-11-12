"use strict";var _strict = _interopRequireDefault(require("node:assert/strict"));
var _nodeFs = _interopRequireDefault(require("node:fs"));
var _nodeOs = _interopRequireDefault(require("node:os"));
var _nodePath = _interopRequireDefault(require("node:path"));
var _nodeTest = _interopRequireDefault(require("node:test"));
var _models = require("../src/models.js");
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.default)("M90: Queue Backpressure Matrix, Priority Load-Shedding Simulator & DLQ Replay Workbench", async () => {
  const tmpDir = _nodeFs.default.mkdtempSync(_nodePath.default.join(_nodeOs.default.tmpdir(), "ansts-m90-q-"));
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

    // Seed a DEAD_LETTER task and a queued P3_BATCH task
    const dlqTask = new _models.TaskSpec({
      task_id: "task-m90-dlq-1",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P2_STANDARD,
      prompt: "M90 DLQ replay target"
    });
    dlqTask.state = _models.TaskState.DEAD_LETTER;
    dlqTask.retry_count = 4;
    srv.managementPlane.deps.scheduler.allTasks.set(dlqTask.task_id, dlqTask);

    srv.managementPlane.executeQueueManagementAction({
      action: "enqueue_task",
      taskId: "task-m90-p3-1",
      tenantId: "Northstar",
      priority: 3,
      actor: "chen",
      role: "SRE_ADMIN"
    });

    // 1. Low utilization (0.50): all P0..P3 admitted, HTTP 200
    const lowResp = await fetch(`${baseUrl}/v1beta1/console/queues/backpressure?utilizationRatio=0.50`, {
      headers: { authorization: `Bearer ${adminToken}` }
    });
    _strict.default.equal(lowResp.status, 200);
    const lowBody = await lowResp.json();





    _strict.default.equal(lowBody.backpressureActive, false);
    _strict.default.equal(lowBody.backpressureHttpStatus, 200);
    _strict.default.ok(lowBody.admissionMatrix.every((r) => r.admitted === true));
    _strict.default.ok(lowBody.deadLetterCount >= 1);

    // 2. High utilization (0.95): P0 admitted, P2/P3 shed, HTTP 429 backpressure active
    const highResp = await fetch(`${baseUrl}/v1beta1/console/queues/backpressure?utilizationRatio=0.95`, {
      headers: { authorization: `Bearer ${adminToken}` }
    });
    _strict.default.equal(highResp.status, 200);
    const highBody = await highResp.json();






    _strict.default.equal(highBody.backpressureActive, true);
    _strict.default.equal(highBody.backpressureHttpStatus, 429);
    _strict.default.ok(highBody.retryAfterSec >= 1);
    _strict.default.equal(highBody.admissionMatrix.find((r) => r.priority === 0)?.admitted, true);
    _strict.default.equal(highBody.admissionMatrix.find((r) => r.priority === 3)?.admitted, false);
    _strict.default.ok(highBody.shedCandidateTaskIds.includes("task-m90-p3-1"));

    // 3. RBAC VIEWER blocked from DLQ replay (403)
    const rbacRes = srv.managementPlane.replayDeadLetterTasks({
      taskIds: ["task-m90-dlq-1"],
      actor: "xu",
      role: "VIEWER"
    });
    _strict.default.equal(rbacRes.status, 403);
    _strict.default.equal(rbacRes.body.error, "RBAC_FORBIDDEN");

    // 4. Replay DEAD_LETTER task via POST /v1beta1/management/queues/dead-letter/replay
    const replayResp = await fetch(`${baseUrl}/v1beta1/management/queues/dead-letter/replay`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      },
      body: JSON.stringify({
        taskIds: ["task-m90-dlq-1"],
        targetPriority: 1,
        actor: "chen",
        role: "SRE_ADMIN"
      })
    });
    _strict.default.equal(replayResp.status, 200);
    const replayBody = await replayResp.json();
    _strict.default.equal(replayBody.replayedCount, 1);
    _strict.default.deepEqual(replayBody.replayedTaskIds, ["task-m90-dlq-1"]);
    _strict.default.equal(dlqTask.state, _models.TaskState.QUEUED);
    _strict.default.equal(dlqTask.priority, 1);
    _strict.default.equal(dlqTask.retry_count, 0);

    // 5. CLI mgmt-queue-backpressure & mgmt-queue-dlq-replay
    const cliBp = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-queue-backpressure", "0.92"], baseUrl);
    _strict.default.equal(cliBp.exitCode, 0);
    const cliBpJson = JSON.parse(cliBp.stdout);
    _strict.default.equal(cliBpJson.backpressureActive, true);

    const cliReplay = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-queue-dlq-replay", "task-m90-dlq-1"], baseUrl);
    _strict.default.equal(cliReplay.exitCode, 0);

    // 6. Frontend UI markers
    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = _nodeFs.default.readFileSync(_nodePath.default.resolve(process.cwd(), rel), "utf8");
      _strict.default.ok(content.includes("simulate-queue-backpressure"), `${rel} missing simulate-queue-backpressure`);
      _strict.default.ok(content.includes("replay-queue-dead-letters"), `${rel} missing replay-queue-dead-letters`);
      _strict.default.ok(content.includes("/v1beta1/console/queues/backpressure"), `${rel} missing /v1beta1/console/queues/backpressure`);
    }
  } finally {
    await srv.stop();
    _nodeFs.default.rmSync(tmpDir, { recursive: true, force: true });
  }
});