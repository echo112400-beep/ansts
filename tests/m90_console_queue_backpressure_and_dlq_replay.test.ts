import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { TaskPriority, TaskSpec, TaskState } from "../src/models.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

test("M90: Queue Backpressure Matrix, Priority Load-Shedding Simulator & DLQ Replay Workbench", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m90-q-"));
  const srv = new AnstsLocalAndComposeServer({
    apiPort: 0,
    dashboardPort: 0,
    storageDir: tmpDir,
  });
  const { apiPort } = await srv.start();
  const baseUrl = `http://127.0.0.1:${apiPort}`;

  try {
    const guard = new ConsoleRbacAuthGuard();
    const adminToken = guard.issuePrincipalToken("chen");

    // Seed a DEAD_LETTER task and a queued P3_BATCH task
    const dlqTask = new TaskSpec({
      task_id: "task-m90-dlq-1",
      tenant_id: "Northstar",
      priority: TaskPriority.P2_STANDARD,
      prompt: "M90 DLQ replay target",
    });
    dlqTask.state = TaskState.DEAD_LETTER;
    dlqTask.retry_count = 4;
    srv.managementPlane.deps.scheduler.allTasks.set(dlqTask.task_id, dlqTask);

    srv.managementPlane.executeQueueManagementAction({
      action: "enqueue_task",
      taskId: "task-m90-p3-1",
      tenantId: "Northstar",
      priority: 3,
      actor: "chen",
      role: "SRE_ADMIN",
    });

    // 1. Low utilization (0.50): all P0..P3 admitted, HTTP 200
    const lowResp = await fetch(`${baseUrl}/v1beta1/console/queues/backpressure?utilizationRatio=0.50`, {
      headers: { authorization: `Bearer ${adminToken}` },
    });
    assert.equal(lowResp.status, 200);
    const lowBody = (await lowResp.json()) as {
      backpressureActive: boolean;
      backpressureHttpStatus: number;
      admissionMatrix: Array<{ priority: number; admitted: boolean }>;
      deadLetterCount: number;
    };
    assert.equal(lowBody.backpressureActive, false);
    assert.equal(lowBody.backpressureHttpStatus, 200);
    assert.ok(lowBody.admissionMatrix.every((r) => r.admitted === true));
    assert.ok(lowBody.deadLetterCount >= 1);

    // 2. High utilization (0.95): P0 admitted, P2/P3 shed, HTTP 429 backpressure active
    const highResp = await fetch(`${baseUrl}/v1beta1/console/queues/backpressure?utilizationRatio=0.95`, {
      headers: { authorization: `Bearer ${adminToken}` },
    });
    assert.equal(highResp.status, 200);
    const highBody = (await highResp.json()) as {
      backpressureActive: boolean;
      backpressureHttpStatus: number;
      retryAfterSec: number;
      admissionMatrix: Array<{ priority: number; admitted: boolean }>;
      shedCandidateTaskIds: string[];
    };
    assert.equal(highBody.backpressureActive, true);
    assert.equal(highBody.backpressureHttpStatus, 429);
    assert.ok(highBody.retryAfterSec >= 1);
    assert.equal(highBody.admissionMatrix.find((r) => r.priority === 0)?.admitted, true);
    assert.equal(highBody.admissionMatrix.find((r) => r.priority === 3)?.admitted, false);
    assert.ok(highBody.shedCandidateTaskIds.includes("task-m90-p3-1"));

    // 3. RBAC VIEWER blocked from DLQ replay (403)
    const rbacRes = srv.managementPlane.replayDeadLetterTasks({
      taskIds: ["task-m90-dlq-1"],
      actor: "xu",
      role: "VIEWER",
    });
    assert.equal(rbacRes.status, 403);
    assert.equal(rbacRes.body.error, "RBAC_FORBIDDEN");

    // 4. Replay DEAD_LETTER task via POST /v1beta1/management/queues/dead-letter/replay
    const replayResp = await fetch(`${baseUrl}/v1beta1/management/queues/dead-letter/replay`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      },
      body: JSON.stringify({
        taskIds: ["task-m90-dlq-1"],
        targetPriority: 1,
        actor: "chen",
        role: "SRE_ADMIN",
      }),
    });
    assert.equal(replayResp.status, 200);
    const replayBody = (await replayResp.json()) as { replayedCount: number; replayedTaskIds: string[] };
    assert.equal(replayBody.replayedCount, 1);
    assert.deepEqual(replayBody.replayedTaskIds, ["task-m90-dlq-1"]);
    assert.equal(dlqTask.state, TaskState.QUEUED);
    assert.equal(dlqTask.priority, 1);
    assert.equal(dlqTask.retry_count, 0);

    // 5. CLI mgmt-queue-backpressure & mgmt-queue-dlq-replay
    const cliBp = await runAnstsCtlRemote(["mgmt-queue-backpressure", "0.92"], baseUrl);
    assert.equal(cliBp.exitCode, 0);
    const cliBpJson = JSON.parse(cliBp.stdout) as { backpressureActive: boolean };
    assert.equal(cliBpJson.backpressureActive, true);

    const cliReplay = await runAnstsCtlRemote(["mgmt-queue-dlq-replay", "task-m90-dlq-1"], baseUrl);
    assert.equal(cliReplay.exitCode, 0);

    // 6. Frontend UI markers
    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = fs.readFileSync(path.resolve(process.cwd(), rel), "utf8");
      assert.ok(content.includes("simulate-queue-backpressure"), `${rel} missing simulate-queue-backpressure`);
      assert.ok(content.includes("replay-queue-dead-letters"), `${rel} missing replay-queue-dead-letters`);
      assert.ok(content.includes("/v1beta1/console/queues/backpressure"), `${rel} missing /v1beta1/console/queues/backpressure`);
    }
  } finally {
    await srv.stop();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});
