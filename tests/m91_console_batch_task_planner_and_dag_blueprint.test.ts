import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

test("M91: Batch Task Planning, Kahn Topological Wave Derivation, DAG Cycle Guard & Atomic Escrow Dispatch", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m91-batch-"));
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

    // 1. GET /v1beta1/console/tasks/blueprints returns 3 preset blueprints
    const bpResp = await fetch(`${baseUrl}/v1beta1/console/tasks/blueprints`, {
      headers: { authorization: `Bearer ${adminToken}` },
    });
    assert.equal(bpResp.status, 200);
    const bpBody = (await bpResp.json()) as {
      presets: Array<{ blueprintId: string; stages: unknown[] }>;
      customBlueprints: unknown[];
    };
    assert.equal(bpBody.presets.length, 3);
    assert.ok(bpBody.presets.some((p) => p.blueprintId === "fullstack-feature-pipeline"));

    // 2. Dry-Run fullstack-feature-pipeline: derives 3 topological waves with maxParallelism=2
    const dryResp = await fetch(`${baseUrl}/v1beta1/management/tasks/batch-plan`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      },
      body: JSON.stringify({
        mode: "dry_run",
        blueprintId: "fullstack-feature-pipeline",
        actor: "chen",
        role: "SRE_ADMIN",
      }),
    });
    assert.equal(dryResp.status, 200);
    const dryBody = (await dryResp.json()) as {
      valid: boolean;
      stageCount: number;
      waveCount: number;
      maxParallelism: number;
      waves: string[][];
      resourceSummary: { totalActCpuCores: number; requiredEscrowSlots: number };
    };
    assert.equal(dryBody.valid, true);
    assert.equal(dryBody.stageCount, 4);
    assert.equal(dryBody.waveCount, 3);
    assert.equal(dryBody.maxParallelism, 2);
    assert.deepEqual(dryBody.waves[0], ["plan-arch-spec"]);
    assert.deepEqual(dryBody.waves[2], ["verify-ci-canary"]);
    assert.equal(dryBody.resourceSummary.requiredEscrowSlots, 4);

    // 3. Cycle detection & RBAC guard
    const cycleRes = srv.managementPlane.planAndDispatchTaskBatch({
      mode: "dry_run",
      stages: [
        { taskId: "step-a", prompt: "Step A", dependsOn: ["step-b"] },
        { taskId: "step-b", prompt: "Step B", dependsOn: ["step-a"] },
      ],
      actor: "chen",
      role: "SRE_ADMIN",
    });
    assert.equal(cycleRes.status, 400);
    assert.equal(cycleRes.body.valid, false);
    assert.ok(
      (cycleRes.body.validationErrors as string[]).some((e) => e.includes("DAG_CYCLE_DETECTED"))
    );

    const rbacRes = srv.managementPlane.planAndDispatchTaskBatch({
      mode: "commit_and_dispatch",
      blueprintId: "distributed-chaos-verif",
      actor: "xu",
      role: "VIEWER",
    });
    assert.equal(rbacRes.status, 403);
    assert.equal(rbacRes.body.error, "RBAC_FORBIDDEN");

    // 4. Commit & Dispatch batch blueprint atomically into Scheduler, Queue & DagEscrow
    const commitResp = await fetch(`${baseUrl}/v1beta1/management/tasks/batch-plan`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      },
      body: JSON.stringify({
        mode: "commit_and_dispatch",
        blueprintId: "distributed-chaos-verif",
        actor: "chen",
        role: "SRE_ADMIN",
      }),
    });
    assert.equal(commitResp.status, 201);
    const commitBody = (await commitResp.json()) as {
      valid: boolean;
      batchId: string;
      coordinatorTaskId: string;
      dispatchedCount: number;
      dispatchedTaskIds: string[];
    };
    assert.equal(commitBody.valid, true);
    assert.equal(commitBody.dispatchedCount, 3);
    assert.ok(srv.managementPlane.deps.scheduler.allTasks.has(commitBody.coordinatorTaskId));
    for (const id of commitBody.dispatchedTaskIds) {
      assert.ok(srv.managementPlane.deps.scheduler.allTasks.has(id), `Missing dispatched task ${id}`);
    }

    // 5. CLI mgmt-task-blueprints & mgmt-task-batch-plan
    const cliBp = await runAnstsCtlRemote(["mgmt-task-blueprints"], baseUrl);
    assert.equal(cliBp.exitCode, 0);
    const cliBpJson = JSON.parse(cliBp.stdout) as { customBlueprints: unknown[] };
    assert.equal(cliBpJson.customBlueprints.length, 1);

    const cliPlan = await runAnstsCtlRemote(
      ["mgmt-task-batch-plan", "dry_run", "multi-tenant-finops-sweep"],
      baseUrl
    );
    assert.equal(cliPlan.exitCode, 0);
    const cliPlanJson = JSON.parse(cliPlan.stdout) as { valid: boolean; waveCount: number };
    assert.equal(cliPlanJson.valid, true);
    assert.equal(cliPlanJson.waveCount, 2);

    // 6. Frontend UI markers
    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = fs.readFileSync(path.resolve(process.cwd(), rel), "utf8");
      assert.ok(content.includes("open-batch-task-planner-dialog"), `${rel} missing open-batch-task-planner-dialog`);
      assert.ok(content.includes("batch-plan-dry-run"), `${rel} missing batch-plan-dry-run`);
      assert.ok(content.includes("batch-plan-commit-dispatch"), `${rel} missing batch-plan-commit-dispatch`);
      assert.ok(content.includes("/v1beta1/management/tasks/batch-plan"), `${rel} missing /v1beta1/management/tasks/batch-plan`);
    }
  } finally {
    await srv.stop();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});
