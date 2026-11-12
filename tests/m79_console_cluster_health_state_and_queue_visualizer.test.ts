/**
 * @fileoverview M79: TanStack Console Cluster Management, Health Matrix, State Visualization & DRF Queue Visualizer Suite
 * Verifies:
 * 1. [M79-1] `HierarchicalQueueManager.inspectQueueSnapshot()` & Live Queue BFF (`GET /v1beta1/console/queues`, `POST /v1beta1/management/queues/action`, `anstsctl mgmt-queues`):
 *    - Real-time per-tenant Binary Max-Heap inspection (`baseScore`, `agingBonus`, `preemptionBonus`, `drfPenalty`, `effectiveScore`, `nextTournamentWinner`).
 *    - Interactive `enqueue_task`, in-place `reprioritize_queued_task` (`P2 -> P0`), tournament `pop_and_schedule_next` (transitioning to `RUNNING_ACT` in SQLite `v25_scheduler_tasks` + `TurnWalEngine`), `remove_queued_task`, and `enqueue_multi_tenant_burst`.
 * 2. [M79-2] Cluster Health Matrix & State Machine Visualization (`GET /v1beta1/console/health` & `GET /v1beta1/management/state`):
 *    - 6 live subsystem health probes (`PROBE-GATEWAY`, `PROBE-CELLD`, `PROBE-SHARD-RING`, `PROBE-SQLITE-RPO0`, `PROBE-TUNNEL-HA`, `PROBE-AUDIT-CHAIN`) + real V8 process memory (`rssMb`, `heapUsedMb`).
 *    - `stateVisualization.taskStateDistribution` (`QUEUED`, `LEASED`, `RUNNING_ACT`, `COMPLETED`, etc.) and `celldNodeLoad` (`/state` counters).
 * 3. [M79-3] TanStack Console UI (`docs/console-management/prototype.js` & `docs/index.html`) Queue, Health & State Visualization Workbench.
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M79 TanStack Console Cluster Management, Health Matrix, State Visualization & DRF Queue Visualizer Suite", () => {
  it("[M79-3] Exposes renderQueuesAndHealthSection, queue workbench actions, and health/state visual panels in prototype.js & docs/index.html", () => {
    const indexPath = path.resolve(process.cwd(), "docs", "index.html");
    const protoPath = path.resolve(process.cwd(), "docs", "console-management", "prototype.js");

    for (const p of [indexPath, protoPath]) {
      const content = fs.readFileSync(p, "utf-8");
      assert.equal(content.includes("renderQueuesAndHealthSection"), true);
      assert.equal(content.includes("open-enqueue-task-dialog"), true);
      assert.equal(content.includes("queue-enqueue-burst"), true);
      assert.equal(content.includes("queue-pop-schedule"), true);
      assert.equal(content.includes("queue-promote-p0"), true);
      assert.equal(content.includes("queue-remove-task"), true);
      assert.equal(content.includes("/v1beta1/management/queues/action"), true);
    }
  });

  it("[M79-1 & M79-2] Executes live DRF Binary Max-Heap Queue visualization, in-place P0 promotion, tournament pop-and-schedule, and 6-probe Health & State Matrix", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m79-"));
    const server = new AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new ConsoleRbacAuthGuard();
      const adminToken = guard.issuePrincipalToken("chen");
      const viewerToken = guard.issuePrincipalToken("xu");
      const adminHeaders = {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      };

      // 1. Verify GET /v1beta1/console/health returns 6 healthy probes, processMemory, and stateVisualization
      const healthResp = await fetch(`${baseUrl}/v1beta1/console/health`);
      assert.equal(healthResp.status, 200);
      const healthData = (await healthResp.json()) as {
        healthMatrix: {
          overallHealthy: boolean;
          overallStatus: string;
          probes: Array<{ id: string; status: string }>;
          processMemory: { rssMb: number; heapUsedMb: number };
        };
        stateVisualization: {
          taskStateDistribution: Record<string, number>;
          celldNodeLoad: { owned_cells: number; resident_cells: number; capacity_waiting: number };
        };
      };
      assert.equal(healthData.healthMatrix.overallHealthy, true);
      assert.equal(healthData.healthMatrix.overallStatus, "HEALTHY");
      assert.equal(healthData.healthMatrix.probes.length, 6);
      assert.equal(
        healthData.healthMatrix.probes.every((p) => p.status === "HEALTHY"),
        true
      );
      assert.ok(healthData.healthMatrix.processMemory.rssMb > 0);
      assert.ok(healthData.stateVisualization.taskStateDistribution.QUEUED >= 3);
      assert.ok(healthData.stateVisualization.celldNodeLoad.owned_cells >= 7);

      // 2. Verify VIEWER token is blocked with 403 on POST /v1beta1/management/queues/action
      const viewerQueueResp = await fetch(`${baseUrl}/v1beta1/management/queues/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${viewerToken}`,
        },
        body: JSON.stringify({ action: "pop_and_schedule_next" }),
      });
      assert.equal(viewerQueueResp.status, 403);

      // 3. Enqueue a P2 task "task-vg-m79" for Vanguard -> reprioritize it in-place to P0 -> verify it becomes nextTournamentWinner -> pop & schedule!
      const enqResp = await fetch(`${baseUrl}/v1beta1/management/queues/action`, {
        method: "POST",
        headers: adminHeaders,
        body: JSON.stringify({
          action: "enqueue_task",
          taskId: "task-vg-m79",
          tenantId: "Vanguard",
          priority: 2,
          prompt: "Vanguard M79 queue test task",
        }),
      });
      assert.equal(enqResp.status, 201);

      const promoteResp = await fetch(`${baseUrl}/v1beta1/management/queues/action`, {
        method: "POST",
        headers: adminHeaders,
        body: JSON.stringify({
          action: "reprioritize_queued_task",
          taskId: "task-vg-m79",
          priority: 0,
        }),
      });
      assert.equal(promoteResp.status, 200);
      const promoteBody = (await promoteResp.json()) as {
        reprioritized: boolean;
        newPriority: number;
        queueVisualization: {
          nextTournamentWinner: { taskId: string; tenantId: string; priority: number; effectiveScore: number };
        };
      };
      assert.equal(promoteBody.reprioritized, true);
      assert.equal(promoteBody.newPriority, 0);
      assert.equal(promoteBody.queueVisualization.nextTournamentWinner.taskId, "task-vg-m79");
      assert.equal(promoteBody.queueVisualization.nextTournamentWinner.tenantId, "Vanguard");
      assert.ok(promoteBody.queueVisualization.nextTournamentWinner.effectiveScore >= 1000);

      // Pop & schedule the tournament winner ("task-vg-m79") and verify it transitions to RUNNING_ACT in SQLite & TurnWalEngine
      const popResp = await fetch(`${baseUrl}/v1beta1/management/queues/action`, {
        method: "POST",
        headers: adminHeaders,
        body: JSON.stringify({
          action: "pop_and_schedule_next",
        }),
      });
      assert.equal(popResp.status, 200);
      const popBody = (await popResp.json()) as {
        popped: boolean;
        scheduledTaskId: string;
        assignedBodyHostId: string;
      };
      assert.equal(popBody.popped, true);
      assert.equal(popBody.scheduledTaskId, "task-vg-m79");
      assert.ok(popBody.assignedBodyHostId.startsWith("body-"));

      const taskCheckResp = await fetch(`${baseUrl}/v1/tasks/task-vg-m79`);
      assert.equal(taskCheckResp.status, 200);
      const taskCheckBody = (await taskCheckResp.json()) as { state: string };
      assert.equal(taskCheckBody.state, "RUNNING_ACT");

      // 4. Remove a queued task ("task-vg-1049") and verify CLI `mgmt-queues`
      const remResp = await fetch(`${baseUrl}/v1beta1/management/queues/action`, {
        method: "POST",
        headers: adminHeaders,
        body: JSON.stringify({
          action: "remove_queued_task",
          taskId: "task-vg-1049",
        }),
      });
      assert.equal(remResp.status, 200);

      const cliInspect = await runAnstsCtlRemote(["mgmt-queues", "inspect"], baseUrl);
      assert.equal(cliInspect.exitCode, 0);
      const cliInspectJson = JSON.parse(cliInspect.stdout) as { totalQueuedCount: number };
      assert.equal(cliInspectJson.totalQueuedCount, 2);

      const cliBurst = await runAnstsCtlRemote(["mgmt-queues", "enqueue_multi_tenant_burst"], baseUrl);
      assert.equal(cliBurst.exitCode, 0);
      const cliBurstJson = JSON.parse(cliBurst.stdout) as {
        enqueuedBurstCount: number;
        queueVisualization: { totalQueuedCount: number };
      };
      assert.equal(cliBurstJson.enqueuedBurstCount, 4);
      assert.equal(cliBurstJson.queueVisualization.totalQueuedCount, 6);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
