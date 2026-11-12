import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M86 Live DAG Dependency & Escrow Topology Visualizer, Sub-Agent Lifecycle & Batch Task Governance Suite", () => {
  it("[M86-1] Exposes live DAG escrows & edges via GET /v1beta1/console/dag and executes spawn/complete/fail/batch-cancel", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m86-dag-"));
    const server = new AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new ConsoleRbacAuthGuard();
      const chenToken = guard.issuePrincipalToken("chen");
      const xuToken = guard.issuePrincipalToken("xu");

      // 1. Inspect initial dagVisualization via GET /v1beta1/console/dag
      const initResp = await fetch(`${baseUrl}/v1beta1/console/dag`);
      assert.equal(initResp.status, 200);
      const initDag = (await initResp.json()) as {
        totalEscrowsCount: number;
        totalEdgesCount: number;
        escrows: Array<{
          parentTaskId: string;
          usedSlots: number;
          escrowedSlots: number;
          children: Array<{ taskId: string; state: string; dependsOn: string[] }>;
        }>;
      };
      assert.equal(initDag.totalEscrowsCount, 1);
      assert.equal(initDag.totalEdgesCount, 2);
      assert.equal(initDag.escrows[0].parentTaskId, "task-ns-8842");
      assert.equal(initDag.escrows[0].usedSlots, 2);
      assert.equal(initDag.escrows[0].escrowedSlots, 3);
      assert.equal(initDag.escrows[0].children.length, 2);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/tasks/dag-action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`,
        },
        body: JSON.stringify({
          action: "spawn_sub_agent",
          parentTaskId: "task-ns-8842",
        }),
      });
      assert.equal(viewerResp.status, 403);

      // 3. Spawn a 3rd sub-agent and complete it
      const spawnResp = await fetch(`${baseUrl}/v1beta1/management/tasks/dag-action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "spawn_sub_agent",
          parentTaskId: "task-ns-8842",
          childTaskId: "task-ns-8842-sub3",
          prompt: "Verify M86 dynamic sub-agent",
        }),
      });
      assert.equal(spawnResp.status, 201);
      const spawnBody = (await spawnResp.json()) as {
        childTaskId: string;
        dagVisualization: { totalEdgesCount: number; escrows: Array<{ usedSlots: number }> };
      };
      assert.equal(spawnBody.childTaskId, "task-ns-8842-sub3");
      assert.equal(spawnBody.dagVisualization.totalEdgesCount, 3);
      assert.equal(spawnBody.dagVisualization.escrows[0].usedSlots, 3);

      const compResp = await fetch(`${baseUrl}/v1beta1/management/tasks/dag-action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "complete_sub_agent",
          parentTaskId: "task-ns-8842",
          childTaskId: "task-ns-8842-sub3",
        }),
      });
      assert.equal(compResp.status, 200);

      // 4. Batch cancel tasks
      const batchResp = await fetch(`${baseUrl}/v1beta1/management/tasks/dag-action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "batch_cancel_tasks",
          taskIds: ["task-ns-8842-sub1", "task-ns-8842-sub2"],
        }),
      });
      assert.equal(batchResp.status, 200);
      const batchBody = (await batchResp.json()) as { batchCount: number };
      assert.equal(batchBody.batchCount, 2);

      // 5. Verify CLI anstsctl mgmt-dag & UI bindings
      const cliDag = await runAnstsCtlRemote(["mgmt-dag", "inspect"], baseUrl);
      assert.equal(cliDag.exitCode, 0);
      const cliParsed = JSON.parse(cliDag.stdout) as { totalEdgesCount: number };
      assert.ok(cliParsed.totalEdgesCount >= 2);

      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        assert.ok(content.includes("task-batch-check"));
        assert.ok(content.includes("dag-batch-cancel-tasks"));
        assert.ok(content.includes("dag-spawn-subagent"));
        assert.ok(content.includes("dag-complete-subagent"));
        assert.ok(content.includes("dag-fail-subagent"));
      }
    } finally {
      await server.stop();
    }
  });
});
