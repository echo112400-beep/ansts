/**
 * @fileoverview M40 End-to-End Verification: TanStack Control Plane Console UI & BFF Aggregation Routes
 * Tests:
 * 1. Static serving of the self-contained TanStack Console UI (`GET /` & `GET /ansts_console_live.html`)
 * 2. Live 256-slot ring & 11-service topology BFF (`GET /v1beta1/console/topology`)
 * 3. Interactive Brain shard eviction & restoration via Console BFF (`POST /v1beta1/console/shards/rebalance`)
 * 4. Global stateless Task Table fan-out across Brain shards (`GET /v1beta1/console/tasks`)
 * 5. LayerFS Branch & Line-Level Blame explorer BFF (`GET /v1beta1/console/layerfs`)
 */
import "./console_write_auth.js";
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";

describe("M40 TanStack Control Plane Console UI & BFF Aggregation Suite", () => {
  it("serves full TanStack Console HTML and executes all Console BFF endpoints end-to-end", async () => {
    const tmpDir = `/tmp/ansts-m40-${process.pid}-${Date.now()}`;
    fs.mkdirSync(tmpDir, { recursive: true });

    const brain1 = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "brain-node-01",
      nodeRole: "brain",
      storageDir: `${tmpDir}/brain-01`,
    });
    const brain2 = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "brain-node-02",
      nodeRole: "brain",
      storageDir: `${tmpDir}/brain-02`,
    });

    const { apiPort: b1Port } = await brain1.start(0, 0);
    const { apiPort: b2Port } = await brain2.start(0, 0);

    const peerBrainUrls = {
      "brain-node-01": `http://127.0.0.1:${b1Port}`,
      "brain-node-02": `http://127.0.0.1:${b2Port}`,
    };

    const gateway = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      peerBrainUrls,
      storageDir: `${tmpDir}/gw-01`,
    });
    const { apiPort: gwPort } = await gateway.start(0, 0);
    const baseUrl = `http://127.0.0.1:${gwPort}`;

    try {
      // 1. Verify HTML Console UI delivery
      const htmlResp = await fetch(`${baseUrl}/ansts_console_live.html`);
      assert.equal(htmlResp.status, 200);
      const htmlText = await htmlResp.text();
      assert.ok(htmlText.length > 20000, "Console HTML should be a rich, self-contained application");
      assert.ok(htmlText.includes("ANSTS TanStack Control Plane Console"));
      assert.ok(htmlText.includes("id=\"view-overview\""));
      assert.ok(htmlText.includes("id=\"view-scheduler\""));
      assert.ok(htmlText.includes("id=\"view-layerfs\""));
      assert.ok(htmlText.includes("id=\"view-a2a\""));
      assert.ok(htmlText.includes("id=\"view-governance\""));

      // 2. Verify /v1beta1/console/topology
      const topoResp = await fetch(`${baseUrl}/v1beta1/console/topology`);
      assert.equal(topoResp.status, 200);
      const topoData = (await topoResp.json()) as {
        virtualSlots: number;
        slotOwners: string[];
        slotDistribution: Record<string, number>;
        servicesCount: number;
      };
      assert.equal(topoData.virtualSlots, 256);
      assert.equal(topoData.slotOwners.length, 256);
      assert.equal(topoData.servicesCount, 11);
      assert.ok((topoData.slotDistribution["brain-node-01"] ?? 0) > 0);
      assert.ok((topoData.slotDistribution["brain-node-02"] ?? 0) > 0);

      // 3. Verify interactive Brain shard rebalance (evict brain-node-02, then restore)
      const evictResp = await fetch(`${baseUrl}/v1beta1/console/shards/rebalance`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action: "evict", brainNodeId: "brain-node-02" }),
      });
      assert.equal(evictResp.status, 200);
      const evictData = (await evictResp.json()) as {
        slotDistribution: Record<string, number>;
      };
      assert.equal(evictData.slotDistribution["brain-node-01"], 256);
      assert.equal(evictData.slotDistribution["brain-node-02"] ?? 0, 0);

      const restoreResp = await fetch(`${baseUrl}/v1beta1/console/shards/rebalance`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ action: "register", brainNodeId: "brain-node-02" }),
      });
      assert.equal(restoreResp.status, 200);
      const restoreData = (await restoreResp.json()) as {
        slotDistribution: Record<string, number>;
      };
      assert.ok((restoreData.slotDistribution["brain-node-02"] ?? 0) > 0);

      // 4. Dispatch a task and verify /v1beta1/console/tasks stateless fan-out
      const dispResp = await fetch(`${baseUrl}/v1beta1/tasks/dispatch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-console-m40",
          tenantId: "tenant-ui",
          promptText: "Verify TanStack Console Task Table fan-out",
        }),
      });
      assert.equal(dispResp.status, 202);

      const tasksResp = await fetch(`${baseUrl}/v1beta1/console/tasks`);
      assert.equal(tasksResp.status, 200);
      const tasksData = (await tasksResp.json()) as {
        totalTasks: number;
        tasks: Array<{ task_id: string; tenant_id: string; shardSlot: number }>;
      };
      assert.ok(tasksData.totalTasks >= 1);
      assert.ok(tasksData.tasks.some((t) => t.task_id === "task-console-m40"));

      // 5. Verify /v1beta1/console/layerfs branch summary & blame
      const lfsResp = await fetch(`${baseUrl}/v1beta1/console/layerfs?branch=main&file=package.json`);
      assert.equal(lfsResp.status, 200);
      const lfsData = (await lfsResp.json()) as {
        branches: Array<{ branchName: string; headCommitId: string }>;
        blame: Array<{ lineNumber: number; stepId: string }>;
      };
      assert.ok(lfsData.branches.some((b) => b.branchName === "main" && b.headCommitId === "C0"));
      assert.ok(Array.isArray(lfsData.blame) && lfsData.blame.length >= 1);
      assert.equal(lfsData.blame[0].stepId, "bootstrap-init");
    } finally {
      await gateway.stop();
      await brain1.stop();
      await brain2.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
