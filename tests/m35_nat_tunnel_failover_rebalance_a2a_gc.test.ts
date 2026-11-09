/**
 * @fileoverview M35 Deep Production Optimization E2E Suite:
 * 1. [M35-1] NAT-Traversal Outbound Reverse Tunnel Command Queue, Polling & Execution (`peerBodyUrls: {}`)
 * 2. [M35-2] Unhealthy Brain Shard Eviction, 256-Slot Rebalancing & Automatic S3 LTX Task Takeover
 * 3. [M35-3] Live LayerFS CAS Mark-and-Sweep Garbage Collection (`DELETE /v1/layerfs/branches/:branch` & `POST /v1beta1/storage/gc`)
 * 4. [M35-4] Live A2A Shared Blackboard CAS, Git Patch Arbiter & Wait-For-Graph Deadlock Detection over HTTP (`POST /v1beta1/a2a/blackboard`)
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as http from "node:http";
import { MultiNodeDevServer } from "../src/index.js";

describe("M35 Deep Production Optimization E2E Suite (NAT Reverse Tunnel, Brain Failover, CAS GC & A2A HTTP)", () => {
  it("1. [M35-1] Dispatches task to NAT-isolated Body daemon purely over outbound Reverse Tunnel polling (zero inbound Body ports)", async () => {
    // Gateway has NO peerBodyUrls configured (simulating Body behind strict NAT)
    const gw = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      peerBodyUrls: {},
    });
    const { apiPort: gwPort } = await gw.start(0, 0);

    // Body daemon dials out to Gateway via primaryGatewayUrl and polls /internal/v1beta1/tunnel/poll
    const bodyNat = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "body-nat-01",
      nodeRole: "body",
      primaryGatewayUrl: `http://127.0.0.1:${gwPort}`,
    });
    await bodyNat.start(0, 0);

    try {
      const dispRes = await fetch(`http://127.0.0.1:${gwPort}/v1beta1/tasks/dispatch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          tenantId: "tenant-nat",
          taskId: "task-nat-tunnel-001",
          promptText: "execute behind strict NAT via outbound reverse tunnel",
          targetBodyHostId: "body-nat-01",
          dispatchMode: "reverse_tunnel",
        }),
      });
      assert.equal(dispRes.status, 202);
      const dispJson = (await dispRes.json()) as {
        dispatched: boolean;
        bodyExecution: {
          executedByBodyId: string;
          deliveredViaReverseTunnel: boolean;
          routedViaGatewayId: string;
          tunnelRole: string;
          commitId: string;
          status: string;
        };
      };
      assert.equal(dispJson.dispatched, true);
      assert.ok(dispJson.bodyExecution);
      assert.equal(dispJson.bodyExecution.executedByBodyId, "body-nat-01");
      assert.equal(dispJson.bodyExecution.deliveredViaReverseTunnel, true);
      assert.equal(dispJson.bodyExecution.routedViaGatewayId, "celld-gw-01");
      assert.equal(dispJson.bodyExecution.tunnelRole, "PRIMARY");
      assert.equal(dispJson.bodyExecution.status, "COMPLETED");
    } finally {
      await bodyNat.stop();
      await gw.stop();
    }
  });

  it("2. [M35-2] Automatically evicts crashed Brain shard, rebalances 256-slot hash ring, and recovers tasks from S3 LTX on surviving Brain", async () => {
    const s3Objects = new Map<string, Buffer>();
    const s3Server = http.createServer((req, res) => {
      const u = new URL(req.url ?? "/", "http://127.0.0.1");
      if (req.method === "PUT") {
        const chunks: Buffer[] = [];
        req.on("data", (c: Buffer) => chunks.push(c));
        req.on("end", () => {
          s3Objects.set(u.pathname, Buffer.concat(chunks));
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ stored: true }));
        });
        return;
      }
      if (req.method === "GET" && s3Objects.has(u.pathname)) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(s3Objects.get(u.pathname));
        return;
      }
      res.writeHead(404).end();
    });
    const s3Port = await new Promise<number>((resolve) => {
      s3Server.listen(0, "127.0.0.1", () => {
        const addr = s3Server.address();
        resolve(typeof addr === "object" && addr ? addr.port : 0);
      });
    });
    const s3Endpoint = `http://127.0.0.1:${s3Port}`;

    const brain1 = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "brain-node-01",
      nodeRole: "brain",
      s3Endpoint,
    });
    const brain2 = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "brain-node-02",
      nodeRole: "brain",
      s3Endpoint,
    });
    const b1 = await brain1.start(0, 0);
    const b2 = await brain2.start(0, 0);

    const peerBrainUrls = {
      "brain-node-01": `http://127.0.0.1:${b1.apiPort}`,
      "brain-node-02": `http://127.0.0.1:${b2.apiPort}`,
    };

    const gw = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      peerBrainUrls,
    });
    const g = await gw.start(0, 0);

    try {
      // 1. Seed a task directly onto brain-node-01 so it flushes its LTX snapshot to S3
      const preRes = await fetch(`http://127.0.0.1:${b1.apiPort}/internal/v1beta1/brain/schedule`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-prior-on-b1",
          tenantId: "tenant-ha",
          promptText: "persisted to S3 before crash",
        }),
      });
      assert.equal(preRes.status, 200);

      // 2. Crash/stop brain-node-01!
      await brain1.stop();

      // 3. Find a task key that originally maps to brain-node-01 and dispatch it through Gateway
      let candidateTaskId = "task-failover-0";
      for (let i = 0; i < 50; i++) {
        const tid = `task-failover-${i}`;
        if (gw.shardCoordinator.resolveBrainForTask("tenant-ha", tid).brainNodeId === "brain-node-01") {
          candidateTaskId = tid;
          break;
        }
      }

      const dispRes = await fetch(`http://127.0.0.1:${g.apiPort}/v1beta1/tasks/dispatch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          tenantId: "tenant-ha",
          taskId: candidateTaskId,
          promptText: "trigger automatic shard failover to brain-node-02",
        }),
      });
      assert.equal(dispRes.status, 202);
      const dispJson = (await dispRes.json()) as {
        routedBrainNodeId: string;
        brainFailover: {
          evictedBrainNodeId: string;
          newOwnerBrainNodeId: string;
          newTopologyGeneration: number;
        };
      };
      assert.equal(dispJson.routedBrainNodeId, "brain-node-02");
      assert.ok(dispJson.brainFailover);
      assert.equal(dispJson.brainFailover.evictedBrainNodeId, "brain-node-01");
      assert.equal(dispJson.brainFailover.newOwnerBrainNodeId, "brain-node-02");

      // 4. Verify brain-node-02 also recovered `task-prior-on-b1` from brain-node-01's S3 LTX snapshot!
      const recoveredRes = await fetch(`http://127.0.0.1:${g.apiPort}/v1/tasks/task-prior-on-b1`);
      assert.equal(recoveredRes.status, 200);
      const recoveredTask = (await recoveredRes.json()) as { task_id: string };
      assert.equal(recoveredTask.task_id, "task-prior-on-b1");
    } finally {
      await gw.stop();
      await brain2.stop();
      await new Promise<void>((resolve) => s3Server.close(() => resolve()));
    }
  });

  it("3. [M35-3 & M35-4] Reclaims orphan LayerFS blocks via POST /v1beta1/storage/gc and arbitrates A2A Blackboard & WFG Deadlocks over HTTP", async () => {
    const srv = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "body-gc-a2a-01",
      nodeRole: "body",
    });
    const { apiPort } = await srv.start(0, 0);

    try {
      // 1. Create an ephemeral branch `ephemeral-aborted` and write a unique blob
      await fetch(`http://127.0.0.1:${apiPort}/v1/layerfs/exec`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          branch: "ephemeral-aborted",
          agent_id: "agent-gc",
          step_id: "step-orphan",
          command: "printf 'unreferenced-large-blob-content-1234567890' > orphan.bin",
        }),
      });

      // 2. Delete the ephemeral branch and run mark-and-sweep GC
      const delRes = await fetch(`http://127.0.0.1:${apiPort}/v1/layerfs/branches/ephemeral-aborted`, {
        method: "DELETE",
      });
      assert.equal(delRes.status, 200);

      const gcRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/storage/gc`, {
        method: "POST",
      });
      assert.equal(gcRes.status, 200);
      const gcJson = (await gcRes.json()) as {
        retainedBlobIds: string[];
        sweptBlobIds: string[];
        reclaimedBytes: number;
      };
      assert.equal(gcJson.sweptBlobIds.length, 1);
      assert.ok(gcJson.reclaimedBytes > 0);
      assert.ok(gcJson.retainedBlobIds.length >= 2); // main branch objects preserved

      // 3. Test A2A Blackboard CAS + Patch Arbitration + WFG Deadlock Detection over HTTP
      const cas1 = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/a2a/blackboard`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "putCas",
          key: "arch-plan",
          value: { step: 1 },
          expectedVersion: 0,
          authorAgentId: "agent-alpha",
        }),
      });
      assert.equal(cas1.status, 200);

      // Stale expectedVersion=0 should return 409 Conflict
      const casConflict = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/a2a/blackboard`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "putCas",
          key: "arch-plan",
          value: { step: 2 },
          expectedVersion: 0,
          authorAgentId: "agent-beta",
        }),
      });
      assert.equal(casConflict.status, 409);

      // Add WFG edges agent-A -> agent-B -> agent-A and verify deadlock cycle detection
      await fetch(`http://127.0.0.1:${apiPort}/v1beta1/a2a/blackboard`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "addWaitEdge",
          waiterAgentId: "agent-A",
          holderAgentId: "agent-B",
        }),
      });
      const wfgRes = await fetch(`http://127.0.0.1:${apiPort}/v1beta1/a2a/blackboard`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "addWaitEdge",
          waiterAgentId: "agent-B",
          holderAgentId: "agent-A",
        }),
      });
      const wfgJson = (await wfgRes.json()) as { hasDeadlock: boolean; cyclePath: string[] };
      assert.equal(wfgJson.hasDeadlock, true);
      assert.ok(wfgJson.cyclePath.length >= 3);
    } finally {
      await srv.stop();
    }
  });
});
