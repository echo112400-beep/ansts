/**
 * @fileoverview M95: Dynamic Cluster Topology Discovery, Multi-Role (Brain/Body/Gateway) Self-Registration,
 * 256-Slot Dynamic Rebalancing, Live RPC Routing & Heartbeat TTL Auto-Eviction E2E Suite
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M95 Dynamic Cluster Topology Discovery, Self-Registration & Multi-Role Elastic Scaling Suite", () => {
  it("auto-registers new brain, body, and gateway nodes on startup, rebalances 256 slots across 4 brains, routes live tasks to body-host-04, and auto-evicts expired nodes", async () => {
    const tmpGw = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m95-gw-"));
    const tmpBrain4 = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m95-brain4-"));
    const tmpBody4 = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m95-body4-"));

    const gwServer = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      apiPort: 0,
      dashboardPort: 0,
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: tmpGw,
    });
    const { apiPort: gwPort } = await gwServer.start(0, 0);
    const gwBaseUrl = `http://127.0.0.1:${gwPort}`;

    let brain4Server: AnstsLocalAndComposeServer | null = null;
    let body4Server: AnstsLocalAndComposeServer | null = null;

    try {
      const guard = new ConsoleRbacAuthGuard();
      const adminToken = guard.issuePrincipalToken("chen");
      const viewerToken = guard.issuePrincipalToken("xu");

      // 1. Verify initial topology has 3 brains, 3 bodies, 2 gateways
      const initTopoRes = await fetch(`${gwBaseUrl}/v1beta1/console/topology`);
      assert.equal(initTopoRes.status, 200);
      const initTopo = (await initTopoRes.json()) as {
        countsByRole: { brain: number; body: number; gateway: number };
        slotDistribution: Record<string, number>;
        slotOwners: string[];
      };
      assert.equal(initTopo.countsByRole.brain, 3);
      assert.equal(initTopo.countsByRole.body, 3);
      assert.equal(initTopo.countsByRole.gateway, 2);
      assert.equal(initTopo.slotOwners.length, 256);

      // 2. Boot a real 4th Brain node (`celld-brain-04`) pointing seedGatewayUrls at gwBaseUrl -> auto-registers on startup!
      brain4Server = new AnstsLocalAndComposeServer({
        host: "127.0.0.1",
        apiPort: 0,
        dashboardPort: 0,
        nodeId: "celld-brain-04",
        nodeRole: "brain",
        region: "ap-northeast-1",
        seedGatewayUrls: [gwBaseUrl],
        storageDir: tmpBrain4,
      });
      const { apiPort: brain4Port } = await brain4Server.start(0, 0);
      const brain4Url = `http://127.0.0.1:${brain4Port}`;

      assert.equal(gwServer.peerBrainUrls["brain-node-04"], brain4Url);
      assert.equal(gwServer.peerBrainUrls["celld-brain-04"], brain4Url);

      const topoAfterBrain4 = (await (await fetch(`${gwBaseUrl}/v1beta1/console/topology`)).json()) as {
        countsByRole: { brain: number; body: number; gateway: number };
        activeBrainMembers: string[];
        slotDistribution: Record<string, number>;
        brains: Array<{ nodeId: string; brainKey: string; ownedSlots: number; baseUrl: string; registeredVia: string }>;
      };
      assert.equal(topoAfterBrain4.countsByRole.brain, 4);
      assert.ok(topoAfterBrain4.activeBrainMembers.includes("brain-node-04"));
      assert.ok((topoAfterBrain4.slotDistribution["brain-node-04"] ?? 0) > 40);
      const totalSlots4 = Object.values(topoAfterBrain4.slotDistribution).reduce((a, b) => a + b, 0);
      assert.equal(totalSlots4, 256);
      const b4Entry = topoAfterBrain4.brains.find((b) => b.nodeId === "celld-brain-04");
      assert.ok(b4Entry);
      assert.equal(b4Entry.baseUrl, brain4Url);
      assert.equal(b4Entry.registeredVia, "SELF_REG");

      // 3. Boot a real 4th Body node (`body-host-04`) pointing primaryGatewayUrl at gwBaseUrl -> auto-registers both cluster topology & reverse tunnel!
      body4Server = new AnstsLocalAndComposeServer({
        host: "127.0.0.1",
        apiPort: 0,
        dashboardPort: 0,
        nodeId: "body-host-04",
        nodeRole: "body",
        region: "us-west-2",
        primaryGatewayUrl: gwBaseUrl,
        storageDir: tmpBody4,
      });
      const { apiPort: body4Port } = await body4Server.start(0, 0);
      const body4Url = `http://127.0.0.1:${body4Port}`;

      assert.equal(gwServer.peerBodyUrls["body-host-04"], body4Url);

      // Dispatch a distributed task targeting the newly self-registered body-host-04
      const dispRes = await fetch(`${gwBaseUrl}/v1beta1/tasks/dispatch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          tenantId: "Northstar",
          taskId: "task-dynamic-body4-01",
          promptText: "Execute on dynamically registered body-host-04",
          targetBodyHostId: "body-host-04",
        }),
      });
      assert.equal(dispRes.status, 202);
      const dispBody = (await dispRes.json()) as {
        dispatched: boolean;
        targetBodyHostId: string;
        bodyExecution: { executedByBodyId: string; status: string };
      };
      assert.equal(dispBody.dispatched, true);
      assert.equal(dispBody.targetBodyHostId, "body-host-04");
      assert.equal(dispBody.bodyExecution.executedByBodyId, "body-host-04");
      assert.equal(dispBody.bodyExecution.status, "COMPLETED");

      // 4. Scale out a 3rd Gateway node (`celld-gw-03`, role: "gateway") via /v1beta1/management/nodes/scale
      const viewerScale = await fetch(`${gwBaseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${viewerToken}`,
        },
        body: JSON.stringify({
          action: "scale_out",
          nodeId: "celld-gw-03",
          role: "gateway",
          region: "ap-northeast-1",
          baseUrl: "http://127.0.0.1:18086",
        }),
      });
      assert.equal(viewerScale.status, 403);

      const adminScaleGw = await fetch(`${gwBaseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({
          action: "scale_out",
          nodeId: "celld-gw-03",
          role: "gateway",
          region: "ap-northeast-1",
          baseUrl: "http://127.0.0.1:18086",
        }),
      });
      assert.equal(adminScaleGw.status, 201);
      assert.equal(gwServer.peerGatewayUrls["celld-gw-03"], "http://127.0.0.1:18086");

      const stateAfterGw3 = (await (await fetch(`${gwBaseUrl}/v1beta1/management/state`)).json()) as {
        nodes: Array<{ id: string; role: string; tunnelGw: string }>;
        dynamicTopology: {
          countsByRole: { brain: number; body: number; gateway: number };
          activeGatewayUrls: string[];
        };
      };
      assert.equal(stateAfterGw3.dynamicTopology.countsByRole.gateway, 3);
      assert.ok(stateAfterGw3.dynamicTopology.activeGatewayUrls.includes("http://127.0.0.1:18086"));
      const gw3Node = stateAfterGw3.nodes.find((n) => n.id === "celld-gw-03");
      assert.ok(gw3Node);
      assert.ok(gw3Node.role.includes("Edge Gateway"));
      assert.ok(gw3Node.tunnelGw.includes("INGRESS_GW"));

      // 5. Simulate stale heartbeat on celld-brain-04 and trigger TTL auto-eviction
      const staleTs = Date.now() - 45_000;
      await fetch(`${gwBaseUrl}/internal/v1beta1/cluster/heartbeat`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          nodeId: "celld-brain-04",
          role: "brain",
          cpuPct: 91.2,
          psiPct: 18.5,
          nowMs: staleTs,
        }),
      });

      const evictRes = await fetch(`${gwBaseUrl}/v1beta1/management/topology/evict-expired`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({ ttlMs: 15000 }),
      });
      assert.equal(evictRes.status, 200);
      const evictBody = (await evictRes.json()) as {
        evictedCount: number;
        evictedNodes: Array<{ nodeId: string; role: string }>;
        dynamicTopology: {
          activeBrainMembers: string[];
          slotDistribution: Record<string, number>;
        };
      };
      assert.equal(evictBody.evictedCount, 1);
      assert.equal(evictBody.evictedNodes[0].nodeId, "celld-brain-04");
      assert.equal(evictBody.dynamicTopology.activeBrainMembers.includes("brain-node-04"), false);
      assert.equal(evictBody.dynamicTopology.slotDistribution["brain-node-04"], undefined);
      assert.equal(gwServer.peerBrainUrls["brain-node-04"], undefined);

      // 6. Verify CLI commands anstsctl mgmt-topology and anstsctl cluster-register
      const cliTopo = await runAnstsCtlRemote(["mgmt-topology"], gwBaseUrl);
      assert.equal(cliTopo.exitCode, 0);
      const parsedTopo = JSON.parse(cliTopo.stdout) as { totalRegisteredNodes: number };
      assert.ok(parsedTopo.totalRegisteredNodes >= 9);

      const cliReg = await runAnstsCtlRemote(
        ["cluster-register", "celld-brain-05", "brain", "http://127.0.0.1:18089", "eu-central-1"],
        gwBaseUrl
      );
      assert.equal(cliReg.exitCode, 0);
      const parsedReg = JSON.parse(cliReg.stdout) as {
        registered: boolean;
        nodeId: string;
        dynamicTopology: { activeBrainMembers: string[] };
      };
      assert.equal(parsedReg.registered, true);
      assert.ok(parsedReg.dynamicTopology.activeBrainMembers.includes("brain-node-05"));
    } finally {
      if (body4Server) await body4Server.stop();
      if (brain4Server) await brain4Server.stop();
      await gwServer.stop();
      fs.rmSync(tmpGw, { recursive: true, force: true });
      fs.rmSync(tmpBrain4, { recursive: true, force: true });
      fs.rmSync(tmpBody4, { recursive: true, force: true });
    }
  });

  it("verifies zero-hardcode dynamic slot rendering and multi-role topology controls in prototype.js & docs/index.html", () => {
    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = fs.readFileSync(path.resolve(process.cwd(), rel), "utf-8");
      assert.ok(content.includes("quick-self-register-node"), `${rel} must include quick-self-register-node`);
      assert.ok(content.includes("evict-expired-cluster-nodes"), `${rel} must include evict-expired-cluster-nodes`);
      assert.ok(content.includes("send-node-heartbeat"), `${rel} must include send-node-heartbeat`);
      assert.ok(content.includes('<option value="gateway">'), `${rel} scale dialog must include gateway role option`);
      assert.ok(content.includes("state.slotOwners"), `${rel} must render 256-slot ring from dynamic state.slotOwners`);
    }
  });
});
