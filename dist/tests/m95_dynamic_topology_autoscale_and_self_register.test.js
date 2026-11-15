"use strict";



var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M95: Dynamic Cluster Topology Discovery, Multi-Role (Brain/Body/Gateway) Self-Registration,
 * 256-Slot Dynamic Rebalancing, Live RPC Routing & Heartbeat TTL Auto-Eviction E2E Suite
 */(0, _nodeTest.describe)("M95 Dynamic Cluster Topology Discovery, Self-Registration & Multi-Role Elastic Scaling Suite", () => {(0, _nodeTest.it)("auto-registers new brain, body, and gateway nodes on startup, rebalances 256 slots across 4 brains, routes live tasks to body-host-04, and auto-evicts expired nodes", async () => {
    const tmpGw = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m95-gw-"));
    const tmpBrain4 = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m95-brain4-"));
    const tmpBody4 = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m95-body4-"));

    const gwServer = new _server_main.AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      apiPort: 0,
      dashboardPort: 0,
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: tmpGw
    });
    const { apiPort: gwPort } = await gwServer.start(0, 0);
    const gwBaseUrl = `http://127.0.0.1:${gwPort}`;

    let brain4Server = null;
    let body4Server = null;

    try {
      const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
      const adminToken = guard.issuePrincipalToken("chen");
      const viewerToken = guard.issuePrincipalToken("xu");

      // 1. Verify initial topology has 3 brains, 3 bodies, 2 gateways
      const initTopoRes = await fetch(`${gwBaseUrl}/v1beta1/console/topology`);
      _strict.default.equal(initTopoRes.status, 200);
      const initTopo = await initTopoRes.json();




      _strict.default.equal(initTopo.countsByRole.brain, 3);
      _strict.default.equal(initTopo.countsByRole.body, 3);
      _strict.default.equal(initTopo.countsByRole.gateway, 2);
      _strict.default.equal(initTopo.slotOwners.length, 256);

      // 2. Boot a real 4th Brain node (`celld-brain-04`) pointing seedGatewayUrls at gwBaseUrl -> auto-registers on startup!
      brain4Server = new _server_main.AnstsLocalAndComposeServer({
        host: "127.0.0.1",
        apiPort: 0,
        dashboardPort: 0,
        nodeId: "celld-brain-04",
        nodeRole: "brain",
        region: "ap-northeast-1",
        seedGatewayUrls: [gwBaseUrl],
        storageDir: tmpBrain4
      });
      const { apiPort: brain4Port } = await brain4Server.start(0, 0);
      const brain4Url = `http://127.0.0.1:${brain4Port}`;

      _strict.default.equal(gwServer.peerBrainUrls["brain-node-04"], brain4Url);
      _strict.default.equal(gwServer.peerBrainUrls["celld-brain-04"], brain4Url);

      const topoAfterBrain4 = await (await fetch(`${gwBaseUrl}/v1beta1/console/topology`)).json();





      _strict.default.equal(topoAfterBrain4.countsByRole.brain, 4);
      _strict.default.ok(topoAfterBrain4.activeBrainMembers.includes("brain-node-04"));
      _strict.default.ok((topoAfterBrain4.slotDistribution["brain-node-04"] ?? 0) > 40);
      const totalSlots4 = Object.values(topoAfterBrain4.slotDistribution).reduce((a, b) => a + b, 0);
      _strict.default.equal(totalSlots4, 256);
      const b4Entry = topoAfterBrain4.brains.find((b) => b.nodeId === "celld-brain-04");
      _strict.default.ok(b4Entry);
      _strict.default.equal(b4Entry.baseUrl, brain4Url);
      _strict.default.equal(b4Entry.registeredVia, "SELF_REG");

      // 3. Boot a real 4th Body node (`body-host-04`) pointing primaryGatewayUrl at gwBaseUrl -> auto-registers both cluster topology & reverse tunnel!
      body4Server = new _server_main.AnstsLocalAndComposeServer({
        host: "127.0.0.1",
        apiPort: 0,
        dashboardPort: 0,
        nodeId: "body-host-04",
        nodeRole: "body",
        region: "us-west-2",
        primaryGatewayUrl: gwBaseUrl,
        storageDir: tmpBody4
      });
      const { apiPort: body4Port } = await body4Server.start(0, 0);
      const body4Url = `http://127.0.0.1:${body4Port}`;

      _strict.default.equal(gwServer.peerBodyUrls["body-host-04"], body4Url);

      // Dispatch a distributed task targeting the newly self-registered body-host-04
      const dispRes = await fetch(`${gwBaseUrl}/v1beta1/tasks/dispatch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          tenantId: "Northstar",
          taskId: "task-dynamic-body4-01",
          promptText: "Execute on dynamically registered body-host-04",
          targetBodyHostId: "body-host-04"
        })
      });
      _strict.default.equal(dispRes.status, 202);
      const dispBody = await dispRes.json();




      _strict.default.equal(dispBody.dispatched, true);
      _strict.default.equal(dispBody.targetBodyHostId, "body-host-04");
      _strict.default.equal(dispBody.bodyExecution.executedByBodyId, "body-host-04");
      _strict.default.equal(dispBody.bodyExecution.status, "COMPLETED");

      // 4. Scale out a 3rd Gateway node (`celld-gw-03`, role: "gateway") via /v1beta1/management/nodes/scale
      const viewerScale = await fetch(`${gwBaseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${viewerToken}`
        },
        body: JSON.stringify({
          action: "scale_out",
          nodeId: "celld-gw-03",
          role: "gateway",
          region: "ap-northeast-1",
          baseUrl: "http://127.0.0.1:18086"
        })
      });
      _strict.default.equal(viewerScale.status, 403);

      const adminScaleGw = await fetch(`${gwBaseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`
        },
        body: JSON.stringify({
          action: "scale_out",
          nodeId: "celld-gw-03",
          role: "gateway",
          region: "ap-northeast-1",
          baseUrl: "http://127.0.0.1:18086"
        })
      });
      _strict.default.equal(adminScaleGw.status, 201);
      _strict.default.equal(gwServer.peerGatewayUrls["celld-gw-03"], "http://127.0.0.1:18086");

      const stateAfterGw3 = await (await fetch(`${gwBaseUrl}/v1beta1/management/state`)).json();






      _strict.default.equal(stateAfterGw3.dynamicTopology.countsByRole.gateway, 3);
      _strict.default.ok(stateAfterGw3.dynamicTopology.activeGatewayUrls.includes("http://127.0.0.1:18086"));
      const gw3Node = stateAfterGw3.nodes.find((n) => n.id === "celld-gw-03");
      _strict.default.ok(gw3Node);
      _strict.default.ok(gw3Node.role.includes("Edge Gateway"));
      _strict.default.ok(gw3Node.tunnelGw.includes("INGRESS_GW"));

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
          nowMs: staleTs
        })
      });

      const evictRes = await fetch(`${gwBaseUrl}/v1beta1/management/topology/evict-expired`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`
        },
        body: JSON.stringify({ ttlMs: 15000 })
      });
      _strict.default.equal(evictRes.status, 200);
      const evictBody = await evictRes.json();







      _strict.default.equal(evictBody.evictedCount, 1);
      _strict.default.equal(evictBody.evictedNodes[0].nodeId, "celld-brain-04");
      _strict.default.equal(evictBody.dynamicTopology.activeBrainMembers.includes("brain-node-04"), false);
      _strict.default.equal(evictBody.dynamicTopology.slotDistribution["brain-node-04"], undefined);
      _strict.default.equal(gwServer.peerBrainUrls["brain-node-04"], undefined);

      // 6. Verify CLI commands anstsctl mgmt-topology and anstsctl cluster-register
      const cliTopo = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-topology"], gwBaseUrl);
      _strict.default.equal(cliTopo.exitCode, 0);
      const parsedTopo = JSON.parse(cliTopo.stdout);
      _strict.default.ok(parsedTopo.totalRegisteredNodes >= 9);

      const cliReg = await (0, _anstsctl.runAnstsCtlRemote)(
      ["cluster-register", "celld-brain-05", "brain", "http://127.0.0.1:18089", "eu-central-1"],
      gwBaseUrl);

      _strict.default.equal(cliReg.exitCode, 0);
      const parsedReg = JSON.parse(cliReg.stdout);




      _strict.default.equal(parsedReg.registered, true);
      _strict.default.ok(parsedReg.dynamicTopology.activeBrainMembers.includes("brain-node-05"));
    } finally {
      if (body4Server) await body4Server.stop();
      if (brain4Server) await brain4Server.stop();
      await gwServer.stop();
      fs.rmSync(tmpGw, { recursive: true, force: true });
      fs.rmSync(tmpBrain4, { recursive: true, force: true });
      fs.rmSync(tmpBody4, { recursive: true, force: true });
    }
  });

  (0, _nodeTest.it)("verifies zero-hardcode dynamic slot rendering and multi-role topology controls in prototype.js & docs/index.html", () => {
    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = fs.readFileSync(path.resolve(process.cwd(), rel), "utf-8");
      _strict.default.ok(content.includes("quick-self-register-node"), `${rel} must include quick-self-register-node`);
      _strict.default.ok(content.includes("evict-expired-cluster-nodes"), `${rel} must include evict-expired-cluster-nodes`);
      _strict.default.ok(content.includes("send-node-heartbeat"), `${rel} must include send-node-heartbeat`);
      _strict.default.ok(content.includes('<option value="gateway">'), `${rel} scale dialog must include gateway role option`);
      _strict.default.ok(content.includes("state.slotOwners"), `${rel} must render 256-slot ring from dynamic state.slotOwners`);
    }
  });
});