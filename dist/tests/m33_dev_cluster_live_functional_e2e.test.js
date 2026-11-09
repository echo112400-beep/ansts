"use strict";





var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M33 Multi-Node Dev Cluster Live Functional E2E Suite (`gateway` -> `brain` -> `body`).
 * Verifies real cross-node HTTP RPC forwarding across `ansts-gateway-01/02`, `celld-brain-01..03`,
 * and `body-daemon-01..02`, Dual-Homed Reverse Tunnel registration (`EdgeTunnelHaRouter`),
 * 256-slot deterministic shard routing (`HaSchedulerShardCoordinator`), and LayerFS CoW execution.
 */(0, _nodeTest.describe)("M33 Multi-Node Dev Cluster Live Functional E2E Suite (Gateway -> Brain -> Body)", () => {(0, _nodeTest.it)("1. Spins up 3 Brains, 2 Body Daemons, and 2 Gateways and executes end-to-end distributed dispatch + LayerFS CoW", async () => {// 1. Start 3 Brain nodes on ephemeral ports
    const brain1 = new _index.MultiNodeDevServer({ host: "127.0.0.1", nodeId: "brain-node-01", nodeRole: "brain" });const brain2 = new _index.MultiNodeDevServer({ host: "127.0.0.1", nodeId: "brain-node-02", nodeRole: "brain" });
    const brain3 = new _index.MultiNodeDevServer({ host: "127.0.0.1", nodeId: "brain-node-03", nodeRole: "brain" });
    const b1 = await brain1.start(0, 0);
    const b2 = await brain2.start(0, 0);
    const b3 = await brain3.start(0, 0);

    // 2. Start 2 Body Daemons on ephemeral ports
    const body1 = new _index.MultiNodeDevServer({ host: "127.0.0.1", nodeId: "body-host-01", nodeRole: "body" });
    const body2 = new _index.MultiNodeDevServer({ host: "127.0.0.1", nodeId: "body-host-02", nodeRole: "body" });
    const bd1 = await body1.start(0, 0);
    const bd2 = await body2.start(0, 0);

    const peerBrainUrls = {
      "brain-node-01": `http://127.0.0.1:${b1.apiPort}`,
      "brain-node-02": `http://127.0.0.1:${b2.apiPort}`,
      "brain-node-03": `http://127.0.0.1:${b3.apiPort}`
    };
    const peerBodyUrls = {
      "body-host-01": `http://127.0.0.1:${bd1.apiPort}`,
      "body-host-02": `http://127.0.0.1:${bd2.apiPort}`
    };

    // 3. Start 2 Gateways wired to the 3 Brains and 2 Bodies
    const gw1 = new _index.MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      peerBrainUrls,
      peerBodyUrls
    });
    const gw2 = new _index.MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-02",
      nodeRole: "gateway",
      peerBrainUrls,
      peerBodyUrls
    });
    const g1 = await gw1.start(0, 0);
    const g2 = await gw2.start(0, 0);

    try {
      // 4. Verify `/state` on Brains and `/health` on Bodies & Gateways
      const stateRes = await fetch(`http://127.0.0.1:${b1.apiPort}/state`);
      assert.equal(stateRes.status, 200);
      const stateJson = await stateRes.json();
      assert.equal(stateJson.nodeId, "brain-node-01");
      assert.equal(stateJson.nodeRole, "brain");

      // 5. Register Dual-Homed Reverse Tunnels on GW-01 (PRIMARY) & GW-02 (SECONDARY)
      const tun1Res = await fetch(`http://127.0.0.1:${g1.apiPort}/internal/v1beta1/tunnel/register`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          hostId: "body-host-01",
          gatewayId: "celld-gw-01",
          role: "PRIMARY",
          connGeneration: 1,
          epoch: 1
        })
      });
      assert.equal(tun1Res.status, 200);
      const tun1Json = await tun1Res.json();
      assert.equal(tun1Json.registered, true);
      assert.equal(tun1Json.activeGatewayId, "celld-gw-01");

      const tun2Res = await fetch(`http://127.0.0.1:${g2.apiPort}/internal/v1beta1/tunnel/register`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          hostId: "body-host-01",
          gatewayId: "celld-gw-02",
          role: "SECONDARY",
          connGeneration: 1,
          epoch: 1
        })
      });
      assert.equal(tun2Res.status, 200);

      // 6. Dispatch distributed task through GW-01 -> Brain Shard Owner -> Body-01 LayerFS CoW
      const dispRes = await fetch(`http://127.0.0.1:${g1.apiPort}/v1beta1/tasks/dispatch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          tenantId: "tenant-alpha",
          taskId: "task-distributed-001",
          repoId: "ansts-monorepo",
          promptText: "verify distributed celld dispatch",
          targetBodyHostId: "body-host-01"
        })
      });
      assert.equal(dispRes.status, 202);
      const dispJson = await dispRes.json();






      assert.equal(dispJson.dispatched, true);
      assert.equal(dispJson.gatewayNodeId, "celld-gw-01");
      assert.ok(["brain-node-01", "brain-node-02", "brain-node-03"].includes(dispJson.routedBrainNodeId));
      assert.ok(dispJson.fencingToken >= 1);
      assert.ok(dispJson.bodyExecution);
      assert.equal(dispJson.bodyExecution.executedByBodyId, "body-host-01");
      assert.equal(dispJson.bodyExecution.status, "COMPLETED");
      assert.ok(dispJson.bodyExecution.commitHash.length > 0);

      // 7. Verify Console BFF endpoints (`/v1beta1/console/overview` & `/v1beta1/console/shards`)
      const ovRes = await fetch(`http://127.0.0.1:${g1.apiPort}/v1beta1/console/overview?region=us-west-2`);
      assert.equal(ovRes.status, 200);
      const ovJson = await ovRes.json();
      assert.equal(ovJson.status, "HEALTHY");
      assert.equal(ovJson.configuredBrains.length, 3);

      const shRes = await fetch(`http://127.0.0.1:${g1.apiPort}/v1beta1/console/shards`);
      assert.equal(shRes.status, 200);
      const shJson = await shRes.json();
      assert.equal(shJson.virtualSlots, 256);
      assert.equal(shJson.brains.length, 3);
    } finally {
      await gw1.stop();
      await gw2.stop();
      await body1.stop();
      await body2.stop();
      await brain1.stop();
      await brain2.stop();
      await brain3.stop();
    }
  });
});