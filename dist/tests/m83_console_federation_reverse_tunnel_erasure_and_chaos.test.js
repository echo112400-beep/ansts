"use strict";var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.describe)("M83 TanStack Console Multi-Region Federation, Dual-Homed Reverse Tunnel, RS(4,2) Erasure Coding & Chaos Suite", () => {
  (0, _nodeTest.it)("[M83-1] Exposes Federation, Dual-Homed Reverse Tunnels, RS(4,2) Erasure Coding & Chaos via GET /v1beta1/console/federation and executes failover/rebuild", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m83-fed-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
      const chenToken = guard.issuePrincipalToken("chen");
      const xuToken = guard.issuePrincipalToken("xu");

      // 1. Inspect initial federationVisualization via GET /v1beta1/console/federation
      const initResp = await fetch(`${baseUrl}/v1beta1/console/federation`);
      _strict.default.equal(initResp.status, 200);
      const initFed = await initResp.json();












      _strict.default.equal(initFed.federatedRegions.length, 3);
      _strict.default.equal(initFed.reverseTunnels.totalBindingsCount, 6);
      _strict.default.equal(initFed.reverseTunnels.healthyBindingsCount, 6);
      _strict.default.equal(initFed.reverseTunnels.hosts[0].activeGatewayId, "celld-gw-01");
      _strict.default.equal(initFed.erasureCoding.manifestsCount, 1);
      _strict.default.equal(initFed.erasureCoding.manifests[0].shards.length, 6);
      _strict.default.equal(initFed.chaosEngineering.activePartitionCount, 0);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`
        },
        body: JSON.stringify({
          action: "inject_chaos_partition",
          targetHost: "body-host-02"
        })
      });
      _strict.default.equal(viewerResp.status, 403);

      // 3. Toggle PRIMARY tunnel gateway down on body-host-01 -> auto-failover to SECONDARY (celld-gw-02)
      const toggleResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "toggle_tunnel_gateway_health",
          hostId: "body-host-01",
          gatewayId: "celld-gw-01",
          healthy: false
        })
      });
      _strict.default.equal(toggleResp.status, 200);
      const toggleBody = await toggleResp.json();


      _strict.default.equal(toggleBody.activeTunnel.gatewayId, "celld-gw-02");
      _strict.default.equal(toggleBody.activeTunnel.role, "SECONDARY");

      // Dispatch reverse tunnel probe over surviving SECONDARY gateway
      const probeResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "dispatch_tunnel_probe",
          hostId: "body-host-01"
        })
      });
      _strict.default.equal(probeResp.status, 200);
      const probeBody = await probeResp.json();
      _strict.default.equal(probeBody.ack.viaGateway, "celld-gw-02");
      _strict.default.equal(probeBody.ack.status, "TUNNEL_RTT_OK");

      // 4. Encode a new RS(4,2) WAL manifest and simulate 2-region outage lossless rebuild
      const encResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "encode_erasure_wal",
          walId: "wal-m83-test",
          payloadText: '{"taskId":"task-m83","turn":9,"rpo":0}'
        })
      });
      _strict.default.equal(encResp.status, 201);

      const rebuildResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "simulate_dual_region_outage_rebuild",
          walId: "wal-m83-test",
          droppedShardIndices: [1, 3]
        })
      });
      _strict.default.equal(rebuildResp.status, 200);
      const rebuildBody = await rebuildResp.json();







      _strict.default.equal(rebuildBody.rebuildReport.rebuiltSuccessfully, true);
      _strict.default.equal(rebuildBody.rebuildReport.crc32Match, true);
      _strict.default.equal(rebuildBody.rebuildReport.missingShardsCount, 2);
      _strict.default.equal(rebuildBody.rebuildReport.reconstructedDataText, '{"taskId":"task-m83","turn":9,"rpo":0}');

      // 5. Inject and heal chaos partition
      const injResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "inject_chaos_partition",
          targetHost: "body-host-02",
          ttlMs: 60000
        })
      });
      _strict.default.equal(injResp.status, 200);
      const injBody = await injResp.json();
      _strict.default.ok(injBody.activeFaults.partitionedHosts.includes("body-host-02"));

      const healResp = await fetch(`${baseUrl}/v1beta1/management/federation/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({ action: "heal_all_chaos_faults" })
      });
      _strict.default.equal(healResp.status, 200);
      const healBody = await healResp.json();
      _strict.default.equal(healBody.healedCount, 1);

      // 6. Verify CLI anstsctl mgmt-federation-console & UI bindings
      const cliFed = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-federation-console", "inspect"], baseUrl);
      _strict.default.equal(cliFed.exitCode, 0);
      const cliFedParsed = JSON.parse(cliFed.stdout);
      _strict.default.equal(cliFedParsed.erasureCoding.manifestsCount, 2);

      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        _strict.default.ok(content.includes("renderFederationSection"));
        _strict.default.ok(content.includes("fed-toggle-tunnel-gw"));
        _strict.default.ok(content.includes("fed-dispatch-tunnel-cmd"));
        _strict.default.ok(content.includes("fed-encode-erasure-wal"));
        _strict.default.ok(content.includes("fed-rebuild-erasure-wal"));
        _strict.default.ok(content.includes("fed-inject-partition"));
        _strict.default.ok(content.includes("fed-heal-partitions"));
      }
    } finally {
      await server.stop();
    }
  });
});