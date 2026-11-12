"use strict";


var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M93: celld Dev Service Live Boot, Multi-Endpoint Proxy/CORS Wiring & Console Live Backend Workbench Suite
 */(0, _nodeTest.describe)("M93 celld Dev Service Live Boot & Console Live Backend Wiring Suite", () => {
  (0, _nodeTest.it)("exposes live celld dev status, executes 6-point celld subsystem probe, enforces CORS + RBAC, and delegates Console BFF on dashboardPort", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m93-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
      nodeId: "celld-gw-01",
      nodeRole: "gateway"
    });
    const { apiPort, dashboardPort } = await server.start(0, 0);
    const apiBaseUrl = `http://127.0.0.1:${apiPort}`;
    const dashBaseUrl = `http://127.0.0.1:${dashboardPort}`;
    const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
    const adminToken = guard.issuePrincipalToken("chen");
    const viewerToken = guard.issuePrincipalToken("xu");

    try {
      // 1. Verify OPTIONS preflight & CORS headers on both apiPort and dashboardPort
      const preflightRes = await fetch(`${apiBaseUrl}/v1beta1/management/state`, {
        method: "OPTIONS",
        headers: {
          Origin: "http://localhost:8088",
          "Access-Control-Request-Method": "POST",
          "Access-Control-Request-Headers": "Content-Type, Authorization"
        }
      });
      _strict.default.equal(preflightRes.status, 204);
      _strict.default.equal(preflightRes.headers.get("access-control-allow-origin"), "*");
      _strict.default.ok(preflightRes.headers.get("access-control-allow-headers")?.includes("Authorization"));

      // 2. Verify GET /v1beta1/console/celld-dev-status returns live celld runtime & DO cell placements
      const statusRes = await fetch(`${apiBaseUrl}/v1beta1/console/celld-dev-status`);
      _strict.default.equal(statusRes.status, 200);
      _strict.default.equal(statusRes.headers.get("access-control-allow-origin"), "*");
      const statusBody = await statusRes.json();









      _strict.default.equal(statusBody.serviceMode, "DEV_LIVE_CELLD");
      _strict.default.equal(statusBody.status, "RUNNING");
      _strict.default.equal(statusBody.nodeId, "celld-gw-01");
      _strict.default.equal(statusBody.corsEnabled, true);
      _strict.default.equal(statusBody.bearerAuthForwarded, true);
      _strict.default.equal(statusBody.sseLiveStreamingEnabled, true);
      _strict.default.ok(statusBody.registeredCells.length >= 5);
      _strict.default.ok(statusBody.candidateOrigins.some((c) => c.origin === "http://127.0.0.1:8088"));

      // 3. Verify VIEWER role is blocked with 403 on POST /v1beta1/management/celld/probe
      const viewerProbe = await fetch(`${apiBaseUrl}/v1beta1/management/celld/probe`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${viewerToken}`
        },
        body: JSON.stringify({ targetOrigin: apiBaseUrl })
      });
      _strict.default.equal(viewerProbe.status, 403);

      // 4. Verify SRE_ADMIN executes 6/6 celld dev service probes and records audit log
      const adminProbe = await fetch(`${apiBaseUrl}/v1beta1/management/celld/probe`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`
        },
        body: JSON.stringify({ targetOrigin: apiBaseUrl, reconcileCells: true })
      });
      _strict.default.equal(adminProbe.status, 200);
      const probeBody = await adminProbe.json();





      _strict.default.equal(probeBody.allHealthy, true);
      _strict.default.equal(probeBody.passedCount, 6);
      _strict.default.equal(probeBody.totalCount, 6);
      _strict.default.ok(probeBody.probes.every((p) => p.passed));

      // 5. Verify dashboardPort delegates Console HTML, BFF state, and native celld endpoints seamlessly
      const dashHtmlRes = await fetch(`${dashBaseUrl}/index.html`);
      _strict.default.equal(dashHtmlRes.status, 200);
      const dashHtml = await dashHtmlRes.text();
      _strict.default.ok(dashHtml.includes("probe-celld-dev-service"));

      const dashCelldHealthRes = await fetch(`${dashBaseUrl}/.well-known/celld/health`);
      _strict.default.equal(dashCelldHealthRes.status, 200);
      const dashCelldHealth = await dashCelldHealthRes.json();
      _strict.default.equal(dashCelldHealth.healthy, true);
      _strict.default.equal(dashCelldHealth.nodeId, "celld-gw-01");

      const dashMgmtStateRes = await fetch(`${dashBaseUrl}/v1beta1/management/state`);
      _strict.default.equal(dashMgmtStateRes.status, 200);
      const dashMgmtState = await dashMgmtStateRes.json();


      _strict.default.equal(dashMgmtState.celldDevService.serviceMode, "DEV_LIVE_CELLD");
      _strict.default.equal(dashMgmtState.celldDevService.lastProbeReport.allHealthy, true);

      // 6. Verify CLI commands anstsctl mgmt-celld-status and mgmt-celld-probe
      const cliStatus = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-celld-status"], apiBaseUrl);
      _strict.default.equal(cliStatus.exitCode, 0);
      const parsedCliStatus = JSON.parse(cliStatus.stdout);
      _strict.default.equal(parsedCliStatus.serviceMode, "DEV_LIVE_CELLD");

      const cliProbe = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-celld-probe", apiBaseUrl], apiBaseUrl);
      _strict.default.equal(cliProbe.exitCode, 0);
      const parsedCliProbe = JSON.parse(cliProbe.stdout);
      _strict.default.equal(parsedCliProbe.allHealthy, true);
      _strict.default.equal(parsedCliProbe.passedCount, 6);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  (0, _nodeTest.it)("verifies dev_cluster_local_supervisor.mjs, nginx-tanstack-console.conf, and Console UI live backend discovery wiring", () => {
    const supervisorCode = fs.readFileSync(
    path.resolve(process.cwd(), "scripts", "dev_cluster_local_supervisor.mjs"),
    "utf-8");

    _strict.default.ok(supervisorCode.includes('forwardHeaders["authorization"] = req.headers["authorization"]'));
    _strict.default.ok(supervisorCode.includes('cleanPath === "/.well-known/celld/health"'));
    _strict.default.ok(supervisorCode.includes('cleanPath === "/state"'));
    _strict.default.ok(supervisorCode.includes("upstreamRes.pipe(res)"));

    const nginxConf = fs.readFileSync(
    path.resolve(process.cwd(), "deploy", "dev-cluster", "nginx-tanstack-console.conf"),
    "utf-8");

    _strict.default.ok(nginxConf.includes(".well-known/celld/health"));
    _strict.default.ok(nginxConf.includes("Access-Control-Allow-Origin"));

    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = fs.readFileSync(path.resolve(process.cwd(), rel), "utf-8");
      _strict.default.ok(content.includes("CANDIDATE_DEV_ORIGINS"), `${rel} must define CANDIDATE_DEV_ORIGINS`);
      _strict.default.ok(content.includes("probe-celld-dev-service"), `${rel} must include probe-celld-dev-service action`);
      _strict.default.ok(content.includes("open-celld-endpoint-dialog"), `${rel} must include open-celld-endpoint-dialog action`);
      _strict.default.ok(content.includes("inspect-celld-native-state"), `${rel} must include inspect-celld-native-state action`);
    }
  });
});