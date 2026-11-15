"use strict";





var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M97 Zero-Stub API & UI Real Backend Wiring Audit & Verification Suite
 * Verifies:
 * 1. `ui/src/components/hrui/AnstsHruiConsole.tsx` and `docs/console-management/prototype.js` contain zero hardcoded topology/SLA fallbacks and zero `Math.random()` fake client telemetry, while wiring all 7 governance tabs, Bearer JWT auth, and `?live=1` SSE to real backend endpoints.
 * 2. Backend APIs (`GET /v1beta1/console/runtime`, `GET /v1beta1/console/celld-dev-status`, `GET /v1beta1/console/topology`, `GET /v1beta1/console/shards`, `GET /v1beta1/console/overview`, `GET /v1beta1/management/state`, `GET /state`, `POST /internal/v1beta1/cluster/heartbeat`) dynamically derive all cells, origins, SLA ratios, router endpoints, and node load from live SQLite & runtime instances with zero hardcoded stub objects.
 */(0, _nodeTest.describe)("M97 Zero-Stub API & UI Real Backend Wiring Audit Suite", () => {(0, _nodeTest.it)("verifies zero hardcoded fallbacks and zero Math.random fake telemetry across ui/ Next.js and docs/ console files", () => {const repoRoot = process.cwd();const hruiConsolePath = path.join(repoRoot, "ui/src/components/hrui/AnstsHruiConsole.tsx");
    const protoJsPath = path.join(repoRoot, "docs/console-management/prototype.js");
    const indexHtmlPath = path.join(repoRoot, "docs/index.html");
    const serverTsPath = path.join(repoRoot, "src/dashboard/server.ts");

    const hruiConsole = fs.readFileSync(hruiConsolePath, "utf-8");
    const protoJs = fs.readFileSync(protoJsPath, "utf-8");
    const indexHtml = fs.readFileSync(indexHtmlPath, "utf-8");
    const serverTs = fs.readFileSync(serverTsPath, "utf-8");

    // 1. Next.js AnstsHruiConsole.tsx must have zero hardcoded slot/node fallbacks and wire all real BFF endpoints
    _strict.default.equal(hruiConsole.includes("'brain-node-01': 85"), false, "AnstsHruiConsole.tsx must not hardcode 85/86/85 slot fallback");
    _strict.default.equal(hruiConsole.includes("totalRegisteredNodes ?? 8"), false, "AnstsHruiConsole.tsx must not hardcode 8 nodes fallback");
    _strict.default.equal(hruiConsole.includes("Math.random"), false, "AnstsHruiConsole.tsx must not use Math.random()");

    const requiredEndpointsInNextUi = [
    "/v1beta1/management/auth/token",
    "/v1beta1/management/events/stream?live=1",
    "/v1beta1/management/state",
    "/v1beta1/console/topology",
    "/v1beta1/console/finops",
    "/v1beta1/console/queues",
    "/v1beta1/console/federation",
    "/v1beta1/console/dag",
    "/v1beta1/console/tasks/blueprints",
    "/v1beta1/console/runtime",
    "/v1beta1/console/configs",
    "/v1beta1/console/profiler",
    "/v1beta1/console/swarm",
    "/v1beta1/console/approvals/saga",
    "/v1beta1/console/security",
    "/v1beta1/console/observability",
    "/v1beta1/console/celld-dev-status",
    "/v1beta1/console/layerfs/file",
    "/v1beta1/management/tenants/config",
    "/v1beta1/management/finops/action",
    "/v1beta1/management/queues/action",
    "/v1beta1/management/nodes/batch",
    "/v1beta1/tasks/dispatch",
    "/v1beta1/management/tasks/time-travel",
    "/v1beta1/management/tasks/cancel",
    "/v1beta1/management/router/inference-pipeline",
    "/v1beta1/management/approvals/decide",
    "/v1beta1/management/celld/probe"];

    for (const ep of requiredEndpointsInNextUi) {
      _strict.default.equal(
      hruiConsole.includes(ep),
      true,
      `Next.js AnstsHruiConsole.tsx must wire real backend endpoint ${ep}`);

    }

    // 2. prototype.js and docs/index.html must have zero Math.random() and zero hardcoded fake SLO/SLA fallbacks
    _strict.default.equal(protoJs.includes("Math.random"), false, "prototype.js must contain zero Math.random()");
    _strict.default.equal(indexHtml.includes("Math.random"), false, "docs/index.html must contain zero Math.random()");
    _strict.default.equal(protoJs.includes("0.9996"), false, "prototype.js must not contain hardcoded 0.9996 SLO availability");
    _strict.default.equal(protoJs.includes("0.742"), false, "prototype.js must not contain hardcoded 0.742 utilization fallback");

    // 3. src/dashboard/server.ts must not contain hardcoded defaultCells or static ep-qwen-38 push in buildRuntimeVisualization
    _strict.default.equal(
    serverTs.includes("if (!routerEndpoints.some((e) => e.endpointId === \"ep-qwen-38\"))"),
    false,
    "buildRuntimeVisualization must not push a synthetic ep-qwen-38 object");

    _strict.default.equal(
    serverTs.includes("const defaultCells: Array<{ cellId: string; hostNodeId: string; txId: number }> = ["),
    false,
    "buildCelldDevServiceStatus must derive cells dynamically from v25_cluster_registry rather than a fixed defaultCells list");

  });

  (0, _nodeTest.it)("verifies backend APIs dynamically derive cells, topology servicesCount, SLA metrics, and router endpoints from live state", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m97-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
      const adminToken = guard.issuePrincipalToken("chen");
      const headers = {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`
      };

      // 1. Check initial celld-dev-status & topology before scaling out
      const topoBefore = await (await fetch(`${baseUrl}/v1beta1/console/topology`)).json();



      _strict.default.equal(topoBefore.totalRegisteredNodes, 8);
      _strict.default.equal(topoBefore.servicesCount, 11);

      const celldBefore = await (await fetch(`${baseUrl}/v1beta1/console/celld-dev-status`)).json();


      _strict.default.equal(celldBefore.registeredCells.length, 8);

      // 2. Register new brain, body, and gateway nodes and verify celld-dev-status, topology, shards, overview & /state update dynamically
      await fetch(`${baseUrl}/internal/v1beta1/cluster/register`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          nodeId: "celld-brain-04",
          role: "brain",
          region: "us-west-2",
          baseUrl: "http://127.0.0.1:18094"
        })
      });
      await fetch(`${baseUrl}/internal/v1beta1/cluster/register`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          nodeId: "body-host-04",
          role: "body",
          region: "us-west-2",
          baseUrl: "http://127.0.0.1:19504"
        })
      });
      await fetch(`${baseUrl}/internal/v1beta1/cluster/register`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          nodeId: "celld-gw-03",
          role: "gateway",
          region: "eu-central-1",
          baseUrl: "http://127.0.0.1:18089"
        })
      });

      const topoAfter = await (await fetch(`${baseUrl}/v1beta1/console/topology`)).json();



      _strict.default.equal(topoAfter.totalRegisteredNodes, 11);
      _strict.default.equal(topoAfter.servicesCount, 14);

      const celldAfter = await (await fetch(`${baseUrl}/v1beta1/console/celld-dev-status`)).json();




      _strict.default.equal(celldAfter.registeredCells.length, 11);
      _strict.default.equal(
      celldAfter.registeredCells.some((c) => c.cellId === "AnstsSchedulerCell:shard-04" && c.hostNodeId === "celld-brain-04"),
      true);

      _strict.default.equal(
      celldAfter.registeredCells.some((c) => c.cellId === "LayerFsSandboxCell:body-04" && c.hostNodeId === "body-host-04"),
      true);

      _strict.default.equal(
      celldAfter.registeredCells.some((c) => c.cellId === "TunnelGatewayCell:celld-gw-03" && c.hostNodeId === "celld-gw-03"),
      true);

      _strict.default.equal(celldAfter.topologyEndpoints.gatewayUrls.includes("http://127.0.0.1:18089"), true);
      _strict.default.equal(celldAfter.candidateOrigins.some((o) => o.origin === "http://127.0.0.1:18094"), true);

      // 3. Verify heartbeat without cpuPct/psiPct measures real backend metrics
      const hbResp = await fetch(`${baseUrl}/internal/v1beta1/cluster/heartbeat`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          nodeId: "body-host-04",
          role: "body"
        })
      });
      _strict.default.equal(hbResp.status, 200);
      const hbBody = await hbResp.json();
      _strict.default.equal(typeof hbBody.cpuPct, "number");
      _strict.default.ok(hbBody.cpuPct > 0);
      _strict.default.equal(typeof hbBody.psiPct, "number");
      _strict.default.ok(hbBody.psiPct > 0);

      // 4. Verify ep-qwen-38 in GET /v1beta1/console/runtime is a real CognitiveRouter endpoint
      const rtBefore = await (await fetch(`${baseUrl}/v1beta1/console/runtime`)).json();


      const qwenBefore = rtBefore.cognitiveRouterEndpoints.find((e) => e.endpointId === "ep-qwen-38");
      _strict.default.ok(qwenBefore, "ep-qwen-38 must be registered in real CognitiveRouter");

      const exhaustResp = await fetch(`${baseUrl}/v1beta1/management/router/inference-pipeline`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action: "exhaust_tpm_bucket",
          endpointId: "ep-qwen-38"
        })
      });
      _strict.default.equal(exhaustResp.status, 200);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});