"use strict";var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.describe)("M84 TanStack Console Global Command Palette (⌘K), Bayesian EWMA Resource Profiler, Predictive Prewarmer & SSE Stream Bus Suite", () => {
  (0, _nodeTest.it)("[M84-1] Exposes Bayesian EWMA Profiler, Predictive Prewarmer & SSE Stream Bus via GET /v1beta1/console/profiler and executes interactive tuning", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m84-prof-"));
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

      // 1. Inspect initial profilerVisualization via GET /v1beta1/console/profiler
      const initResp = await fetch(`${baseUrl}/v1beta1/console/profiler`);
      _strict.default.equal(initResp.status, 200);
      const initProf = await initResp.json();

















      _strict.default.equal(initProf.tenantProfiles.length, 3);
      const nsProf = initProf.tenantProfiles.find((p) => p.tenantId === "Northstar");
      _strict.default.ok(nsProf && nsProf.samples >= 4);
      _strict.default.equal(initProf.predictivePrewarmer.ewmaArrivalRatePerSec, 5);
      _strict.default.ok(initProf.sseStreamBus.recentEventsCount >= 1);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/profiler/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`
        },
        body: JSON.stringify({
          action: "record_tenant_sample",
          tenantId: "Northstar"
        })
      });
      _strict.default.equal(viewerResp.status, 403);

      // 3. Record new execution sample and apply right-sized profile to Northstar tasks
      const sampleResp = await fetch(`${baseUrl}/v1beta1/management/profiler/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "record_tenant_sample",
          tenantId: "Northstar",
          observedPeakCpu: 3.8,
          observedPeakMemoryMb: 3400
        })
      });
      _strict.default.equal(sampleResp.status, 200);
      const sampleBody = await sampleResp.json();


      _strict.default.ok(sampleBody.recommendedProfile.act_cpu_cores > 3.5);
      _strict.default.ok(sampleBody.recommendedProfile.act_memory_mb > 3200);

      const applyResp = await fetch(`${baseUrl}/v1beta1/management/profiler/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "apply_right_sized_profile",
          tenantId: "Northstar"
        })
      });
      _strict.default.equal(applyResp.status, 200);
      const applyBody = await applyResp.json();
      _strict.default.ok(applyBody.updatedTaskIds.includes("task-ns-8841"));

      // 4. Trigger Predictive EWMA Burst Prewarming and SSE Diagnostic Broadcast
      const burstResp = await fetch(`${baseUrl}/v1beta1/management/profiler/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "predict_and_prewarm_burst",
          observedArrivalsPerSec: 15.0,
          avgSandboxHoldSec: 4.0
        })
      });
      _strict.default.equal(burstResp.status, 200);
      const burstBody = await burstResp.json();



      _strict.default.equal(burstBody.ewmaArrivalRatePerSec, 9);
      _strict.default.equal(burstBody.recommendedWarmPoolSize, 45);

      const sseResp = await fetch(`${baseUrl}/v1beta1/management/profiler/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "publish_sse_diagnostic_event",
          eventType: "console.sre.m84",
          message: "M84 SSE broadcast verified"
        })
      });
      _strict.default.equal(sseResp.status, 200);
      const sseBody = await sseResp.json();



      _strict.default.equal(sseBody.envelope.event, "console.sre.m84");
      _strict.default.ok(sseBody.profilerVisualization.sseStreamBus.recentEvents.some((e) => e.event === "console.sre.m84"));

      // 5. Verify CLI anstsctl mgmt-profiler & Global Command Palette UI bindings
      const cliProf = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-profiler", "inspect"], baseUrl);
      _strict.default.equal(cliProf.exitCode, 0);
      const cliProfParsed = JSON.parse(cliProf.stdout);


      _strict.default.equal(cliProfParsed.predictivePrewarmer.ewmaArrivalRatePerSec, 9);

      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        _strict.default.ok(content.includes("open-command-palette"));
        _strict.default.ok(content.includes("prof-record-sample"));
        _strict.default.ok(content.includes("prof-apply-rightsize"));
        _strict.default.ok(content.includes("prof-predict-burst"));
        _strict.default.ok(content.includes("prof-publish-sse"));
      }
    } finally {
      await server.stop();
    }
  });
});