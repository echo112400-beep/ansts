"use strict";var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };}

(0, _nodeTest.describe)("M81 TanStack Console Full-Link SRE Observability, W3C Trace Waterfall, SLO Burn-Rate Alerting & Self-Healing Runbook Suite", () => {
  (0, _nodeTest.it)("[M81-1] Exposes W3C Trace Waterfalls, SLO Burn-Rate, Quantiles & Runbooks via GET /v1beta1/console/observability and enforces RBAC", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m81-obs-"));
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

      // 1. Inspect initial observabilityVisualization via GET /v1beta1/console/observability
      const obsResp = await fetch(`${baseUrl}/v1beta1/console/observability`);
      _strict.default.equal(obsResp.status, 200);
      const obsData = await obsResp.json();















      _strict.default.equal(obsData.sloEvaluation.alertSeverity, "HEALTHY");
      _strict.default.ok(obsData.latencyQuantiles.count >= 9);
      _strict.default.ok(obsData.traceWaterfalls.length >= 1);
      _strict.default.ok(obsData.traceWaterfalls[0].traceparent.startsWith("00-"));
      _strict.default.equal(obsData.traceWaterfalls[0].spans.length, 4);
      _strict.default.equal(obsData.sreRunbooks.length, 4);

      // 2. VIEWER role (xu) is blocked with 403 on record_custom_trace and execute_auto_remediation_runbook
      const viewerTraceResp = await fetch(`${baseUrl}/v1beta1/management/ops/observability`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`
        },
        body: JSON.stringify({
          action: "record_custom_trace",
          operationName: "Unauthorized Trace"
        })
      });
      _strict.default.equal(viewerTraceResp.status, 403);

      // 3. Record custom W3C Trace Waterfall as SRE_ADMIN (chen)
      const traceResp = await fetch(`${baseUrl}/v1beta1/management/ops/observability`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "record_custom_trace",
          operationName: "POST /v1beta1/tasks/dispatch (M81 Trace)"
        })
      });
      _strict.default.equal(traceResp.status, 200);
      const traceBody = await traceResp.json();



      _strict.default.equal(traceBody.recordedTrace.spans.length, 5);
      _strict.default.equal(
      traceBody.observabilityVisualization.traceWaterfalls[0].operationName,
      "POST /v1beta1/tasks/dispatch (M81 Trace)");


      // 4. Simulate SLO Burn-Rate spike (PAGE_P0)
      const spikeResp = await fetch(`${baseUrl}/v1beta1/management/ops/observability`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "probe_slo_traces_and_metrics",
          simulatedErrorRequests: 180,
          simulatedP99LatencyMs: 420
        })
      });
      _strict.default.equal(spikeResp.status, 200);
      const spikeBody = await spikeResp.json();


      _strict.default.equal(spikeBody.sloEvaluation.alertSeverity, "PAGE_P0");
      _strict.default.ok(spikeBody.sloEvaluation.burnRate1h >= 14.4);

      // 5. Seed DLQ samples and execute Automated Self-Healing Runbook RB-SLO-BURN-AND-DLQ-HEAL
      const seedResp = await fetch(`${baseUrl}/v1beta1/management/ops/dlq-replay`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({ action: "seed_dlq_samples" })
      });
      _strict.default.equal(seedResp.status, 200);

      const rbResp = await fetch(`${baseUrl}/v1beta1/management/ops/observability`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`
        },
        body: JSON.stringify({
          action: "execute_auto_remediation_runbook",
          runbookId: "RB-SLO-BURN-AND-DLQ-HEAL"
        })
      });
      _strict.default.equal(rbResp.status, 200);
      const rbBody = await rbResp.json();







      _strict.default.equal(rbBody.runbookId, "RB-SLO-BURN-AND-DLQ-HEAL");
      _strict.default.ok(rbBody.healedTaskIds.includes("task-dlq-9901"));
      _strict.default.equal(rbBody.observabilityVisualization.sloEvaluation.alertSeverity, "HEALTHY");
      const rbStat = rbBody.observabilityVisualization.sreRunbooks.find(
      (r) => r.runbookId === "RB-SLO-BURN-AND-DLQ-HEAL");

      _strict.default.equal(rbStat?.executedCount, 1);

      // 6. Verify CLI anstsctl mgmt-observability & UI bindings
      const cliObs = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-observability", "inspect"], baseUrl);
      _strict.default.equal(cliObs.exitCode, 0);
      const cliObsParsed = JSON.parse(cliObs.stdout);


      _strict.default.equal(cliObsParsed.sloEvaluation.alertSeverity, "HEALTHY");

      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        _strict.default.ok(content.includes("renderObservabilitySection"));
        _strict.default.ok(content.includes("obs-record-trace"));
        _strict.default.ok(content.includes("obs-simulate-slo-burn"));
        _strict.default.ok(content.includes("obs-exec-runbook"));
        _strict.default.ok(content.includes("obs-seed-dlq"));
        _strict.default.ok(content.includes("obs-replay-dlq-task"));
        _strict.default.ok(content.includes("obs-replay-outbox-msg"));
      }
    } finally {
      await server.stop();
    }
  });
});