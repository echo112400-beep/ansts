"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M12 & M13 Production Capabilities & Non-K8s Manual Fleet Scaling Suite (6 tests).
 */











(0, _nodeTest.describe)("M12 & M13 Production Capabilities & Manual Fleet Scaling Suite", () => {
  (0, _nodeTest.it)("1. OtelAndPrometheusExporter parses W3C traceparent and renders Prometheus metrics", () => {
    const exporter = new _index.OtelAndPrometheusExporter();
    const tp = _index.OtelAndPrometheusExporter.createTraceparent();
    const parsed = _index.OtelAndPrometheusExporter.parseTraceparent(tp);
    assert.equal(parsed.version, "00");
    exporter.incCounter("ansts_tasks_submitted_total", 5);
    const text = exporter.renderPrometheusMetrics();
    assert.ok(text.includes("ansts_tasks_submitted_total 5"));
  });

  (0, _nodeTest.it)("2. McpDynamicToolRegistry registers MCP tools into SagaToolGateway with JSON-RPC 2.0", () => {
    const registry = new _index.McpDynamicToolRegistry();
    const saga = new _index.SagaToolGateway();
    registry.registerMcpServerTools(
    "github-mcp",
    [
    {
      toolName: "gh_create_issue",
      description: "Create GitHub issue",
      effectClass: _index.ToolCallEffectClass.COMPENSATABLE,
      inputSchema: { type: "object" }
    }],

    saga,
    (_method, params) => ({ ok: true, tool: params.name }));

    const [out] = saga.invokeTool("t1", 1, "gh_create_issue", { title: "Bug" });
    assert.equal(out.ok, true);
  });

  (0, _nodeTest.it)("3. FinOpsBudgetController auto-downgrades LLM model at 80% budget and blocks at 100%", () => {
    const finops = new _index.FinOpsBudgetController();
    finops.setPolicy({
      tenantId: "t-fin",
      dailyBudgetUsd: 10.0,
      softDowngradeThresholdRatio: 0.8,
      preferredModel: "claude-3-7-sonnet",
      fallbackEconomyModel: "gpt-4o-mini"
    });
    finops.recordSpend("t-fin", 8.5);
    const soft = finops.evaluateBudgetAndModel("t-fin");
    assert.equal(soft.allowed, true);
    assert.equal(soft.downgraded, true);
    assert.equal(soft.selectedModel, "gpt-4o-mini");

    finops.recordSpend("t-fin", 2.0);
    const hard = finops.evaluateBudgetAndModel("t-fin");
    assert.equal(hard.allowed, false);
  });

  (0, _nodeTest.it)("4. MultiRegionFederationRouter selects lowest-RTT region matching sovereignty zone", () => {
    const fed = new _index.MultiRegionFederationRouter();
    fed.registerRegion({
      regionCode: "eu-central-1",
      baseUrl: "https://eu.ansts.internal",
      healthy: true,
      rttEwmaMs: 28,
      sovereigntyZone: "EU"
    });
    const selected = fed.selectOptimalRegion({ requiredSovereigntyZone: "EU" });
    assert.equal(selected.regionCode, "eu-central-1");
  });

  (0, _nodeTest.it)("5. anstsctl CLI submits tasks and executes manual bare-metal scale-out without K8s", () => {
    const rt = new _index.AnstsClusterRuntime();
    const sub = (0, _index.runCli)(["submit", "cli-t1", "tenant-1", "Run diagnostics"], rt);
    assert.equal(sub.exitCode, 0);
    const scale = (0, _index.runCli)(["scale-out", "bm-worker-05", "10.20.2.15"], rt);
    assert.equal(scale.exitCode, 0);
    assert.equal(rt.fleetController.listNodes().length, 1);
  });

  (0, _nodeTest.it)("6. DashboardBackendServer serves live cluster overview JSON and HTML UI", async () => {
    const sched = new _index.CoreScheduler5D();
    const bridge = new _index.DashboardBridgeService(sched);
    const server = new _index.DashboardBackendServer(bridge);
    const port = await server.listen(0);
    const res = await fetch(`http://127.0.0.1:${port}/api/overview`);
    const body = await res.json();
    assert.equal(body.totalTasks, 0);
    await server.close();
  });
});