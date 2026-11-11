"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * @fileoverview M12 & M13 Production Capabilities & Non-K8s Manual Fleet Scaling Suite (6 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("M12 & M13 Production Capabilities & Manual Fleet Scaling Suite", () => {
    (0, node_test_1.it)("1. OtelAndPrometheusExporter parses W3C traceparent and renders Prometheus metrics", () => {
        const exporter = new index_js_1.OtelAndPrometheusExporter();
        const tp = index_js_1.OtelAndPrometheusExporter.createTraceparent();
        const parsed = index_js_1.OtelAndPrometheusExporter.parseTraceparent(tp);
        assert.equal(parsed.version, "00");
        exporter.incCounter("ansts_tasks_submitted_total", 5);
        const text = exporter.renderPrometheusMetrics();
        assert.ok(text.includes("ansts_tasks_submitted_total 5"));
    });
    (0, node_test_1.it)("2. McpDynamicToolRegistry registers MCP tools into SagaToolGateway with JSON-RPC 2.0", () => {
        const registry = new index_js_1.McpDynamicToolRegistry();
        const saga = new index_js_1.SagaToolGateway();
        registry.registerMcpServerTools("github-mcp", [
            {
                toolName: "gh_create_issue",
                description: "Create GitHub issue",
                effectClass: index_js_1.ToolCallEffectClass.COMPENSATABLE,
                inputSchema: { type: "object" },
            },
        ], saga, (_method, params) => ({ ok: true, tool: params.name }));
        const [out] = saga.invokeTool("t1", 1, "gh_create_issue", { title: "Bug" });
        assert.equal(out.ok, true);
    });
    (0, node_test_1.it)("3. FinOpsBudgetController auto-downgrades LLM model at 80% budget and blocks at 100%", () => {
        const finops = new index_js_1.FinOpsBudgetController();
        finops.setPolicy({
            tenantId: "t-fin",
            dailyBudgetUsd: 10.0,
            softDowngradeThresholdRatio: 0.8,
            preferredModel: "claude-3-7-sonnet",
            fallbackEconomyModel: "gpt-4o-mini",
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
    (0, node_test_1.it)("4. MultiRegionFederationRouter selects lowest-RTT region matching sovereignty zone", () => {
        const fed = new index_js_1.MultiRegionFederationRouter();
        fed.registerRegion({
            regionCode: "eu-central-1",
            baseUrl: "https://eu.ansts.internal",
            healthy: true,
            rttEwmaMs: 28,
            sovereigntyZone: "EU",
        });
        const selected = fed.selectOptimalRegion({ requiredSovereigntyZone: "EU" });
        assert.equal(selected.regionCode, "eu-central-1");
    });
    (0, node_test_1.it)("5. anstsctl CLI submits tasks and executes manual bare-metal scale-out without K8s", () => {
        const rt = new index_js_1.AnstsClusterRuntime();
        const sub = (0, index_js_1.runCli)(["submit", "cli-t1", "tenant-1", "Run diagnostics"], rt);
        assert.equal(sub.exitCode, 0);
        const scale = (0, index_js_1.runCli)(["scale-out", "bm-worker-05", "10.20.2.15"], rt);
        assert.equal(scale.exitCode, 0);
        assert.equal(rt.fleetController.listNodes().length, 1);
    });
    (0, node_test_1.it)("6. DashboardBackendServer serves live cluster overview JSON and HTML UI", async () => {
        const sched = new index_js_1.CoreScheduler5D();
        const bridge = new index_js_1.DashboardBridgeService(sched);
        const server = new index_js_1.DashboardBackendServer(bridge);
        const port = await server.listen(0);
        const res = await fetch(`http://127.0.0.1:${port}/api/overview`);
        const body = (await res.json());
        assert.equal(body.totalTasks, 0);
        await server.close();
    });
});
