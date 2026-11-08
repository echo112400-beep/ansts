/**
 * @fileoverview M12 & M13 Production Capabilities & Non-K8s Manual Fleet Scaling Suite (6 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  OtelAndPrometheusExporter,
  McpDynamicToolRegistry,
  SagaToolGateway,
  ToolCallEffectClass,
  FinOpsBudgetController,
  MultiRegionFederationRouter,
  runCli,
  AnstsClusterRuntime,
  DashboardBridgeService,
  DashboardBackendServer,
  CoreScheduler5D,
} from "../src/index.js";

describe("M12 & M13 Production Capabilities & Manual Fleet Scaling Suite", () => {
  it("1. OtelAndPrometheusExporter parses W3C traceparent and renders Prometheus metrics", () => {
    const exporter = new OtelAndPrometheusExporter();
    const tp = OtelAndPrometheusExporter.createTraceparent();
    const parsed = OtelAndPrometheusExporter.parseTraceparent(tp);
    assert.equal(parsed.version, "00");
    exporter.incCounter("ansts_tasks_submitted_total", 5);
    const text = exporter.renderPrometheusMetrics();
    assert.ok(text.includes("ansts_tasks_submitted_total 5"));
  });

  it("2. McpDynamicToolRegistry registers MCP tools into SagaToolGateway with JSON-RPC 2.0", () => {
    const registry = new McpDynamicToolRegistry();
    const saga = new SagaToolGateway();
    registry.registerMcpServerTools(
      "github-mcp",
      [
        {
          toolName: "gh_create_issue",
          description: "Create GitHub issue",
          effectClass: ToolCallEffectClass.COMPENSATABLE,
          inputSchema: { type: "object" },
        },
      ],
      saga,
      (_method, params) => ({ ok: true, tool: params.name })
    );
    const [out] = saga.invokeTool("t1", 1, "gh_create_issue", { title: "Bug" });
    assert.equal(out.ok, true);
  });

  it("3. FinOpsBudgetController auto-downgrades LLM model at 80% budget and blocks at 100%", () => {
    const finops = new FinOpsBudgetController();
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

  it("4. MultiRegionFederationRouter selects lowest-RTT region matching sovereignty zone", () => {
    const fed = new MultiRegionFederationRouter();
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

  it("5. anstsctl CLI submits tasks and executes manual bare-metal scale-out without K8s", () => {
    const rt = new AnstsClusterRuntime();
    const sub = runCli(["submit", "cli-t1", "tenant-1", "Run diagnostics"], rt);
    assert.equal(sub.exitCode, 0);
    const scale = runCli(["scale-out", "bm-worker-05", "10.20.2.15"], rt);
    assert.equal(scale.exitCode, 0);
    assert.equal(rt.fleetController.listNodes().length, 1);
  });

  it("6. DashboardBackendServer serves live cluster overview JSON and HTML UI", async () => {
    const sched = new CoreScheduler5D();
    const bridge = new DashboardBridgeService(sched);
    const server = new DashboardBackendServer(bridge);
    const port = await server.listen(0);
    const res = await fetch(`http://127.0.0.1:${port}/api/overview`);
    const body = (await res.json()) as Record<string, unknown>;
    assert.equal(body.totalTasks, 0);
    await server.close();
  });
});
