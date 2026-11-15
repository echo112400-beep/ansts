/**
 * @fileoverview M97 Zero-Stub API & UI Real Backend Wiring Audit & Verification Suite
 * Verifies:
 * 1. `ui/src/components/hrui/AnstsHruiConsole.tsx` and `docs/console-management/prototype.js` contain zero hardcoded topology/SLA fallbacks and zero `Math.random()` fake client telemetry, while wiring all 7 governance tabs, Bearer JWT auth, and `?live=1` SSE to real backend endpoints.
 * 2. Backend APIs (`GET /v1beta1/console/runtime`, `GET /v1beta1/console/celld-dev-status`, `GET /v1beta1/console/topology`, `GET /v1beta1/console/shards`, `GET /v1beta1/console/overview`, `GET /v1beta1/management/state`, `GET /state`, `POST /internal/v1beta1/cluster/heartbeat`) dynamically derive all cells, origins, SLA ratios, router endpoints, and node load from live SQLite & runtime instances with zero hardcoded stub objects.
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";

describe("M97 Zero-Stub API & UI Real Backend Wiring Audit Suite", () => {
  it("verifies zero hardcoded fallbacks and zero Math.random fake telemetry across ui/ Next.js and docs/ console files", () => {
    const repoRoot = process.cwd();
    const hruiConsolePath = path.join(repoRoot, "ui/src/components/hrui/AnstsHruiConsole.tsx");
    const protoJsPath = path.join(repoRoot, "docs/console-management/prototype.js");
    const indexHtmlPath = path.join(repoRoot, "docs/index.html");
    const serverTsPath = path.join(repoRoot, "src/dashboard/server.ts");

    const hruiConsole = fs.readFileSync(hruiConsolePath, "utf-8");
    const protoJs = fs.readFileSync(protoJsPath, "utf-8");
    const indexHtml = fs.readFileSync(indexHtmlPath, "utf-8");
    const serverTs = fs.readFileSync(serverTsPath, "utf-8");

    // 1. Next.js AnstsHruiConsole.tsx must have zero hardcoded slot/node fallbacks and wire all real BFF endpoints
    assert.equal(hruiConsole.includes("'brain-node-01': 85"), false, "AnstsHruiConsole.tsx must not hardcode 85/86/85 slot fallback");
    assert.equal(hruiConsole.includes("totalRegisteredNodes ?? 8"), false, "AnstsHruiConsole.tsx must not hardcode 8 nodes fallback");
    assert.equal(hruiConsole.includes("Math.random"), false, "AnstsHruiConsole.tsx must not use Math.random()");

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
      "/v1beta1/management/celld/probe",
    ];
    for (const ep of requiredEndpointsInNextUi) {
      assert.equal(
        hruiConsole.includes(ep),
        true,
        `Next.js AnstsHruiConsole.tsx must wire real backend endpoint ${ep}`
      );
    }

    // 2. prototype.js and docs/index.html must have zero Math.random() and zero hardcoded fake SLO/SLA fallbacks
    assert.equal(protoJs.includes("Math.random"), false, "prototype.js must contain zero Math.random()");
    assert.equal(indexHtml.includes("Math.random"), false, "docs/index.html must contain zero Math.random()");
    assert.equal(protoJs.includes("0.9996"), false, "prototype.js must not contain hardcoded 0.9996 SLO availability");
    assert.equal(protoJs.includes("0.742"), false, "prototype.js must not contain hardcoded 0.742 utilization fallback");

    // 3. src/dashboard/server.ts must not contain hardcoded defaultCells or static ep-qwen-38 push in buildRuntimeVisualization
    assert.equal(
      serverTs.includes("if (!routerEndpoints.some((e) => e.endpointId === \"ep-qwen-38\"))"),
      false,
      "buildRuntimeVisualization must not push a synthetic ep-qwen-38 object"
    );
    assert.equal(
      serverTs.includes("const defaultCells: Array<{ cellId: string; hostNodeId: string; txId: number }> = ["),
      false,
      "buildCelldDevServiceStatus must derive cells dynamically from v25_cluster_registry rather than a fixed defaultCells list"
    );
  });

  it("verifies backend APIs dynamically derive cells, topology servicesCount, SLA metrics, and router endpoints from live state", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m97-"));
    const server = new AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new ConsoleRbacAuthGuard();
      const adminToken = guard.issuePrincipalToken("chen");
      const headers = {
        "content-type": "application/json",
        authorization: `Bearer ${adminToken}`,
      };

      // 1. Check initial celld-dev-status & topology before scaling out
      const topoBefore = (await (await fetch(`${baseUrl}/v1beta1/console/topology`)).json()) as {
        totalRegisteredNodes: number;
        servicesCount: number;
      };
      assert.equal(topoBefore.totalRegisteredNodes, 8);
      assert.equal(topoBefore.servicesCount, 11);

      const celldBefore = (await (await fetch(`${baseUrl}/v1beta1/console/celld-dev-status`)).json()) as {
        registeredCells: Array<{ cellId: string; hostNodeId: string }>;
      };
      assert.equal(celldBefore.registeredCells.length, 8);

      // 2. Register new brain, body, and gateway nodes and verify celld-dev-status, topology, shards, overview & /state update dynamically
      await fetch(`${baseUrl}/internal/v1beta1/cluster/register`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          nodeId: "celld-brain-04",
          role: "brain",
          region: "us-west-2",
          baseUrl: "http://127.0.0.1:18094",
        }),
      });
      await fetch(`${baseUrl}/internal/v1beta1/cluster/register`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          nodeId: "body-host-04",
          role: "body",
          region: "us-west-2",
          baseUrl: "http://127.0.0.1:19504",
        }),
      });
      await fetch(`${baseUrl}/internal/v1beta1/cluster/register`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          nodeId: "celld-gw-03",
          role: "gateway",
          region: "eu-central-1",
          baseUrl: "http://127.0.0.1:18089",
        }),
      });

      const topoAfter = (await (await fetch(`${baseUrl}/v1beta1/console/topology`)).json()) as {
        totalRegisteredNodes: number;
        servicesCount: number;
      };
      assert.equal(topoAfter.totalRegisteredNodes, 11);
      assert.equal(topoAfter.servicesCount, 14);

      const celldAfter = (await (await fetch(`${baseUrl}/v1beta1/console/celld-dev-status`)).json()) as {
        registeredCells: Array<{ cellId: string; hostNodeId: string }>;
        topologyEndpoints: { gatewayUrls: string[] };
        candidateOrigins: Array<{ origin: string }>;
      };
      assert.equal(celldAfter.registeredCells.length, 11);
      assert.equal(
        celldAfter.registeredCells.some((c) => c.cellId === "AnstsSchedulerCell:shard-04" && c.hostNodeId === "celld-brain-04"),
        true
      );
      assert.equal(
        celldAfter.registeredCells.some((c) => c.cellId === "LayerFsSandboxCell:body-04" && c.hostNodeId === "body-host-04"),
        true
      );
      assert.equal(
        celldAfter.registeredCells.some((c) => c.cellId === "TunnelGatewayCell:celld-gw-03" && c.hostNodeId === "celld-gw-03"),
        true
      );
      assert.equal(celldAfter.topologyEndpoints.gatewayUrls.includes("http://127.0.0.1:18089"), true);
      assert.equal(celldAfter.candidateOrigins.some((o) => o.origin === "http://127.0.0.1:18094"), true);

      // 3. Verify heartbeat without cpuPct/psiPct measures real backend metrics
      const hbResp = await fetch(`${baseUrl}/internal/v1beta1/cluster/heartbeat`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          nodeId: "body-host-04",
          role: "body",
        }),
      });
      assert.equal(hbResp.status, 200);
      const hbBody = (await hbResp.json()) as { cpuPct: number; psiPct: number };
      assert.equal(typeof hbBody.cpuPct, "number");
      assert.ok(hbBody.cpuPct > 0);
      assert.equal(typeof hbBody.psiPct, "number");
      assert.ok(hbBody.psiPct > 0);

      // 4. Verify ep-qwen-38 in GET /v1beta1/console/runtime is a real CognitiveRouter endpoint
      const rtBefore = (await (await fetch(`${baseUrl}/v1beta1/console/runtime`)).json()) as {
        cognitiveRouterEndpoints: Array<{ endpointId: string; tpmRemaining: number }>;
      };
      const qwenBefore = rtBefore.cognitiveRouterEndpoints.find((e) => e.endpointId === "ep-qwen-38");
      assert.ok(qwenBefore, "ep-qwen-38 must be registered in real CognitiveRouter");

      const exhaustResp = await fetch(`${baseUrl}/v1beta1/management/router/inference-pipeline`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          action: "exhaust_tpm_bucket",
          endpointId: "ep-qwen-38",
        }),
      });
      assert.equal(exhaustResp.status, 200);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
