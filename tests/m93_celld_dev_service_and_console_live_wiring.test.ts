/**
 * @fileoverview M93: celld Dev Service Live Boot, Multi-Endpoint Proxy/CORS Wiring & Console Live Backend Workbench Suite
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M93 celld Dev Service Live Boot & Console Live Backend Wiring Suite", () => {
  it("exposes live celld dev status, executes 6-point celld subsystem probe, enforces CORS + RBAC, and delegates Console BFF on dashboardPort", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m93-"));
    const server = new AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
    });
    const { apiPort, dashboardPort } = await server.start(0, 0);
    const apiBaseUrl = `http://127.0.0.1:${apiPort}`;
    const dashBaseUrl = `http://127.0.0.1:${dashboardPort}`;
    const guard = new ConsoleRbacAuthGuard();
    const adminToken = guard.issuePrincipalToken("chen");
    const viewerToken = guard.issuePrincipalToken("xu");

    try {
      // 1. Verify OPTIONS preflight & CORS headers on both apiPort and dashboardPort
      const preflightRes = await fetch(`${apiBaseUrl}/v1beta1/management/state`, {
        method: "OPTIONS",
        headers: {
          Origin: "http://localhost:8088",
          "Access-Control-Request-Method": "POST",
          "Access-Control-Request-Headers": "Content-Type, Authorization",
        },
      });
      assert.equal(preflightRes.status, 204);
      assert.equal(preflightRes.headers.get("access-control-allow-origin"), "*");
      assert.ok(preflightRes.headers.get("access-control-allow-headers")?.includes("Authorization"));

      // 2. Verify GET /v1beta1/console/celld-dev-status returns live celld runtime & DO cell placements
      const statusRes = await fetch(`${apiBaseUrl}/v1beta1/console/celld-dev-status`);
      assert.equal(statusRes.status, 200);
      assert.equal(statusRes.headers.get("access-control-allow-origin"), "*");
      const statusBody = (await statusRes.json()) as {
        serviceMode: string;
        status: string;
        nodeId: string;
        corsEnabled: boolean;
        bearerAuthForwarded: boolean;
        sseLiveStreamingEnabled: boolean;
        registeredCells: Array<{ cellId: string; hostNodeId: string; epochFencingToken: number; state: string }>;
        candidateOrigins: Array<{ id: string; origin: string }>;
      };
      assert.equal(statusBody.serviceMode, "DEV_LIVE_CELLD");
      assert.equal(statusBody.status, "RUNNING");
      assert.equal(statusBody.nodeId, "celld-gw-01");
      assert.equal(statusBody.corsEnabled, true);
      assert.equal(statusBody.bearerAuthForwarded, true);
      assert.equal(statusBody.sseLiveStreamingEnabled, true);
      assert.ok(statusBody.registeredCells.length >= 5);
      assert.ok(statusBody.candidateOrigins.some((c) => c.origin === "http://127.0.0.1:8088"));

      // 3. Verify VIEWER role is blocked with 403 on POST /v1beta1/management/celld/probe
      const viewerProbe = await fetch(`${apiBaseUrl}/v1beta1/management/celld/probe`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${viewerToken}`,
        },
        body: JSON.stringify({ targetOrigin: apiBaseUrl }),
      });
      assert.equal(viewerProbe.status, 403);

      // 4. Verify SRE_ADMIN executes 6/6 celld dev service probes and records audit log
      const adminProbe = await fetch(`${apiBaseUrl}/v1beta1/management/celld/probe`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({ targetOrigin: apiBaseUrl, reconcileCells: true }),
      });
      assert.equal(adminProbe.status, 200);
      const probeBody = (await adminProbe.json()) as {
        allHealthy: boolean;
        passedCount: number;
        totalCount: number;
        probes: Array<{ id: string; passed: boolean }>;
      };
      assert.equal(probeBody.allHealthy, true);
      assert.equal(probeBody.passedCount, 6);
      assert.equal(probeBody.totalCount, 6);
      assert.ok(probeBody.probes.every((p) => p.passed));

      // 5. Verify dashboardPort delegates Console HTML, BFF state, and native celld endpoints seamlessly
      const dashHtmlRes = await fetch(`${dashBaseUrl}/index.html`);
      assert.equal(dashHtmlRes.status, 200);
      const dashHtml = await dashHtmlRes.text();
      assert.ok(dashHtml.includes("probe-celld-dev-service"));

      const dashCelldHealthRes = await fetch(`${dashBaseUrl}/.well-known/celld/health`);
      assert.equal(dashCelldHealthRes.status, 200);
      const dashCelldHealth = (await dashCelldHealthRes.json()) as { healthy: boolean; nodeId: string };
      assert.equal(dashCelldHealth.healthy, true);
      assert.equal(dashCelldHealth.nodeId, "celld-gw-01");

      const dashMgmtStateRes = await fetch(`${dashBaseUrl}/v1beta1/management/state`);
      assert.equal(dashMgmtStateRes.status, 200);
      const dashMgmtState = (await dashMgmtStateRes.json()) as {
        celldDevService: { serviceMode: string; lastProbeReport: { allHealthy: boolean } };
      };
      assert.equal(dashMgmtState.celldDevService.serviceMode, "DEV_LIVE_CELLD");
      assert.equal(dashMgmtState.celldDevService.lastProbeReport.allHealthy, true);

      // 6. Verify CLI commands anstsctl mgmt-celld-status and mgmt-celld-probe
      const cliStatus = await runAnstsCtlRemote(["mgmt-celld-status"], apiBaseUrl);
      assert.equal(cliStatus.exitCode, 0);
      const parsedCliStatus = JSON.parse(cliStatus.stdout) as { serviceMode: string };
      assert.equal(parsedCliStatus.serviceMode, "DEV_LIVE_CELLD");

      const cliProbe = await runAnstsCtlRemote(["mgmt-celld-probe", apiBaseUrl], apiBaseUrl);
      assert.equal(cliProbe.exitCode, 0);
      const parsedCliProbe = JSON.parse(cliProbe.stdout) as { allHealthy: boolean; passedCount: number };
      assert.equal(parsedCliProbe.allHealthy, true);
      assert.equal(parsedCliProbe.passedCount, 6);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("verifies dev_cluster_local_supervisor.mjs, nginx-tanstack-console.conf, and Console UI live backend discovery wiring", () => {
    const supervisorCode = fs.readFileSync(
      path.resolve(process.cwd(), "scripts", "dev_cluster_local_supervisor.mjs"),
      "utf-8"
    );
    assert.ok(supervisorCode.includes('forwardHeaders["authorization"] = req.headers["authorization"]'));
    assert.ok(supervisorCode.includes('cleanPath === "/.well-known/celld/health"'));
    assert.ok(supervisorCode.includes('cleanPath === "/state"'));
    assert.ok(supervisorCode.includes("upstreamRes.pipe(res)"));

    const nginxConf = fs.readFileSync(
      path.resolve(process.cwd(), "deploy", "dev-cluster", "nginx-tanstack-console.conf"),
      "utf-8"
    );
    assert.ok(nginxConf.includes(".well-known/celld/health"));
    assert.ok(nginxConf.includes("Access-Control-Allow-Origin"));

    for (const rel of ["docs/index.html", "docs/console-management/prototype.js"]) {
      const content = fs.readFileSync(path.resolve(process.cwd(), rel), "utf-8");
      assert.ok(content.includes("CANDIDATE_DEV_ORIGINS"), `${rel} must define CANDIDATE_DEV_ORIGINS`);
      assert.ok(content.includes("probe-celld-dev-service"), `${rel} must include probe-celld-dev-service action`);
      assert.ok(content.includes("open-celld-endpoint-dialog"), `${rel} must include open-celld-endpoint-dialog action`);
      assert.ok(content.includes("inspect-celld-native-state"), `${rel} must include inspect-celld-native-state action`);
    }
  });
});
