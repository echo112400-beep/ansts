import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M84 TanStack Console Global Command Palette (⌘K), Bayesian EWMA Resource Profiler, Predictive Prewarmer & SSE Stream Bus Suite", () => {
  it("[M84-1] Exposes Bayesian EWMA Profiler, Predictive Prewarmer & SSE Stream Bus via GET /v1beta1/console/profiler and executes interactive tuning", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m84-prof-"));
    const server = new AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new ConsoleRbacAuthGuard();
      const chenToken = guard.issuePrincipalToken("chen");
      const xuToken = guard.issuePrincipalToken("xu");

      // 1. Inspect initial profilerVisualization via GET /v1beta1/console/profiler
      const initResp = await fetch(`${baseUrl}/v1beta1/console/profiler`);
      assert.equal(initResp.status, 200);
      const initProf = (await initResp.json()) as {
        tenantProfiles: Array<{
          tenantId: string;
          peakCpuEwma: number;
          peakMemMbEwma: number;
          samples: number;
          recommendedActCores: number;
          recommendedActMemoryMb: number;
        }>;
        predictivePrewarmer: {
          ewmaArrivalRatePerSec: number;
          recommendedWarmPoolSize: number;
        };
        sseStreamBus: {
          recentEventsCount: number;
          recentEvents: Array<{ id: string; event: string }>;
        };
      };
      assert.equal(initProf.tenantProfiles.length, 3);
      const nsProf = initProf.tenantProfiles.find((p) => p.tenantId === "Northstar");
      assert.ok(nsProf && nsProf.samples >= 4);
      assert.equal(initProf.predictivePrewarmer.ewmaArrivalRatePerSec, 5);
      assert.ok(initProf.sseStreamBus.recentEventsCount >= 1);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/profiler/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`,
        },
        body: JSON.stringify({
          action: "record_tenant_sample",
          tenantId: "Northstar",
        }),
      });
      assert.equal(viewerResp.status, 403);

      // 3. Record new execution sample and apply right-sized profile to Northstar tasks
      const sampleResp = await fetch(`${baseUrl}/v1beta1/management/profiler/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "record_tenant_sample",
          tenantId: "Northstar",
          observedPeakCpu: 3.8,
          observedPeakMemoryMb: 3400,
        }),
      });
      assert.equal(sampleResp.status, 200);
      const sampleBody = (await sampleResp.json()) as {
        recommendedProfile: { act_cpu_cores: number; act_memory_mb: number };
      };
      assert.ok(sampleBody.recommendedProfile.act_cpu_cores > 3.5);
      assert.ok(sampleBody.recommendedProfile.act_memory_mb > 3200);

      const applyResp = await fetch(`${baseUrl}/v1beta1/management/profiler/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "apply_right_sized_profile",
          tenantId: "Northstar",
        }),
      });
      assert.equal(applyResp.status, 200);
      const applyBody = (await applyResp.json()) as { updatedTaskIds: string[] };
      assert.ok(applyBody.updatedTaskIds.includes("task-ns-8841"));

      // 4. Trigger Predictive EWMA Burst Prewarming and SSE Diagnostic Broadcast
      const burstResp = await fetch(`${baseUrl}/v1beta1/management/profiler/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "predict_and_prewarm_burst",
          observedArrivalsPerSec: 15.0,
          avgSandboxHoldSec: 4.0,
        }),
      });
      assert.equal(burstResp.status, 200);
      const burstBody = (await burstResp.json()) as {
        ewmaArrivalRatePerSec: number;
        recommendedWarmPoolSize: number;
      };
      assert.equal(burstBody.ewmaArrivalRatePerSec, 9);
      assert.equal(burstBody.recommendedWarmPoolSize, 45);

      const sseResp = await fetch(`${baseUrl}/v1beta1/management/profiler/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "publish_sse_diagnostic_event",
          eventType: "console.sre.m84",
          message: "M84 SSE broadcast verified",
        }),
      });
      assert.equal(sseResp.status, 200);
      const sseBody = (await sseResp.json()) as {
        envelope: { event: string };
        profilerVisualization: { sseStreamBus: { recentEvents: Array<{ event: string }> } };
      };
      assert.equal(sseBody.envelope.event, "console.sre.m84");
      assert.ok(sseBody.profilerVisualization.sseStreamBus.recentEvents.some((e) => e.event === "console.sre.m84"));

      // 5. Verify CLI anstsctl mgmt-profiler & Global Command Palette UI bindings
      const cliProf = await runAnstsCtlRemote(["mgmt-profiler", "inspect"], baseUrl);
      assert.equal(cliProf.exitCode, 0);
      const cliProfParsed = JSON.parse(cliProf.stdout) as {
        predictivePrewarmer: { ewmaArrivalRatePerSec: number };
      };
      assert.equal(cliProfParsed.predictivePrewarmer.ewmaArrivalRatePerSec, 9);

      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        assert.ok(content.includes("open-command-palette"));
        assert.ok(content.includes("prof-record-sample"));
        assert.ok(content.includes("prof-apply-rightsize"));
        assert.ok(content.includes("prof-predict-burst"));
        assert.ok(content.includes("prof-publish-sse"));
      }
    } finally {
      await server.stop();
    }
  });
});
