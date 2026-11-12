import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M81 TanStack Console Full-Link SRE Observability, W3C Trace Waterfall, SLO Burn-Rate Alerting & Self-Healing Runbook Suite", () => {
  it("[M81-1] Exposes W3C Trace Waterfalls, SLO Burn-Rate, Quantiles & Runbooks via GET /v1beta1/console/observability and enforces RBAC", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m81-obs-"));
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

      // 1. Inspect initial observabilityVisualization via GET /v1beta1/console/observability
      const obsResp = await fetch(`${baseUrl}/v1beta1/console/observability`);
      assert.equal(obsResp.status, 200);
      const obsData = (await obsResp.json()) as {
        sloEvaluation: { alertSeverity: string; burnRate1h: number; errorBudgetRemainingPct: number };
        latencyQuantiles: { count: number; p50: number; p95: number; p99: number };
        traceWaterfalls: Array<{
          traceId: string;
          traceparent: string;
          operationName: string;
          totalDurationMs: number;
          spans: Array<{ spanId: string; name: string; offsetPct: number; widthPct: number }>;
        }>;
        dlqAndOutbox: {
          deadLetterTaskCount: number;
          keyRotation: { keyRotationGeneration: number };
        };
        sreRunbooks: Array<{ runbookId: string; executedCount: number }>;
      };
      assert.equal(obsData.sloEvaluation.alertSeverity, "HEALTHY");
      assert.ok(obsData.latencyQuantiles.count >= 9);
      assert.ok(obsData.traceWaterfalls.length >= 1);
      assert.ok(obsData.traceWaterfalls[0].traceparent.startsWith("00-"));
      assert.equal(obsData.traceWaterfalls[0].spans.length, 4);
      assert.equal(obsData.sreRunbooks.length, 4);

      // 2. VIEWER role (xu) is blocked with 403 on record_custom_trace and execute_auto_remediation_runbook
      const viewerTraceResp = await fetch(`${baseUrl}/v1beta1/management/ops/observability`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`,
        },
        body: JSON.stringify({
          action: "record_custom_trace",
          operationName: "Unauthorized Trace",
        }),
      });
      assert.equal(viewerTraceResp.status, 403);

      // 3. Record custom W3C Trace Waterfall as SRE_ADMIN (chen)
      const traceResp = await fetch(`${baseUrl}/v1beta1/management/ops/observability`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "record_custom_trace",
          operationName: "POST /v1beta1/tasks/dispatch (M81 Trace)",
        }),
      });
      assert.equal(traceResp.status, 200);
      const traceBody = (await traceResp.json()) as {
        recordedTrace: { traceId: string; spans: unknown[] };
        observabilityVisualization: { traceWaterfalls: Array<{ operationName: string }> };
      };
      assert.equal(traceBody.recordedTrace.spans.length, 5);
      assert.equal(
        traceBody.observabilityVisualization.traceWaterfalls[0].operationName,
        "POST /v1beta1/tasks/dispatch (M81 Trace)"
      );

      // 4. Simulate SLO Burn-Rate spike (PAGE_P0)
      const spikeResp = await fetch(`${baseUrl}/v1beta1/management/ops/observability`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "probe_slo_traces_and_metrics",
          simulatedErrorRequests: 180,
          simulatedP99LatencyMs: 420,
        }),
      });
      assert.equal(spikeResp.status, 200);
      const spikeBody = (await spikeResp.json()) as {
        sloEvaluation: { alertSeverity: string; burnRate1h: number };
      };
      assert.equal(spikeBody.sloEvaluation.alertSeverity, "PAGE_P0");
      assert.ok(spikeBody.sloEvaluation.burnRate1h >= 14.4);

      // 5. Seed DLQ samples and execute Automated Self-Healing Runbook RB-SLO-BURN-AND-DLQ-HEAL
      const seedResp = await fetch(`${baseUrl}/v1beta1/management/ops/dlq-replay`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({ action: "seed_dlq_samples" }),
      });
      assert.equal(seedResp.status, 200);

      const rbResp = await fetch(`${baseUrl}/v1beta1/management/ops/observability`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "execute_auto_remediation_runbook",
          runbookId: "RB-SLO-BURN-AND-DLQ-HEAL",
        }),
      });
      assert.equal(rbResp.status, 200);
      const rbBody = (await rbResp.json()) as {
        runbookId: string;
        healedTaskIds: string[];
        observabilityVisualization: {
          sloEvaluation: { alertSeverity: string };
          sreRunbooks: Array<{ runbookId: string; executedCount: number }>;
        };
      };
      assert.equal(rbBody.runbookId, "RB-SLO-BURN-AND-DLQ-HEAL");
      assert.ok(rbBody.healedTaskIds.includes("task-dlq-9901"));
      assert.equal(rbBody.observabilityVisualization.sloEvaluation.alertSeverity, "HEALTHY");
      const rbStat = rbBody.observabilityVisualization.sreRunbooks.find(
        (r) => r.runbookId === "RB-SLO-BURN-AND-DLQ-HEAL"
      );
      assert.equal(rbStat?.executedCount, 1);

      // 6. Verify CLI anstsctl mgmt-observability & UI bindings
      const cliObs = await runAnstsCtlRemote(["mgmt-observability", "inspect"], baseUrl);
      assert.equal(cliObs.exitCode, 0);
      const cliObsParsed = JSON.parse(cliObs.stdout) as {
        sloEvaluation: { alertSeverity: string };
      };
      assert.equal(cliObsParsed.sloEvaluation.alertSeverity, "HEALTHY");

      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        assert.ok(content.includes("renderObservabilitySection"));
        assert.ok(content.includes("obs-record-trace"));
        assert.ok(content.includes("obs-simulate-slo-burn"));
        assert.ok(content.includes("obs-exec-runbook"));
        assert.ok(content.includes("obs-seed-dlq"));
        assert.ok(content.includes("obs-replay-dlq-task"));
        assert.ok(content.includes("obs-replay-outbox-msg"));
      }
    } finally {
      await server.stop();
    }
  });
});
