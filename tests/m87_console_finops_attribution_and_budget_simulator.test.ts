import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M87 Live FinOps Multi-Dimensional Cost Attribution, Prefix-Cache Savings & Budget Gate Simulator Suite", () => {
  it("[M87-1] Exposes FinOps multi-dimensional attribution via GET /v1beta1/console/finops and executes spend simulation / 402 hard gate / reset", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m87-finops-"));
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

      // 1. Inspect initial finopsVisualization via GET /v1beta1/console/finops
      const initResp = await fetch(`${baseUrl}/v1beta1/console/finops`);
      assert.equal(initResp.status, 200);
      const initFin = (await initResp.json()) as {
        totalSpentUsd: number;
        totalBudgetUsd: number;
        totalPrefixSavedUsd: number;
        attributions: Array<{ tenantId: string; utilizationPct: number; activeModel: string }>;
      };
      assert.equal(initFin.attributions.length, 4);
      assert.ok(initFin.totalPrefixSavedUsd > 0);
      assert.ok(initFin.totalBudgetUsd >= 2900);

      // 2. VIEWER role (xu) is blocked with 403 RBAC_FORBIDDEN
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/finops/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${xuToken}`,
        },
        body: JSON.stringify({
          action: "simulate_spend_step",
          tenantId: "Northstar",
          amountUsd: 200,
        }),
      });
      assert.equal(viewerResp.status, 403);

      // 3. Simulate spend step on Northstar ($840 -> $1040 / $1200 = 86.7% -> soft-downgrade to Qwen-3.8)
      const step1Resp = await fetch(`${baseUrl}/v1beta1/management/finops/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "simulate_spend_step",
          tenantId: "Northstar",
          amountUsd: 200,
        }),
      });
      assert.equal(step1Resp.status, 200);
      const step1Body = (await step1Resp.json()) as {
        totalSpentUsd: number;
        downgraded: boolean;
        selectedModel: string;
      };
      assert.equal(step1Body.totalSpentUsd, 1040);
      assert.equal(step1Body.downgraded, true);
      assert.equal(step1Body.selectedModel, "Qwen-3.8");

      // 4. Exhaust remaining budget ($1040 -> $1200) and verify next call triggers 402 PAYMENT_REQUIRED_BUDGET_EXHAUSTED
      const step2Resp = await fetch(`${baseUrl}/v1beta1/management/finops/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "simulate_spend_step",
          tenantId: "Northstar",
          amountUsd: 200,
        }),
      });
      assert.equal(step2Resp.status, 200);
      const step2Body = (await step2Resp.json()) as { hardBlocked: boolean };
      assert.equal(step2Body.hardBlocked, true);

      const gate402Resp = await fetch(`${baseUrl}/v1beta1/management/finops/action`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${chenToken}`,
        },
        body: JSON.stringify({
          action: "simulate_spend_step",
          tenantId: "Northstar",
          amountUsd: 50,
        }),
      });
      assert.equal(gate402Resp.status, 402);

      // 5. Reset Northstar spend via CLI anstsctl mgmt-finops
      const cliReset = await runAnstsCtlRemote(["mgmt-finops", "reset_tenant_spend", "Northstar"], baseUrl);
      assert.equal(cliReset.exitCode, 0);
      const cliParsed = JSON.parse(cliReset.stdout) as { totalSpentUsd: number; activeModel: string };
      assert.equal(cliParsed.totalSpentUsd, 0);
      assert.equal(cliParsed.activeModel, "DeepSeek-R1");

      // 6. Verify UI bindings in prototype.js & docs/index.html
      const protoJs = fs.readFileSync(path.resolve(process.cwd(), "docs/console-management/prototype.js"), "utf-8");
      const indexHtml = fs.readFileSync(path.resolve(process.cwd(), "docs/index.html"), "utf-8");
      for (const content of [protoJs, indexHtml]) {
        assert.ok(content.includes("finops-simulate-spend"));
        assert.ok(content.includes("finops-reset-spend"));
        assert.ok(content.includes("/v1beta1/management/finops/action"));
      }
    } finally {
      await server.stop();
    }
  });
});
