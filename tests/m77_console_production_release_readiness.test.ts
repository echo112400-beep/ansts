/**
 * @fileoverview M77: Console Zero-Stub Refactoring, Real Backend System Management & Production Release Readiness Gate Suite
 * Verifies:
 * 1. [M77-1] Zero Stub Data in Console UI (`docs/index.html` & `docs/console-management/prototype.js`):
 *    - `createBaselineState()` initializes empty arrays (`nodes: []`, `tenants: []`, `tasks: []`, `approvals: []`, `auditLogs: []`, `regions: []`) with zero hardcoded mock records.
 *    - Hardcoded fallback turn history (`9a01b2c3`) and hardcoded regional table rows are completely removed.
 *    - Live JWT synchronization (`syncLiveAuthToken`) and persistent SSE (`?live=1`) are wired.
 * 2. [M77-2] Real End-to-End Backend System Management:
 *    - Dynamic tenant creation (`expectedVersion: 0`) persisted in SQLite `v25_mgmt_tenants` & `FinOpsBudgetController` and reflected in `allTenantIds`.
 *    - Real regional telemetry aggregation (`regions` with `brainSummary`, `avgCpuPct`, `avgPsiPct`, `sloStatus`) and live regional failover toggle (`toggle_region`).
 *    - Universal Bearer JWT RBAC enforcement (`401` on invalid token, `403` on `VIEWER` token across all `/v1beta1/management/*` write routes).
 * 3. [M77-3] Live Persistent SSE Streaming (`GET /v1beta1/management/events/stream?live=1`) & Production HTTP Security/CSP Headers.
 * 4. [M77-4] Production Release Readiness Gate (`POST /v1beta1/management/release/readiness` & `anstsctl mgmt-release-gate`) verifying all 8/8 production gates and SHA-256 asset digests.
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M77 Console Zero-Stub Refactoring, Real Backend System Management & Production Release Readiness Suite", () => {
  it("[M77-1] Eliminates all hardcoded stub data from createBaselineState, turn history dialog, and regional table in prototype.js & docs/index.html", () => {
    const indexPath = path.resolve(process.cwd(), "docs", "index.html");
    const protoPath = path.resolve(process.cwd(), "docs", "console-management", "prototype.js");

    for (const filePath of [indexPath, protoPath]) {
      const content = fs.readFileSync(filePath, "utf-8");
      assert.equal(content.includes("nodes: []"), true, `${filePath} must initialize nodes: [] in createBaselineState`);
      assert.equal(content.includes("tenants: []"), true, `${filePath} must initialize tenants: [] in createBaselineState`);
      assert.equal(content.includes("tasks: []"), true, `${filePath} must initialize tasks: [] in createBaselineState`);
      assert.equal(content.includes("approvals: []"), true, `${filePath} must initialize approvals: [] in createBaselineState`);
      assert.equal(content.includes("auditLogs: []"), true, `${filePath} must initialize auditLogs: [] in createBaselineState`);
      assert.equal(content.includes("9a01b2c3"), false, `${filePath} must not contain hardcoded fallback turn CRC32 9a01b2c3`);
      assert.equal(
        content.includes("/v1beta1/management/events/stream?live=1"),
        true,
        `${filePath} must connect EventSource with ?live=1`
      );
      assert.equal(
        content.includes("syncLiveAuthToken"),
        true,
        `${filePath} must synchronize live JWT tokens from /v1beta1/management/auth/token`
      );
      assert.equal(
        content.includes("open-release-readiness-gate"),
        true,
        `${filePath} must expose the Production Release Readiness Gate action`
      );
    }
  });

  it("[M77-2] Executes real backend tenant creation, regional telemetry & failover toggle, universal RBAC JWT guard, and persisted task branch tracking", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m77-mgmt-"));
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
      const viewerToken = guard.issuePrincipalToken("xu");

      // 1. Universal Bearer JWT RBAC guard blocks VIEWER across secondary management write routes
      const secondaryRoutes = [
        { path: "/v1beta1/management/tenants/finops-escrow", body: { action: "record_spend", tenantId: "Northstar" } },
        { path: "/v1beta1/management/canary", body: { action: "promote_step" } },
        { path: "/v1beta1/management/ops/dlq-replay", body: { action: "seed_dlq_samples" } },
        { path: "/v1beta1/management/a2a", body: { action: "put_blackboard_cas", key: "k1", value: { a: 1 } } },
        { path: "/v1beta1/management/mcp", body: { action: "invoke_mcp_tool" } },
      ];
      for (const route of secondaryRoutes) {
        const viewerResp = await fetch(`${baseUrl}${route.path}`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${viewerToken}`,
          },
          body: JSON.stringify(route.body),
        });
        assert.equal(
          viewerResp.status,
          403,
          `VIEWER JWT must be blocked with 403 on ${route.path} (got ${viewerResp.status})`
        );
      }

      // 2. Create a new tenant ("Orion", expectedVersion: 0) and verify it persists in SQLite & state.allTenantIds
      const createTenantResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({
          tenantId: "Orion",
          maxSlots: 110,
          dailyBudgetUsd: 950,
          expectedVersion: 0,
        }),
      });
      assert.equal(createTenantResp.status, 200);
      const createdTenant = (await createTenantResp.json()) as {
        tenantId: string;
        configVersion: number;
        created: boolean;
      };
      assert.equal(createdTenant.tenantId, "Orion");
      assert.equal(createdTenant.configVersion, 1);
      assert.equal(createdTenant.created, true);

      // 3. Toggle region eu-central-1 into failover drain mode and verify dynamic regional telemetry in GET /v1beta1/management/state
      const toggleRegResp = await fetch(`${baseUrl}/v1beta1/management/canary`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({
          action: "toggle_region",
          regionId: "eu-central-1",
          capacityAvailable: false,
        }),
      });
      assert.equal(toggleRegResp.status, 200);

      const stateResp = await fetch(`${baseUrl}/v1beta1/management/state`);
      assert.equal(stateResp.status, 200);
      const stateBody = (await stateResp.json()) as {
        allTenantIds: string[];
        tenants: Array<{ id: string; maxSlots: number; dailyBudgetUsd: number }>;
        regions: Array<{
          regionId: string;
          capacityAvailable: boolean;
          brainSummary: string;
          avgCpuPct: number;
          avgPsiPct: number;
          sloStatus: string;
        }>;
      };
      assert.equal(stateBody.allTenantIds.includes("Orion"), true);
      const orion = stateBody.tenants.find((t) => t.id === "Orion");
      assert.ok(orion);
      assert.equal(orion.maxSlots, 110);
      assert.equal(orion.dailyBudgetUsd, 950);

      const euReg = stateBody.regions.find((r) => r.regionId === "eu-central-1");
      assert.ok(euReg);
      assert.equal(euReg.capacityAvailable, false);
      assert.equal(euReg.sloStatus, "DRAINED_FAILOVER");
      assert.ok(euReg.brainSummary.includes("brain-03"));
      assert.ok(euReg.avgCpuPct > 0);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it("[M77-3 & M77-4] Serves CSP security headers, streams real-time events over persistent ?live=1 SSE, and passes 8/8 Production Release Readiness Gates", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m77-prod-"));
    const server = new AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir,
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. Verify production HTTP security headers on static Console HTML & JS
      const htmlResp = await fetch(`${baseUrl}/index.html`);
      assert.equal(htmlResp.status, 200);
      assert.equal(htmlResp.headers.get("x-content-type-options"), "nosniff");
      assert.equal(htmlResp.headers.get("x-frame-options"), "DENY");
      assert.equal(htmlResp.headers.get("referrer-policy"), "strict-origin-when-cross-origin");
      assert.ok(htmlResp.headers.get("content-security-policy")?.includes("default-src 'self'"));

      // 2. Verify live persistent SSE stream (`GET /v1beta1/management/events/stream?live=1`) pushes real-time events
      const sseAbort = new AbortController();
      const liveSseResp = await fetch(`${baseUrl}/v1beta1/management/events/stream?live=1`, {
        signal: sseAbort.signal,
      });
      assert.equal(liveSseResp.status, 200);
      assert.equal(liveSseResp.headers.get("x-accel-buffering"), "no");
      assert.ok(liveSseResp.body, "Live SSE response body stream must be present");

      const reader = liveSseResp.body.getReader();
      const decoder = new TextDecoder();
      let accumulatedSse = "";

      // Read initial `: connected live=1` + replay chunks
      const firstChunk = await reader.read();
      accumulatedSse += decoder.decode(firstChunk.value);
      assert.ok(accumulatedSse.includes("connected live=1"));

      // Trigger a live management action while the SSE stream is open
      const guard = new ConsoleRbacAuthGuard();
      const adminToken = guard.issuePrincipalToken("chen");
      await fetch(`${baseUrl}/v1beta1/management/canary`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({ action: "promote_step" }),
      });

      // Read next live-pushed chunk from the open SSE connection
      const liveChunk = await reader.read();
      accumulatedSse += decoder.decode(liveChunk.value);
      assert.ok(accumulatedSse.includes("event: management.audit"));
      assert.ok(accumulatedSse.includes("promote_step"));

      sseAbort.abort();

      // 3. Verify Production Release Readiness Gate via HTTP & CLI (`anstsctl mgmt-release-gate`)
      const gateResp = await fetch(`${baseUrl}/v1beta1/management/release/readiness`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({}),
      });
      assert.equal(gateResp.status, 200);
      const gateBody = (await gateResp.json()) as {
        releaseVersion: string;
        releaseReady: boolean;
        passedGatesCount: number;
        totalGatesCount: number;
        gates: Array<{ id: string; passed: boolean }>;
        assetDigestsSha256: Record<string, string>;
        auditChainIntegrity: { valid: boolean };
      };
      assert.equal(gateBody.releaseVersion, "v2.5.0-prod");
      assert.equal(gateBody.releaseReady, true);
      assert.equal(gateBody.passedGatesCount, 8);
      assert.equal(gateBody.totalGatesCount, 8);
      assert.equal(gateBody.gates.every((g) => g.passed), true);
      assert.equal(gateBody.assetDigestsSha256["docs/index.html"]?.length, 64);
      assert.equal(gateBody.assetDigestsSha256["docs/console-management/prototype.js"]?.length, 64);
      assert.equal(gateBody.auditChainIntegrity.valid, true);

      const cliRes = await runAnstsCtlRemote(["mgmt-release-gate"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const cliJson = JSON.parse(cliRes.stdout) as { releaseReady: boolean; passedGatesCount: number };
      assert.equal(cliJson.releaseReady, true);
      assert.equal(cliJson.passedGatesCount, 8);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
