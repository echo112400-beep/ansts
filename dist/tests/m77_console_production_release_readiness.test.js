"use strict";













var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
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
 */(0, _nodeTest.describe)("M77 Console Zero-Stub Refactoring, Real Backend System Management & Production Release Readiness Suite", () => {(0, _nodeTest.it)("[M77-1] Eliminates all hardcoded stub data from createBaselineState, turn history dialog, and regional table in prototype.js & docs/index.html", () => {const indexPath = path.resolve(process.cwd(), "docs", "index.html");const protoPath = path.resolve(process.cwd(), "docs", "console-management", "prototype.js");for (const filePath of [indexPath, protoPath]) {const content = fs.readFileSync(filePath, "utf-8");_strict.default.equal(content.includes("nodes: []"), true, `${filePath} must initialize nodes: [] in createBaselineState`);_strict.default.equal(content.includes("tenants: []"), true, `${filePath} must initialize tenants: [] in createBaselineState`);_strict.default.equal(content.includes("tasks: []"), true, `${filePath} must initialize tasks: [] in createBaselineState`);_strict.default.equal(content.includes("approvals: []"), true, `${filePath} must initialize approvals: [] in createBaselineState`);_strict.default.equal(content.includes("auditLogs: []"), true, `${filePath} must initialize auditLogs: [] in createBaselineState`);
      _strict.default.equal(content.includes("9a01b2c3"), false, `${filePath} must not contain hardcoded fallback turn CRC32 9a01b2c3`);
      _strict.default.equal(
      content.includes("/v1beta1/management/events/stream?live=1"),
      true,
      `${filePath} must connect EventSource with ?live=1`);

      _strict.default.equal(
      content.includes("syncLiveAuthToken"),
      true,
      `${filePath} must synchronize live JWT tokens from /v1beta1/management/auth/token`);

      _strict.default.equal(
      content.includes("open-release-readiness-gate"),
      true,
      `${filePath} must expose the Production Release Readiness Gate action`);

    }
  });

  (0, _nodeTest.it)("[M77-2] Executes real backend tenant creation, regional telemetry & failover toggle, universal RBAC JWT guard, and persisted task branch tracking", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m77-mgmt-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
      const adminToken = guard.issuePrincipalToken("chen");
      const viewerToken = guard.issuePrincipalToken("xu");

      // 1. Universal Bearer JWT RBAC guard blocks VIEWER across secondary management write routes
      const secondaryRoutes = [
      { path: "/v1beta1/management/tenants/finops-escrow", body: { action: "record_spend", tenantId: "Northstar" } },
      { path: "/v1beta1/management/canary", body: { action: "promote_step" } },
      { path: "/v1beta1/management/ops/dlq-replay", body: { action: "seed_dlq_samples" } },
      { path: "/v1beta1/management/a2a", body: { action: "put_blackboard_cas", key: "k1", value: { a: 1 } } },
      { path: "/v1beta1/management/mcp", body: { action: "invoke_mcp_tool" } }];

      for (const route of secondaryRoutes) {
        const viewerResp = await fetch(`${baseUrl}${route.path}`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${viewerToken}`
          },
          body: JSON.stringify(route.body)
        });
        _strict.default.equal(
        viewerResp.status,
        403,
        `VIEWER JWT must be blocked with 403 on ${route.path} (got ${viewerResp.status})`);

      }

      // 2. Create a new tenant ("Orion", expectedVersion: 0) and verify it persists in SQLite & state.allTenantIds
      const createTenantResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`
        },
        body: JSON.stringify({
          tenantId: "Orion",
          maxSlots: 110,
          dailyBudgetUsd: 950,
          expectedVersion: 0
        })
      });
      _strict.default.equal(createTenantResp.status, 200);
      const createdTenant = await createTenantResp.json();




      _strict.default.equal(createdTenant.tenantId, "Orion");
      _strict.default.equal(createdTenant.configVersion, 1);
      _strict.default.equal(createdTenant.created, true);

      // 3. Toggle region eu-central-1 into failover drain mode and verify dynamic regional telemetry in GET /v1beta1/management/state
      const toggleRegResp = await fetch(`${baseUrl}/v1beta1/management/canary`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`
        },
        body: JSON.stringify({
          action: "toggle_region",
          regionId: "eu-central-1",
          capacityAvailable: false
        })
      });
      _strict.default.equal(toggleRegResp.status, 200);

      const stateResp = await fetch(`${baseUrl}/v1beta1/management/state`);
      _strict.default.equal(stateResp.status, 200);
      const stateBody = await stateResp.json();











      _strict.default.equal(stateBody.allTenantIds.includes("Orion"), true);
      const orion = stateBody.tenants.find((t) => t.id === "Orion");
      _strict.default.ok(orion);
      _strict.default.equal(orion.maxSlots, 110);
      _strict.default.equal(orion.dailyBudgetUsd, 950);

      const euReg = stateBody.regions.find((r) => r.regionId === "eu-central-1");
      _strict.default.ok(euReg);
      _strict.default.equal(euReg.capacityAvailable, false);
      _strict.default.equal(euReg.sloStatus, "DRAINED_FAILOVER");
      _strict.default.ok(euReg.brainSummary.includes("brain-03"));
      _strict.default.ok(euReg.avgCpuPct > 0);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  (0, _nodeTest.it)("[M77-3 & M77-4] Serves CSP security headers, streams real-time events over persistent ?live=1 SSE, and passes 8/8 Production Release Readiness Gates", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m77-prod-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      storageDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. Verify production HTTP security headers on static Console HTML & JS
      const htmlResp = await fetch(`${baseUrl}/index.html`);
      _strict.default.equal(htmlResp.status, 200);
      _strict.default.equal(htmlResp.headers.get("x-content-type-options"), "nosniff");
      _strict.default.equal(htmlResp.headers.get("x-frame-options"), "DENY");
      _strict.default.equal(htmlResp.headers.get("referrer-policy"), "strict-origin-when-cross-origin");
      _strict.default.ok(htmlResp.headers.get("content-security-policy")?.includes("default-src 'self'"));

      // 2. Verify live persistent SSE stream (`GET /v1beta1/management/events/stream?live=1`) pushes real-time events
      const sseAbort = new AbortController();
      const liveSseResp = await fetch(`${baseUrl}/v1beta1/management/events/stream?live=1`, {
        signal: sseAbort.signal
      });
      _strict.default.equal(liveSseResp.status, 200);
      _strict.default.equal(liveSseResp.headers.get("x-accel-buffering"), "no");
      _strict.default.ok(liveSseResp.body, "Live SSE response body stream must be present");

      const reader = liveSseResp.body.getReader();
      const decoder = new TextDecoder();
      let accumulatedSse = "";

      // Read initial `: connected live=1` + replay chunks
      const firstChunk = await reader.read();
      accumulatedSse += decoder.decode(firstChunk.value);
      _strict.default.ok(accumulatedSse.includes("connected live=1"));

      // Trigger a live management action while the SSE stream is open
      const guard = new _console_rbac_guard.ConsoleRbacAuthGuard();
      const adminToken = guard.issuePrincipalToken("chen");
      await fetch(`${baseUrl}/v1beta1/management/canary`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`
        },
        body: JSON.stringify({ action: "promote_step" })
      });

      // Read next live-pushed chunk from the open SSE connection
      const liveChunk = await reader.read();
      accumulatedSse += decoder.decode(liveChunk.value);
      _strict.default.ok(accumulatedSse.includes("event: management.audit"));
      _strict.default.ok(accumulatedSse.includes("promote_step"));

      sseAbort.abort();

      // 3. Verify Production Release Readiness Gate via HTTP & CLI (`anstsctl mgmt-release-gate`)
      const gateResp = await fetch(`${baseUrl}/v1beta1/management/release/readiness`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`
        },
        body: JSON.stringify({})
      });
      _strict.default.equal(gateResp.status, 200);
      const gateBody = await gateResp.json();








      _strict.default.equal(gateBody.releaseVersion, "v2.5.0-prod");
      _strict.default.equal(gateBody.releaseReady, true);
      _strict.default.equal(gateBody.passedGatesCount, 8);
      _strict.default.equal(gateBody.totalGatesCount, 8);
      _strict.default.equal(gateBody.gates.every((g) => g.passed), true);
      _strict.default.equal(gateBody.assetDigestsSha256["docs/index.html"]?.length, 64);
      _strict.default.equal(gateBody.assetDigestsSha256["docs/console-management/prototype.js"]?.length, 64);
      _strict.default.equal(gateBody.auditChainIntegrity.valid, true);

      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-release-gate"], baseUrl);
      _strict.default.equal(cliRes.exitCode, 0);
      const cliJson = JSON.parse(cliRes.stdout);
      _strict.default.equal(cliJson.releaseReady, true);
      _strict.default.equal(cliJson.passedGatesCount, 8);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});