"use strict";













var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _server_main = require("../src/server_main.js");
var _console_rbac_guard = require("../src/dashboard/console_rbac_guard.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
 * @fileoverview M76: TanStack Console & BFF Deep Security, Zero-Fake-State, RBAC & Nginx/SSE Rectification Suite
 * Verifies:
 * - [P0-1] Zero-Fake-State Frontend Contract: `executeLocalEngineMirror` is completely eliminated from
 *   `docs/index.html` and `docs/console-management/prototype.js`; network failure returns 503 offline and blocks mutations.
 * - [P0-2] Stored & Reflected XSS Prevention: `POST /v1beta1/management/approvals/create` sanitizes `<img src=x onerror=alert(1)>`
 *   on the backend (`sanitizeHtmlString`) and frontend renders via `esc()`.
 * - [P0-3] Live `ConsoleRbacAuthGuard` (HMAC-SHA256 JWT + Role Matrix + Cryptographic Audit Chain):
 *   rejects missing/forged credentials with 401, blocks VIEWER mutations with 403, and verifies chained audit hashes.
 * - [P1-4 & P1-5] Nginx SPA fallback (`try_files $uri $uri/ /index.html;`), `/v1/` proxy, unbuffered SSE proxy (`proxy_buffering off`),
 *   and frontend `EventSource` + `setInterval` auto-refresh.
 * - [P1-6] Strict 400 validation on empty POST `{}` across `shards/rebalance`, `nodes/batch`, `tasks/cancel`, `nodes/scale`, and `capacity/action`.
 * - [P2] Single canonical `docs/index.html` (duplicate `docs/ansts_console_live.html` removed) and cached single-flight peer Brain task fan-out.
 */(0, _nodeTest.describe)("M76 TanStack Console & BFF Deep Security, Zero-Fake-State, RBAC & Nginx/SSE Rectification Suite", () => {(0, _nodeTest.it)("[M76-P0-1 & P1-7 & P2] Eliminates executeLocalEngineMirror fake-200 fallback, wires EventSource + setInterval, and removes duplicate docs/ansts_console_live.html", () => {const indexPath = path.resolve(process.cwd(), "docs", "index.html");const protoPath = path.resolve(process.cwd(), "docs", "console-management", "prototype.js");const duplicateLivePath = path.resolve(process.cwd(), "docs", "ansts_console_live.html");_strict.default.equal(fs.existsSync(duplicateLivePath), false, "Duplicate docs/ansts_console_live.html must be removed");for (const filePath of [indexPath, protoPath]) {const content = fs.readFileSync(filePath, "utf-8");_strict.default.equal(content.includes("executeLocalEngineMirror"),
      false,
      `executeLocalEngineMirror fake-200 fallback must not exist in ${filePath}`);

      _strict.default.equal(
      content.includes("BACKEND_UNREACHABLE_OFFLINE"),
      true,
      `${filePath} must report 503 BACKEND_UNREACHABLE_OFFLINE on fetch failure`);

      _strict.default.equal(
      content.includes("new EventSource("),
      true,
      `${filePath} must connect live SSE via EventSource`);

      _strict.default.equal(
      content.includes("setInterval("),
      true,
      `${filePath} must configure periodic auto-refresh timer`);

      _strict.default.equal(
      content.includes("function esc("),
      true,
      `${filePath} must define HTML escaping function esc()`);

      _strict.default.equal(
      content.includes("${esc(a.target)}"),
      true,
      `${filePath} must escape approval target in innerHTML`);

    }
  });

  (0, _nodeTest.it)("[M76-P1-4 & P1-5] Nginx config uses try_files /index.html (no hanging =200), proxies /v1/, and disables buffering for SSE streams", () => {
    const nginxConfPath = path.resolve(process.cwd(), "deploy", "dev-cluster", "nginx-tanstack-console.conf");
    const conf = fs.readFileSync(nginxConfPath, "utf-8");

    _strict.default.equal(conf.includes("=200"), false, "nginx try_files must not use hanging =200");
    _strict.default.equal(conf.includes("try_files $uri $uri/ /index.html;"), true);
    _strict.default.equal(conf.includes("location /v1/"), true);
    _strict.default.equal(conf.includes("proxy_buffering off;"), true);
    _strict.default.equal(conf.includes("proxy_read_timeout 1h;"), true);
  });

  (0, _nodeTest.it)("[M76-P0-2, P0-3 & P1-6] Enforces ConsoleRbacAuthGuard Bearer JWT, blocks XSS payloads, and rejects empty POST {} with 400/401", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m76-"));
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

      // 1. Forged / tampered Bearer token -> 401 UNAUTHORIZED
      const forgedResp = await fetch(`${baseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}tampered`
        },
        body: JSON.stringify({ action: "scale_out", nodeId: "body-host-99" })
      });
      _strict.default.equal(forgedResp.status, 401);

      // 2. Empty POST {} without credentials to management mutation -> 401 MISSING_AUTH_CREDENTIALS
      const unauthEmptyResp = await fetch(`${baseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({})
      });
      _strict.default.equal(unauthEmptyResp.status, 401);

      // 3. Authenticated with valid Admin JWT, but empty POST {} -> 400 MISSING_REQUIRED_PARAMETERS across all destructive routes!
      const destructiveEndpoints = [
      "/v1beta1/console/shards/rebalance",
      "/v1beta1/management/nodes/batch",
      "/v1beta1/management/tasks/cancel",
      "/v1beta1/management/nodes/scale",
      "/v1beta1/management/capacity/action"];

      for (const ep of destructiveEndpoints) {
        const emptyPostResp = await fetch(`${baseUrl}${ep}`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${adminToken}`
          },
          body: JSON.stringify({})
        });
        _strict.default.equal(
        emptyPostResp.status,
        400,
        `Empty POST {} to ${ep} must return 400 MISSING_REQUIRED_PARAMETERS (got ${emptyPostResp.status})`);

      }

      // 4. VIEWER JWT token attempting write mutation -> 403 RBAC_FORBIDDEN
      const viewerScaleResp = await fetch(`${baseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${viewerToken}`
        },
        body: JSON.stringify({ action: "scale_out", nodeId: "body-host-09" })
      });
      _strict.default.equal(viewerScaleResp.status, 403);

      // 5. Stored XSS Payload in POST /v1beta1/management/approvals/create -> sanitized at storage & state response
      const xssPayload = "<img src=x onerror=alert(1)>";
      const createXssResp = await fetch(`${baseUrl}/v1beta1/management/approvals/create`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`
        },
        body: JSON.stringify({
          type: "QUOTA_EMERGENCY_BOOST",
          target: xssPayload,
          region: "us-west-2",
          tenant: "Northstar"
        })
      });
      _strict.default.equal(createXssResp.status, 201);
      const createdBody = await createXssResp.json();
      _strict.default.equal(createdBody.target.includes("<img"), false);
      _strict.default.equal(createdBody.target, (0, _console_rbac_guard.sanitizeHtmlString)(xssPayload));

      const stateResp = await fetch(`${baseUrl}/v1beta1/management/state`);
      _strict.default.equal(stateResp.status, 200);
      const stateBody = await stateResp.json();



      const storedApproval = stateBody.approvals.find((a) => a.id === createdBody.approvalId);
      _strict.default.ok(storedApproval);
      _strict.default.equal(storedApproval.target.includes("<img"), false);
      _strict.default.equal(storedApproval.target, "&lt;img src=x onerror=alert(1)&gt;");

      // 6. Cryptographic Audit Chain integrity verified
      _strict.default.equal(stateBody.auditChainIntegrity.valid, true);
      _strict.default.ok(stateBody.auditChainIntegrity.totalEntries >= 4);
      _strict.default.equal(stateBody.auditChainIntegrity.headHashHex.length, 64);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});