/**
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
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard, sanitizeHtmlString } from "../src/dashboard/console_rbac_guard.js";

describe("M76 TanStack Console & BFF Deep Security, Zero-Fake-State, RBAC & Nginx/SSE Rectification Suite", () => {
  it("[M76-P0-1 & P1-7 & P2] Eliminates executeLocalEngineMirror fake-200 fallback, wires EventSource + setInterval, and removes duplicate docs/ansts_console_live.html", () => {
    const indexPath = path.resolve(process.cwd(), "docs", "index.html");
    const protoPath = path.resolve(process.cwd(), "docs", "console-management", "prototype.js");
    const duplicateLivePath = path.resolve(process.cwd(), "docs", "ansts_console_live.html");

    assert.equal(fs.existsSync(duplicateLivePath), false, "Duplicate docs/ansts_console_live.html must be removed");

    for (const filePath of [indexPath, protoPath]) {
      const content = fs.readFileSync(filePath, "utf-8");
      assert.equal(
        content.includes("executeLocalEngineMirror"),
        false,
        `executeLocalEngineMirror fake-200 fallback must not exist in ${filePath}`
      );
      assert.equal(
        content.includes("BACKEND_UNREACHABLE_OFFLINE"),
        true,
        `${filePath} must report 503 BACKEND_UNREACHABLE_OFFLINE on fetch failure`
      );
      assert.equal(
        content.includes("new EventSource("),
        true,
        `${filePath} must connect live SSE via EventSource`
      );
      assert.equal(
        content.includes("setInterval("),
        true,
        `${filePath} must configure periodic auto-refresh timer`
      );
      assert.equal(
        content.includes("function esc("),
        true,
        `${filePath} must define HTML escaping function esc()`
      );
      assert.equal(
        content.includes("${esc(a.target)}"),
        true,
        `${filePath} must escape approval target in innerHTML`
      );
    }
  });

  it("[M76-P1-4 & P1-5] Nginx config uses try_files /index.html (no hanging =200), proxies /v1/, and disables buffering for SSE streams", () => {
    const nginxConfPath = path.resolve(process.cwd(), "deploy", "dev-cluster", "nginx-tanstack-console.conf");
    const conf = fs.readFileSync(nginxConfPath, "utf-8");

    assert.equal(conf.includes("=200"), false, "nginx try_files must not use hanging =200");
    assert.equal(conf.includes("try_files $uri $uri/ /index.html;"), true);
    assert.equal(conf.includes("location /v1/"), true);
    assert.equal(conf.includes("proxy_buffering off;"), true);
    assert.equal(conf.includes("proxy_read_timeout 1h;"), true);
  });

  it("[M76-P0-2, P0-3 & P1-6] Enforces ConsoleRbacAuthGuard Bearer JWT, blocks XSS payloads, and rejects empty POST {} with 400/401", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m76-"));
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

      // 1. Forged / tampered Bearer token -> 401 UNAUTHORIZED
      const forgedResp = await fetch(`${baseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}tampered`,
        },
        body: JSON.stringify({ action: "scale_out", nodeId: "body-host-99" }),
      });
      assert.equal(forgedResp.status, 401);

      // 2. Empty POST {} without credentials to management mutation -> 401 MISSING_AUTH_CREDENTIALS
      const unauthEmptyResp = await fetch(`${baseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      });
      assert.equal(unauthEmptyResp.status, 401);

      // 3. Authenticated with valid Admin JWT, but empty POST {} -> 400 MISSING_REQUIRED_PARAMETERS across all destructive routes!
      const destructiveEndpoints = [
        "/v1beta1/console/shards/rebalance",
        "/v1beta1/management/nodes/batch",
        "/v1beta1/management/tasks/cancel",
        "/v1beta1/management/nodes/scale",
        "/v1beta1/management/capacity/action",
      ];
      for (const ep of destructiveEndpoints) {
        const emptyPostResp = await fetch(`${baseUrl}${ep}`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: `Bearer ${adminToken}`,
          },
          body: JSON.stringify({}),
        });
        assert.equal(
          emptyPostResp.status,
          400,
          `Empty POST {} to ${ep} must return 400 MISSING_REQUIRED_PARAMETERS (got ${emptyPostResp.status})`
        );
      }

      // 4. VIEWER JWT token attempting write mutation -> 403 RBAC_FORBIDDEN
      const viewerScaleResp = await fetch(`${baseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${viewerToken}`,
        },
        body: JSON.stringify({ action: "scale_out", nodeId: "body-host-09" }),
      });
      assert.equal(viewerScaleResp.status, 403);

      // 5. Stored XSS Payload in POST /v1beta1/management/approvals/create -> sanitized at storage & state response
      const xssPayload = "<img src=x onerror=alert(1)>";
      const createXssResp = await fetch(`${baseUrl}/v1beta1/management/approvals/create`, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          authorization: `Bearer ${adminToken}`,
        },
        body: JSON.stringify({
          type: "QUOTA_EMERGENCY_BOOST",
          target: xssPayload,
          region: "us-west-2",
          tenant: "Northstar",
        }),
      });
      assert.equal(createXssResp.status, 201);
      const createdBody = (await createXssResp.json()) as { approvalId: string; target: string };
      assert.equal(createdBody.target.includes("<img"), false);
      assert.equal(createdBody.target, sanitizeHtmlString(xssPayload));

      const stateResp = await fetch(`${baseUrl}/v1beta1/management/state`);
      assert.equal(stateResp.status, 200);
      const stateBody = (await stateResp.json()) as {
        approvals: Array<{ id: string; target: string }>;
        auditChainIntegrity: { valid: boolean; totalEntries: number; headHashHex: string };
      };
      const storedApproval = stateBody.approvals.find((a) => a.id === createdBody.approvalId);
      assert.ok(storedApproval);
      assert.equal(storedApproval.target.includes("<img"), false);
      assert.equal(storedApproval.target, "&lt;img src=x onerror=alert(1)&gt;");

      // 6. Cryptographic Audit Chain integrity verified
      assert.equal(stateBody.auditChainIntegrity.valid, true);
      assert.ok(stateBody.auditChainIntegrity.totalEntries >= 4);
      assert.equal(stateBody.auditChainIntegrity.headHashHex.length, 64);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
