"use strict";











var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var _server_main = require("../src/server_main.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M41 End-to-End Verification: ANSTS Platform Management Console Prototype & BFF APIs
 * Verifies:
 * 1. Delivery of `/ansts_console_management.html`, `/console-management/prototype.css`, `/console-management/prototype.js`, and `/console-management-design.md`
 * 2. Presence of all 11 operational scenarios and 4 RBAC identities from the prototype specification
 * 3. Management Plane BFF endpoints:
 *    - `GET /v1beta1/management/state`
 *    - `POST /v1beta1/management/tenants/config` (RBAC 403 & Optimistic CAS 409 conflict detection)
 *    - `POST /v1beta1/management/nodes/batch` (207 Partial Failure on pinned lease vs 200 on forced migration)
 *    - `POST /v1beta1/management/tasks/cancel` (202 CANCEL_UNCONFIRMED vs 200 FENCED_TERMINATED)
 *    - `POST /v1beta1/management/approvals/decide` (4-Eyes Dual-Admin enforcement: 409 on self-approval, 200 on peer SRE_ADMIN)
 */(0, _nodeTest.describe)("M41 Platform Management Console Prototype & BFF Suite", () => {(0, _nodeTest.it)("serves Management Console prototype assets and enforces RBAC, CAS, Partial Batch, Cancel-Unconfirmed & 4-Eyes Approval", async () => {const tmpDir = `/tmp/ansts-m41-${process.pid}-${Date.now()}`;fs.mkdirSync(tmpDir, { recursive: true });const server = new _server_main.AnstsLocalAndComposeServer({ host: "127.0.0.1", nodeId: "celld-gw-01", nodeRole: "gateway", storageDir: tmpDir
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. Verify static assets delivery
      const htmlResp = await fetch(`${baseUrl}/ansts_console_management.html`);
      assert.equal(htmlResp.status, 200);
      const html = await htmlResp.text();
      assert.ok(html.includes("ANSTS · 平台管理控制台"));
      assert.ok(html.includes('id="scenario"'));
      assert.ok(html.includes('id="identity"'));
      assert.ok(html.includes('id="dialog"'));

      const cssResp = await fetch(`${baseUrl}/console-management/prototype.css`);
      assert.equal(cssResp.status, 200);
      const css = await cssResp.text();
      assert.ok(css.includes("aside.rail"));

      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      assert.equal(jsResp.status, 200);
      const js = await jsResp.text();
      for (const sc of [
      "normal",
      "stale",
      "offline",
      "capacity",
      "quota",
      "budget",
      "partial",
      "cancel-unknown",
      "conflict",
      "history-missing",
      "verification"])
      {
        assert.ok(js.includes(sc), `prototype.js should handle scenario: ${sc}`);
      }

      const mdResp = await fetch(`${baseUrl}/console-management-design.md`);
      assert.equal(mdResp.status, 200);
      const md = await mdResp.text();
      assert.ok(md.includes("零伪造状态"));

      // 2. Verify GET /v1beta1/management/state
      const stateResp = await fetch(`${baseUrl}/v1beta1/management/state`);
      assert.equal(stateResp.status, 200);
      const stateData = await stateResp.json();
      assert.ok(stateData.tenants.some((t) => t.id === "Northstar"));

      // 3. Verify POST /v1beta1/management/tenants/config (VIEWER 403, CAS 200, then stale CAS 409)
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ role: "VIEWER", tenantId: "Northstar", expectedVersion: 14, maxSlots: 160 })
      });
      assert.equal(viewerResp.status, 403);

      const casOkResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          role: "SRE_ADMIN",
          actor: "chen",
          tenantId: "Northstar",
          expectedVersion: 14,
          maxSlots: 160,
          dailyBudgetUsd: 1500
        })
      });
      assert.equal(casOkResp.status, 200);

      const casConflictResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          role: "SRE_ADMIN",
          actor: "zhou",
          tenantId: "Northstar",
          expectedVersion: 14,
          maxSlots: 180
        })
      });
      assert.equal(casConflictResp.status, 409);

      // 4. Verify POST /v1beta1/management/nodes/batch (207 partial failure vs 200 forced lease migration)
      const batchPartialResp = await fetch(`${baseUrl}/v1beta1/management/nodes/batch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          operation: "drain",
          nodeIds: ["celld-brain-01", "body-host-01", "body-host-02"],
          forceMigrateLeases: false
        })
      });
      assert.equal(batchPartialResp.status, 207);
      const batchPartialData = await batchPartialResp.json();
      assert.equal(batchPartialData.partialFailure, true);

      const batchRetryResp = await fetch(`${baseUrl}/v1beta1/management/nodes/batch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          operation: "drain",
          nodeIds: ["body-host-02"],
          forceMigrateLeases: true
        })
      });
      assert.equal(batchRetryResp.status, 200);

      // 5. Verify POST /v1beta1/management/tasks/cancel (202 CANCEL_UNCONFIRMED vs 200 FENCED_TERMINATED)
      const cancelUnconfResp = await fetch(`${baseUrl}/v1beta1/management/tasks/cancel`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ taskId: "task-ns-8841", mode: "graceful", currentFence: 12 })
      });
      assert.equal(cancelUnconfResp.status, 202);
      const cancelUnconfData = await cancelUnconfResp.json();
      assert.equal(cancelUnconfData.state, "CANCEL_UNCONFIRMED");
      assert.equal(cancelUnconfData.confirmed, false);

      const forceFenceResp = await fetch(`${baseUrl}/v1beta1/management/tasks/cancel`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ taskId: "task-ns-8841", mode: "force_fence", currentFence: 12 })
      });
      assert.equal(forceFenceResp.status, 200);
      const forceFenceData = await forceFenceResp.json();
      assert.equal(forceFenceData.state, "FENCED_TERMINATED");
      assert.equal(forceFenceData.newFencingToken, 13);

      // 6. Verify POST /v1beta1/management/approvals/decide (4-eyes dual-admin check)
      const selfApproveResp = await fetch(`${baseUrl}/v1beta1/management/approvals/decide`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          approvalId: "APR-2026-091",
          initiator: "chen",
          approver: "chen",
          role: "SRE_ADMIN",
          decision: "approve"
        })
      });
      assert.equal(selfApproveResp.status, 409);

      const peerApproveResp = await fetch(`${baseUrl}/v1beta1/management/approvals/decide`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          approvalId: "APR-2026-091",
          initiator: "chen",
          approver: "zhou",
          role: "SRE_ADMIN",
          decision: "approve"
        })
      });
      assert.equal(peerApproveResp.status, 200);
      const peerApproveData = await peerApproveResp.json();
      assert.equal(peerApproveData.status, "APPROVED (zhou)");
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});