/**
 * @fileoverview M41 End-to-End Verification: ANSTS Platform Management Console Prototype & Real Backend Integration
 * Verifies that all Management Console endpoints (`/v1beta1/management/*`) are backed by real
 * stateful subsystems (`PlatformManagementPlaneEngine`, `CoreScheduler5D`, `SchedulerV25SingleWritePathStore`,
 * `TurnWalEngine`, `DeterministicTimeTravelReplayer`, `LayerFsWorkspaceDriver`, `FinOpsBudgetController`,
 * `CapacitySlaEnforcer`) rather than stateless stubs.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";

describe("M41 Platform Management Console Prototype & Real Backend BFF Suite", () => {
  it("executes real SQLite-persisted tenant CAS, P0 lease migration drain, Turn WAL time-travel fork, and 4-eyes approval", async () => {
    const tmpDir = `/tmp/ansts-m41-${process.pid}-${Date.now()}`;
    fs.mkdirSync(tmpDir, { recursive: true });

    const server = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: tmpDir,
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. Verify prototype static assets & visual style tokens
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
      assert.ok(css.includes("--bg-rail: #12161f"));
      assert.ok(css.includes("aside.rail"));

      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      assert.equal(jsResp.status, 200);
      const js = await jsResp.text();
      assert.ok(js.includes("/v1beta1/management/state"));
      assert.ok(js.includes("/v1beta1/management/tasks/time-travel"));

      // 2. Verify GET /v1beta1/management/state returns real seeded tasks, tenants, SLA, and audit logs
      const stateResp = await fetch(`${baseUrl}/v1beta1/management/state`);
      assert.equal(stateResp.status, 200);
      const stateData = (await stateResp.json()) as {
        tenants: Array<{ id: string; maxSlots: number; configVersion: number }>;
        tasks: Array<{ id: string; body: string; fence: number; turn: number }>;
        sla: { compliant: boolean; p0QueueWaitP99Ms: number };
      };
      const ns = stateData.tenants.find((t) => t.id === "Northstar");
      assert.ok(ns);
      assert.equal(ns.maxSlots, 120);
      assert.equal(ns.configVersion, 14);
      assert.equal(stateData.sla.compliant, true);
      const task8841 = stateData.tasks.find((t) => t.id === "task-ns-8841");
      assert.ok(task8841);
      assert.equal(task8841.body, "body-host-02");
      assert.equal(task8841.fence, 12);
      assert.equal(task8841.turn, 5);

      // 3. Verify POST /v1beta1/management/tenants/config mutates SQLite & FinOpsBudgetController and enforces CAS
      const viewerResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ role: "VIEWER", tenantId: "Northstar", expectedVersion: 14, maxSlots: 160 }),
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
          dailyBudgetUsd: 1800,
        }),
      });
      assert.equal(casOkResp.status, 200);

      // Stale version 14 must now fail with 409
      const casConflictResp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          role: "SRE_ADMIN",
          actor: "zhou",
          tenantId: "Northstar",
          expectedVersion: 14,
          maxSlots: 200,
        }),
      });
      assert.equal(casConflictResp.status, 409);

      // Verify state reflects persisted v15 and maxSlots=160
      const afterTenantState = (await (await fetch(`${baseUrl}/v1beta1/management/state`)).json()) as {
        tenants: Array<{ id: string; maxSlots: number; configVersion: number }>;
      };
      const updatedNs = afterTenantState.tenants.find((t) => t.id === "Northstar")!;
      assert.equal(updatedNs.maxSlots, 160);
      assert.equal(updatedNs.configVersion, 15);

      // 4. Verify POST /v1beta1/management/nodes/batch checks real P0 task lease on body-host-02 and migrates it on retry
      const batchPartialResp = await fetch(`${baseUrl}/v1beta1/management/nodes/batch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          operation: "drain",
          nodeIds: ["celld-brain-01", "body-host-01", "body-host-02"],
          forceMigrateLeases: false,
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(batchPartialResp.status, 207);

      const batchMigrateResp = await fetch(`${baseUrl}/v1beta1/management/nodes/batch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          operation: "drain",
          nodeIds: ["body-host-02"],
          forceMigrateLeases: true,
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(batchMigrateResp.status, 200);

      // Verify task-ns-8841 was genuinely migrated to body-host-01 and its fencing token incremented to 13
      const migratedTaskResp = await fetch(`${baseUrl}/v1/tasks/task-ns-8841`);
      assert.equal(migratedTaskResp.status, 200);
      const migratedTask = (await migratedTaskResp.json()) as {
        assigned_node_id: string;
        lease_fencing_token: number;
      };
      assert.equal(migratedTask.assigned_node_id, "body-host-01");
      assert.equal(migratedTask.lease_fencing_token, 13);

      // 5. Verify POST /v1beta1/management/tasks/time-travel genuinely forks task & LayerFS branch
      const ttForkResp = await fetch(`${baseUrl}/v1beta1/management/tasks/time-travel`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-ns-8841",
          targetTurnIndex: 3,
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(ttForkResp.status, 200);
      const ttForkData = (await ttForkResp.json()) as { forkedTaskId: string; layerfsBranch: string };
      assert.equal(ttForkData.forkedTaskId, "task-ns-8841-fork-t3");

      // Verify forked task exists in real scheduler/v25Store and LayerFS branch exists
      const getForkedResp = await fetch(`${baseUrl}/v1/tasks/task-ns-8841-fork-t3`);
      assert.equal(getForkedResp.status, 200);
      const headCommit = server.layerfs.getBranchHeadCommit(ttForkData.layerfsBranch);
      assert.equal(headCommit.commitId, "C0");

      // 6. Verify POST /v1beta1/management/tasks/cancel (202 CANCEL_UNCONFIRMED -> 200 FENCED_TERMINATED)
      const cancelUnconfResp = await fetch(`${baseUrl}/v1beta1/management/tasks/cancel`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ taskId: "task-ns-8842", mode: "graceful", actor: "chen", role: "SRE_ADMIN" }),
      });
      assert.equal(cancelUnconfResp.status, 202);

      const forceFenceResp = await fetch(`${baseUrl}/v1beta1/management/tasks/cancel`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ taskId: "task-ns-8842", mode: "force_fence", actor: "chen", role: "SRE_ADMIN" }),
      });
      assert.equal(forceFenceResp.status, 200);
      const forceFenceData = (await forceFenceResp.json()) as { state: string; newFencingToken: number };
      assert.equal(forceFenceData.state, "FENCED_TERMINATED");
      assert.equal(forceFenceData.newFencingToken, 9);

      // 7. Verify POST /v1beta1/management/approvals/decide enforces 4-eyes and mutates SQLite approval + tenant quota
      const selfApproveResp = await fetch(`${baseUrl}/v1beta1/management/approvals/decide`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          approvalId: "APR-2026-091",
          approver: "chen",
          role: "SRE_ADMIN",
          decision: "approve",
        }),
      });
      assert.equal(selfApproveResp.status, 409);

      const peerApproveResp = await fetch(`${baseUrl}/v1beta1/management/approvals/decide`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          approvalId: "APR-2026-091",
          approver: "zhou",
          role: "SRE_ADMIN",
          decision: "approve",
        }),
      });
      assert.equal(peerApproveResp.status, 200);

      // 8. Verify offline scenario returns real HTTP 503 and ?recover=1 restores 200
      await fetch(`${baseUrl}/v1beta1/management/scenario`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ scenario: "offline", actor: "chen" }),
      });
      const offlineResp = await fetch(`${baseUrl}/v1beta1/management/state`);
      assert.equal(offlineResp.status, 503);

      const recoverResp = await fetch(`${baseUrl}/v1beta1/management/state?recover=1`);
      assert.equal(recoverResp.status, 200);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
