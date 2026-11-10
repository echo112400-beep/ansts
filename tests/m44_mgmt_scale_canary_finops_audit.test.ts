/**
 * @fileoverview M44 End-to-End Verification: Management Plane Node Scale-Out/Decommission,
 * Closed-Loop Approval Execution Engine, Progressive Canary Rollout Controller,
 * Live Tenant FinOps/Escrow Metering, Interactive Task Turn Stepping & Searchable Audit Explorer.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";

describe("M44 Management Plane Deep Frontend & Backend Optimization Suite", () => {
  it("executes dynamic node scale-out/decommission, closed-loop approval execution, canary promotion/rollback, FinOps/Escrow metering, task turn stepping & audit search", async () => {
    const tmpDir = `/tmp/ansts-m44-${process.pid}-${Date.now()}`;
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
      // 1. [M44-1] Dynamic Brain Shard Node Scale-Out (`celld-brain-04`) & Decommission
      const scaleOutResp = await fetch(`${baseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "scale_out",
          nodeId: "celld-brain-04",
          role: "brain",
          region: "ap-northeast-1",
          actor: "chen",
          actorRole: "SRE_ADMIN",
        }),
      });
      assert.equal(scaleOutResp.status, 201);

      const stateAfterScale = (await (await fetch(`${baseUrl}/v1beta1/management/state`)).json()) as {
        nodes: Array<{ id: string; slots: number }>;
        slotDistribution: Record<string, number>;
      };
      assert.ok(stateAfterScale.nodes.some((n) => n.id === "celld-brain-04"));
      assert.ok((stateAfterScale.slotDistribution["brain-node-04"] ?? 0) > 0);

      const decomResp = await fetch(`${baseUrl}/v1beta1/management/nodes/scale`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "decommission",
          nodeId: "celld-brain-04",
          role: "brain",
          region: "ap-northeast-1",
          actor: "chen",
          actorRole: "SRE_ADMIN",
        }),
      });
      assert.equal(decomResp.status, 200);

      // 2. [M44-2] Closed-Loop Approval Execution (`BRAIN_SHARD_DRAIN` auto-drains `celld-brain-02`) & Audit Explorer
      const createDrainApr = await fetch(`${baseUrl}/v1beta1/management/approvals/create`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          type: "BRAIN_SHARD_DRAIN",
          target: "celld-brain-02 紧急分片排水",
          region: "us-west-2",
          tenant: "all",
          initiator: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(createDrainApr.status, 201);
      const drainTicket = (await createDrainApr.json()) as { approvalId: string };

      const decideDrainApr = await fetch(`${baseUrl}/v1beta1/management/approvals/decide`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          approvalId: drainTicket.approvalId,
          approver: "zhou",
          role: "SRE_ADMIN",
          decision: "approve",
        }),
      });
      assert.equal(decideDrainApr.status, 200);

      // Verify celld-brain-02 is now CORDONED in management state
      const stateAfterApprove = (await (await fetch(`${baseUrl}/v1beta1/management/state`)).json()) as {
        nodes: Array<{ id: string; status: string }>;
      };
      const b2Node = stateAfterApprove.nodes.find((n) => n.id === "celld-brain-02")!;
      assert.equal(b2Node.status, "CORDONED");

      // Search Audit Log Explorer (`GET /v1beta1/management/audits`)
      const auditsResp = await fetch(`${baseUrl}/v1beta1/management/audits?q=celld-brain-02&role=SRE_ADMIN`);
      assert.equal(auditsResp.status, 200);
      const auditsData = (await auditsResp.json()) as { total: number; audits: Array<{ action: string }> };
      assert.ok(auditsData.total >= 1);

      // 3. [M44-3] Progressive Canary Rollout Controller (`promote_step` -> `inject_anomaly` -> `reset_canary`)
      const step1 = await (
        await fetch(`${baseUrl}/v1beta1/management/canary`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "promote_step", actor: "chen", role: "SRE_ADMIN" }),
        })
      ).json();
      assert.equal(step1.trafficWeightPct, 25);

      const rollback = await (
        await fetch(`${baseUrl}/v1beta1/management/canary`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "inject_anomaly", actor: "chen", role: "SRE_ADMIN" }),
        })
      ).json();
      assert.equal(rollback.state, "ROLLED_BACK");
      assert.equal(rollback.trafficWeightPct, 0);

      const resetCanary = await (
        await fetch(`${baseUrl}/v1beta1/management/canary`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "reset_canary", actor: "chen", role: "SRE_ADMIN" }),
        })
      ).json();
      assert.equal(resetCanary.state, "CANARY_ACTIVE");
      assert.equal(resetCanary.trafficWeightPct, 5);

      // 4. [M44-4] Live Tenant FinOps Spend Step, DAG Escrow Reservation & Task Turn Stepping
      const spendStep1 = await (
        await fetch(`${baseUrl}/v1beta1/management/tenants/finops-escrow`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            action: "record_spend",
            tenantId: "Northstar",
            spendDeltaUsd: 200,
            actor: "chen",
            role: "SRE_ADMIN",
          }),
        })
      ).json();
      // 840 + 200 = 1040 / 1200 (86.7% >= 80% soft threshold) -> downgraded: true, allowed: true
      assert.equal(spendStep1.spentUsd, 1040);
      assert.equal(spendStep1.downgraded, true);
      assert.equal(spendStep1.allowed, true);

      const escrowStep = await (
        await fetch(`${baseUrl}/v1beta1/management/tenants/finops-escrow`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            action: "reserve_escrow",
            tenantId: "Helios",
            escrowDeltaSlots: 4,
            actor: "chen",
            role: "SRE_ADMIN",
          }),
        })
      ).json();
      assert.equal(escrowStep.escrowReserved, 12);

      // Advance task-ns-8841 from Turn #5 -> Turn #6 via POST /v1beta1/management/tasks/step
      const taskStepResp = await fetch(`${baseUrl}/v1beta1/management/tasks/step`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ taskId: "task-ns-8841", actor: "chen", role: "SRE_ADMIN" }),
      });
      assert.equal(taskStepResp.status, 200);
      const taskStepData = (await taskStepResp.json()) as { turnIndex: number; crc32Hex: string };
      assert.equal(taskStepData.turnIndex, 6);
      assert.equal(taskStepData.crc32Hex.length, 8);

      const turnsVerify = (await (
        await fetch(`${baseUrl}/v1beta1/management/tasks/task-ns-8841/turns`)
      ).json()) as { turns: unknown[] };
      assert.equal(turnsVerify.turns.length, 6);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
