/**
 * @fileoverview M45 End-to-End Verification: Multi-Agent A2A Governance (Shared Blackboard CAS,
 * Git Patch Touch Arbitration, WFG Cycle Deadlock Detection/Resolution & Zero-Copy CoW Git Bisect),
 * Live Heavy Task Cross-Node Migration, LayerFS Line-Level Blame Inspector,
 * Distributed CAS Two-Phase Mark-and-Sweep GC + Epoch GC Audit, and Rolling-Upgrade Fleet Diagnostics.
 */
import "./console_write_auth.js";
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M45 Multi-Agent A2A Governance, Live Task Migration, LayerFS Blame/GC & Rolling-Upgrade Diagnostics Suite", () => {
  it("executes A2A Blackboard CAS + WFG deadlock + CoW bisect, live cross-node task migration, LayerFS blame, CAS Mark-and-Sweep GC, and rolling-upgrade fleet diagnostics", async () => {
    const tmpDir = `/tmp/ansts-m45-${process.pid}-${Date.now()}`;
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
      // 1. [M45-1] Multi-Agent A2A Governance Engine (`POST /v1beta1/management/a2a`)
      // 1a. Blackboard CAS update & 409 version conflict check
      const bbSuccess = await fetch(`${baseUrl}/v1beta1/management/a2a`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "put_blackboard_cas",
          key: "arch/zero-trust-policy",
          expectedVersion: 1,
          value: { mTLS: "STRICT_SPIFFE_V2", updatedBy: "chen" },
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(bbSuccess.status, 200);
      const bbData = (await bbSuccess.json()) as { entry: { version: number } };
      assert.equal(bbData.entry.version, 2);

      // Stale expectedVersion=1 now returns HTTP 409 A2A_BLACKBOARD_CAS_CONFLICT
      const bbConflict = await fetch(`${baseUrl}/v1beta1/management/a2a`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "put_blackboard_cas",
          key: "arch/zero-trust-policy",
          expectedVersion: 1,
          actor: "lin",
          role: "OPERATOR",
        }),
      });
      assert.equal(bbConflict.status, 409);

      // 1b. Multi-Agent Git Patch Touch Arbitration
      const gitArb = await (
        await fetch(`${baseUrl}/v1beta1/management/a2a`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "arbitrate_git_patches", actor: "chen", role: "SRE_ADMIN" }),
        })
      ).json();
      assert.deepEqual(gitArb.acceptedAgentIds, ["agent-ns-auth-p0", "agent-hl-ledger-p1"]);
      assert.deepEqual(gitArb.conflictedAgentIds, ["agent-ns-refactor-p2"]);

      // 1c. WFG Cycle Deadlock Injection & Resolution
      const dlInject = await (
        await fetch(`${baseUrl}/v1beta1/management/a2a`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "inject_deadlock_cycle", actor: "chen", role: "SRE_ADMIN" }),
        })
      ).json();
      assert.equal(dlInject.hasDeadlock, true);
      assert.ok(dlInject.cyclePath.length >= 3);

      const dlBreak = await (
        await fetch(`${baseUrl}/v1beta1/management/a2a`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "break_deadlock", victimAgentId: "agent-ns-worker-3", actor: "chen", role: "SRE_ADMIN" }),
        })
      ).json();
      assert.equal(dlBreak.hasDeadlock, false);

      // 1d. Parallel Copy-on-Write Git Bisect
      const cowBisect = await (
        await fetch(`${baseUrl}/v1beta1/management/a2a`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "run_cow_bisect", actor: "chen", role: "SRE_ADMIN" }),
        })
      ).json();
      assert.ok(cowBisect.firstBadCommit);
      assert.ok(cowBisect.probesExecuted >= 1);

      // 2. [M45-2] Live Heavy Task Cross-Node Migration (`POST /v1beta1/management/tasks/migrate`) & LayerFS Blame (`GET /v1beta1/management/layerfs/blame`)
      const migCli = await runAnstsCtlRemote(["mgmt-migrate", "task-ns-8841", "body-host-03"], baseUrl);
      assert.equal(migCli.exitCode, 0);
      const migData = JSON.parse(migCli.stdout) as {
        sourceNodeId: string;
        targetNodeId: string;
        newFencingToken: number;
        targetRestored: boolean;
      };
      assert.equal(migData.sourceNodeId, "body-host-02");
      assert.equal(migData.targetNodeId, "body-host-03");
      assert.equal(migData.newFencingToken, 13);
      assert.equal(migData.targetRestored, true);

      const blameResp = await fetch(`${baseUrl}/v1beta1/management/layerfs/blame?taskId=task-ns-8841`);
      assert.equal(blameResp.status, 200);
      const blameData = (await blameResp.json()) as { branchName: string; lines: Array<{ lineNumber: number; commitId: string }> };
      assert.equal(blameData.branchName, "feat-ns-auth");
      assert.ok(blameData.lines.length >= 2);

      // 3. [M45-3] Distributed CAS Mark-and-Sweep GC & Epoch GC Safe Audit (`POST /v1beta1/management/storage/gc-sweep`)
      const seedOrphans = await (
        await fetch(`${baseUrl}/v1beta1/management/storage/gc-sweep`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "seed_orphans", actor: "chen", role: "SRE_ADMIN" }),
        })
      ).json();
      assert.equal(seedOrphans.seededOrphanBlobIds.length, 3);

      const sweepCli = await runAnstsCtlRemote(["mgmt-gc-sweep", "sweep_cas"], baseUrl);
      assert.equal(sweepCli.exitCode, 0);
      const sweepData = JSON.parse(sweepCli.stdout) as { sweptCount: number; reclaimedBytes: number; retainedCount: number };
      assert.ok(sweepData.sweptCount >= 3);
      assert.ok(sweepData.reclaimedBytes > 0);
      assert.ok(sweepData.retainedCount > 0);

      const epochAudit = await (
        await fetch(`${baseUrl}/v1beta1/management/storage/gc-sweep`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ action: "epoch_gc_audit", graceSecs: 3600, actor: "xu", role: "VIEWER" }),
        })
      ).json();
      assert.equal(epochAudit.dryRun, true);
      assert.equal(epochAudit.deletableEpochCount, 2);

      // 4. [M45-4] Rolling-Upgrade Fleet Diagnostics Probe (`POST /v1beta1/management/diagnostics/probe`)
      const diagCli = await runAnstsCtlRemote(["mgmt-diagnose", "v0.6.3"], baseUrl);
      assert.equal(diagCli.exitCode, 0);
      const diagData = JSON.parse(diagCli.stdout) as {
        rollingUpgradeSafe: boolean;
        prometheusMetrics: string;
        webhookOutbox: { deliveredCount: number };
      };
      assert.equal(diagData.rollingUpgradeSafe, true);
      assert.ok(diagData.prometheusMetrics.includes("ansts_cluster_nodes_total"));
      assert.equal(diagData.webhookOutbox.deliveredCount, 1);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
