/**
 * @fileoverview M50 End-to-End Verification: Full-Stack Architecture Design, Versioned Configuration Center,
 * Runtime Engine & SRE Observability/Ops Deep Optimization Suite.
 * Verifies:
 * 1. [M50-1] Architecture Design Blueprint & Contract Compliance Matrix (`GET /v1beta1/management/design/blueprint`).
 * 2. [M50-2] Versioned Dynamic Configuration Center with Schema Validation, CAS Conflict Detection, Hot-Reload & Revision Rollback (`POST /v1beta1/management/config/center`).
 * 3. [M50-3] Deep Runtime Engine Optimization (`AnstsSchedulerCell` `js_rpc` + `FleetReplicatedDurabilityEngine` + `HotCellOverloadAndQueueGuard` + `WalCompactorAndPagedRestorer` + `LayerFsRemoteStoreSynchronizer`) (`POST /v1beta1/management/runtime/celld-engine`).
 * 4. [M50-4] Closed-Loop SRE Observability, Histogram Quantiles (`p50`/`p95`/`p99`), W3C Distributed Trace Waterfall, SLO Burn-Rate Alerting & Automated Self-Healing Runbook (`POST /v1beta1/management/ops/observability`).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M50 Full-Stack Architecture Design, Versioned Configuration Center, Runtime Engine & SRE Observability/Ops Deep Optimization Suite", () => {
  it("executes design blueprint audit, versioned config center CAS hot-reload & rollback, celld/LayerFS runtime engine tuning, and W3C trace waterfall + SLO burn-rate + self-healing runbook", async () => {
    const tmpDir = `/tmp/ansts-m50-${process.pid}-${Date.now()}`;
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
      // 1. [M50-1] Architecture Design Blueprint & Contract Compliance Matrix
      const designCli = await runAnstsCtlRemote(["mgmt-design"], baseUrl);
      assert.equal(designCli.exitCode, 0);
      const designData = JSON.parse(designCli.stdout) as {
        designVersion: string;
        pillars: string[];
        sourceModulesCount: number;
        invariantsVerified: boolean;
        designMarkdownLength: number;
      };
      assert.equal(designData.designVersion, "v2.5-M50");
      assert.deepEqual(designData.pillars, ["DESIGN", "CONFIGURATION", "RUNTIME", "OBSERVABILITY_OPS"]);
      assert.equal(designData.sourceModulesCount, 86);
      assert.equal(designData.invariantsVerified, true);
      assert.ok(designData.designMarkdownLength > 500);

      // 2. [M50-2] Versioned Dynamic Configuration Center (Inspect -> Validate & Apply -> CAS 409 -> Schema 400 -> Rollback)
      const cfgInspectCli = await runAnstsCtlRemote(["mgmt-config", "inspect_active_config"], baseUrl);
      assert.equal(cfgInspectCli.exitCode, 0);
      const cfgInitial = JSON.parse(cfgInspectCli.stdout) as {
        activeRevision: number;
        sha256Checksum: string;
        activeConfig: { maxCellConcurrentRequests: number };
      };
      assert.equal(cfgInitial.activeRevision, 1);
      assert.equal(cfgInitial.activeConfig.maxCellConcurrentRequests, 64);

      const cfgApplyCli = await runAnstsCtlRemote(["mgmt-config", "validate_and_apply_config", "96"], baseUrl);
      assert.equal(cfgApplyCli.exitCode, 0);
      const cfgApplied = JSON.parse(cfgApplyCli.stdout) as {
        previousRevision: number;
        activeRevision: number;
        changedKeys: string[];
        hotReloaded: boolean;
        activeConfig: { maxCellConcurrentRequests: number };
      };
      assert.equal(cfgApplied.previousRevision, 1);
      assert.equal(cfgApplied.activeRevision, 2);
      assert.ok(cfgApplied.changedKeys.includes("maxCellConcurrentRequests"));
      assert.equal(cfgApplied.hotReloaded, true);
      assert.equal(cfgApplied.activeConfig.maxCellConcurrentRequests, 96);

      // Verify CAS conflict (409) when passing stale expectedRevision=1
      const casConflictResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "validate_and_apply_config",
          expectedRevision: 1,
          runtimeOverrides: { maxCellConcurrentRequests: 128 },
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(casConflictResp.status, 409);

      // Verify Schema validation error (400) when actCpuCores < thinkCpuCores
      const invalidSchemaResp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          action: "validate_and_apply_config",
          runtimeOverrides: { thinkCpuCores: 8.0, actCpuCores: 1.0 },
          actor: "chen",
          role: "SRE_ADMIN",
        }),
      });
      assert.equal(invalidSchemaResp.status, 400);

      // Rollback to revision 1
      const cfgRollbackCli = await runAnstsCtlRemote(["mgmt-config", "rollback_revision", "1"], baseUrl);
      assert.equal(cfgRollbackCli.exitCode, 0);
      const cfgRolledBack = JSON.parse(cfgRollbackCli.stdout) as {
        rolledBackToRevision: number;
        activeRevision: number;
        activeConfig: { maxCellConcurrentRequests: number };
      };
      assert.equal(cfgRolledBack.rolledBackToRevision, 1);
      assert.equal(cfgRolledBack.activeRevision, 3);
      assert.equal(cfgRolledBack.activeConfig.maxCellConcurrentRequests, 64);

      // 3. [M50-3] Deep Runtime Engine Optimization (SchedulerCell RPC/Alarm, Fleet Durability & Overload Guard, LTX Compaction & LayerFS D9)
      const cellRpcCli = await runAnstsCtlRemote(["mgmt-runtime-engine", "scheduler_cell_rpc_and_alarm"], baseUrl);
      assert.equal(cellRpcCli.exitCode, 0);
      const cellRpcData = JSON.parse(cellRpcCli.stdout) as {
        submittedTaskId: string;
        placements: Array<[string, string, number]>;
        rehydratedTasksCount: number;
      };
      assert.ok(cellRpcData.submittedTaskId.startsWith("cell-rpc-"));
      assert.ok(cellRpcData.placements.length >= 1);
      assert.ok(cellRpcData.rehydratedTasksCount >= 1);

      const fleetCli = await runAnstsCtlRemote(["mgmt-runtime-engine", "fleet_durability_and_overload_guard"], baseUrl);
      assert.equal(fleetCli.exitCode, 0);
      const fleetData = JSON.parse(fleetCli.stdout) as {
        fastPeerCommit: { acknowledgedVia: string };
        hedgedPeerCommit: { acknowledgedVia: string; hedgedCopyTriggered: boolean };
        singleNodeFallback: { effectiveDurabilityMode: string; acknowledgedVia: string };
        overloadGuardProbe: { admitted: boolean; statusCode: number };
        queueGroupCommit: { admitted: boolean; committedTransactions: number };
      };
      assert.equal(fleetData.fastPeerCommit.acknowledgedVia, "PEER_FSYNC");
      assert.equal(fleetData.hedgedPeerCommit.acknowledgedVia, "HEDGED_PEER_COPY");
      assert.equal(fleetData.hedgedPeerCommit.hedgedCopyTriggered, true);
      assert.equal(fleetData.singleNodeFallback.effectiveDurabilityMode, "bucket_fallback");
      assert.equal(fleetData.overloadGuardProbe.admitted, false);
      assert.equal(fleetData.overloadGuardProbe.statusCode, 503);
      assert.equal(fleetData.queueGroupCommit.admitted, true);
      assert.equal(fleetData.queueGroupCommit.committedTransactions, 2);

      const ltxCli = await runAnstsCtlRemote(["mgmt-runtime-engine", "ltx_compaction_and_layerfs_d9_sync", "task-ns-8841"], baseUrl);
      assert.equal(ltxCli.exitCode, 0);
      const ltxData = JSON.parse(ltxCli.stdout) as {
        l1Compaction: { compactionTriggered: boolean; batchesMerged: number };
        walTruncation: { shouldTruncateWalAtCheckpoint: boolean; usePagedRestore: boolean };
        uffdRestore: { eagerPagesLoaded: number; lazyDeferredPages: number };
        layerfsSync: { remoteGeneration: number };
      };
      assert.equal(ltxData.l1Compaction.compactionTriggered, true);
      assert.equal(ltxData.l1Compaction.batchesMerged, 2);
      assert.equal(ltxData.walTruncation.shouldTruncateWalAtCheckpoint, true);
      assert.equal(ltxData.walTruncation.usePagedRestore, true);
      assert.ok(ltxData.uffdRestore.eagerPagesLoaded >= 1 && ltxData.uffdRestore.lazyDeferredPages >= 1);
      assert.equal(ltxData.layerfsSync.remoteGeneration, 1);

      // 4. [M50-4] Closed-Loop SRE Observability, W3C Trace Waterfall, SLO Burn-Rate & Automated Self-Healing Runbook
      const obsCli = await runAnstsCtlRemote(["mgmt-ops", "probe_slo_traces_and_metrics"], baseUrl);
      assert.equal(obsCli.exitCode, 0);
      const obsData = JSON.parse(obsCli.stdout) as {
        traceWaterfall: { traceparent: string; spans: Array<{ name: string }> };
        latencyQuantiles: { count: number; p50: number; p95: number; p99: number };
        sloEvaluation: { burnRate1h: number; errorBudgetRemainingPct: number; alertSeverity: string };
        prometheusMetrics: string;
      };
      assert.ok(obsData.traceWaterfall.traceparent.startsWith("00-"));
      assert.equal(obsData.traceWaterfall.spans.length, 5);
      assert.ok(obsData.latencyQuantiles.count >= 9);
      assert.ok(obsData.latencyQuantiles.p99 >= obsData.latencyQuantiles.p50);
      assert.ok(obsData.sloEvaluation.burnRate1h > 0);
      assert.ok(obsData.prometheusMetrics.includes("ansts_task_dispatch_latency_ms"));

      const rbCli = await runAnstsCtlRemote(["mgmt-ops", "execute_auto_remediation_runbook", "RB-SLO-BURN-AND-DLQ-HEAL"], baseUrl);
      assert.equal(rbCli.exitCode, 0);
      const rbData = JSON.parse(rbCli.stdout) as {
        runbookId: string;
        circuitBreakersReset: string[];
        cloudEvent: { type: string };
        webhookOutbox: { deliveredCount: number };
      };
      assert.equal(rbData.runbookId, "RB-SLO-BURN-AND-DLQ-HEAL");
      assert.deepEqual(rbData.circuitBreakersReset, ["ep-deepseek-r1", "ep-qwen-38"]);
      assert.equal(rbData.cloudEvent.type, "io.ansts.sre.runbook.executed");
      assert.ok(rbData.webhookOutbox.deliveredCount >= 1);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
