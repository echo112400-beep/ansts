/**
 * @fileoverview M73: Phase 3 Hierarchical Working-Memory Compactor & Spot Quota Lending Market Suite
 * Verifies:
 * - [M73-1 / F7] Adaptive Context Compactor & Hierarchical Working Memory
 *   (`WalCompactorAndPagedRestorer.compactWorkingMemoryContext`, `dereferenceContextUriPage`,
 *    SQLite persistence of `persisted_context_pointers`, and `AgentHarnessRunner.executeTurnWithHierarchicalMemory`).
 * - [M73-2 / F8] Inter-Tenant Spot Quota Lending & Sub-Second WAL Preemption Market
 *   (`CapacitySlaEnforcer.publishIdleQuotaOffer`, `matchSpotQuotaBid`, `recallLenderQuotaOnUrgentArrival`,
 *    `CoreScheduler5D.freezeAndRequeueSpotTask`, and SQLite persistence of `persisted_spot_quota_market`).
 * - [M73-3] Live Console BFF (`POST /v1beta1/management/ops/phase3-memory-spot-market`),
 *   Console UI bindings, and CLI (`anstsctl mgmt-phase3-market`).
 */
import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { WalCompactorAndPagedRestorer } from "../src/checkpoint/compactor.js";
import { AgentHarnessRunner } from "../src/harness/runner.js";
import { CognitiveRouter } from "../src/cognitive_router.js";
import { SagaToolGateway } from "../src/saga_gateway.js";
import { SagaCompensatorEngine } from "../src/saga/compensator.js";
import { CapacitySlaEnforcer } from "../src/cluster/capacity_sla_enforcer.js";
import { CoreScheduler5D } from "../src/scheduler.js";
import { LeaseCoordinator } from "../src/lease_coordinator.js";
import { TurnWalEngine } from "../src/checkpoint/wal_engine.js";
import { InMemoryCellSqlStorage } from "../src/cloudflare/env.js";
import { LlmEndpointMetrics, NodeHeartbeat, TaskPriority, TaskSpec, TaskState, ToolCallEffectClass } from "../src/models.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M73 Phase 3 Hierarchical Working-Memory Compactor & Spot Quota Lending Market Suite", () => {
  it("[M73-1 / F7] WalCompactorAndPagedRestorer & AgentHarnessRunner compact long-horizon ACT outputs into LayerFS URI pointers and page-in line ranges on demand", () => {
    const sql = new InMemoryCellSqlStorage();
    const compactor = new WalCompactorAndPagedRestorer(4096, sql);

    const verboseLog = Array.from({ length: 50 }, (_, i) => `LINE_${i + 1}: shard_latency=${100 + i}ms`).join("\n");
    const priorTurns = [
      {
        turnIndex: 1,
        toolName: "cluster_log_scan",
        thoughtSummary: "Scanned 50 lines of shard logs",
        rawToolOutput: verboseLog,
        keyFindings: ["peak latency 149ms on line 50"],
      },
      {
        turnIndex: 2,
        toolName: "db_pool_dump",
        thoughtSummary: "Dumped pool connections",
        rawToolOutput: verboseLog,
        keyFindings: ["32 active connections"],
      },
      {
        turnIndex: 3,
        toolName: "quick_status",
        thoughtSummary: "Checked health flag",
        rawToolOutput: "HEALTHY=1",
      },
    ];

    const router = new CognitiveRouter();
    router.registerEndpoint(
      new LlmEndpointMetrics({
        endpoint_id: "ep-f7",
        provider: "anthropic",
        model_name: "claude-3-7-sonnet",
        ttft_ewma_ms: 120,
      })
    );
    const saga = new SagaToolGateway(new SagaCompensatorEngine());
    saga.registerTool("apply_fix", ToolCallEffectClass.READ_ONLY, () => ({ applied: true }));
    const runner = new AgentHarnessRunner(router, saga);

    const task = new TaskSpec({
      task_id: "t-f7-1",
      tenant_id: "Northstar",
      priority: TaskPriority.P1_HIGH,
      max_turns: 15,
    });
    task.current_turn = 3;

    const res = runner.executeTurnWithHierarchicalMemory({
      task,
      toolName: "apply_fix",
      toolArgs: { ok: true },
      priorTurns,
      compactor,
      hotWindowTurns: 1,
      maxInlineCharsPerOutput: 120,
    });

    assert.equal(res.compactionReport.offloadedPointersCount, 2);
    assert.equal(res.compactionReport.tokenReductionRatioPct >= 65, true);
    assert.equal(res.turnSummary.turnIndex, 4);

    // Cold-start SQLite rehydration and on-demand line-slice pagination
    const compactor2 = new WalCompactorAndPagedRestorer(4096, sql);
    const page = compactor2.dereferenceContextUriPage({
      uri: res.compactionReport.offloadedUris[0],
      startLine: 10,
      endLine: 12,
    });
    assert.equal(page.totalLines, 50);
    assert.equal(page.slicedText.includes("LINE_10:"), true);
    assert.equal(page.slicedText.includes("LINE_12:"), true);
    assert.equal(page.slicedText.includes("LINE_13:"), false);
  });

  it("[M73-2 / F8] CapacitySlaEnforcer & CoreScheduler5D clear inter-tenant Spot quota bids and execute sub-second WAL freeze & recall on urgent arrival", () => {
    const sql = new InMemoryCellSqlStorage();
    const sla = new CapacitySlaEnforcer(undefined, sql);
    const sched = new CoreScheduler5D(new LeaseCoordinator(15));
    const wal = new TurnWalEngine();
    const nowSec = Date.now() / 1000;

    sched.registerOrUpdateNode(
      new NodeHeartbeat({
        node_id: "node-f8-1",
        total_cpu_cores: 16,
        free_cpu_cores: 16,
        total_memory_mb: 32768,
        free_memory_mb: 32768,
        warm_pool_ready: 2,
        timestamp: nowSec,
      })
    );

    sla.publishIdleQuotaOffer({
      offerId: "offer-hl-1",
      lenderTenantId: "Helios",
      offeredSlots: 6,
      minBidPricePerSlotUsd: 0.05,
    });

    const spotTask = new TaskSpec({
      task_id: "t-spot-ns-1",
      tenant_id: "Northstar",
      priority: TaskPriority.P3_BATCH,
    });
    sched.submitTask(spotTask);
    sched.scheduleTick(nowSec);
    assert.equal(spotTask.state, TaskState.LEASED);

    const matched = sla.matchSpotQuotaBid({
      leaseId: "lease-spot-1",
      borrowerTenantId: "Northstar",
      taskId: spotTask.task_id,
      requestedSlots: 4,
      maxBidPricePerSlotUsd: 0.09,
    });
    assert.equal(matched.matched, true);
    assert.equal(matched.lease?.clearingPricePerSlotUsd, 0.07);

    // Urgent arrival from Helios recalls lent quota, freezes Spot task in WAL, and re-queues it
    const recalled = sla.recallLenderQuotaOnUrgentArrival({
      lenderTenantId: "Helios",
      urgentTaskId: "t-hl-p0",
      requiredSlots: 4,
      walEngine: wal,
      scheduler: sched,
    });

    assert.equal(recalled.reclaimedSlots, 4);
    assert.equal(spotTask.state, TaskState.QUEUED);
    assert.equal(spotTask.preempted_count, 1);
    assert.equal(sched.nodes.get("node-f8-1")?.free_cpu_cores, 16);

    const sla2 = new CapacitySlaEnforcer(undefined, sql);
    assert.equal(sla2.listSpotLeases()[0]?.status, "RECALLED_BY_LENDER");
  });

  it("[M73-3] Live BFF endpoint POST /v1beta1/management/ops/phase3-memory-spot-market, Console UI & CLI mgmt-phase3-market", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m73-"));
    const server = new AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      dataDir: tmpDir,
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const cliRes = await runAnstsCtlRemote(["mgmt-phase3-market", "run_all_phase3_suite"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout) as {
        allM73Phase3ChecksPassed: boolean;
        f7HierarchicalCompactor: { f7Verified: boolean };
        f8SpotQuotaMarket: { f8Verified: boolean };
      };
      assert.equal(body.allM73Phase3ChecksPassed, true);
      assert.equal(body.f7HierarchicalCompactor.f7Verified, true);
      assert.equal(body.f8SpotQuotaMarket.f8Verified, true);

      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      assert.equal(jsResp.status, 200);
      const jsText = await jsResp.text();
      assert.equal(jsText.includes("open-context-compactor"), true);
      assert.equal(jsText.includes("open-spot-quota-market"), true);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
