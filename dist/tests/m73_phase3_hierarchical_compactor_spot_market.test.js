"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
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
const node_test_1 = require("node:test");
const strict_1 = __importDefault(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const compactor_js_1 = require("../src/checkpoint/compactor.js");
const runner_js_1 = require("../src/harness/runner.js");
const cognitive_router_js_1 = require("../src/cognitive_router.js");
const saga_gateway_js_1 = require("../src/saga_gateway.js");
const compensator_js_1 = require("../src/saga/compensator.js");
const capacity_sla_enforcer_js_1 = require("../src/cluster/capacity_sla_enforcer.js");
const scheduler_js_1 = require("../src/scheduler.js");
const lease_coordinator_js_1 = require("../src/lease_coordinator.js");
const wal_engine_js_1 = require("../src/checkpoint/wal_engine.js");
const env_js_1 = require("../src/cloudflare/env.js");
const models_js_1 = require("../src/models.js");
const server_main_js_1 = require("../src/server_main.js");
const anstsctl_js_1 = require("../src/cli/anstsctl.js");
(0, node_test_1.describe)("M73 Phase 3 Hierarchical Working-Memory Compactor & Spot Quota Lending Market Suite", () => {
    (0, node_test_1.it)("[M73-1 / F7] WalCompactorAndPagedRestorer & AgentHarnessRunner compact long-horizon ACT outputs into LayerFS URI pointers and page-in line ranges on demand", () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const compactor = new compactor_js_1.WalCompactorAndPagedRestorer(4096, sql);
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
        const router = new cognitive_router_js_1.CognitiveRouter();
        router.registerEndpoint(new models_js_1.LlmEndpointMetrics({
            endpoint_id: "ep-f7",
            provider: "anthropic",
            model_name: "claude-3-7-sonnet",
            ttft_ewma_ms: 120,
        }));
        const saga = new saga_gateway_js_1.SagaToolGateway(new compensator_js_1.SagaCompensatorEngine());
        saga.registerTool("apply_fix", models_js_1.ToolCallEffectClass.READ_ONLY, () => ({ applied: true }));
        const runner = new runner_js_1.AgentHarnessRunner(router, saga);
        const task = new models_js_1.TaskSpec({
            task_id: "t-f7-1",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P1_HIGH,
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
        strict_1.default.equal(res.compactionReport.offloadedPointersCount, 2);
        strict_1.default.equal(res.compactionReport.tokenReductionRatioPct >= 65, true);
        strict_1.default.equal(res.turnSummary.turnIndex, 4);
        // Cold-start SQLite rehydration and on-demand line-slice pagination
        const compactor2 = new compactor_js_1.WalCompactorAndPagedRestorer(4096, sql);
        const page = compactor2.dereferenceContextUriPage({
            uri: res.compactionReport.offloadedUris[0],
            startLine: 10,
            endLine: 12,
        });
        strict_1.default.equal(page.totalLines, 50);
        strict_1.default.equal(page.slicedText.includes("LINE_10:"), true);
        strict_1.default.equal(page.slicedText.includes("LINE_12:"), true);
        strict_1.default.equal(page.slicedText.includes("LINE_13:"), false);
    });
    (0, node_test_1.it)("[M73-2 / F8] CapacitySlaEnforcer & CoreScheduler5D clear inter-tenant Spot quota bids and execute sub-second WAL freeze & recall on urgent arrival", () => {
        const sql = new env_js_1.InMemoryCellSqlStorage();
        const sla = new capacity_sla_enforcer_js_1.CapacitySlaEnforcer(undefined, sql);
        const sched = new scheduler_js_1.CoreScheduler5D(new lease_coordinator_js_1.LeaseCoordinator(15));
        const wal = new wal_engine_js_1.TurnWalEngine();
        const nowSec = Date.now() / 1000;
        sched.registerOrUpdateNode(new models_js_1.NodeHeartbeat({
            node_id: "node-f8-1",
            total_cpu_cores: 16,
            free_cpu_cores: 16,
            total_memory_mb: 32768,
            free_memory_mb: 32768,
            warm_pool_ready: 2,
            timestamp: nowSec,
        }));
        sla.publishIdleQuotaOffer({
            offerId: "offer-hl-1",
            lenderTenantId: "Helios",
            offeredSlots: 6,
            minBidPricePerSlotUsd: 0.05,
        });
        const spotTask = new models_js_1.TaskSpec({
            task_id: "t-spot-ns-1",
            tenant_id: "Northstar",
            priority: models_js_1.TaskPriority.P3_BATCH,
        });
        sched.submitTask(spotTask);
        sched.scheduleTick(nowSec);
        strict_1.default.equal(spotTask.state, models_js_1.TaskState.LEASED);
        const matched = sla.matchSpotQuotaBid({
            leaseId: "lease-spot-1",
            borrowerTenantId: "Northstar",
            taskId: spotTask.task_id,
            requestedSlots: 4,
            maxBidPricePerSlotUsd: 0.09,
        });
        strict_1.default.equal(matched.matched, true);
        strict_1.default.equal(matched.lease?.clearingPricePerSlotUsd, 0.07);
        // Urgent arrival from Helios recalls lent quota, freezes Spot task in WAL, and re-queues it
        const recalled = sla.recallLenderQuotaOnUrgentArrival({
            lenderTenantId: "Helios",
            urgentTaskId: "t-hl-p0",
            requiredSlots: 4,
            walEngine: wal,
            scheduler: sched,
        });
        strict_1.default.equal(recalled.reclaimedSlots, 4);
        strict_1.default.equal(spotTask.state, models_js_1.TaskState.QUEUED);
        strict_1.default.equal(spotTask.preempted_count, 1);
        strict_1.default.equal(sched.nodes.get("node-f8-1")?.free_cpu_cores, 16);
        const sla2 = new capacity_sla_enforcer_js_1.CapacitySlaEnforcer(undefined, sql);
        strict_1.default.equal(sla2.listSpotLeases()[0]?.status, "RECALLED_BY_LENDER");
    });
    (0, node_test_1.it)("[M73-3] Live BFF endpoint POST /v1beta1/management/ops/phase3-memory-spot-market, Console UI & CLI mgmt-phase3-market", async () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m73-"));
        const server = new server_main_js_1.AnstsLocalAndComposeServer({
            apiPort: 0,
            dashboardPort: 0,
            dataDir: tmpDir,
        });
        const { apiPort } = await server.start();
        const baseUrl = `http://127.0.0.1:${apiPort}`;
        try {
            const cliRes = await (0, anstsctl_js_1.runAnstsCtlRemote)(["mgmt-phase3-market", "run_all_phase3_suite"], baseUrl);
            strict_1.default.equal(cliRes.exitCode, 0);
            const body = JSON.parse(cliRes.stdout);
            strict_1.default.equal(body.allM73Phase3ChecksPassed, true);
            strict_1.default.equal(body.f7HierarchicalCompactor.f7Verified, true);
            strict_1.default.equal(body.f8SpotQuotaMarket.f8Verified, true);
            const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
            strict_1.default.equal(jsResp.status, 200);
            const jsText = await jsResp.text();
            strict_1.default.equal(jsText.includes("open-context-compactor"), true);
            strict_1.default.equal(jsText.includes("open-spot-quota-market"), true);
        }
        finally {
            await server.stop();
            fs.rmSync(tmpDir, { recursive: true, force: true });
        }
    });
});
