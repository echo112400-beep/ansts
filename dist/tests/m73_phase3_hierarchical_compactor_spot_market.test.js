"use strict";











var _nodeTest = require("node:test");
var _strict = _interopRequireDefault(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var _compactor = require("../src/checkpoint/compactor.js");
var _runner = require("../src/harness/runner.js");
var _cognitive_router = require("../src/cognitive_router.js");
var _saga_gateway = require("../src/saga_gateway.js");
var _compensator = require("../src/saga/compensator.js");
var _capacity_sla_enforcer = require("../src/cluster/capacity_sla_enforcer.js");
var _scheduler = require("../src/scheduler.js");
var _lease_coordinator = require("../src/lease_coordinator.js");
var _wal_engine = require("../src/checkpoint/wal_engine.js");
var _env = require("../src/cloudflare/env.js");
var _models = require("../src/models.js");
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}function _interopRequireDefault(obj) {return obj && obj.__esModule ? obj : { default: obj };} /**
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
 */(0, _nodeTest.describe)("M73 Phase 3 Hierarchical Working-Memory Compactor & Spot Quota Lending Market Suite", () => {(0, _nodeTest.it)("[M73-1 / F7] WalCompactorAndPagedRestorer & AgentHarnessRunner compact long-horizon ACT outputs into LayerFS URI pointers and page-in line ranges on demand", () => {const sql = new _env.InMemoryCellSqlStorage();const compactor = new _compactor.WalCompactorAndPagedRestorer(4096, sql);const verboseLog = Array.from({ length: 50 }, (_, i) => `LINE_${i + 1}: shard_latency=${100 + i}ms`).join("\n");const priorTurns = [{ turnIndex: 1, toolName: "cluster_log_scan",
      thoughtSummary: "Scanned 50 lines of shard logs",
      rawToolOutput: verboseLog,
      keyFindings: ["peak latency 149ms on line 50"]
    },
    {
      turnIndex: 2,
      toolName: "db_pool_dump",
      thoughtSummary: "Dumped pool connections",
      rawToolOutput: verboseLog,
      keyFindings: ["32 active connections"]
    },
    {
      turnIndex: 3,
      toolName: "quick_status",
      thoughtSummary: "Checked health flag",
      rawToolOutput: "HEALTHY=1"
    }];


    const router = new _cognitive_router.CognitiveRouter();
    router.registerEndpoint(
    new _models.LlmEndpointMetrics({
      endpoint_id: "ep-f7",
      provider: "anthropic",
      model_name: "claude-3-7-sonnet",
      ttft_ewma_ms: 120
    }));

    const saga = new _saga_gateway.SagaToolGateway(new _compensator.SagaCompensatorEngine());
    saga.registerTool("apply_fix", _models.ToolCallEffectClass.READ_ONLY, () => ({ applied: true }));
    const runner = new _runner.AgentHarnessRunner(router, saga);

    const task = new _models.TaskSpec({
      task_id: "t-f7-1",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P1_HIGH,
      max_turns: 15
    });
    task.current_turn = 3;

    const res = runner.executeTurnWithHierarchicalMemory({
      task,
      toolName: "apply_fix",
      toolArgs: { ok: true },
      priorTurns,
      compactor,
      hotWindowTurns: 1,
      maxInlineCharsPerOutput: 120
    });

    _strict.default.equal(res.compactionReport.offloadedPointersCount, 2);
    _strict.default.equal(res.compactionReport.tokenReductionRatioPct >= 65, true);
    _strict.default.equal(res.turnSummary.turnIndex, 4);

    // Cold-start SQLite rehydration and on-demand line-slice pagination
    const compactor2 = new _compactor.WalCompactorAndPagedRestorer(4096, sql);
    const page = compactor2.dereferenceContextUriPage({
      uri: res.compactionReport.offloadedUris[0],
      startLine: 10,
      endLine: 12
    });
    _strict.default.equal(page.totalLines, 50);
    _strict.default.equal(page.slicedText.includes("LINE_10:"), true);
    _strict.default.equal(page.slicedText.includes("LINE_12:"), true);
    _strict.default.equal(page.slicedText.includes("LINE_13:"), false);
  });

  (0, _nodeTest.it)("[M73-2 / F8] CapacitySlaEnforcer & CoreScheduler5D clear inter-tenant Spot quota bids and execute sub-second WAL freeze & recall on urgent arrival", () => {
    const sql = new _env.InMemoryCellSqlStorage();
    const sla = new _capacity_sla_enforcer.CapacitySlaEnforcer(undefined, sql);
    const sched = new _scheduler.CoreScheduler5D(new _lease_coordinator.LeaseCoordinator(15));
    const wal = new _wal_engine.TurnWalEngine();
    const nowSec = Date.now() / 1000;

    sched.registerOrUpdateNode(
    new _models.NodeHeartbeat({
      node_id: "node-f8-1",
      total_cpu_cores: 16,
      free_cpu_cores: 16,
      total_memory_mb: 32768,
      free_memory_mb: 32768,
      warm_pool_ready: 2,
      timestamp: nowSec
    }));


    sla.publishIdleQuotaOffer({
      offerId: "offer-hl-1",
      lenderTenantId: "Helios",
      offeredSlots: 6,
      minBidPricePerSlotUsd: 0.05
    });

    const spotTask = new _models.TaskSpec({
      task_id: "t-spot-ns-1",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P3_BATCH
    });
    sched.submitTask(spotTask);
    sched.scheduleTick(nowSec);
    _strict.default.equal(spotTask.state, _models.TaskState.LEASED);

    const matched = sla.matchSpotQuotaBid({
      leaseId: "lease-spot-1",
      borrowerTenantId: "Northstar",
      taskId: spotTask.task_id,
      requestedSlots: 4,
      maxBidPricePerSlotUsd: 0.09
    });
    _strict.default.equal(matched.matched, true);
    _strict.default.equal(matched.lease?.clearingPricePerSlotUsd, 0.07);

    // Urgent arrival from Helios recalls lent quota, freezes Spot task in WAL, and re-queues it
    const recalled = sla.recallLenderQuotaOnUrgentArrival({
      lenderTenantId: "Helios",
      urgentTaskId: "t-hl-p0",
      requiredSlots: 4,
      walEngine: wal,
      scheduler: sched
    });

    _strict.default.equal(recalled.reclaimedSlots, 4);
    _strict.default.equal(spotTask.state, _models.TaskState.QUEUED);
    _strict.default.equal(spotTask.preempted_count, 1);
    _strict.default.equal(sched.nodes.get("node-f8-1")?.free_cpu_cores, 16);

    const sla2 = new _capacity_sla_enforcer.CapacitySlaEnforcer(undefined, sql);
    _strict.default.equal(sla2.listSpotLeases()[0]?.status, "RECALLED_BY_LENDER");
  });

  (0, _nodeTest.it)("[M73-3] Live BFF endpoint POST /v1beta1/management/ops/phase3-memory-spot-market, Console UI & CLI mgmt-phase3-market", async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m73-"));
    const server = new _server_main.AnstsLocalAndComposeServer({
      apiPort: 0,
      dashboardPort: 0,
      dataDir: tmpDir
    });
    const { apiPort } = await server.start();
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      const cliRes = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-phase3-market", "run_all_phase3_suite"], baseUrl);
      _strict.default.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);




      _strict.default.equal(body.allM73Phase3ChecksPassed, true);
      _strict.default.equal(body.f7HierarchicalCompactor.f7Verified, true);
      _strict.default.equal(body.f8SpotQuotaMarket.f8Verified, true);

      const jsResp = await fetch(`${baseUrl}/console-management/prototype.js`);
      _strict.default.equal(jsResp.status, 200);
      const jsText = await jsResp.text();
      _strict.default.equal(jsText.includes("open-context-compactor"), true);
      _strict.default.equal(jsText.includes("open-spot-quota-market"), true);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});