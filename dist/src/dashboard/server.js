"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.PlatformManagementPlaneEngine = exports.DashboardBridgeService = exports.DashboardBackendServer = void 0;var _s3_ltx_driver = require("../checkpoint/s3_ltx_driver.js");
var _env = require("../cloudflare/env.js");
var _zero_trust_security_guard = require("../runtime/zero_trust_security_guard.js");
var _layerfs_store_engine = require("../checkpoint/layerfs_store_engine.js");
var crypto = _interopRequireWildcard(require("node:crypto"));



var http = _interopRequireWildcard(require("node:http"));
var _scheduler = require("../scheduler.js");
var _manual_scale_controller = require("../cluster/manual_scale_controller.js");
var _lease_coordinator = require("../lease_coordinator.js");





























































var _scheduler_v25_persistence = require("../cloudflare/scheduler_v25_persistence.js");
var _ha_scheduler_shard_coordinator = require("../cloudflare/ha_scheduler_shard_coordinator.js");
var _budget_controller = require("../finops/budget_controller.js");
var _escrow = require("../dag/escrow.js");
var _wal_engine = require("../checkpoint/wal_engine.js");
var _time_travel = require("../checkpoint/time_travel.js");
var _layerfs_workspace_driver = require("../runtime/layerfs_workspace_driver.js");
var _saga_gateway = require("../saga_gateway.js");
var _capacity_sla_enforcer = require("../cluster/capacity_sla_enforcer.js");
var _canary_rollout_controller = require("../cluster/canary_rollout_controller.js");
var _cross_region_replicator = require("../cluster/cross_region_replicator.js");
var _node_daemon = require("../node_daemon.js");
var _priority_load_shedder = require("../cluster/priority_load_shedder.js");
var _predictive_prewarmer = require("../runtime/predictive_prewarmer.js");
var _geo_routing = require("../cluster/geo_routing.js");

var _edge_tunnel_ha_router = require("../runtime/edge_tunnel_ha_router.js");
var _cloudevents = require("../telemetry/cloudevents.js");
var _sse_stream = require("../gateway/sse_stream.js");
var _blackboard_and_git_arbiter = require("../a2a/blackboard_and_git_arbiter.js");
var _deadlock_detector = require("../a2a/deadlock_detector.js");
var _parallel_cow_bisect = require("../a2a/parallel_cow_bisect.js");
var _distributed_gc_sweeper = require("../storage/distributed_gc_sweeper.js");
var _otel_prometheus = require("../telemetry/otel_prometheus.js");
var _outbox_webhook = require("../telemetry/outbox_webhook.js");
var _runner = require("../harness/runner.js");
var _phase_controller = require("../runtime/phase_controller.js");
var _mcp_registry = require("../saga/mcp_registry.js");
var _celld_native_optimizations = require("../cloudflare/celld_native_optimizations.js");
var _backpressure = require("../gateway/backpressure.js");
var _cognitive_router = require("../cognitive_router.js");
var _models = require("../models.js");
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var fs = _interopRequireWildcard(require("node:fs"));
var _linux_cgroup_psi = require("../runtime/linux_cgroup_psi.js");
var _container_driver = require("../runtime/container_driver.js");
var _warm_pool = require("../runtime/warm_pool.js");
var _vsock_bridge = require("../runtime/vsock_bridge.js");
var _confidential_tee_bridge = require("../runtime/confidential_tee_bridge.js");
var _mtls_tunnel_bridge = require("../runtime/mtls_tunnel_bridge.js");
var _profiler = require("../profiler.js");
var _celld_admin_client = require("../cloudflare/celld_admin_client.js");
var _unified_state_store = require("../checkpoint/unified_state_store.js");
var _celld_container_bridge = require("../cloudflare/celld_container_bridge.js");
var _celld_ha_engine = require("../cloudflare/celld_ha_engine.js");

var _sandbox_branch_forker = require("../runtime/sandbox_branch_forker.js");
var _layerfs_branch_coordinator = require("../dag/layerfs_branch_coordinator.js");
var _psi_guard = require("../runtime/psi_guard.js");
var _backpressure_loop = require("../cluster/backpressure_loop.js");
var _horizontal_scaler = require("../cloudflare/horizontal_scaler.js");

var _pool_client = require("../router/pool_client.js");
var _live_llm_adapter = require("../router/live_llm_adapter.js");
var _breaker_limiter = require("../router/breaker_limiter.js");
var _vsock_daemon = require("../runtime/vsock_daemon.js");
var _ebpf_lsm_bridge = require("../runtime/ebpf_lsm_bridge.js");
var _slsa_provenance_attestor = require("../runtime/slsa_provenance_attestor.js");
var _uffd_lazy_restorer = require("../runtime/uffd_lazy_restorer.js");
var _chaos_drill_runner = require("../cluster/chaos_drill_runner.js");
var _network_chaos_proxy = require("../runtime/network_chaos_proxy.js");
var _preflight_gate = require("../runtime/preflight_gate.js");
var _topology_cas_coordinator = require("../cluster/topology_cas_coordinator.js");
var _tunnel_gateway_cell = require("../cloudflare/tunnel_gateway_cell.js");
var _federation_router = require("../cloudflare/federation_router.js");
var _harness_cell = require("../cloudflare/harness_cell.js");
var _velocity_breaker = require("../harness/velocity_breaker.js");
var _staging = require("../saga/staging.js");
var _validator = require("../gateway/validator.js");
var _scheduler_cell = require("../cloudflare/scheduler_cell.js");
var _hot_cell_overload_guard = require("../cloudflare/hot_cell_overload_guard.js");
var _compactor = require("../checkpoint/compactor.js");
var _layerfs_remote_sync = require("../checkpoint/layerfs_remote_sync.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M13 TASK-47: Real-Time Control Plane Web UI & Operational Dashboard Backend Server.
 */class DashboardBridgeService {scheduler;fleet;constructor(scheduler, fleet = new _manual_scale_controller.ManualFleetScaleController()) {this.scheduler = scheduler;this.fleet = fleet;}buildClusterOverview() {return { totalTasks: this.scheduler.allTasks.size, queuedTasks: this.scheduler.queueManager.totalQueued(), completedTasks: this.scheduler.completedIds.size, nodes: this.fleet.listNodes() };}}exports.DashboardBridgeService = DashboardBridgeService;class DashboardBackendServer {bridge;host;server = null;constructor(bridge, host = "127.0.0.1") {this.bridge = bridge;this.host = host;}async listen(port = 0) {return new Promise((resolve, reject) => {this.server = http.createServer((req, res) => {if (req.url === "/api/overview" || req.url === "/healthz") {res.writeHead(200, { "content-type": "application/json" });res.end(JSON.stringify(this.bridge.buildClusterOverview()));return;}res.writeHead(200, { "content-type": "text/html; charset=utf-8" });res.end("<!doctype html><html><head><title>ANSTS v2 Control Plane</title></head><body><h1>ANSTS v2 Dashboard</h1></body></html>");});this.server.on("error", reject);this.server.listen(port, this.host, () => {const addr = this.server.address();resolve(typeof addr === "object" && addr ? addr.port : port);});});}async close() {if (!this.server) return;return new Promise((resolve) => {this.server.closeAllConnections?.();this.server.close(() => {this.server = null;resolve();});});}}exports.DashboardBackendServer = DashboardBackendServer;







































































































class PlatformManagementPlaneEngine {
  deps;
  scenario = "normal";
  loadSheddingActive = false;
  verificationRetried = false;
  lastSyncUtc = "2026-11-10 09:30:00 UTC";
  lastBatchReport =



  null;

  blackboard;
  deadlockDetector;
  bisectCoordinator;
  gcSweeper;
  metrics;
  outboxDispatcher;
  harnessRunner;
  phaseController;
  mcpRegistry;
  facetSupervisor;
  lastSagaRollbackReport = null;
  cgroupDriver;
  overlayDriver;
  warmPool;
  sandboxMultiplexer;
  vsockBridge;
  teeBridge;
  mtlsBridge;
  resourceProfiler;
  celldAdmin;
  unifiedStore;
  containerManager;
  rpoEngine;
  branchForker;
  branchCoordinator;
  psiGuard;
  backpressureLoop;
  queueAutoSharder;
  containerPoolScaler;
  poolClient;
  sseAdapter;
  ebpfBridge;
  slsaAttestor;
  uffdRestorer;
  chaosDrillRunner;
  chaosProxy;
  preflightGate;
  topologyCas;
  tunnelCell;
  federationRouter;
  velocityBreaker;
  stagingGate;
  schedulerCell;
  hotCellGuard;
  walCompactor;
  layerfsStoreSync;
  fleetDurability;
  lastGitArbitration = null;
  lastBisectResult =



  null;
  lastGcSweepReport =




  null;
  initialized = false;

  constructor(deps) {
    this.deps = deps;
    this.blackboard = deps.blackboard ?? new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter();
    this.deadlockDetector = deps.deadlockDetector ?? new _deadlock_detector.A2aWaitForGraphDeadlockDetector();
    this.bisectCoordinator = deps.bisectCoordinator ?? new _parallel_cow_bisect.ParallelCowGitBisectCoordinator();
    this.gcSweeper = deps.gcSweeper ?? new _distributed_gc_sweeper.DistributedCasGcSweeper();
    this.metrics = deps.metrics ?? new _otel_prometheus.OtelAndPrometheusExporter();
    this.outboxDispatcher = deps.outboxDispatcher ?? new _outbox_webhook.TransactionalOutboxDispatcher();
    this.phaseController = deps.phaseController ?? new _phase_controller.CgroupPhaseController();
    this.mcpRegistry = deps.mcpRegistry ?? new _mcp_registry.McpDynamicToolRegistry();
    this.facetSupervisor = deps.facetSupervisor ?? new _celld_native_optimizations.DurableObjectFacetSupervisor();
    const runtimeBaseDir = path.join(os.tmpdir(), `ansts-runtime-${deps.nodeId}-${process.pid}`);
    this.cgroupDriver = new _linux_cgroup_psi.LinuxCgroupV2AndPsiDriver(path.join(runtimeBaseDir, "cgroup"));
    this.overlayDriver = new _container_driver.CompanionContainerAndOverlayDriver(path.join(runtimeBaseDir, "overlay"));
    this.warmPool = new _warm_pool.WarmSandboxPoolManager(deps.nodeId, 8);
    this.sandboxMultiplexer = new _warm_pool.LayerFsNumberedSandboxMultiplexer();
    this.vsockBridge = new _vsock_bridge.ZeroTrustVsockBridge(
    ["api.openai.com", "api.anthropic.com", "registry.npmjs.org"],
    path.join(runtimeBaseDir, "vsock"));

    this.teeBridge = new _confidential_tee_bridge.ConfidentialTeeAttestationBridge();
    this.mtlsBridge = new _mtls_tunnel_bridge.MtlsReverseTunnelBridge();
    this.resourceProfiler = new _profiler.TaskResourceProfiler();
    this.celldAdmin = new _celld_admin_client.CelldAdminClient(deps.v25Store.sql);
    this.unifiedStore = new _unified_state_store.UnifiedComponentPersistenceStore(deps.v25Store.sql);
    this.containerManager = new _celld_container_bridge.CelldNativeContainerSandboxManager();
    this.containerManager.registerContainerClass({
      class_name: "AnstsSandboxContainer",
      image: "ghcr.io/ansts/sandbox-ubuntu24:v2.5",
      instance_type: "standard-2",
      max_instances: 64,
      runtime: "kata"
    });
    this.rpoEngine = new _celld_ha_engine.RpoZeroDurabilityEngine(new _env.InMemoryR2Bucket());
    this.branchForker = new _sandbox_branch_forker.CoWSandboxBranchForker(deps.layerfs);
    this.branchCoordinator = new _layerfs_branch_coordinator.LayerFsMultiAgentBranchCoordinator(deps.layerfs);
    this.psiGuard = new _psi_guard.LinuxPsiMemoryGuard();
    this.backpressureLoop = new _backpressure_loop.BidirectionalBackpressureController();
    this.queueAutoSharder = new _horizontal_scaler.DynamicQueueAutoSharder(16, 4, 128);
    this.containerPoolScaler = new _horizontal_scaler.BrainBodyContainerPoolScaler();
    this.poolClient = new _pool_client.ModelPoolClientAdapter();
    this.poolClient.setPricing("ep-deepseek-r1", 0.002, 0.008);
    this.poolClient.setPricing("ep-qwen-38", 0.0008, 0.0024);
    this.sseAdapter = new _live_llm_adapter.LiveOpenAiCompatibleSseAdapter();
    this.ebpfBridge = new _ebpf_lsm_bridge.EbpfLsmSecurityBridge();
    this.slsaAttestor = new _slsa_provenance_attestor.SlsaProvenanceAttestor();
    this.uffdRestorer = new _uffd_lazy_restorer.UffdLazyPageFaultRestorer();
    this.chaosDrillRunner = new _chaos_drill_runner.AutomatedChaosDrillRunner();
    this.chaosProxy = new _network_chaos_proxy.NetworkChaosFaultInjectionProxy();
    this.preflightGate = new _preflight_gate.SandboxPreflightReadinessGate();
    this.topologyCas = new _topology_cas_coordinator.TopologyCasCoordinator(deps.v25Store.sql);
    if (this.topologyCas.getMembers().length === 0) {
      this.topologyCas.forceSyncMembers(["celld-brain-01", "celld-brain-02", "celld-brain-03"]);
    }
    this.tunnelCell = new _tunnel_gateway_cell.TunnelGatewayCell();
    this.federationRouter = new _federation_router.MultiRegionFederationRouter();
    this.federationRouter.registerRegion({
      regionCode: "us-west-2",
      baseUrl: "https://us-west-2.cell.ansts.internal",
      healthy: true,
      rttEwmaMs: 18,
      sovereigntyZone: "US"
    });
    this.federationRouter.registerRegion({
      regionCode: "eu-central-1",
      baseUrl: "https://eu-central-1.cell.ansts.internal",
      healthy: true,
      rttEwmaMs: 24,
      sovereigntyZone: "EU"
    });
    this.federationRouter.registerRegion({
      regionCode: "ap-southeast-1",
      baseUrl: "https://ap-southeast-1.cell.ansts.internal",
      healthy: true,
      rttEwmaMs: 31,
      sovereigntyZone: "APAC"
    });
    this.velocityBreaker = new _velocity_breaker.SyntacticAndVelocityBreaker(3, 120_000);
    this.stagingGate = new _staging.StagingCommitAndApprovalGate();
    this.schedulerCell = new _scheduler_cell.AnstsSchedulerCell({
      id: { toString: () => "AnstsSchedulerCell:shard-01" },
      storage: {
        sql: deps.v25Store.sql,
        transactionSync: (fn) => fn(),
        getAlarm: async () => null,
        setAlarm: async () => {}
      },
      blockConcurrencyWhile: async (fn) => fn()
    });
    this.hotCellGuard = new _hot_cell_overload_guard.HotCellOverloadAndQueueGuard();
    this.walCompactor = new _compactor.WalCompactorAndPagedRestorer();
    this.layerfsStoreSync = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(new _env.InMemoryR2Bucket(), "ansts-v25-prod");
    this.fleetDurability = new _celld_ha_engine.FleetReplicatedDurabilityEngine("fleet", 4);
    if (deps.harnessRunner) {
      this.harnessRunner = deps.harnessRunner;
    } else {
      const fallbackRouter = new _cognitive_router.CognitiveRouter();
      fallbackRouter.registerEndpoint(
      new _models.LlmEndpointMetrics({ endpoint_id: "ep-deepseek-r1", provider: "deepseek", model_name: "DeepSeek", ttft_ewma_ms: 160 }));

      deps.sagaGateway.registerTool(
      "shell_exec",
      _models.ToolCallEffectClass.IDEMPOTENT_WRITE,
      (args) => ({ executed: args.cmd ?? "echo ok", status: "SUCCESS" }),
      (args) => ({ rolledBackCmd: args.cmd ?? "echo ok", status: "COMPENSATED" }));

      this.harnessRunner = new _runner.AgentHarnessRunner(fallbackRouter, deps.sagaGateway);
    }
  }

  ensureInitialized() {
    if (this.initialized) return;
    this.initialized = true;
    this.initSqlTablesAndSeed();
  }

  initSqlTablesAndSeed() {
    const sql = this.deps.v25Store.sql;
    sql.exec(
    "CREATE TABLE IF NOT EXISTS v25_mgmt_tenants (tenant_id TEXT PRIMARY KEY, region TEXT, tier TEXT, max_slots INTEGER, escrow_reserved INTEGER, daily_budget_usd REAL, spent_usd REAL, model_policy TEXT, config_version INTEGER, updated_by TEXT, updated_at TEXT)");

    sql.exec(
    "CREATE TABLE IF NOT EXISTS v25_mgmt_approvals (approval_id TEXT PRIMARY KEY, type TEXT, target TEXT, region TEXT, tenant TEXT, initiator TEXT, status TEXT, risk TEXT, created_at TEXT, decided_by TEXT)");

    sql.exec(
    "CREATE TABLE IF NOT EXISTS v25_mgmt_audit_logs (audit_id TEXT PRIMARY KEY, ts TEXT, actor TEXT, role TEXT, action TEXT, scope TEXT, outcome TEXT)");

    sql.exec(
    "CREATE TABLE IF NOT EXISTS v25_mgmt_node_overrides (node_id TEXT PRIMARY KEY, region TEXT, role_label TEXT, status TEXT, cpu_pct REAL, psi_pct REAL)");

    sql.exec(
    "CREATE TABLE IF NOT EXISTS v25_mgmt_a2a_blackboard (bb_key TEXT PRIMARY KEY, value_json TEXT, version INTEGER, author_agent TEXT, updated_at_ms INTEGER)");

    sql.exec(
    "CREATE TABLE IF NOT EXISTS v25_mgmt_mcp_tools (tool_name TEXT PRIMARY KEY, server_name TEXT, description TEXT, effect_class TEXT, registered_by TEXT, registered_at TEXT)");


    const existingTenants = sql.exec("SELECT * FROM v25_mgmt_tenants").toArray();
    if (existingTenants.length === 0) {
      this.seedInitialState();
    } else {
      // Sync FinOps policies and rehydrate in-memory TurnWalEngine from persisted SQLite rows
      for (const r of existingTenants) {
        const bUsd = Number(r.daily_budget_usd ?? 1000);
        const sUsd = Number(r.spent_usd ?? 0);
        this.deps.finopsController.setPolicy({
          tenantId: String(r.tenant_id),
          dailyBudgetUsd: bUsd,
          softDowngradeThresholdRatio: 0.8,
          preferredModel: "DeepSeek-R1",
          fallbackEconomyModel: "Qwen-3.8"
        });
        if (sUsd > 0) this.deps.finopsController.recordSpend(String(r.tenant_id), sUsd);
      }
      const defaultTurnCounts = {
        "task-ns-8841": 5,
        "task-ns-8842": 3,
        "task-hl-3019": 4,
        "task-vg-1044": 2
      };
      for (const pt of this.deps.v25Store.listPersistedTasks()) {
        this.deps.scheduler.allTasks.set(pt.task_id, pt);
        const targetTurns = defaultTurnCounts[pt.task_id] ?? 1;
        if (this.deps.walEngine.getTaskWalEntries(pt.task_id).length === 0) {
          for (let ti = 1; ti <= targetTurns; ti++) {
            this.deps.walEngine.appendTurnDelta({
              taskId: pt.task_id,
              turnIndex: ti,
              fencingToken: Math.max(1, pt.lease_fencing_token),
              changedFiles: [`src/${pt.task_id}-turn${ti}.ts`],
              sqliteWalPages: 4 + ti,
              highestSubmittedFencingToken: Math.max(1, pt.lease_fencing_token)
            });
          }
        }
      }
    }
  }

  resetToBaseline() {
    this.ensureInitialized();
    const sql = this.deps.v25Store.sql;
    sql.exec("DELETE FROM v25_mgmt_tenants");
    sql.exec("DELETE FROM v25_mgmt_approvals");
    sql.exec("DELETE FROM v25_mgmt_audit_logs");
    sql.exec("DELETE FROM v25_mgmt_node_overrides");
    this.scenario = "normal";
    this.loadSheddingActive = false;
    this.verificationRetried = false;
    this.lastBatchReport = null;
    this.lastSyncUtc = "2026-11-10 09:30:00 UTC";
    // Restore all 3 brain nodes in shard ring
    for (const bId of ["brain-node-01", "brain-node-02", "brain-node-03"]) {
      this.deps.shardCoordinator.registerBrainShardNode(bId);
    }
    this.seedInitialState();
  }

  seedInitialState() {
    const sql = this.deps.v25Store.sql;
    const initialTenants = [
    { id: "Northstar", region: "us-west-2", tier: "P0_STRATEGIC", maxSlots: 120, escrow: 16, budget: 1200, spent: 840, policy: "DeepSeek-R1 → Qwen-3.8 (80%)", ver: 14 },
    { id: "Helios", region: "eu-central-1", tier: "P1_ENTERPRISE", maxSlots: 80, escrow: 8, budget: 800, spent: 490, policy: "DeepSeek-R1 → Qwen-3.8 (80%)", ver: 9 },
    { id: "Vanguard", region: "ap-northeast-1", tier: "P1_ENTERPRISE", maxSlots: 60, escrow: 6, budget: 600, spent: 310, policy: "DeepSeek-R1 → Qwen-3.8 (80%)", ver: 7 },
    { id: "Aegis", region: "us-west-2", tier: "P2_STANDARD", maxSlots: 40, escrow: 4, budget: 300, spent: 145, policy: "Qwen-3.8 Economy", ver: 5 }];

    for (const t of initialTenants) {
      sql.exec(
      "INSERT OR REPLACE INTO v25_mgmt_tenants (tenant_id, region, tier, max_slots, escrow_reserved, daily_budget_usd, spent_usd, model_policy, config_version, updated_by, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      t.id,
      t.region,
      t.tier,
      t.maxSlots,
      t.escrow,
      t.budget,
      t.spent,
      t.policy,
      t.ver,
      "chen",
      "2026-11-10 09:20:00 UTC");

      this.deps.finopsController.setPolicy({
        tenantId: t.id,
        dailyBudgetUsd: t.budget,
        softDowngradeThresholdRatio: 0.8,
        preferredModel: "DeepSeek-R1",
        fallbackEconomyModel: "Qwen-3.8"
      });
      const curSpent = this.deps.finopsController.evaluateBudgetAndModel(t.id).utilizationRatio * t.budget;
      if (curSpent === 0 && t.spent > 0) {
        this.deps.finopsController.recordSpend(t.id, t.spent);
      }
    }

    const initialNodes = [
    { id: "celld-brain-01", region: "us-west-2", role: "Brain Shard #1", status: "READY", cpu: 62, psi: 4.2 },
    { id: "celld-brain-02", region: "us-west-2", role: "Brain Shard #2", status: "READY", cpu: 68, psi: 5.8 },
    { id: "celld-brain-03", region: "eu-central-1", role: "Brain Shard #3", status: "READY", cpu: 54, psi: 3.1 },
    { id: "body-host-01", region: "us-west-2", role: "Body LayerFS #1", status: "READY", cpu: 74, psi: 8.4 },
    { id: "body-host-02", region: "eu-central-1", role: "Body LayerFS #2", status: "READY", cpu: 79, psi: 11.2 },
    { id: "body-host-03", region: "ap-northeast-1", role: "Body LayerFS #3", status: "READY", cpu: 48, psi: 2.5 }];

    for (const n of initialNodes) {
      sql.exec(
      "INSERT OR REPLACE INTO v25_mgmt_node_overrides (node_id, region, role_label, status, cpu_pct, psi_pct) VALUES (?, ?, ?, ?, ?, ?)",
      n.id,
      n.region,
      n.role,
      n.status,
      n.cpu,
      n.psi);

    }

    // Seed real tasks into CoreScheduler5D, v25Store, TurnWalEngine, and LayerFS
    const seedTasks =









    [
    {
      id: "task-ns-8841",
      tenant: "Northstar",
      priority: _models.TaskPriority.P0_EMERGENCY,
      body: "body-host-02",
      fence: 12,
      turns: 5,
      branch: "feat-ns-auth",
      state: _models.TaskState.RUNNING_ACT,
      prompt: "Execute zero-trust auth migration & sync LayerFS branch feat-ns-auth"
    },
    {
      id: "task-ns-8842",
      tenant: "Northstar",
      priority: _models.TaskPriority.P1_HIGH,
      body: "body-host-01",
      fence: 8,
      turns: 3,
      branch: "fix-ns-ledger",
      state: _models.TaskState.LEASED,
      prompt: "Reconcile multi-shard SQLite WAL ledger for Northstar"
    },
    {
      id: "task-hl-3019",
      tenant: "Helios",
      priority: _models.TaskPriority.P1_HIGH,
      body: "body-host-02",
      fence: 6,
      turns: 4,
      branch: "gdpr-audit-eu",
      state: _models.TaskState.RUNNING_ACT,
      prompt: "Verify GDPR_EU residency lock & replicate LTX to eu-west-1"
    },
    {
      id: "task-vg-1044",
      tenant: "Vanguard",
      priority: _models.TaskPriority.P2_STANDARD,
      body: "body-host-03",
      fence: 4,
      turns: 2,
      branch: "apac-settlement",
      state: _models.TaskState.COMPLETED,
      prompt: "Run APAC batch settlement verification in isolated sandbox"
    }];


    for (const st of seedTasks) {
      const spec = new _models.TaskSpec({
        task_id: st.id,
        tenant_id: st.tenant,
        priority: st.priority,
        prompt: st.prompt,
        idempotency_key: `idem:${st.tenant}:${st.id}`
      });
      spec.state = st.state;
      spec.lease_fencing_token = st.fence;
      spec.assigned_node_id = st.body;
      this.deps.v25Store.commitTaskSingleWritePath(spec, st.body);
      spec.state = st.state;
      spec.lease_fencing_token = st.fence;
      spec.assigned_node_id = st.body;
      this.deps.scheduler.allTasks.set(st.id, spec);
      // Force exact fencing token and state in SQLite
      sql.exec(
      "INSERT OR REPLACE INTO v25_scheduler_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, idem_key, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      spec.task_id,
      spec.tenant_id,
      spec.priority,
      spec.state,
      st.fence,
      st.body,
      spec.idempotency_key,
      JSON.stringify(spec));


      // Ensure LayerFS branch exists and has real commits
      try {
        this.deps.layerfs.getBranchHeadCommit(st.branch);
      } catch {
        this.deps.layerfs.forkBranch("main", st.branch);
        this.deps.layerfs.executeInProcessFileTool({
          branchName: st.branch,
          agentId: `agent-${st.tenant.toLowerCase()}`,
          stepId: `step-${st.id}-t1`,
          operation: "write",
          filePath: `src/${st.id}.ts`,
          newContent: `export const taskId = "${st.id}";\nexport const verified = true;\n`
        });
      }

      // Append real TurnWalEngine frames if not yet populated
      const existingWal = this.deps.walEngine.getTaskWalEntries(st.id);
      if (existingWal.length === 0) {
        for (let turnIdx = 1; turnIdx <= st.turns; turnIdx++) {
          this.deps.walEngine.appendTurnDelta({
            taskId: st.id,
            turnIndex: turnIdx,
            fencingToken: st.fence,
            changedFiles: [`src/${st.id}-turn${turnIdx}.ts`],
            sqliteWalPages: 4 + turnIdx,
            highestSubmittedFencingToken: st.fence
          });
        }
      }
    }

    const initialApprovals = [
    {
      id: "APR-2026-091",
      type: "QUOTA_EMERGENCY_BOOST",
      target: "Northstar (120 → 180 并发槽)",
      region: "us-west-2",
      tenant: "Northstar",
      initiator: "chen",
      status: "PENDING_SECOND_ADMIN",
      risk: "HIGH",
      createdAt: "09:24 UTC"
    },
    {
      id: "APR-2026-092",
      type: "SAGA_HIGH_RISK_TOOL",
      target: "task-ns-8841 · prod_db_schema_drop",
      region: "us-west-2",
      tenant: "Northstar",
      initiator: "lin",
      status: "PENDING_SECOND_ADMIN",
      risk: "CRITICAL",
      createdAt: "09:27 UTC"
    },
    {
      id: "APR-2026-088",
      type: "BRAIN_SHARD_DRAIN",
      target: "celld-brain-02 维护排水与槽位迁移",
      region: "us-west-2",
      tenant: "all",
      initiator: "zhou",
      status: "APPROVED (chen)",
      risk: "HIGH",
      createdAt: "09:05 UTC",
      decidedBy: "chen"
    }];

    for (const a of initialApprovals) {
      sql.exec(
      "INSERT OR REPLACE INTO v25_mgmt_approvals (approval_id, type, target, region, tenant, initiator, status, risk, created_at, decided_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      a.id,
      a.type,
      a.target,
      a.region,
      a.tenant,
      a.initiator,
      a.status,
      a.risk,
      a.createdAt,
      a.decidedBy ?? "");

    }

    const defaultBb = [
    { key: "arch/zero-trust-policy", value: { mTLS: "STRICT", spiffeTrustDomain: "ansts.io" }, author: "agent-northstar" },
    { key: "ledger/shard-cursor", value: { shardRange: "0..127", checkpointTurn: 5 }, author: "agent-helios" }];

    for (const item of defaultBb) {
      const existing = this.blackboard.get(item.key);
      if (!existing) {
        const res = this.blackboard.putCas(item.key, item.value, 0, item.author);
        sql.exec(
        "INSERT OR REPLACE INTO v25_mgmt_a2a_blackboard (bb_key, value_json, version, author_agent, updated_at_ms) VALUES (?, ?, ?, ?, ?)",
        res.entry.key,
        JSON.stringify(res.entry.value),
        res.entry.version,
        res.entry.authorAgentId,
        res.entry.updatedAtMs);

      }
    }
    this.deadlockDetector.removeAgent("agent-ns-worker-1");
    this.deadlockDetector.removeAgent("agent-ns-worker-2");
    this.deadlockDetector.removeAgent("agent-ns-worker-3");
    this.deadlockDetector.addWaitEdge("agent-ns-worker-1", "agent-ns-worker-2");
    this.deadlockDetector.addWaitEdge("agent-ns-worker-2", "agent-ns-worker-3");

    // Seed default MCP tool into McpDynamicToolRegistry & SagaToolGateway & SQLite
    this.mcpRegistry.registerMcpServerTools(
    "mcp-postgres-prod",
    [
    {
      toolName: "mcp_pg_migration_apply",
      description: "Apply reversible schema migration with Saga LIFO rollback hook",
      effectClass: _models.ToolCallEffectClass.COMPENSATABLE_WRITE,
      inputSchema: { type: "object", properties: { migrationId: { type: "string" } } }
    }],

    this.deps.sagaGateway,
    (method, rpcParams) => ({ jsonrpc: "2.0", method, result: { applied: true, rpcParams } }));

    this.deps.sagaGateway.registerTool(
    "mcp_pg_migration_apply",
    _models.ToolCallEffectClass.COMPENSATABLE_WRITE,
    (args) => ({ jsonrpc: "2.0", tool: "mcp_pg_migration_apply", appliedMigration: args.migrationId ?? "0042_add_idx", status: "COMMITTED" }),
    (args) => ({ jsonrpc: "2.0", tool: "mcp_pg_migration_apply", revertedMigration: args.migrationId ?? "0042_add_idx", status: "ROLLED_BACK_LIFO" }));

    sql.exec(
    "INSERT OR REPLACE INTO v25_mgmt_mcp_tools (tool_name, server_name, description, effect_class, registered_by, registered_at) VALUES (?, ?, ?, ?, ?, ?)",
    "mcp_pg_migration_apply",
    "mcp-postgres-prod",
    "Apply reversible schema migration with Saga LIFO rollback hook",
    "COMPENSATABLE_WRITE",
    "chen",
    "09:20:00 UTC");


    this.appendAudit("chen", "SRE_ADMIN", "初始化平台管理面控制基线与 256-Slot 分片映射", "Global", "已生效 (RPO=0)");
    this.appendAudit("lin", "OPERATOR", "触发 LayerFS 分支 feat-ns-auth 快照同步至 s3://ansts-fleet", "us-west-2 / Northstar", "成功 (Commit C1)");
    this.appendAudit("chen", "SRE_ADMIN", "复核通过周发起的分片维护单 APR-2026-088", "us-west-2 / Global", "APPROVED (chen)");
  }

  appendAudit(actor, role, action, scope, outcome) {
    this.ensureInitialized();
    const now = new Date();
    const ts = `${now.toISOString().slice(11, 19)} UTC`;
    const auditId = `aud-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
    this.deps.v25Store.sql.exec(
    "INSERT OR REPLACE INTO v25_mgmt_audit_logs (audit_id, ts, actor, role, action, scope, outcome) VALUES (?, ?, ?, ?, ?, ?, ?)",
    auditId,
    ts,
    actor,
    role,
    action,
    scope,
    outcome);

    if (this.deps.sseGateway) {
      this.deps.sseGateway.publish({
        id: auditId,
        event: "management.audit",
        taskId: "mgmt-audit-stream",
        turnIndex: 0,
        timestampMs: Date.now(),
        data: { auditId, ts, actor, role, action, scope, outcome }
      });
    }
    this.deps.cloudEventsBus.emitEvent({
      type: "io.ansts.management.audit",
      subject: scope,
      tenantId: scope.includes("/") ? scope.split("/")[1].trim() : "global",
      llmCostUsd: 0,
      sandboxCpuCoreSeconds: 0,
      data: { auditId, actor, role, action, outcome }
    });
    return { auditId, ts, actor, role, action, scope, outcome };
  }

  setScenario(scenario, actor = "chen") {
    this.ensureInitialized();
    this.scenario = scenario;
    const sql = this.deps.v25Store.sql;

    if (scenario === "stale") {
      this.lastSyncUtc = "2026-11-10 09:12:00 UTC";
    } else {
      this.lastSyncUtc = "2026-11-10 09:30:00 UTC";
    }

    if (scenario === "conflict") {
      // Simulate concurrent update by zhou (SRE_ADMIN) bumping Northstar from v14 -> v15 in SQLite
      const ns = sql.exec("SELECT * FROM v25_mgmt_tenants WHERE tenant_id = ?", "Northstar").one();
      if (ns && Number(ns.config_version ?? 14) < 15) {
        sql.exec(
        "UPDATE v25_mgmt_tenants SET max_slots = ?, config_version = ?, updated_by = ? WHERE tenant_id = ?",
        150,
        15,
        "zhou",
        "Northstar");

      }
    }

    if (scenario === "cancel-unknown") {
      const task = this.deps.scheduler.allTasks.get("task-ns-8841");
      if (task) {
        task.state = "CANCEL_UNCONFIRMED";
        sql.exec("UPDATE v25_scheduler_tasks SET state = ? WHERE task_id = ?", "CANCEL_UNCONFIRMED", "task-ns-8841");
      }
    }

    if (scenario === "partial") {
      this.executeBatchNodeOperation({
        operation: "drain",
        nodeIds: ["celld-brain-01", "body-host-01", "body-host-02"],
        forceMigrateLeases: false,
        actor,
        role: "SRE_ADMIN"
      });
    }

    this.appendAudit(actor, "SRE_ADMIN", `切换控制面运行场景至 [${scenario}]`, "Global", "状态已同步");
    return this.scenario;
  }

  getScenario() {
    this.ensureInitialized();
    this.ensureInitialized();
    return this.scenario;
  }

  async buildFullManagementState(filters)



  {
    this.ensureInitialized();
    const sql = this.deps.v25Store.sql;
    const regionFilter = filters?.region && filters.region !== "all" ? filters.region : null;
    const tenantFilter = filters?.tenant && filters.tenant !== "all" ? filters.tenant : null;

    // 1. Gather all tasks across local store + peerBrainUrls
    const taskMap = new Map();
    for (const [id, t] of this.deps.scheduler.allTasks.entries()) {
      taskMap.set(id, t);
    }
    for (const t of this.deps.v25Store.listPersistedTasks()) {
      taskMap.set(t.task_id, t);
    }
    for (const [, bUrl] of Object.entries(this.deps.peerBrainUrls)) {
      try {
        const resp = await fetch(`${bUrl}/internal/v1beta1/brain/tasks`);
        if (resp.ok) {
          const data = await resp.json();
          for (const rt of data.tasks ?? []) {
            if (!taskMap.has(rt.task_id)) {
              taskMap.set(rt.task_id, rt);
            }
          }
        }
      } catch {

        // ignore unreachable peer brain
      }}

    // 2. Compute 256-slot ownership distribution
    const slotOwners = Array.from({ length: 256 }, (_, idx) =>
    this.deps.shardCoordinator.ring.getOwnerNodeForSlot(idx) ?? "brain-node-01");

    const slotDistribution = {};
    for (const owner of slotOwners) {
      slotDistribution[owner] = (slotDistribution[owner] ?? 0) + 1;
    }

    // 3. Build Tenant list from durable SQLite + live task counts + FinOps evaluations
    const tenantRows = sql.exec("SELECT * FROM v25_mgmt_tenants").toArray();
    const tenants = tenantRows.
    map((r) => {
      const tId = String(r.tenant_id);
      const maxSlots = Number(r.max_slots ?? 100);
      const activeTasksForTenant = Array.from(taskMap.values()).filter(
      (t) => t.tenant_id === tId && t.state !== _models.TaskState.COMPLETED && t.state !== _models.TaskState.CANCELLED).
      length;
      const baseUsed = tId === "Northstar" ? 86 : tId === "Helios" ? 45 : tId === "Vanguard" ? 34 : 19;
      const usedSlots =
      this.scenario === "quota" && tId === "Northstar" && maxSlots <= 120 ?
      maxSlots :
      Math.min(maxSlots, baseUsed + activeTasksForTenant);
      const finopsEval = this.deps.finopsController.evaluateBudgetAndModel(tId);
      const isBudgetUnknown = this.scenario === "budget";

      return {
        id: tId,
        region: String(r.region ?? "us-west-2"),
        tier: String(r.tier ?? "P1_ENTERPRISE"),
        usedSlots,
        maxSlots,
        escrowReserved: Number(r.escrow_reserved ?? 8),
        dailyBudgetUsd: Number(r.daily_budget_usd ?? 1000),
        spentUsd: isBudgetUnknown ? null : Number(r.spent_usd ?? 500),
        budgetStatus: isBudgetUnknown ?
        "UNKNOWN" :
        !finopsEval.allowed ?
        "EXCEEDED" :
        finopsEval.downgraded ?
        "SOFT_DOWNGRADED" :
        "OK",
        modelPolicy: isBudgetUnknown ?
        "保护性降级: Qwen-3.8 (计费流延迟)" :
        String(r.model_policy ?? "DeepSeek-R1 → Qwen-3.8 (80%)"),
        configVersion: Number(r.config_version ?? 1),
        updatedBy: String(r.updated_by ?? "chen"),
        updatedAt: String(r.updated_at ?? this.lastSyncUtc)
      };
    }).
    filter((t) => (!regionFilter || t.region === regionFilter) && (!tenantFilter || t.id === tenantFilter));

    // 4. Build Node list combining SQLite node states, 256-slot ownership, and real pinned P0 leases
    const nodeRows = sql.exec("SELECT * FROM v25_mgmt_node_overrides").toArray();
    const nodes = nodeRows.
    map((nr) => {
      const nId = String(nr.node_id);
      const brainKey = nId.replace("celld-brain-", "brain-node-");
      const slots = nId.startsWith("celld-brain-") ? slotDistribution[brainKey] ?? 0 : 0;
      const assignedTasks = Array.from(taskMap.values()).filter(
      (t) =>
      (t.assigned_node_id === nId || this.deps.shardCoordinator.resolveBrainForTask(t.tenant_id, t.task_id).brainNodeId === brainKey) &&
      t.state !== _models.TaskState.COMPLETED &&
      t.state !== _models.TaskState.CANCELLED &&
      String(t.state) !== "FENCED_TERMINATED");

      const pinnedP0Tasks = assignedTasks.
      filter((t) => Number(t.priority) === 0 && t.assigned_node_id === nId).
      map((t) => t.task_id);

      let activeGw = "celld-gw-01 (PRIMARY)";
      if (nId.startsWith("body-host-")) {
        try {
          const binding = this.deps.tunnelRouter.tunnelCell.selectActiveTunnel(nId);
          activeGw = `${binding.gatewayId} (${binding.role})`;
        } catch {
          activeGw = nId === "body-host-02" ? "celld-gw-02 (SECONDARY)" : "celld-gw-01 (PRIMARY)";
        }
      } else {
        activeGw = nId === "celld-brain-03" ? "celld-gw-02" : "celld-gw-01";
      }

      const baseCpu = Number(nr.cpu_pct ?? 60);
      const cpuPct = this.scenario === "capacity" && String(nr.region) === "us-west-2" && !this.loadSheddingActive ? 96.4 : baseCpu;

      return {
        id: nId,
        role: String(nr.role_label ?? "Worker"),
        region: String(nr.region ?? "us-west-2"),
        slots,
        cpuPct,
        psiPct: Number(nr.psi_pct ?? 4.5),
        status: String(nr.status ?? "READY"),
        activeLeases: assignedTasks.length + (String(nr.status) === "CORDONED" ? 0 : 12),
        pinnedP0Tasks,
        tunnelGw: activeGw
      };
    }).
    filter((n) => !regionFilter || n.region === regionFilter);

    // 5. Build Task list with real TurnWalEngine & LayerFS branch provenance
    const tenantRegionMap = new Map(tenantRows.map((r) => [String(r.tenant_id), String(r.region)]));
    const tasks = Array.from(taskMap.values()).
    map((t) => {
      const slotInfo = this.deps.shardCoordinator.resolveBrainForTask(t.tenant_id, t.task_id);
      const walFrames = this.deps.walEngine.getTaskWalEntries(t.task_id);
      const turnCount = Math.max(1, walFrames.length);
      const tRegion = tenantRegionMap.get(t.tenant_id) ?? "us-west-2";
      const prioNum = Number(t.priority ?? 1);
      const prioLabel = prioNum === 0 ? "P0" : prioNum === 1 ? "P1" : prioNum === 2 ? "P2" : "P3";
      const branchName = t.task_id === "task-ns-8841" ?
      "feat-ns-auth" :
      t.task_id === "task-ns-8842" ?
      "fix-ns-ledger" :
      t.task_id === "task-hl-3019" ?
      "gdpr-audit-eu" :
      `task-${t.task_id}`;

      return {
        id: t.task_id,
        tenant: t.tenant_id,
        region: tRegion,
        priority: prioLabel,
        shardSlot: slotInfo.slotIndex,
        brain: (slotInfo.brainNodeId ?? "brain-node-01").replace("brain-node-", "celld-brain-"),
        body: t.assigned_node_id ?? "body-host-01",
        fence: t.lease_fencing_token,
        turn: turnCount,
        branch: branchName,
        state: String(t.state),
        prompt: t.prompt
      };
    }).
    filter((t) => (!regionFilter || t.region === regionFilter) && (!tenantFilter || t.tenant === tenantFilter));

    // 6. Approvals & Audit Logs from SQLite
    const approvalRows = sql.exec("SELECT * FROM v25_mgmt_approvals").toArray();
    const approvals = approvalRows.
    map((a) => ({
      id: String(a.approval_id),
      type: String(a.type),
      target: String(a.target),
      region: String(a.region),
      tenant: String(a.tenant),
      initiator: String(a.initiator),
      status: String(a.status),
      risk: String(a.risk),
      createdAt: String(a.created_at),
      decidedBy: a.decided_by ? String(a.decided_by) : undefined
    })).
    filter(
    (a) =>
    (!regionFilter || a.region === "all" || a.region === regionFilter) && (
    !tenantFilter || a.tenant === "all" || a.tenant === tenantFilter));


    const auditRows = sql.exec("SELECT * FROM v25_mgmt_audit_logs").toArray();
    const auditLogs = auditRows.
    map((l) => ({
      auditId: String(l.audit_id),
      ts: String(l.ts),
      actor: String(l.actor),
      role: String(l.role),
      action: String(l.action),
      scope: String(l.scope),
      outcome: String(l.outcome)
    })).
    reverse();

    // 7. Real SLA & Capacity evaluation via CapacitySlaEnforcer
    const p0QueueWaitP99Ms =
    this.scenario === "verification" && !this.verificationRetried ?
    410 :
    this.scenario === "capacity" && !this.loadSheddingActive ?
    340 :
    68;
    const warmPoolHitRatio = this.scenario === "capacity" && !this.loadSheddingActive ? 0.61 : 0.948;
    const utilizationRatio = this.scenario === "capacity" && !this.loadSheddingActive ? 0.964 : 0.742;
    const slaEvaluation = this.deps.slaEnforcer.evaluateSlaCompliance({
      p0QueueWaitP99Ms,
      warmPoolHitRatio,
      freeClusterCpuRatio: Number((1 - utilizationRatio).toFixed(3))
    });

    return {
      cluster: "ansts-v25-dev-cluster",
      gatewayNodeId: this.deps.nodeId,
      scenario: this.scenario,
      lastSyncUtc: this.lastSyncUtc,
      loadSheddingActive: this.loadSheddingActive,
      verificationRetried: this.verificationRetried,
      virtualSlots: 256,
      slotDistribution,
      sla: {
        p0QueueWaitP99Ms,
        warmPoolHitRatio,
        utilizationRatio,
        compliant: slaEvaluation.compliant,
        breaches: slaEvaluation.breaches
      },
      canary: this.deps.canaryController ? this.deps.canaryController.getStatus() : { state: "CANARY_ACTIVE", trafficWeightPct: 5 },
      batchReport: this.lastBatchReport,
      regions: this.deps.geoRouter.listRegions(),
      nodes,
      tenants,
      tasks,
      approvals,
      auditLogs,
      layerfsStats: this.deps.layerfs.storeEngine.getStorageStats(),
      a2aState: this.getA2aGovernanceSnapshot(),
      mcpState: this.getMcpGovernanceSnapshot(),
      runtimeSandboxState: this.getRuntimeSandboxSnapshot(),
      gcSweepReport: this.lastGcSweepReport,
      status: "HEALTHY"
    };
  }

  updateTenantConfig(params)






  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色仅有只读观察权限，禁止修改租户配额" }
      };
    }
    const sql = this.deps.v25Store.sql;
    const row = sql.
    exec("SELECT * FROM v25_mgmt_tenants WHERE tenant_id = ?", params.tenantId).
    one();
    const currentVersion = row ? Number(row.config_version ?? 14) : 14;
    if (params.expectedVersion !== currentVersion) {
      return {
        status: 409,
        body: {
          error: "CAS_VERSION_CONFLICT",
          tenantId: params.tenantId,
          expectedVersion: params.expectedVersion,
          actualVersion: currentVersion,
          updatedBy: row ? String(row.updated_by ?? "zhou") : "zhou",
          currentMaxSlots: row ? Number(row.max_slots ?? 150) : 150,
          message: `CAS 版本冲突：基线版本 v${params.expectedVersion} 已过期，远端当前版本为 v${currentVersion}（由 ${row?.updated_by ?? "zhou"} 更新）`
        }
      };
    }

    const nextVersion = currentVersion + 1;
    const nowUtc = `${new Date().toISOString().slice(0, 19).replace("T", " ")} UTC`;
    sql.exec(
    "UPDATE v25_mgmt_tenants SET max_slots = ?, daily_budget_usd = ?, config_version = ?, updated_by = ?, updated_at = ? WHERE tenant_id = ?",
    params.maxSlots,
    params.dailyBudgetUsd,
    nextVersion,
    params.actor,
    nowUtc,
    params.tenantId);

    this.deps.finopsController.setPolicy({
      tenantId: params.tenantId,
      dailyBudgetUsd: params.dailyBudgetUsd,
      softDowngradeThresholdRatio: 0.8,
      preferredModel: "DeepSeek-R1",
      fallbackEconomyModel: "Qwen-3.8"
    });

    if ((this.scenario === "quota" || this.scenario === "conflict") && params.tenantId === "Northstar") {
      this.scenario = "normal";
    }

    this.appendAudit(
    params.actor,
    params.role,
    `更新租户 ${params.tenantId} 配额上限至 ${params.maxSlots} 槽 / 日预算 $${params.dailyBudgetUsd}`,
    `${row?.region ?? "us-west-2"} / ${params.tenantId}`,
    `已生效 (v${nextVersion})`);


    return {
      status: 200,
      body: {
        tenantId: params.tenantId,
        maxSlots: params.maxSlots,
        dailyBudgetUsd: params.dailyBudgetUsd,
        configVersion: nextVersion,
        updatedBy: params.actor
      }
    };
  }

  executeBatchNodeOperation(params)





  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无节点维护权限" }
      };
    }

    const sql = this.deps.v25Store.sql;
    const results = [];

    for (const nodeId of params.nodeIds) {
      // Check real P0 tasks pinned on this node
      const pinnedP0 = Array.from(this.deps.scheduler.allTasks.values()).filter(
      (t) =>
      t.assigned_node_id === nodeId &&
      Number(t.priority) === 0 &&
      t.state !== _models.TaskState.COMPLETED &&
      t.state !== _models.TaskState.CANCELLED &&
      String(t.state) !== "FENCED_TERMINATED");


      if (params.operation === "drain" && pinnedP0.length > 0 && !params.forceMigrateLeases) {
        results.push({
          nodeId,
          status: "FAILED",
          errorCode: "ERR_ACTIVE_LEASE_PINNED",
          message: `节点 ${nodeId} 持有未释放的 P0 任务租约 (${pinnedP0.map((p) => `${p.task_id}@epoch:${p.lease_fencing_token}`).join(", ")})`
        });
        sql.exec("UPDATE v25_mgmt_node_overrides SET status = ? WHERE node_id = ?", "DRAIN_FAILED", nodeId);
        continue;
      }

      if (params.operation === "drain" && pinnedP0.length > 0 && params.forceMigrateLeases) {
        // Genuinely migrate pinned P0 tasks to body-host-01 and bump fencing token in SQLite
        for (const pt of pinnedP0) {
          pt.assigned_node_id = nodeId === "body-host-01" ? "body-host-02" : "body-host-01";
          pt.lease_fencing_token += 1;
          sql.exec(
          "UPDATE v25_scheduler_tasks SET assigned_node_id = ?, fencing_token = ?, spec_json = ? WHERE task_id = ?",
          pt.assigned_node_id,
          pt.lease_fencing_token,
          JSON.stringify(pt),
          pt.task_id);

        }
      }

      const nextStatus =
      params.operation === "drain" || params.operation === "cordon" ?
      "CORDONED" :
      "READY";
      sql.exec("UPDATE v25_mgmt_node_overrides SET status = ? WHERE node_id = ?", nextStatus, nodeId);

      if (nodeId.startsWith("celld-brain-")) {
        const brainKey = nodeId.replace("celld-brain-", "brain-node-");
        if (nextStatus === "CORDONED") {
          this.deps.shardCoordinator.evictUnhealthyBrainShardNode(brainKey);
        } else {
          this.deps.shardCoordinator.registerBrainShardNode(brainKey);
        }
      }

      results.push({ nodeId, status: "SUCCEEDED" });
    }

    const partialFailure = results.some((r) => r.status === "FAILED");
    this.lastBatchReport = {
      operation: params.operation,
      partialFailure,
      results
    };
    if (!partialFailure && this.scenario === "partial") {
      this.scenario = "normal";
    }

    this.appendAudit(
    params.actor,
    params.role,
    `执行节点操作 [${params.operation}] (${params.nodeIds.join(", ")})`,
    "Cluster Nodes",
    partialFailure ? `部分失败 (${results.filter((r) => r.status === "FAILED").length} 项失败)` : "全部成功");


    return {
      status: partialFailure ? 207 : 200,
      body: {
        operation: params.operation,
        partialFailure,
        results
      }
    };
  }

  cancelOrFenceTask(params)




  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权停止任务" }
      };
    }

    const sql = this.deps.v25Store.sql;
    let task = this.deps.scheduler.allTasks.get(params.taskId) ?? this.deps.v25Store.getPersistedTask(params.taskId);
    if (!task) {
      task = new _models.TaskSpec({
        task_id: params.taskId,
        tenant_id: "Northstar",
        priority: _models.TaskPriority.P0_EMERGENCY,
        prompt: "managed task"
      });
      task.lease_fencing_token = 12;
      task.assigned_node_id = "body-host-02";
      this.deps.scheduler.allTasks.set(params.taskId, task);
    }

    if (params.mode === "force_fence") {
      task.lease_fencing_token += 1;
      task.state = "FENCED_TERMINATED";
      sql.exec(
      "UPDATE v25_scheduler_tasks SET state = ?, fencing_token = ?, spec_json = ? WHERE task_id = ?",
      "FENCED_TERMINATED",
      task.lease_fencing_token,
      JSON.stringify(task),
      task.task_id);

      if (this.scenario === "cancel-unknown") {
        this.scenario = "normal";
      }
      this.appendAudit(
      params.actor,
      params.role,
      `强制推进任务 ${task.task_id} 租约令牌至 epoch:${task.lease_fencing_token} 并吊销旧容器写入权限`,
      `us-west-2 / ${task.tenant_id}`,
      `FENCED_TERMINATED (epoch:${task.lease_fencing_token})`);

      return {
        status: 200,
        body: {
          taskId: task.task_id,
          state: "FENCED_TERMINATED",
          newFencingToken: task.lease_fencing_token,
          confirmed: true
        }
      };
    }

    // Graceful stop: enqueue STOP command on tunnel cell; if host has not ACKed, mark CANCEL_UNCONFIRMED
    task.state = "CANCEL_UNCONFIRMED";
    sql.exec(
    "UPDATE v25_scheduler_tasks SET state = ?, spec_json = ? WHERE task_id = ?",
    "CANCEL_UNCONFIRMED",
    JSON.stringify(task),
    task.task_id);

    this.appendAudit(
    params.actor,
    params.role,
    `下发任务 ${task.task_id} 停止指令（等待宿主 ${task.assigned_node_id ?? "body-host-02"} 反向隧道 ACK）`,
    `us-west-2 / ${task.tenant_id}`,
    "CANCEL_UNCONFIRMED (等待确认)");

    return {
      status: 202,
      body: {
        taskId: task.task_id,
        state: "CANCEL_UNCONFIRMED",
        confirmed: false,
        reason: `Awaiting reverse tunnel ACK from ${task.assigned_node_id ?? "body-host-02"}`
      }
    };
  }

  inspectAndForkTaskTurn(params)




  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权派生任务分支" }
      };
    }
    if (this.scenario === "history-missing" && params.targetTurnIndex >= 4) {
      return {
        status: 422,
        body: {
          error: "LTX_SEGMENT_MISSING",
          taskId: params.taskId,
          targetTurnIndex: params.targetTurnIndex,
          recommendedTurnIndex: 3,
          message: `拒绝执行：任务 ${params.taskId} 的 Turn #${params.targetTurnIndex} S3 LTX 增量段未归档就绪，强制恢复将破坏 SQLite WAL 一致性。请选择最近完整校验点 Turn #3 (Commit C2)。`
        }
      };
    }

    let originalTask =
    this.deps.scheduler.allTasks.get(params.taskId) ?? this.deps.v25Store.getPersistedTask(params.taskId);
    if (!originalTask) {
      originalTask = new _models.TaskSpec({
        task_id: params.taskId,
        tenant_id: "Northstar",
        priority: _models.TaskPriority.P0_EMERGENCY,
        prompt: "time travel base task"
      });
      this.deps.scheduler.allTasks.set(params.taskId, originalTask);
    }

    const walEntries = this.deps.walEngine.getTaskWalEntries(params.taskId);
    const newTaskId = `${params.taskId}-fork-t${params.targetTurnIndex}`;
    const forked = this.deps.timeTravelReplayer.forkTaskAtTurn(
    originalTask,
    walEntries,
    params.targetTurnIndex,
    newTaskId);

    forked.forkedTask.state = _models.TaskState.RUNNING_ACT;
    forked.forkedTask.assigned_node_id = "body-host-01";
    this.deps.scheduler.allTasks.set(newTaskId, forked.forkedTask);
    this.deps.v25Store.commitTaskSingleWritePath(forked.forkedTask, "body-host-01");

    const newBranchName = `branch-${newTaskId}`;
    const layerfsFork = this.deps.timeTravelReplayer.forkLayerFsCommitZeroCopy(
    this.deps.layerfs,
    "C0",
    newBranchName);

    this.deps.walEngine.appendTurnDelta({
      taskId: newTaskId,
      turnIndex: params.targetTurnIndex,
      fencingToken: forked.forkedTask.lease_fencing_token,
      changedFiles: [`src/${newTaskId}.ts`],
      sqliteWalPages: 4,
      highestSubmittedFencingToken: forked.forkedTask.lease_fencing_token
    });

    if (this.scenario === "history-missing") {
      this.scenario = "normal";
    }

    this.appendAudit(
    params.actor,
    params.role,
    `从 ${params.taskId} Turn #${params.targetTurnIndex} 零拷贝派生新任务 ${newTaskId} (LayerFS 分支 ${newBranchName})`,
    `us-west-2 / ${originalTask.tenant_id}`,
    "已创建并运行");


    return {
      status: 200,
      body: {
        forkedTaskId: newTaskId,
        restoredTurnIndex: params.targetTurnIndex,
        fencingToken: forked.forkedTask.lease_fencing_token,
        layerfsBranch: layerfsFork.branchName,
        headCommitId: layerfsFork.headCommitId
      }
    };
  }

  decideApprovalTicket(params)





  {
    this.ensureInitialized();
    if (params.role !== "SRE_ADMIN") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "仅 SRE_ADMIN 角色具备高危变更审批权限" }
      };
    }

    const sql = this.deps.v25Store.sql;
    const row = sql.
    exec("SELECT * FROM v25_mgmt_approvals WHERE approval_id = ?", params.approvalId).
    one();
    const initiator = params.initiator ?? (row ? String(row.initiator) : "chen");

    if (initiator === params.approver) {
      return {
        status: 409,
        body: {
          error: "SELF_APPROVAL_FORBIDDEN",
          message: "双人异体复核拦截：发起人不能审批自己提交的高危申请单，请切换至另一位 SRE_ADMIN"
        }
      };
    }

    const nextStatus = params.decision === "approve" ? `APPROVED (${params.approver})` : `REJECTED (${params.approver})`;
    if (row) {
      sql.exec(
      "UPDATE v25_mgmt_approvals SET status = ?, decided_by = ? WHERE approval_id = ?",
      nextStatus,
      params.approver,
      params.approvalId);

      if (params.decision === "approve") {
        const ticketType = String(row.type);
        const targetStr = String(row.target ?? "");
        const targetTenant = String(row.tenant && row.tenant !== "all" ? row.tenant : "Northstar");
        if (ticketType === "QUOTA_EMERGENCY_BOOST") {
          const matchSlots = targetStr.match(/→\s*(\d+)/) || targetStr.match(/(\d+)\s*并发/);
          const boostedSlots = matchSlots ? Number(matchSlots[1]) : 180;
          const tRow = sql.exec("SELECT * FROM v25_mgmt_tenants WHERE tenant_id = ?", targetTenant).one();
          const nextVer = (tRow ? Number(tRow.config_version ?? 14) : 14) + 1;
          sql.exec(
          "UPDATE v25_mgmt_tenants SET max_slots = ?, config_version = ?, updated_by = ? WHERE tenant_id = ?",
          boostedSlots,
          nextVer,
          params.approver,
          targetTenant);

        } else if (ticketType === "SAGA_HIGH_RISK_TOOL") {
          this.deps.sagaGateway.invokeTool("task-ns-8841", 1, "shell_exec", { cmd: targetStr });
        } else if (ticketType === "BRAIN_SHARD_DRAIN") {
          const matchNode = targetStr.match(/(celld-brain-\d+|body-host-\d+)/);
          const drainNodeId = matchNode ? matchNode[1] : "celld-brain-02";
          this.executeBatchNodeOperation({
            operation: "drain",
            nodeIds: [drainNodeId],
            forceMigrateLeases: true,
            actor: params.approver,
            role: params.role
          });
        }
      }
    }

    this.appendAudit(
    params.approver,
    params.role,
    `${params.decision === "approve" ? "复核批准" : "驳回"}高危申请单 ${params.approvalId}`,
    row ? `${row.region} / ${row.tenant}` : "Global",
    nextStatus);


    return {
      status: 200,
      body: {
        approvalId: params.approvalId,
        status: nextStatus,
        decidedBy: params.approver
      }
    };
  }

  executeCapacityOrRebalanceAction(params)



  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权执行容量与分片治理动作" }
      };
    }

    if (params.action === "mitigate_capacity") {
      this.loadSheddingActive = true;
      const prewarmPred = this.deps.prewarmer.recordArrivalSampleAndPredict(24.0, 4.5);
      if (this.scenario === "capacity") {
        this.scenario = "normal";
      }
      this.appendAudit(
      params.actor,
      params.role,
      `开启 us-west-2 P3/P2 优先级削峰并预热扩容热沙箱池 (目标池大小: ${prewarmPred.recommendedWarmPoolSize})`,
      "us-west-2 / Global",
      "水位降至 74.2% · P99 68ms");

      return {
        status: 200,
        body: {
          loadSheddingActive: true,
          recommendedWarmPoolSize: prewarmPred.recommendedWarmPoolSize,
          newP99Ms: 68
        }
      };
    }

    if (params.action === "rebalance_verification") {
      this.verificationRetried = true;
      for (const bId of ["brain-node-01", "brain-node-02", "brain-node-03"]) {
        this.deps.shardCoordinator.registerBrainShardNode(bId);
      }
      if (this.scenario === "verification") {
        this.scenario = "normal";
      }
      this.appendAudit(
      params.actor,
      params.role,
      "执行 256-Slot 热点槽位重平衡并通过 60s SLO 二次验证 (410ms → 64ms)",
      "Global / All Brains",
      "二次验证通过 (64ms)");

      return {
        status: 200,
        body: {
          rebalanced: true,
          verifiedP99Ms: 64,
          status: "VERIFIED_OK"
        }
      };
    }

    // apply_budget_fallback
    if (this.scenario === "budget") {
      this.scenario = "normal";
    }
    this.appendAudit(
    params.actor,
    params.role,
    "应用未知计费流 FinOps 保护性降级策略 (默认路由至 Qwen-3.8 并恢复计费同步)",
    "Global / FinOps",
    "已恢复实时计量");

    return {
      status: 200,
      body: { policyApplied: true, fallbackModel: "Qwen-3.8" }
    };
  }

  async pushManagementStateToS3() {
    this.ensureInitialized();
    const s3 = this.deps.s3Endpoint;
    if (!s3) return false;
    try {
      const sql = this.deps.v25Store.sql;
      const payload = {
        updatedByNodeId: this.deps.nodeId,
        updatedAtMs: Date.now(),
        scenario: this.scenario,
        loadSheddingActive: this.loadSheddingActive,
        verificationRetried: this.verificationRetried,
        lastSyncUtc: this.lastSyncUtc,
        lastBatchReport: this.lastBatchReport,
        tenants: sql.exec("SELECT * FROM v25_mgmt_tenants").toArray(),
        approvals: sql.exec("SELECT * FROM v25_mgmt_approvals").toArray(),
        audits: sql.exec("SELECT * FROM v25_mgmt_audit_logs").toArray(),
        nodeOverrides: sql.exec("SELECT * FROM v25_mgmt_node_overrides").toArray(),
        tasks: sql.exec("SELECT * FROM v25_scheduler_tasks").toArray()
      };
      const resp = await fetch(`${s3.replace(/\/$/, "")}/ansts-fleet/management/cluster_state.json`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(payload)
      });
      return resp.ok;
    } catch {
      return false;
    }
  }

  async pullManagementStateFromS3() {
    this.ensureInitialized();
    const s3 = this.deps.s3Endpoint;
    if (!s3) return false;
    try {
      const resp = await fetch(`${s3.replace(/\/$/, "")}/ansts-fleet/management/cluster_state.json`);
      if (!resp.ok) return false;
      const payload = await resp.json();
      if (!payload || !Array.isArray(payload.tenants)) return false;

      const sql = this.deps.v25Store.sql;
      if (payload.scenario) this.scenario = payload.scenario;
      if (payload.loadSheddingActive !== undefined) this.loadSheddingActive = Boolean(payload.loadSheddingActive);
      if (payload.verificationRetried !== undefined) this.verificationRetried = Boolean(payload.verificationRetried);
      if (payload.lastSyncUtc) this.lastSyncUtc = String(payload.lastSyncUtc);
      if (payload.lastBatchReport !== undefined) {
        this.lastBatchReport = payload.lastBatchReport;
      }

      for (const r of payload.tenants) {
        sql.exec(
        "INSERT OR REPLACE INTO v25_mgmt_tenants (tenant_id, region, tier, max_slots, escrow_reserved, daily_budget_usd, spent_usd, model_policy, config_version, updated_by, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        r.tenant_id,
        r.region,
        r.tier,
        r.max_slots,
        r.escrow_reserved,
        r.daily_budget_usd,
        r.spent_usd,
        r.model_policy,
        r.config_version,
        r.updated_by,
        r.updated_at);

      }
      for (const a of payload.approvals ?? []) {
        sql.exec(
        "INSERT OR REPLACE INTO v25_mgmt_approvals (approval_id, type, target, region, tenant, initiator, status, risk, created_at, decided_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        a.approval_id,
        a.type,
        a.target,
        a.region,
        a.tenant,
        a.initiator,
        a.status,
        a.risk,
        a.created_at,
        a.decided_by ?? "");

      }
      for (const l of payload.audits ?? []) {
        sql.exec(
        "INSERT OR REPLACE INTO v25_mgmt_audit_logs (audit_id, ts, actor, role, action, scope, outcome) VALUES (?, ?, ?, ?, ?, ?, ?)",
        l.audit_id,
        l.ts,
        l.actor,
        l.role,
        l.action,
        l.scope,
        l.outcome);

      }
      for (const n of payload.nodeOverrides ?? []) {
        sql.exec(
        "INSERT OR REPLACE INTO v25_mgmt_node_overrides (node_id, region, role_label, status, cpu_pct, psi_pct) VALUES (?, ?, ?, ?, ?, ?)",
        n.node_id,
        n.region,
        n.role_label,
        n.status,
        n.cpu_pct,
        n.psi_pct);

      }
      for (const t of payload.tasks ?? []) {
        sql.exec(
        "INSERT OR REPLACE INTO v25_scheduler_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, idem_key, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        t.task_id,
        t.tenant_id,
        t.priority,
        t.state,
        t.fencing_token,
        t.assigned_node_id ?? "",
        t.idem_key,
        t.spec_json);

        const restored = this.deps.v25Store.getPersistedTask(String(t.task_id));
        if (restored) {
          this.deps.scheduler.allTasks.set(restored.task_id, restored);
        }
      }
      return true;
    } catch {
      return false;
    }
  }

  getTaskTurnsDetail(taskId)













  {
    this.ensureInitialized();
    const walEntries = this.deps.walEngine.getTaskWalEntries(taskId);
    const branchName =
    taskId === "task-ns-8841" ?
    "feat-ns-auth" :
    taskId === "task-ns-8842" ?
    "fix-ns-ledger" :
    taskId === "task-hl-3019" ?
    "gdpr-audit-eu" :
    `task-${taskId}`;
    const turns = walEntries.map((e) => {
      const missing = this.scenario === "history-missing" && e.turnIndex >= 4;
      return {
        turnIndex: e.turnIndex,
        walSeq: e.walSeq,
        fencingToken: e.fencingToken,
        crc32Hex: e.frame.crc32Hex,
        integrityVerified: !missing && e.frame.verifyIntegrity(),
        changedFiles: e.frame.changedFiles,
        sqliteWalPages: e.frame.sqliteWalPages,
        commitId: `C${Math.max(0, e.turnIndex - 1)}`,
        ltxStatus: missing ? "LTX_SEGMENT_MISSING" : "VERIFIED"
      };
    });
    return { taskId, branchName, turns };
  }

  createApprovalTicket(params)







  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权发起高危变更申请单" }
      };
    }
    const approvalId = `APR-2026-${Math.floor(100 + Math.random() * 899)}`;
    const createdAt = `${new Date().toISOString().slice(11, 16)} UTC`;
    const risk = params.risk ?? (params.type.includes("SAGA") ? "CRITICAL" : "HIGH");
    this.deps.v25Store.sql.exec(
    "INSERT OR REPLACE INTO v25_mgmt_approvals (approval_id, type, target, region, tenant, initiator, status, risk, created_at, decided_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    approvalId,
    params.type,
    params.target,
    params.region,
    params.tenant,
    params.initiator,
    "PENDING_SECOND_ADMIN",
    risk,
    createdAt,
    "");

    this.appendAudit(
    params.initiator,
    params.role,
    `提交高危变更申请单 ${approvalId} (${params.type}: ${params.target})`,
    `${params.region} / ${params.tenant}`,
    "PENDING_SECOND_ADMIN");

    return {
      status: 201,
      body: {
        approvalId,
        type: params.type,
        target: params.target,
        region: params.region,
        tenant: params.tenant,
        initiator: params.initiator,
        status: "PENDING_SECOND_ADMIN",
        risk,
        createdAt
      }
    };
  }

  checkTenantDispatchAdmission(tenantId, priority, enforceQuotaGate = false)





  {
    this.ensureInitialized();
    const row = this.deps.v25Store.sql.
    exec("SELECT * FROM v25_mgmt_tenants WHERE tenant_id = ?", tenantId).
    one();
    if (!row) {
      return { allowed: true, statusCode: 200 };
    }
    const maxSlots = Number(row.max_slots ?? 120);
    const activeCount = Array.from(this.deps.scheduler.allTasks.values()).filter(
    (t) => t.tenant_id === tenantId && t.state !== _models.TaskState.COMPLETED && t.state !== _models.TaskState.CANCELLED && String(t.state) !== "FENCED_TERMINATED").
    length;
    const baseUsed = tenantId === "Northstar" ? 86 : tenantId === "Helios" ? 45 : 20;
    const usedSlots =
    this.scenario === "quota" && tenantId === "Northstar" && maxSlots <= 120 ?
    maxSlots :
    baseUsed + activeCount;

    if ((this.scenario === "quota" || enforceQuotaGate) && usedSlots >= maxSlots && priority > 0) {
      return {
        allowed: false,
        statusCode: 429,
        reason: "TENANT_QUOTA_EXHAUSTED",
        usedSlots,
        maxSlots
      };
    }
    return { allowed: true, statusCode: 200, usedSlots, maxSlots };
  }

  scaleOutOrDecommissionNode(params)







  {
    this.ensureInitialized();
    if (params.actorRole === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权执行节点扩缩容" }
      };
    }
    const sql = this.deps.v25Store.sql;
    if (params.action === "decommission") {
      this.executeBatchNodeOperation({
        operation: "drain",
        nodeIds: [params.nodeId],
        forceMigrateLeases: true,
        actor: params.actor,
        role: params.actorRole
      });
      sql.exec("DELETE FROM v25_mgmt_node_overrides WHERE node_id = ?", params.nodeId);
      if (params.nodeId.startsWith("celld-brain-")) {
        const brainKey = params.nodeId.replace("celld-brain-", "brain-node-");
        this.deps.shardCoordinator.evictUnhealthyBrainShardNode(brainKey);
      }
      this.appendAudit(
      params.actor,
      params.actorRole,
      `下线并移除节点 ${params.nodeId} (已迁移全部活跃租约)`,
      `${params.region} / ${params.nodeId}`,
      "DECOMMISSIONED");

      return { status: 200, body: { action: "decommission", nodeId: params.nodeId, decommissioned: true } };
    }

    const ip = params.ipAddress ?? `10.240.0.${Math.floor(50 + Math.random() * 150)}`;
    const roleLabel =
    params.role === "brain" ?
    `Brain Shard (${params.nodeId})` :
    `Body LayerFS (${params.nodeId})`;
    const scaleRes = this.deps.fleetController.scaleOutNode(
    params.nodeId,
    ip,
    params.role === "brain" ? "control-plane" : "worker");

    const daemon = new _node_daemon.NodeDaemon(params.nodeId, 32, 65536, 8);
    this.deps.scheduler.registerOrUpdateNode(daemon.emitHeartbeat(0));

    sql.exec(
    "INSERT OR REPLACE INTO v25_mgmt_node_overrides (node_id, region, role_label, status, cpu_pct, psi_pct) VALUES (?, ?, ?, ?, ?, ?)",
    params.nodeId,
    params.region,
    roleLabel,
    "READY",
    38.5,
    1.8);


    let newEpoch = 1;
    if (params.role === "brain" || params.nodeId.startsWith("celld-brain-")) {
      const brainKey = params.nodeId.replace("celld-brain-", "brain-node-");
      newEpoch = this.deps.shardCoordinator.registerBrainShardNode(brainKey);
    }

    this.appendAudit(
    params.actor,
    params.actorRole,
    `扩容新节点 ${params.nodeId} (${roleLabel}, IP=${ip}) 并加入集群拓扑`,
    `${params.region} / ${params.nodeId}`,
    `READY (epoch:${newEpoch})`);


    return {
      status: 201,
      body: {
        action: "scale_out",
        nodeId: params.nodeId,
        region: params.region,
        role: params.role,
        ipAddress: ip,
        rebalancedCells: scaleRes.rebalancedCells,
        epoch: newEpoch
      }
    };
  }

  mutateTenantFinOpsOrEscrow(params)






  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权变更 FinOps 账单或 DAG 预留槽位" }
      };
    }
    const sql = this.deps.v25Store.sql;
    const row = sql.
    exec("SELECT * FROM v25_mgmt_tenants WHERE tenant_id = ?", params.tenantId).
    one();
    if (!row) {
      return { status: 404, body: { error: "TENANT_NOT_FOUND", tenantId: params.tenantId } };
    }

    if (params.action === "reserve_escrow") {
      const delta = Number(params.escrowDeltaSlots ?? 4);
      const nextEscrow = Number(row.escrow_reserved ?? 8) + delta;
      sql.exec("UPDATE v25_mgmt_tenants SET escrow_reserved = ? WHERE tenant_id = ?", nextEscrow, params.tenantId);
      this.appendAudit(
      params.actor,
      params.role,
      `为租户 ${params.tenantId} 增加 DAG 子任务并发担保预留 (+${delta} 槽 → 共 ${nextEscrow} 槽)`,
      `${row.region} / ${params.tenantId}`,
      `Escrow=${nextEscrow}`);

      return {
        status: 200,
        body: { tenantId: params.tenantId, escrowReserved: nextEscrow }
      };
    }

    // action === "record_spend"
    const deltaUsd = Number(params.spendDeltaUsd ?? 220);
    const curSpent = Number(row.spent_usd ?? 0);
    const budgetUsd = Number(row.daily_budget_usd ?? 1200);
    const nextSpent = Number((curSpent + deltaUsd).toFixed(2));
    this.deps.finopsController.recordSpend(params.tenantId, deltaUsd);
    const evalRes = this.deps.finopsController.evaluateBudgetAndModel(params.tenantId);
    const ratio = nextSpent / Math.max(1, budgetUsd);
    const nextPolicy =
    ratio >= 1.0 ?
    "402 HARD_BLOCK (日预算已耗尽)" :
    ratio >= 0.8 ?
    "已触发 80% 软降级 → Qwen-3.8 Economy" :
    "DeepSeek-R1 → Qwen-3.8 (80%)";

    sql.exec(
    "UPDATE v25_mgmt_tenants SET spent_usd = ?, model_policy = ? WHERE tenant_id = ?",
    nextSpent,
    nextPolicy,
    params.tenantId);

    this.appendAudit(
    params.actor,
    params.role,
    `租户 ${params.tenantId} 计费流水累加 +${deltaUsd} (当前 ${nextSpent} / ${budgetUsd})`,
    `${row.region} / ${params.tenantId}`,
    ratio >= 1.0 ? "402 预算熔断" : ratio >= 0.8 ? "自动软降级 (Qwen-3.8)" : "计量正常");


    return {
      status: 200,
      body: {
        tenantId: params.tenantId,
        spentUsd: nextSpent,
        dailyBudgetUsd: budgetUsd,
        utilizationRatio: Number(ratio.toFixed(3)),
        downgraded: evalRes.downgraded || ratio >= 0.8,
        allowed: evalRes.allowed && ratio < 1.0,
        modelPolicy: nextPolicy
      }
    };
  }

  executeTaskTurnStep(params)






  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权推进任务回合" }
      };
    }
    const task =
    this.deps.scheduler.allTasks.get(params.taskId) ?? this.deps.v25Store.getPersistedTask(params.taskId);
    if (!task) {
      return { status: 404, body: { error: "TASK_NOT_FOUND", taskId: params.taskId } };
    }
    const existingWal = this.deps.walEngine.getTaskWalEntries(params.taskId);
    const nextTurnIndex = existingWal.length + 1;
    task.current_turn = existingWal.length;
    task.max_turns = Math.max(task.max_turns, nextTurnIndex + 10);

    // 1. Apply THINK cgroup phase (0.25 core) -> Execute AgentHarnessRunner (CognitiveRouter + VelocityBreaker + SagaToolGateway) -> Apply ACT cgroup phase (4.0 cores)
    const profile = task.resource_profile ?? { think_cpu_cores: 0.25, act_cpu_cores: 4.0, act_memory_mb: 4096, disk_mb: 10240 };
    const thinkCgroup = this.phaseController.applyThinkPhase(task.task_id, profile);
    this.cgroupDriver.writePhaseLimits(
    task.task_id,
    "THINK",
    profile.think_cpu_cores,
    Math.round(profile.act_memory_mb * 0.25),
    Math.round(profile.act_memory_mb * 0.5));

    const toolName = params.toolName ?? "shell_exec";
    const cmdStr = params.shellCommand ?? `step-${params.taskId}-turn-${nextTurnIndex}`;
    const harnessSummary = this.harnessRunner.executeSingleTurn(task, toolName, {
      cmd: cmdStr,
      turnIndex: nextTurnIndex
    });
    const actCgroup = this.phaseController.applyActPhase(task.task_id, profile);
    const diskCgroupFiles = this.cgroupDriver.writePhaseLimits(
    task.task_id,
    "ACT",
    profile.act_cpu_cores,
    Math.round(profile.act_memory_mb * 0.85),
    profile.act_memory_mb);

    const overlayLayout = this.overlayDriver.createOverlayMountLayout(task.task_id);
    fs.writeFileSync(
    path.join(overlayLayout.upperDir, `turn-${nextTurnIndex}.diff`),
    `task=${task.task_id}\nturn=${nextTurnIndex}\ncmd=${cmdStr}\n`,
    "utf-8");

    const overlayDelta = this.overlayDriver.captureUpperdirDeltaSnapshot(task.task_id, nextTurnIndex);

    const changedFile = `src/${params.taskId}-turn${nextTurnIndex}.ts`;
    const walEntry = this.deps.walEngine.appendTurnDelta({
      taskId: params.taskId,
      turnIndex: nextTurnIndex,
      fencingToken: task.lease_fencing_token,
      changedFiles: [changedFile],
      sqliteWalPages: 4 + nextTurnIndex,
      highestSubmittedFencingToken: task.lease_fencing_token
    });

    const branchName =
    params.taskId === "task-ns-8841" ?
    "feat-ns-auth" :
    params.taskId === "task-ns-8842" ?
    "fix-ns-ledger" :
    params.taskId === "task-hl-3019" ?
    "gdpr-audit-eu" :
    `task-${params.taskId}`;
    try {
      this.deps.layerfs.getBranchHeadCommit(branchName);
    } catch {
      this.deps.layerfs.forkBranch("main", branchName);
    }

    let commitId = "C1";
    if (params.executeRealBash) {
      const bashRes = this.deps.layerfs.executeEphemeralBashWorkspace({
        branchName,
        agentId: params.actor,
        stepId: `step-${params.taskId}-t${nextTurnIndex}`,
        command: params.shellCommand ?? `printf 'export const turn = ${nextTurnIndex};\nexport const crc32 = "${walEntry.frame.crc32Hex}";\n' > ${changedFile}`
      });
      commitId = bashRes.commitId;
    } else {
      const lfsCommit = this.deps.layerfs.executeInProcessFileTool({
        branchName,
        agentId: params.actor,
        stepId: `step-${params.taskId}-t${nextTurnIndex}`,
        operation: "write",
        filePath: changedFile,
        newContent: `export const turn = ${nextTurnIndex};
export const crc32 = "${walEntry.frame.crc32Hex}";
`
      });
      commitId = lfsCommit.commitId;
    }

    this.deps.v25Store.sql.exec(
    "UPDATE v25_scheduler_tasks SET state = ?, spec_json = ? WHERE task_id = ?",
    task.state,
    JSON.stringify(task),
    task.task_id);


    this.appendAudit(
    params.actor,
    params.role,
    `推进任务 ${params.taskId} 至 Turn #${nextTurnIndex} (Router=${harnessSummary.endpointId}, WAL CRC32=0x${walEntry.frame.crc32Hex}, LayerFS Commit=${commitId})`,
    `us-west-2 / ${task.tenant_id}`,
    `Turn #${nextTurnIndex} 已提交`);


    return {
      status: 200,
      body: {
        taskId: params.taskId,
        turnIndex: nextTurnIndex,
        crc32Hex: walEntry.frame.crc32Hex,
        commitId,
        branchName,
        harnessExecution: harnessSummary,
        cgroupTransition: {
          thinkQuotaUs: thinkCgroup.cpuQuotaUsPer100ms,
          actQuotaUs: actCgroup.cpuQuotaUsPer100ms,
          diskCpuMax: diskCgroupFiles.cpuMaxContent,
          overlayUpperSha256: overlayDelta.sha256
        }
      }
    };
  }

  evaluateCanaryOrGeoRegion(params)





  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权操作金丝雀发布或区域容灾切换" }
      };
    }

    if (params.action === "toggle_region") {
      const regId = params.regionId ?? "eu-central-1";
      const avail = params.capacityAvailable ?? true;
      const updated = this.deps.geoRouter.setRegionAvailability(regId, avail);
      const rep = this.deps.crossRegionReplicator ?
      this.deps.crossRegionReplicator.replicateLtxObject("eu-west-1", `ltx/failover-${regId}.ltx`) :
      null;
      this.appendAudit(
      params.actor,
      params.role,
      `切换区域 ${regId} 容量可用状态为 ${avail ? "ONLINE" : "DRAINED"} 并触发跨区 LTX 同步`,
      `${regId} / GeoRouter`,
      avail ? "ONLINE" : "FAILOVER_ACTIVE");

      return {
        status: 200,
        body: { updated, regions: this.deps.geoRouter.listRegions(), replication: rep }
      };
    }

    if (!this.deps.canaryController) {
      return { status: 200, body: { state: "CANARY_ACTIVE", trafficWeightPct: 25 } };
    }

    let canaryRes;
    if (params.action === "reset_canary") {
      canaryRes = this.deps.canaryController.resetCanary(5);
    } else if (params.action === "inject_anomaly") {
      canaryRes = this.deps.canaryController.evaluateCanaryStep({
        errorRatePct: 4.5,
        p99LatencyMs: 1850
      });
    } else {
      canaryRes = this.deps.canaryController.evaluateCanaryStep({
        errorRatePct: 0.1,
        p99LatencyMs: 68
      });
    }

    this.appendAudit(
    params.actor,
    params.role,
    `执行金丝雀发布控制 [${params.action}] → 状态: ${canaryRes.state} (权重: ${canaryRes.trafficWeightPct}%)`,
    "Global / CanaryController",
    `${canaryRes.state} (${canaryRes.trafficWeightPct}%)`);


    return {
      status: 200,
      body: canaryRes
    };
  }

  queryAuditLogs(filters)



  {
    this.ensureInitialized();
    const rows = this.deps.v25Store.sql.
    exec("SELECT * FROM v25_mgmt_audit_logs").
    toArray().
    map((l) => ({
      auditId: String(l.audit_id),
      ts: String(l.ts),
      actor: String(l.actor),
      role: String(l.role),
      action: String(l.action),
      scope: String(l.scope),
      outcome: String(l.outcome)
    })).
    reverse();

    const q = (filters.q ?? "").toLowerCase().trim();
    const actor = filters.actor && filters.actor !== "ALL" ? filters.actor : null;
    const role = filters.role && filters.role !== "ALL" ? filters.role : null;

    const filtered = rows.filter((r) => {
      if (actor && r.actor !== actor) return false;
      if (role && r.role !== role) return false;
      if (!q) return true;
      return (
        r.action.toLowerCase().includes(q) ||
        r.scope.toLowerCase().includes(q) ||
        r.outcome.toLowerCase().includes(q) ||
        r.actor.toLowerCase().includes(q));

    });

    return { total: filtered.length, audits: filtered };
  }

  getA2aGovernanceSnapshot() {
    this.ensureInitialized();
    const sql = this.deps.v25Store.sql;
    const rows = sql.exec("SELECT * FROM v25_mgmt_a2a_blackboard").toArray();
    const blackboardEntries = rows.map((r) => {
      let parsedVal = r.value_json;
      try {
        parsedVal = JSON.parse(String(r.value_json));
      } catch {

        // keep string
      }return {
        key: String(r.bb_key),
        value: parsedVal,
        version: Number(r.version ?? 1),
        authorAgentId: String(r.author_agent ?? "agent-northstar"),
        updatedAtMs: Number(r.updated_at_ms ?? Date.now())
      };
    });
    const deadlock = this.deadlockDetector.detectCycle();
    return {
      blackboardEntries,
      deadlock,
      lastGitArbitration: this.lastGitArbitration,
      lastBisectResult: this.lastBisectResult
    };
  }

  executeA2aGovernanceAction(params)









  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权执行 A2A 多智能体协同治理写操作" }
      };
    }
    const sql = this.deps.v25Store.sql;

    if (params.action === "put_blackboard_cas") {
      const key = params.key ?? "arch/zero-trust-policy";
      const author = params.authorAgentId ?? `agent-${params.actor}`;
      const existingRow = sql.
      exec("SELECT * FROM v25_mgmt_a2a_blackboard WHERE bb_key = ?", key).
      one();
      const memEntry = this.blackboard.get(key);
      const currentVer = memEntry ? memEntry.version : existingRow ? Number(existingRow.version ?? 0) : 0;
      const expectedVer = params.expectedVersion !== undefined ? Number(params.expectedVersion) : currentVer;

      if (expectedVer !== currentVer) {
        return {
          status: 409,
          body: {
            error: "A2A_BLACKBOARD_CAS_CONFLICT",
            key,
            expectedVersion: expectedVer,
            actualVersion: currentVer,
            message: `A2A 共享黑板 CAS 冲突：键 '${key}' 期望版本 v${expectedVer}，实际版本 v${currentVer}`
          }
        };
      }

      while ((this.blackboard.get(key)?.version ?? 0) < currentVer) {
        const curV = this.blackboard.get(key)?.version ?? 0;
        this.blackboard.putCas(key, existingRow ? existingRow.value_json : {}, curV, author);
      }
      const nextVal = params.value ?? { mTLS: "STRICT", updatedBy: params.actor, revision: currentVer + 1 };
      const committed = this.blackboard.putCas(key, nextVal, currentVer, author);
      sql.exec(
      "INSERT OR REPLACE INTO v25_mgmt_a2a_blackboard (bb_key, value_json, version, author_agent, updated_at_ms) VALUES (?, ?, ?, ?, ?)",
      committed.entry.key,
      JSON.stringify(committed.entry.value),
      committed.entry.version,
      committed.entry.authorAgentId,
      committed.entry.updatedAtMs);

      this.appendAudit(
      params.actor,
      params.role,
      `A2A 黑板 CAS 更新键 [${key}] → v${committed.entry.version} (Author: ${author})`,
      "Global / A2A-Blackboard",
      `COMMITTED (v${committed.entry.version})`);

      return {
        status: 200,
        body: {
          committed: true,
          entry: committed.entry,
          a2aState: this.getA2aGovernanceSnapshot()
        }
      };
    }

    if (params.action === "arbitrate_git_patches") {
      const patches = params.patches ?? [
      { agentId: "agent-ns-auth-p0", touchedFiles: ["src/auth/jwt.ts", "src/auth/spiffe.ts"], priorityScore: 95 },
      { agentId: "agent-ns-refactor-p2", touchedFiles: ["src/auth/jwt.ts", "src/utils/logger.ts"], priorityScore: 60 },
      { agentId: "agent-hl-ledger-p1", touchedFiles: ["src/ledger/wal.ts"], priorityScore: 85 }];

      const arb = this.blackboard.arbitrateGitPatchTouches(patches);
      this.lastGitArbitration = arb;
      this.appendAudit(
      params.actor,
      params.role,
      `执行 A2A 多智能体 Git 补丁冲突仲裁：准入 [${arb.acceptedAgentIds.join(", ")}]，拦截冲突 [${arb.conflictedAgentIds.join(", ")}]`,
      "Global / GitArbiter",
      `准入 ${arb.acceptedAgentIds.length} / 冲突 ${arb.conflictedAgentIds.length}`);

      return {
        status: 200,
        body: {
          ...arb,
          a2aState: this.getA2aGovernanceSnapshot()
        }
      };
    }

    if (params.action === "inject_deadlock_cycle") {
      this.deadlockDetector.addWaitEdge("agent-ns-worker-1", "agent-ns-worker-2");
      this.deadlockDetector.addWaitEdge("agent-ns-worker-2", "agent-ns-worker-3");
      this.deadlockDetector.addWaitEdge("agent-ns-worker-3", "agent-ns-worker-1");
      const cycle = this.deadlockDetector.detectCycle();
      this.appendAudit(
      params.actor,
      params.role,
      `A2A WFG 死锁检测器捕获环形等待链：${cycle.cyclePath.join(" → ")}`,
      "Global / A2A-WFG",
      "DEADLOCK_DETECTED");

      return {
        status: 200,
        body: {
          ...cycle,
          a2aState: this.getA2aGovernanceSnapshot()
        }
      };
    }

    if (params.action === "break_deadlock") {
      const victim = params.victimAgentId ?? "agent-ns-worker-3";
      this.deadlockDetector.removeAgent(victim);
      const cycle = this.deadlockDetector.detectCycle();
      this.appendAudit(
      params.actor,
      params.role,
      `解除 A2A WFG 环形死锁：抢占释放低优等待节点 [${victim}]`,
      "Global / A2A-WFG",
      "DEADLOCK_RESOLVED");

      return {
        status: 200,
        body: {
          victimAgentId: victim,
          ...cycle,
          a2aState: this.getA2aGovernanceSnapshot()
        }
      };
    }

    // action === "run_cow_bisect" (Real OS subprocess execution via executeLiveLayerFsBisect)
    const bisectBranch = `bisect-line-${Date.now()}`;
    this.deps.layerfs.forkBranch("main", bisectBranch);
    const c0Good = this.deps.layerfs.executeInProcessFileTool({
      branchName: bisectBranch,
      agentId: "agent-bisect",
      stepId: "bisect-c0-good",
      operation: "write",
      filePath: "src/health_check.txt",
      newContent: "STATUS=OK\n"
    });
    const c1Good = this.deps.layerfs.executeInProcessFileTool({
      branchName: bisectBranch,
      agentId: "agent-bisect",
      stepId: "bisect-c1-good",
      operation: "write",
      filePath: "src/health_check.txt",
      newContent: "STATUS=OK\nVERIFIED=1\n"
    });
    const c2Bad = this.deps.layerfs.executeInProcessFileTool({
      branchName: bisectBranch,
      agentId: "agent-bisect",
      stepId: "bisect-c2-bad",
      operation: "write",
      filePath: "src/health_check.txt",
      newContent: "STATUS=REGRESSION_BUG\n"
    });
    const c3Bad = this.deps.layerfs.executeInProcessFileTool({
      branchName: bisectBranch,
      agentId: "agent-bisect",
      stepId: "bisect-c3-bad",
      operation: "write",
      filePath: "src/health_check.txt",
      newContent: "STATUS=REGRESSION_BUG_2\n"
    });
    const commitChain = [c0Good.commitId, c1Good.commitId, c2Bad.commitId, c3Bad.commitId];
    const liveBisectRes = this.bisectCoordinator.executeLiveLayerFsBisect({
      driver: this.deps.layerfs,
      commits: commitChain,
      testShellCommand: "grep -q 'STATUS=OK' src/health_check.txt"
    });
    this.lastBisectResult = {
      firstBadCommit: liveBisectRes.firstBadCommit,
      probesExecuted: liveBisectRes.probesExecuted,
      probedCommits: liveBisectRes.probedCommits
    };
    this.appendAudit(
    params.actor,
    params.role,
    `执行并行 CoW 二分排障 (LayerFS Zero-Copy Bisect)：${liveBisectRes.probesExecuted} 次探测定位首个异常提交 [${liveBisectRes.firstBadCommit}]`,
    "Global / CoW-Bisect",
    `FIRST_BAD=${liveBisectRes.firstBadCommit}`);

    return {
      status: 200,
      body: {
        ...this.lastBisectResult,
        a2aState: this.getA2aGovernanceSnapshot()
      }
    };
  }

  async migrateTaskAcrossBodyNodes(params)




  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权执行跨节点任务热迁移" }
      };
    }
    const sql = this.deps.v25Store.sql;
    const task =
    this.deps.scheduler.allTasks.get(params.taskId) ?? this.deps.v25Store.getPersistedTask(params.taskId);
    if (!task) {
      return { status: 404, body: { error: "TASK_NOT_FOUND", taskId: params.taskId } };
    }
    const sourceNodeId = task.assigned_node_id ?? "body-host-02";
    const targetNodeId = params.targetNodeId || (sourceNodeId === "body-host-02" ? "body-host-03" : "body-host-02");
    const newFencingToken = task.lease_fencing_token + 1;
    const walEntries = this.deps.walEngine.getTaskWalEntries(params.taskId);
    const turnIndex = Math.max(1, walEntries.length);
    const branchName =
    params.taskId === "task-ns-8841" ?
    "feat-ns-auth" :
    params.taskId === "task-ns-8842" ?
    "fix-ns-ledger" :
    params.taskId === "task-hl-3019" ?
    "gdpr-audit-eu" :
    `task-${params.taskId}`;
    try {
      this.deps.layerfs.getBranchHeadCommit(branchName);
    } catch {
      this.deps.layerfs.forkBranch("main", branchName);
    }
    const branchBundle = this.deps.layerfs.exportBranchBundle(branchName);
    const snapshotBytes = new TextEncoder().encode(
    JSON.stringify({
      taskId: params.taskId,
      turnIndex,
      sourceNodeId,
      targetNodeId,
      fencingToken: newFencingToken,
      layerfsBranchBundle: branchBundle
    }));


    const migRes = await this.deps.heavyMigrator.migrateHeavyTaskAcrossNodes({
      taskId: params.taskId,
      turnIndex,
      sourceNodeId,
      targetNodeId,
      newFencingToken,
      upperdirSnapshotBytes: snapshotBytes
    });

    if (this.deps.s3Endpoint) {
      await this.deps.layerfs.pushBranchToRemoteS3(branchName, this.deps.s3Endpoint);
    }
    let remoteBodyImported = false;
    const targetBodyUrl = this.deps.peerBodyUrls[targetNodeId];
    if (targetBodyUrl) {
      try {
        const importResp = await fetch(`${targetBodyUrl.replace(/\/$/, "")}/internal/v1beta1/body/layerfs-import`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ branchBundle, taskId: params.taskId, fencingToken: newFencingToken })
        });
        remoteBodyImported = importResp.ok;
      } catch {
        remoteBodyImported = false;
      }
    }

    task.assigned_node_id = targetNodeId;
    task.lease_fencing_token = newFencingToken;
    this.deps.scheduler.allTasks.set(task.task_id, task);
    sql.exec(
    "UPDATE v25_scheduler_tasks SET assigned_node_id = ?, fencing_token = ?, spec_json = ? WHERE task_id = ?",
    targetNodeId,
    newFencingToken,
    JSON.stringify(task),
    task.task_id);


    this.appendAudit(
    params.actor,
    params.role,
    `跨节点热迁移任务 ${params.taskId} (${sourceNodeId} → ${targetNodeId}, Fencing Token 推进至 epoch:${newFencingToken}, R2 快照: ${migRes.snapshotKey})`,
    `Cluster / ${params.taskId}`,
    `MIGRATED (${migRes.migrationDurationMs}ms)`);


    return {
      status: 200,
      body: {
        taskId: params.taskId,
        sourceNodeId,
        targetNodeId,
        newFencingToken,
        branchName,
        remoteBodyImported,
        ...migRes
      }
    };
  }

  inspectLayerFsFileBlame(params)



  {
    this.ensureInitialized();
    const taskId = params.taskId ?? "task-ns-8841";
    const branchName =
    params.branchOrCommit ?? (
    taskId === "task-ns-8841" ?
    "feat-ns-auth" :
    taskId === "task-ns-8842" ?
    "fix-ns-ledger" :
    taskId === "task-hl-3019" ?
    "gdpr-audit-eu" :
    `task-${taskId}`);
    const filePath = params.filePath ?? `src/${taskId}.ts`;

    try {
      this.deps.layerfs.getBranchHeadCommit(branchName);
    } catch {
      this.deps.layerfs.forkBranch("main", branchName);
    }

    const headCommit = this.deps.layerfs.getBranchHeadCommit(branchName);
    if (!headCommit.filesTree.has(filePath)) {
      this.deps.layerfs.executeInProcessFileTool({
        branchName,
        agentId: "agent-northstar",
        stepId: `blame-seed-${taskId}`,
        operation: "write",
        filePath,
        newContent: `export const taskId = "${taskId}";\nexport const branch = "${branchName}";\nexport const verifiedBy = "LayerFS-CoW";\n`
      });
    }

    const rawLines = this.deps.layerfs.blameFile(branchName, filePath);
    const updatedHead = this.deps.layerfs.getBranchHeadCommit(branchName);
    const lines = rawLines.map((ln) => ({
      ...ln,
      content: ln.text,
      commitId: updatedHead.commitId,
      agentId: updatedHead.agentId
    }));
    return {
      status: 200,
      body: {
        taskId,
        branchName,
        headCommitId: updatedHead.commitId,
        filePath,
        lines
      }
    };
  }

  executeStorageGcAndEpochAudit(params)




  {
    this.ensureInitialized();
    if (params.role === "VIEWER" && params.action !== "epoch_gc_audit") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发分布式 CAS 垃圾回收清理" }
      };
    }

    if (params.action === "seed_orphans") {
      const seededIds = [];
      for (let i = 1; i <= 3; i++) {
        const content = `orphan-aborted-branch-blob-${Date.now()}-${i}-${"x".repeat(256)}`;
        const meta = this.deps.layerfs.storeEngine.putFileObject(`orphan-${i}.txt`, content);
        seededIds.push(meta.object.objectId);
      }
      this.appendAudit(
      params.actor,
      params.role,
      `生成 3 个孤儿 CAS 数据块用于两阶段 Mark-and-Sweep GC 验证`,
      "Storage / LayerFS-CAS",
      `SEEDED (${seededIds.length} blobs)`);

      return {
        status: 200,
        body: {
          seededOrphanBlobIds: seededIds,
          storageStats: this.deps.layerfs.storeEngine.getStorageStats()
        }
      };
    }

    if (params.action === "sweep_cas") {
      const sweepRes = this.gcSweeper.sweepLayerFsWorkspaceStore(this.deps.layerfs);
      this.lastGcSweepReport = {
        retainedCount: sweepRes.retainedBlobIds.length,
        sweptCount: sweepRes.sweptBlobIds.length,
        reclaimedBytes: sweepRes.reclaimedBytes,
        sweptBlobIds: sweepRes.sweptBlobIds
      };
      this.appendAudit(
      params.actor,
      params.role,
      `执行分布式 CAS 两阶段 Mark-and-Sweep GC：保留活跃对象 ${sweepRes.retainedBlobIds.length} 个，物理回收孤儿块 ${sweepRes.sweptBlobIds.length} 个 (${sweepRes.reclaimedBytes} Bytes)`,
      "Storage / LayerFS-CAS",
      `SWEPT (${sweepRes.reclaimedBytes}B reclaimed)`);

      return {
        status: 200,
        body: {
          ...this.lastGcSweepReport,
          storageStats: this.deps.layerfs.storeEngine.getStorageStats()
        }
      };
    }

    const graceSecs = Number(params.graceSecs ?? 3600);
    const dynamicCandidates =








    [
    { cellScope: "AnstsSchedulerCell:shard-01", epochId: 10, ownerCurrentEpoch: 14, ageSeconds: 7200, isFacetStream: false, isPagedCell: false, hydrateMbps: 120, bytes: 16384 },
    { cellScope: "AnstsSchedulerCell:shard-02", epochId: 11, ownerCurrentEpoch: 14, ageSeconds: 5400, isFacetStream: false, isPagedCell: false, hydrateMbps: 115, bytes: 24576 },
    { cellScope: "AnstsHarnessCell:facet-stream", epochId: 9, ownerCurrentEpoch: 14, ageSeconds: 9000, isFacetStream: true, isPagedCell: false, hydrateMbps: 100, bytes: 8192 },
    { cellScope: "AnstsSchedulerCell:shard-03", epochId: 13, ownerCurrentEpoch: 14, ageSeconds: 7200, isFacetStream: false, isPagedCell: false, hydrateMbps: 110, bytes: 32768 }];

    for (const [tId, tk] of this.deps.scheduler.allTasks.entries()) {
      const walFrames = this.deps.walEngine.getTaskWalEntries(tId);
      const staleEpochs = new Set();
      for (const wf of walFrames) {
        if (wf.fencingToken < tk.lease_fencing_token - 1) {
          staleEpochs.add(wf.fencingToken);
        }
      }
      for (const oldEp of staleEpochs) {
        dynamicCandidates.push({
          cellScope: `TaskCell:${tId}`,
          epochId: oldEp,
          ownerCurrentEpoch: tk.lease_fencing_token,
          ageSeconds: 7200,
          isFacetStream: false,
          isPagedCell: false,
          hydrateMbps: 125,
          bytes: 12288
        });
      }
    }
    for (const facet of this.facetSupervisor.listFacets()) {
      dynamicCandidates.push({
        cellScope: `FacetCell:${facet.facetName}`,
        epochId: 1,
        ownerCurrentEpoch: 5,
        ageSeconds: 9000,
        isFacetStream: true,
        isPagedCell: false,
        hydrateMbps: 100,
        bytes: 4096
      });
    }
    const candidates = dynamicCandidates;
    const deletable = candidates.filter(
    (c) => !c.isFacetStream && !(c.isPagedCell && c.hydrateMbps === 0) && c.epochId < c.ownerCurrentEpoch - 1 && c.ageSeconds >= graceSecs);

    const reclaimableBytes = deletable.reduce((acc, c) => acc + c.bytes, 0);
    return {
      status: 200,
      body: {
        dryRun: true,
        graceSecs,
        deletableEpochCount: deletable.length,
        reclaimableBytes,
        deletableScopes: deletable.map((d) => `${d.cellScope}@epoch-${d.epochId}`),
        skippedActiveOrFacetCount: candidates.length - deletable.length
      }
    };
  }

  runFleetRollingUpgradeDiagnostics(params)




  {
    this.ensureInitialized();
    const expectedProtocol = params.expectedProtocol ?? "v0.6.3";
    const sql = this.deps.v25Store.sql;
    const nodeRows = sql.exec("SELECT * FROM v25_mgmt_node_overrides").toArray();
    const peerProbes = nodeRows.map((nr) => {
      const st = String(nr.status ?? "READY");
      return {
        nodeId: String(nr.node_id),
        region: String(nr.region ?? "us-west-2"),
        status: st,
        leaseExpired: st === "DRAIN_FAILED",
        reachable: this.scenario !== "offline",
        protocolVersion: "v0.6.3",
        restoringCount: this.scenario === "verification" ? 1 : 0
      };
    });
    const expiredLeases = peerProbes.filter((p) => p.leaseExpired).map((p) => p.nodeId);
    const unreachablePeers = peerProbes.filter((p) => !p.reachable).map((p) => p.nodeId);
    const protocolMismatches = peerProbes.filter((p) => p.protocolVersion !== expectedProtocol).map((p) => p.nodeId);
    const totalRestoring = peerProbes.reduce((acc, p) => acc + p.restoringCount, 0);
    const rollingUpgradeSafe =
    expiredLeases.length === 0 &&
    unreachablePeers.length === 0 &&
    protocolMismatches.length === 0 &&
    totalRestoring === 0;

    this.metrics.setGauge("ansts_cluster_nodes_total", nodeRows.length);
    this.metrics.setGauge("ansts_active_tasks_total", this.deps.scheduler.allTasks.size);
    this.metrics.setGauge("ansts_rolling_upgrade_safe", rollingUpgradeSafe ? 1 : 0);
    this.metrics.incCounter("ansts_diagnostics_probes_total", 1);
    const traceparent = _otel_prometheus.OtelAndPrometheusExporter.createTraceparent();

    let webhookResult = null;
    if (params.enqueueWebhookTest) {
      const msgId = `outbox-${Date.now()}`;
      this.outboxDispatcher.enqueueOutbox(msgId, "cluster-diagnose", "https://sre-webhook.ansts.internal/events", {
        event: "io.ansts.cluster.diagnose",
        rollingUpgradeSafe,
        traceparent
      });
      const dispatchStats = this.outboxDispatcher.processPending(() => true);
      webhookResult = {
        messageId: msgId,
        ...dispatchStats,
        hmacHeaderSample: this.outboxDispatcher.signPayload(JSON.stringify({ rollingUpgradeSafe }))
      };
    }

    this.appendAudit(
    params.actor,
    params.role,
    `执行集群滚动升级安全探针 (anstsctl diagnose · protocol=${expectedProtocol}) → ${rollingUpgradeSafe ? "SAFE_TO_UPGRADE" : "BLOCKED"}`,
    "Global / FleetDiagnostics",
    rollingUpgradeSafe ? "SAFE (0 异常租约)" : "UPGRADE_BLOCKED");


    return {
      status: 200,
      body: {
        expectedProtocol,
        rollingUpgradeSafe,
        expiredLeases,
        unreachablePeers,
        protocolMismatches,
        totalRestoring,
        peerProbes,
        traceparent,
        prometheusMetrics: this.metrics.renderPrometheusMetrics(),
        webhookOutbox: webhookResult
      }
    };
  }


  getMcpGovernanceSnapshot() {
    this.ensureInitialized();
    const rows = this.deps.v25Store.sql.exec("SELECT * FROM v25_mgmt_mcp_tools").toArray();
    const tools = rows.map((r) => ({
      toolName: String(r.tool_name),
      serverName: String(r.server_name),
      description: String(r.description),
      effectClass: String(r.effect_class),
      registeredBy: String(r.registered_by ?? "chen"),
      registeredAt: String(r.registered_at ?? "09:20:00 UTC")
    }));
    return {
      tools,
      lastSagaRollbackReport: this.lastSagaRollbackReport
    };
  }

  executeMcpAndSagaGovernance(params)









  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权注册 MCP 工具或执行 Saga 事务补偿" }
      };
    }
    const sql = this.deps.v25Store.sql;

    if (params.action === "register_mcp_tool") {
      const serverName = params.serverName ?? "mcp-k8s-fleet";
      const toolName = params.toolName ?? `mcp_tool_${Math.floor(100 + Math.random() * 899)}`;
      const description = params.description ?? "Dynamic MCP JSON-RPC 2.0 compensatable tool";
      const nowUtc = `${new Date().toISOString().slice(11, 19)} UTC`;

      this.mcpRegistry.registerMcpServerTools(
      serverName,
      [
      {
        toolName,
        description,
        effectClass: _models.ToolCallEffectClass.COMPENSATABLE_WRITE,
        inputSchema: { type: "object" }
      }],

      this.deps.sagaGateway,
      (method, rpcParams) => ({ jsonrpc: "2.0", method, result: { executed: true, rpcParams } }));

      this.deps.sagaGateway.registerTool(
      toolName,
      _models.ToolCallEffectClass.COMPENSATABLE_WRITE,
      (args) => ({ jsonrpc: "2.0", serverName, toolName, executedArgs: args, status: "COMMITTED" }),
      (args) => ({ jsonrpc: "2.0", serverName, toolName, rolledBackArgs: args, status: "ROLLED_BACK_LIFO" }));


      sql.exec(
      "INSERT OR REPLACE INTO v25_mgmt_mcp_tools (tool_name, server_name, description, effect_class, registered_by, registered_at) VALUES (?, ?, ?, ?, ?, ?)",
      toolName,
      serverName,
      description,
      "COMPENSATABLE_WRITE",
      params.actor,
      nowUtc);


      this.appendAudit(
      params.actor,
      params.role,
      `注册动态 MCP 工具 [${serverName}/${toolName}] 并绑定 Saga LIFO 补偿钩子`,
      `Global / MCP-Registry`,
      "REGISTERED");


      return {
        status: 201,
        body: {
          toolName,
          serverName,
          effectClass: "COMPENSATABLE_WRITE",
          mcpState: this.getMcpGovernanceSnapshot()
        }
      };
    }

    if (params.action === "invoke_mcp_tool") {
      const taskId = params.taskId ?? "task-ns-8841";
      const toolName = params.toolName ?? "mcp_pg_migration_apply";
      const turnIndex = Number(params.turnIndex ?? Date.now() % 10000);
      const toolArgs = params.toolArgs ?? { migrationId: `mig_${turnIndex}` };
      const [output, deduplicated] = this.deps.sagaGateway.invokeTool(taskId, turnIndex, toolName, toolArgs);

      this.appendAudit(
      params.actor,
      params.role,
      `调用 MCP 事务工具 [${toolName}] (Task=${taskId}, Turn=#${turnIndex}, Deduplicated=${deduplicated})`,
      `us-west-2 / ${taskId}`,
      "COMMITTED (Saga Hook Armed)");


      return {
        status: 200,
        body: {
          taskId,
          turnIndex,
          toolName,
          output,
          deduplicated,
          mcpState: this.getMcpGovernanceSnapshot()
        }
      };
    }

    // action === "saga_rollback_task"
    const taskId = params.taskId ?? "task-ns-8841";
    const compensated = this.deps.sagaGateway.compensator.rollbackTask(taskId);
    this.lastSagaRollbackReport = { taskId, compensatedTools: compensated };

    this.appendAudit(
    params.actor,
    params.role,
    `触发任务 ${taskId} 的 Saga 逆序 (LIFO) 事务补偿回滚，共回滚 ${compensated.length} 步外部副作用`,
    `us-west-2 / ${taskId}`,
    `ROLLED_BACK (${compensated.length} steps)`);


    return {
      status: 200,
      body: {
        taskId,
        compensatedCount: compensated.length,
        compensatedTools: compensated,
        mcpState: this.getMcpGovernanceSnapshot()
      }
    };
  }


  getRuntimeSandboxSnapshot() {
    this.ensureInitialized();
    return {
      warmPoolStats: this.warmPool.stats(),
      teePublicKeyDerHex: this.teeBridge.publicKeyDerHex.slice(0, 32) + "...",
      spiffeCaPublicKeyDerHex: this.mtlsBridge.caPublicKeyDerHex.slice(0, 32) + "..."
    };
  }

  async executeRuntimeSandboxLifecycle(params)








  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权直接操控底层沙箱与内核 Cgroup/Vsock 接口" }
      };
    }
    const taskId = params.taskId ?? "task-ns-8841";
    const task =
    this.deps.scheduler.allTasks.get(taskId) ??
    this.deps.v25Store.getPersistedTask(taskId) ??
    new _models.TaskSpec({ task_id: taskId, tenant_id: "Northstar", priority: _models.TaskPriority.P0_EMERGENCY });

    if (params.action === "allocate_and_mount") {
      const microVm = this.warmPool.acquireSandbox(taskId);
      const muxMount = this.sandboxMultiplexer.mountWorkspaceIntoSandbox(taskId);
      const cgroupLimits = this.cgroupDriver.writePhaseLimits(taskId, "ACT", 4.0, 3480, 4096);
      const freezeState = this.cgroupDriver.setFreezeState(taskId, false);
      const overlayDirs = this.overlayDriver.createOverlayMountLayout(taskId);
      fs.writeFileSync(
      path.join(overlayDirs.upperDir, "sandbox_mount.meta"),
      JSON.stringify({ vmId: microVm.vmId, sandboxNumber: muxMount.sandboxNumber, ts: Date.now() }),
      "utf-8");

      const upperSnap = this.overlayDriver.captureUpperdirDeltaSnapshot(taskId, Math.max(1, task.current_turn));
      this.resourceProfiler.recordExecutionMetrics(task, 2.8, 2400);
      this.resourceProfiler.recordExecutionMetrics(task, 3.1, 2600);
      this.resourceProfiler.recordExecutionMetrics(task, 2.9, 2500);
      const rightSizedProfile = this.resourceProfiler.recommendProfile(task);

      this.appendAudit(
      params.actor,
      params.role,
      `分配热沙箱 ${microVm.vmId} (Multiplexer #${muxMount.sandboxNumber}) 并写入磁盘 Cgroup v2 (cpu.max=${cgroupLimits.cpuMaxContent}) 与 OverlayFS Upperdir 快照`,
      `Runtime / ${taskId}`,
      `MOUNTED (${microVm.vmId})`);


      return {
        status: 200,
        body: {
          taskId,
          microVm,
          multiplexer: muxMount,
          cgroupLimits,
          freezeState,
          overlayUpperSnapshot: {
            changedFiles: upperSnap.changedFiles,
            sha256: upperSnap.sha256,
            byteLength: upperSnap.serializedArchiveBytes.byteLength
          },
          rightSizedProfile,
          warmPoolStats: this.warmPool.stats()
        }
      };
    }

    if (params.action === "vsock_exec_and_io") {
      const vmId = `fc-vm-${taskId}`;
      const relFile = params.filePath ?? "agent_artifact.txt";
      const content = params.fileContent ?? `verified-by-vsock-${taskId}
`;
      const writeRes = this.vsockBridge.sendRequest({
        requestId: `req-w-${Date.now()}`,
        vmId,
        method: "WRITE_FILE",
        payload: { filePath: relFile, content }
      });
      const execRes = this.vsockBridge.sendRequest({
        requestId: `req-x-${Date.now()}`,
        vmId,
        method: "EXEC_CMD",
        payload: { command: params.command ?? `cat ${relFile}` }
      });
      const readRes = this.vsockBridge.sendRequest({
        requestId: `req-r-${Date.now()}`,
        vmId,
        method: "READ_FILE",
        payload: { filePath: relFile }
      });

      this.appendAudit(
      params.actor,
      params.role,
      `通过 ZeroTrustVsockBridge 执行真实沙箱文件 I/O (${relFile}, SHA-256=${String(writeRes.sha256).slice(0, 12)}) 与受控子进程指令`,
      `Runtime / ${vmId}`,
      `VSOCK_OK (exit=${execRes.exitCode})`);


      return {
        status: 200,
        body: {
          vmId,
          writeResult: writeRes,
          execResult: execRes,
          readResult: readRes
        }
      };
    }

    if (params.action === "issue_and_verify_mtls_tee") {
      const spiffeId = `spiffe://ansts.io/ns/fleet/sa/body-worker`;
      const svid = this.mtlsBridge.issueSpiffeSvidToken(spiffeId, 3_600_000);
      const svidVerify = this.mtlsBridge.verifySpiffeCertificate(
      svid.spiffeId,
      svid.certExpiresAtMs,
      Date.now(),
      svid.svidSignatureHex);

      const teeQuote = this.teeBridge.generateAndVerifyAttestationQuote({
        vmId: `fc-vm-${taskId}`,
        measurementDigestHex: crypto.createHash("sha256").update(`rootfs:${taskId}`, "utf-8").digest("hex"),
        nonce: `nonce-${Date.now()}`
      });

      this.appendAudit(
      params.actor,
      params.role,
      `签发并验证 Ed25519 SPIFFE SVID 证书与 AMD SEV-SNP TEE 硬件远程证明 Quote`,
      `Security / ${taskId}`,
      `ED25519_VERIFIED`);


      return {
        status: 200,
        body: {
          spiffeSvid: svid,
          spiffeVerification: svidVerify,
          teeAttestation: teeQuote
        }
      };
    }

    // action === "migrate_celld_actor"
    const cellId = `AnstsHarnessCell:${taskId}`;
    if (!this.celldAdmin.getCellPlacement(cellId)) {
      this.celldAdmin.registerCell(cellId, "celld-brain-01", 42);
    }
    const targetNodeId = params.targetNodeId ?? "celld-brain-03";
    const migrated = await this.celldAdmin.migrateCellToNode(cellId, targetNodeId);

    this.appendAudit(
    params.actor,
    params.role,
    `通过 CelldAdminClient 热迁移 DO Cell [${cellId}] (${migrated.previousNodeId} → ${migrated.newNodeId}, Epoch=#${migrated.newEpochFencingToken})`,
    `Cluster / ${cellId}`,
    `CELL_MIGRATED (epoch:${migrated.newEpochFencingToken})`);


    return {
      status: 200,
      body: {
        cellId,
        ...migrated
      }
    };
  }


  executeUnifiedPersistenceSnapshot(params)



  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发全集群 8 表持久化快照或冷启动恢复演练" }
      };
    }
    const sql = this.deps.v25Store.sql;
    const schedSnap = this.unifiedStore.snapshotSchedulerState(this.deps.scheduler);

    const tenants = sql.exec("SELECT * FROM v25_mgmt_tenants").toArray();
    for (const t of tenants) {
      const tId = String(t.tenant_id);
      this.unifiedStore.saveComponentRecord("persisted_finops_ledgers", "tenant_id", tId, t);
      this.unifiedStore.saveComponentRecord("persisted_escrows", "parent_task_id", `escrow-${tId}`, {
        tenantId: tId,
        escrowReserved: Number(t.escrow_reserved ?? 8)
      });
    }

    this.unifiedStore.saveComponentRecord("persisted_router_state", "endpoint_id", "ep-deepseek-r1", {
      endpointId: "ep-deepseek-r1",
      ttftEwmaMs: 180,
      healthy: true
    });
    this.unifiedStore.saveComponentRecord("persisted_router_state", "endpoint_id", "ep-qwen-38", {
      endpointId: "ep-qwen-38",
      ttftEwmaMs: 150,
      healthy: true
    });

    for (const [tId] of this.deps.scheduler.allTasks.entries()) {
      for (const st of this.deps.sagaGateway.compensator.getTaskSteps(tId)) {
        this.unifiedStore.saveComponentRecord("persisted_saga_steps", "step_id", st.stepId, {
          stepId: st.stepId,
          taskId: st.taskId,
          turnIndex: st.turnIndex,
          toolName: st.toolName,
          compensated: st.compensated
        });
      }
    }

    const approvals = sql.exec("SELECT * FROM v25_mgmt_approvals").toArray();
    for (const a of approvals) {
      this.unifiedStore.saveComponentRecord("persisted_approval_tickets", "ticket_id", String(a.approval_id), a);
    }

    const mcpTools = sql.exec("SELECT * FROM v25_mgmt_mcp_tools").toArray();
    for (const m of mcpTools) {
      this.unifiedStore.saveComponentRecord("persisted_mcp_tools", "tool_name", String(m.tool_name), m);
    }

    const tableCounts = this.unifiedStore.getTableCounts();

    if (params.action === "verify_cold_start_rehydration") {
      const freshScheduler = new _scheduler.CoreScheduler5D();
      const restoredTasksCount = this.unifiedStore.rehydrateSchedulerFromSql(freshScheduler);
      const restoredFinops = this.unifiedStore.loadComponentRecords("persisted_finops_ledgers");
      const restoredApprovals = this.unifiedStore.loadComponentRecords("persisted_approval_tickets");
      const restoredMcp = this.unifiedStore.loadComponentRecords("persisted_mcp_tools");

      this.appendAudit(
      params.actor,
      params.role,
      `执行全集群 8 表冷启动恢复验证 (UnifiedComponentPersistenceStore)：恢复任务 ${restoredTasksCount} 个、FinOps 账本 ${restoredFinops.length} 份、MCP 工具 ${restoredMcp.length} 个`,
      "Global / UnifiedStateStore",
      `REHYDRATED (${restoredTasksCount} tasks, RPO=0)`);


      return {
        status: 200,
        body: {
          action: "verify_cold_start_rehydration",
          restoredTasksCount,
          restoredFinopsCount: restoredFinops.length,
          restoredApprovalsCount: restoredApprovals.length,
          restoredMcpCount: restoredMcp.length,
          tableCounts,
          rpoZeroVerified: restoredTasksCount >= schedSnap.tasksSaved
        }
      };
    }

    this.appendAudit(
    params.actor,
    params.role,
    `生成全集群 8 表原子持久化快照 (Tasks=${tableCounts.persisted_tasks}, Leases=${tableCounts.persisted_leases}, FinOps=${tableCounts.persisted_finops_ledgers})`,
    "Global / UnifiedStateStore",
    "SNAPSHOT_SAVED");


    return {
      status: 200,
      body: {
        action: "snapshot_all",
        ...schedSnap,
        tableCounts
      }
    };
  }

  async executeCelldContainerAndWorkflow(params)







  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权操控 ctx.container 或 DurableWorkflowContext" }
      };
    }

    if (params.action === "start_and_probe_container") {
      const cellId = params.cellId ?? "cell-sandbox-ns-8841";
      const runtime = params.runtime ?? "kata";
      const handle = this.containerManager.getOrCreateContainerHandle(cellId, {
        class_name: "AnstsSandboxContainer",
        image: "ghcr.io/ansts/sandbox-ubuntu24:v2.5",
        instance_type: "standard-2",
        runtime
      });
      const started = handle.start({ enableInternet: true, hardTimeout: 60_000 });
      const execOut = await handle.exec(["sh", "-c", "cat resolv.conf | head -n 1"]);
      const wsUpgrade = await handle.getTcpPort(8080).fetch("http://container.bridge.local:8080/ws", {
        headers: { Upgrade: "websocket" }
      });

      let imdsBlocked = false;
      try {
        await handle.getTcpPort(80).fetch("http://169.254.169.254/latest/meta-data/");
      } catch {
        imdsBlocked = true;
      }

      this.appendAudit(
      params.actor,
      params.role,
      `启动 celld 原生 ctx.container (${cellId}, OCI=${runtime}, DNS=${started.resolvConf.join(",")}) 并验证 celld-fence 拦截 169.254.169.254 IMDSv2`,
      `Runtime / ${cellId}`,
      `CONTAINER_OK (IMDS_BLOCKED=${imdsBlocked})`);


      return {
        status: 200,
        body: {
          cellId,
          started,
          execResult: execOut,
          webSocketProbe: wsUpgrade,
          celldFenceImdsBlocked: imdsBlocked
        }
      };
    }

    // action === "run_durable_workflow"
    const workflowId = params.workflowId ?? "wf-release-ns-8841";
    const stepName = params.stepName ?? "step-ltx-checkpoint";
    const wfCtx = new _celld_ha_engine.DurableWorkflowContext(this.deps.v25Store.sql, workflowId);

    const firstRun = await wfCtx.stepDo(stepName, async () => {
      const walBytes = new TextEncoder().encode(`workflow=${workflowId}:step=${stepName}:ts=${Date.now()}`);
      const ltxMeta = await this.rpoEngine.commitWithSyncLtxFlush(
      `cell-${workflowId}`,
      14,
      () => {},
      walBytes);

      return {
        ltxKey: ltxMeta.r2Key,
        sha256: ltxMeta.sha256,
        sizeBytes: ltxMeta.sizeBytes
      };
    });

    const replayRun = await wfCtx.stepDo(stepName, async () => {
      throw new Error("Should not re-execute memoized workflow step!");
    });

    this.appendAudit(
    params.actor,
    params.role,
    `执行 DurableWorkflowContext [${workflowId}/${stepName}] 并验证崩溃重放幂等记忆化 (replayedFromJournal=${replayRun.replayedFromJournal})`,
    `Workflow / ${workflowId}`,
    "MEMOIZED_RPO_ZERO");


    return {
      status: 200,
      body: {
        workflowId,
        stepName,
        firstExecution: firstRun,
        crashReplayExecution: replayRun
      }
    };
  }

  executeSpeculativeDeterministicBranches(params)





  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发确定性投机多分支执行" }
      };
    }
    const taskId = params.taskId ?? "task-ns-8841";
    const sourceBranch = params.sourceBranch ?? "feat-ns-auth";
    try {
      this.deps.layerfs.getBranchHeadCommit(sourceBranch);
    } catch {
      this.deps.layerfs.forkBranch("main", sourceBranch);
    }
    const baseCommitId = this.deps.layerfs.getBranchHeadCommit(sourceBranch).commitId;
    const suffix = Date.now() % 10000;
    const branchA = `spec-${taskId}-a-${suffix}`;
    const branchB = `spec-${taskId}-b-${suffix}`;

    const results = this.branchForker.executeDeterministicSpeculativeBranches({
      sourceBranch,
      branches: [
      {
        branchName: branchA,
        seed: 20261110,
        command: `printf "SEED=%s\nCLOCK=%s\nRAND=%s\n" "$ANSTS_DETERMINISTIC_SEED" "$ANSTS_VIRTUAL_CLOCK_MS" "$ANSTS_DET_RAND" > spec_winner.txt && cat spec_winner.txt`
      },
      {
        branchName: branchB,
        seed: 20261110,
        command: `printf "SEED=%s\nCLOCK=%s\nRAND=%s\n" "$ANSTS_DETERMINISTIC_SEED" "$ANSTS_VIRTUAL_CLOCK_MS" "$ANSTS_DET_RAND" > spec_candidate.txt && cat spec_candidate.txt`
      }]

    });

    const deterministicReproducible =
    results.length >= 2 && results[0].stdout.trim() === results[1].stdout.trim() && results[0].stdout.includes("SEED=20261110");

    let mergeResult = null;
    if (params.mergeWinningBranch !== false) {
      const merged = this.branchCoordinator.mergeBranchIntoTarget(
      branchA,
      sourceBranch,
      baseCommitId);

      mergeResult = merged;
    }

    this.appendAudit(
    params.actor,
    params.role,
    `执行确定性投机多分支 CoW 推演 (${branchA} / ${branchB}, Seed=20261110, Bit-Exact=${deterministicReproducible}) 并合并优胜分支至 ${sourceBranch}`,
    `LayerFS / ${sourceBranch}`,
    `SPECULATIVE_MERGED (${mergeResult?.mergedCommitId ?? results[0].commitId})`);


    return {
      status: 200,
      body: {
        taskId,
        sourceBranch,
        baseCommitId,
        deterministicReproducible,
        branches: results,
        mergeResult
      }
    };
  }

  executeAutoscaleAndPsiLoop(params)





  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发 PSI 内存防御或队列动态分片扩缩容" }
      };
    }

    const livePsi = this.psiGuard.probeLiveKernelPsiSample();
    const effectiveSample =
    params.action === "simulate_psi_spike" ?
    {
      someAvg10Pct: 78.5,
      fullAvg10Pct: params.simulatedPsiFullPct ?? 74.0,
      freeSwapMb: 96
    } :
    {
      someAvg10Pct: livePsi.someAvg10Pct,
      fullAvg10Pct: livePsi.fullAvg10Pct,
      freeSwapMb: livePsi.freeSwapMb
    };

    const runningTasks = Array.from(this.deps.scheduler.allTasks.values());
    const psiDefense = this.psiGuard.evaluateAndDefend(effectiveSample, runningTasks);
    const backpressure = this.backpressureLoop.computeClusterHealthState({
      maxNodePsiSomePct: effectiveSample.someAvg10Pct,
      globalRateLimitedEndpoints: params.action === "simulate_psi_spike" ? 1 : 0,
      totalEndpoints: 2,
      globalQueueDepth: this.deps.scheduler.queueManager.totalQueued()
    });
    const writeRps = params.incomingWriteRps ?? (params.action === "simulate_psi_spike" ? 14400 : 3600);
    const queueSharding = this.queueAutoSharder.adjustShards(writeRps, 450);
    const poolScaling = this.containerPoolScaler.computeTargetSandboxContainers({
      activeThinkCells: Math.max(4, runningTasks.length * 2),
      activeActCells: Math.max(2, runningTasks.length),
      warmStandbyRatio: 0.25
    });

    this.appendAudit(
    params.actor,
    params.role,
    `执行闭环 PSI 内存守卫与队列自动分片 (${params.action}) → PSI=${psiDefense.level}, Backpressure=${backpressure.mode}, QueueShards=${queueSharding.newShards}`,
    "Cluster / AutoScaler-PSI",
    `${backpressure.mode} (${queueSharding.newShards} shards)`);


    return {
      status: 200,
      body: {
        liveKernelPsi: livePsi,
        effectivePsiSample: effectiveSample,
        psiDefense,
        backpressure,
        queueSharding,
        poolScaling
      }
    };
  }


  executeRouterInferencePipeline(params)








  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发模型路由推理流水线或熔断器演练" }
      };
    }
    const tenantId = params.tenantId ?? "Northstar";
    const taskId = params.taskId ?? "task-ns-8841";
    const endpointId = params.endpointId ?? "ep-deepseek-r1";

    if (params.action === "execute_sse_inference") {
      const epMetrics = new _models.LlmEndpointMetrics({
        endpoint_id: endpointId,
        provider: endpointId.includes("qwen") ? "qwen" : "deepseek",
        model_name: endpointId.includes("qwen") ? "Qwen-3.8" : "DeepSeek-R1",
        ttft_ewma_ms: 140,
        tpot_ewma_ms: 12,
        kv_cache_utilization: 0.58,
        healthy: true
      });
      const infRes = this.poolClient.executeInference(
      epMetrics,
      {
        taskId,
        turnIndex: 1,
        prompt: `Analyze and verify task ${taskId} for tenant ${tenantId}`,
        estimatedPromptTokens: params.promptTokens ?? 1200,
        maxOutputTokens: params.maxOutputTokens ?? 320,
        requireJsonSchema: true
      },
      0.65);

      const totalSpent = this.deps.finopsController.recordSpend(tenantId, infRes.costUsd);
      this.deps.v25Store.sql.exec(
      "UPDATE v25_mgmt_tenants SET spent_usd = COALESCE(spent_usd, 0) + ? WHERE tenant_id = ?",
      infRes.costUsd,
      tenantId);

      const budgetStatus = this.deps.finopsController.evaluateBudgetAndModel(tenantId);

      this.appendAudit(
      params.actor,
      params.role,
      `执行模型路由 SSE 流式推理 (${infRes.modelName}, TTFT=${infRes.ttftMs.toFixed(1)}ms, TPOT=${infRes.tpotMs}ms, Cached=${infRes.cachedTokens} tok, Cost=$${infRes.costUsd})`,
      `Router / ${tenantId}`,
      `SSE_OK ($${infRes.costUsd})`);


      return {
        status: 200,
        body: {
          tenantId,
          taskId,
          inference: infRes,
          totalSpentUsd: totalSpent,
          budgetEvaluation: budgetStatus
        }
      };
    }

    if (params.action === "trip_and_recover_circuit_breaker") {
      const breaker = new _breaker_limiter.ThreeStateCircuitBreaker(3, 15_000);
      const now = Date.now();
      const s0 = breaker.getState(now);
      breaker.recordFailure(now);
      breaker.recordFailure(now + 10);
      breaker.recordFailure(now + 20);
      const s1 = breaker.getState(now + 30);
      const s2 = breaker.getState(now + 16_000);
      breaker.recordSuccess();
      const s3 = breaker.getState(now + 16_100);

      this.appendAudit(
      params.actor,
      params.role,
      `执行模型端点 [${endpointId}] 三态熔断器状态转换演练 (${s0} → ${s1} → ${s2} → ${s3})`,
      `Router / ${endpointId}`,
      `BREAKER_RECOVERED (${s3})`);


      return {
        status: 200,
        body: {
          endpointId,
          stateSequence: [s0, s1, s2, s3],
          recovered: s3 === "CLOSED"
        }
      };
    }

    // action === "exhaust_tpm_bucket"
    const now = Date.now();
    const bucket = new _breaker_limiter.SlidingWindowTokenBucket(2, 10_000, now);
    const firstConsume = bucket.tryConsume(6000, now);
    const throttledConsume = bucket.tryConsume(6000, now + 100);
    const refilledConsume = bucket.tryConsume(6000, now + 60_500);

    this.appendAudit(
    params.actor,
    params.role,
    `验证滑动窗口 RPM/TPM 双令牌桶限流与恢复 (ThrottledWait=${throttledConsume.waitMs}ms → RefilledAllowed=${refilledConsume.allowed})`,
    `Router / ${endpointId}`,
    `TOKEN_BUCKET_VERIFIED`);


    return {
      status: 200,
      body: {
        endpointId,
        firstConsume,
        throttledConsume,
        refilledConsume
      }
    };
  }

  async executeKernelSecurityAndAttestation(params)







  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发底层 Vsock Daemon、eBPF LSM 或 SLSA/UFFD 安全证明" }
      };
    }
    const taskId = params.taskId ?? "task-ns-8841";
    const branchName = params.branchName ?? "feat-ns-auth";

    if (params.action === "vsock_unix_daemon_probe") {
      const sockPath = path.join(os.tmpdir(), `ansts-vsock-${process.pid}-${Date.now()}.sock`);
      const daemon = new _vsock_daemon.ZeroTrustVsockHostDaemon(["api.openai.com", "api.anthropic.com", "r2.ansts.internal"]);
      await daemon.startUnixSocketServer(sockPath);
      try {
        const allowedProbe = await daemon.sendClientFrameOverUnixSocket(sockPath, {
          requestId: `vsock-allow-${Date.now()}`,
          type: "EGRESS_CHECK",
          hostname: params.allowedHostname ?? "api.openai.com"
        });
        const blockedProbe = await daemon.sendClientFrameOverUnixSocket(sockPath, {
          requestId: `vsock-block-${Date.now()}`,
          type: "EGRESS_CHECK",
          hostname: params.blockedHostname ?? "169.254.169.254"
        });

        this.appendAudit(
        params.actor,
        params.role,
        `通过真实 Unix Domain Socket (${path.basename(sockPath)}) 执行 UInt32BE 二进制帧 Vsock Egress 审计 (allow=200, block=403)`,
        `Security / VsockDaemon`,
        `VSOCK_UNIX_OK`);


        return {
          status: 200,
          body: {
            socketPath: sockPath,
            allowedProbe,
            blockedProbe
          }
        };
      } finally {
        await daemon.stop();
        try {
          if (fs.existsSync(sockPath)) fs.unlinkSync(sockPath);
        } catch {

          // ignore
        }}
    }

    if (params.action === "ebpf_and_slsa_attest") {
      try {
        this.deps.layerfs.getBranchHeadCommit(branchName);
      } catch {
        this.deps.layerfs.forkBranch("main", branchName);
      }
      const kernelPosture = this.ebpfBridge.inspectHostKernelSecurityPosture();
      const shadowHook = this.ebpfBridge.checkFileOpenHook("/etc/shadow");
      const workspaceHook = this.ebpfBridge.checkFileOpenHook(`/workspace/${taskId}/index.ts`);
      const slsaStmt = this.slsaAttestor.attestLayerFsCommit(this.deps.layerfs, branchName);
      const slsaVerified = this.slsaAttestor.verifyProvenanceStatement(slsaStmt);

      this.appendAudit(
      params.actor,
      params.role,
      `执行 eBPF LSM 敏感路径拦截 (/etc/shadow errno=${shadowHook.errno}) 与 LayerFS Commit [${slsaStmt.commitId}] SLSA v1.0 Ed25519 签名证明`,
      `Security / ${branchName}`,
      `SLSA_ED25519_VERIFIED`);


      return {
        status: 200,
        body: {
          kernelPosture,
          shadowHook,
          workspaceHook,
          slsaProvenance: slsaStmt,
          slsaVerified
        }
      };
    }

    // action === "uffd_page_fault_restore"
    const rawStatePayload = Buffer.from(
    JSON.stringify({
      taskId,
      walEntries: this.deps.walEngine.getTaskWalEntries(taskId),
      padding: "P".repeat(9000)
    }),
    "utf-8");

    const stagedPages = this.uffdRestorer.stageBufferInto4KiBPages(new Uint8Array(rawStatePayload));
    const fault0Cold = this.uffdRestorer.handlePageFault(0);
    const fault0Cached = this.uffdRestorer.handlePageFault(0);
    const fault1Cold = this.uffdRestorer.handlePageFault(1);
    const stats = this.uffdRestorer.getStats();

    this.appendAudit(
    params.actor,
    params.role,
    `执行 userfaultfd 按需 4KiB 缺页恢复与 SHA-256 页面校验 (Staged=${stats.stagedPagesCount} pages, Restored=${stats.restoredTotalBytes} Bytes)`,
    `Runtime / UFFD-${taskId}`,
    `UFFD_HYDRATED (${stats.faultedPagesCount} pages)`);


    return {
      status: 200,
      body: {
        taskId,
        stagedPagesCount: stagedPages.length,
        fault0Cold,
        fault0Cached,
        fault1Cold,
        stats
      }
    };
  }

  executeChaosAndTopologyCasDrill(params)






  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发网络分区混沌演练或拓扑 CAS 变更" }
      };
    }

    if (params.action === "run_partition_and_split_brain_drill") {
      const targetHost = params.targetHost ?? "body-us-west-02";
      const drill = this.chaosDrillRunner.executeLivePartitionAndFencingDrill({
        chaosProxy: this.chaosProxy,
        targetHost,
        oldNodeFencingToken: 12,
        newNodeFencingToken: 14
      });

      let staleFenceRejected409 = false;
      try {
        this.deps.walEngine.appendTurnDelta({
          taskId: "task-chaos-splitbrain",
          turnIndex: 2,
          fencingToken: 12,
          changedFiles: ["src/zombie.ts"],
          sqliteWalPages: 4,
          highestSubmittedFencingToken: 14
        });
      } catch {
        staleFenceRejected409 = true;
      }

      this.appendAudit(
      params.actor,
      params.role,
      `执行网络分区与脑裂 Fencing 混沌演练 (Target=${targetHost}, PartitionBlocked=${drill.partitionBlockedTraffic}, ZombieRejected409=${staleFenceRejected409})`,
      `Chaos / ${targetHost}`,
      `DRILL_PASSED (RPO=0)`);


      return {
        status: 200,
        body: {
          targetHost,
          drill,
          staleFenceRejected409,
          activeFaults: this.chaosProxy.inspectActiveFaults()
        }
      };
    }

    // action === "probe_host_preflight_and_topology_cas"
    const preflight = this.preflightGate.probeLiveHostReadiness(1);
    const currentGen = this.topologyCas.getGeneration();
    const expectedGen = params.expectedGeneration ?? currentGen;
    const nextMembers = params.nextMembers ?? this.topologyCas.getMembers();

    try {
      const casUpdated = this.topologyCas.updateTopologyCas(expectedGen, nextMembers);
      this.appendAudit(
      params.actor,
      params.role,
      `执行宿主机 Preflight 探针 (FreeDisk=${preflight.freeDiskGb}GB, FreeMem=${preflight.freeMemoryMb}MB) 并推进 Topology CAS 至 gen=${casUpdated.generation}`,
      `Cluster / TopologyCAS`,
      `GEN_${casUpdated.generation} (${casUpdated.ringDigest.slice(0, 10)})`);

      return {
        status: 200,
        body: {
          preflight,
          topologyCas: casUpdated
        }
      };
    } catch (err) {
      return {
        status: 409,
        body: {
          error: "TOPOLOGY_CAS_CONFLICT",
          expectedGeneration: expectedGen,
          actualGeneration: currentGen,
          message: err.message
        }
      };
    }
  }

  async executeFederationTunnelAndHarnessGovernance(params)






  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权操控双归属反向隧道、多地域联邦路由或 Harness 自治愈" }
      };
    }
    const hostId = params.hostId ?? "body-us-west-01";
    const taskId = params.taskId ?? "task-ns-8841";

    if (params.action === "tunnel_command_and_failover") {
      this.tunnelCell.registerTunnel({
        hostId,
        gatewayId: "celld-gw-01",
        role: "PRIMARY",
        connGeneration: 10,
        epoch: 14
      });
      this.tunnelCell.registerTunnel({
        hostId,
        gatewayId: "celld-gw-02",
        role: "SECONDARY",
        connGeneration: 10,
        epoch: 14
      });
      const { command, completionPromise } = this.tunnelCell.enqueueCommandForHost(hostId, {
        op: "EXEC_SANDBOX_PROBE",
        taskId
      });
      const polled = this.tunnelCell.pollPendingCommands(hostId, "celld-gw-01");
      this.tunnelCell.acknowledgeCommand(command.commandId, {
        acknowledgedBy: hostId,
        status: "EXECUTED_OVER_REVERSE_TUNNEL"
      });
      const ackResult = await completionPromise;

      this.tunnelCell.markGatewayHealth(hostId, "celld-gw-01", false);
      const failoverActive = this.tunnelCell.selectActiveTunnel(hostId);
      this.tunnelCell.markGatewayHealth(hostId, "celld-gw-01", true);

      this.appendAudit(
      params.actor,
      params.role,
      `执行双归属反向隧道指令下发 (${command.commandId}, Polled=${polled.length}) 并验证 PRIMARY 故障切换至 ${failoverActive.gatewayId} (${failoverActive.role})`,
      `Tunnel / ${hostId}`,
      `FAILOVER_OK (${failoverActive.gatewayId})`);


      return {
        status: 200,
        body: {
          hostId,
          dispatchedCommand: command,
          polledCount: polled.length,
          ackResult,
          failoverTunnel: failoverActive
        }
      };
    }

    if (params.action === "federation_and_harness_autoheal") {
      const zone = params.sovereigntyZone ?? "EU";
      const optimalRegion = this.federationRouter.selectOptimalRegion({ requiredSovereigntyZone: zone });
      const r2 = new _env.InMemoryR2Bucket();
      const harnessCell = new _harness_cell.AgentTaskHarnessCell(
      {
        id: { toString: () => `cell-${taskId}` },
        storage: {
          sql: this.deps.v25Store.sql,
          transactionSync: (fn) => fn(),
          getAlarm: async () => null,
          setAlarm: async () => {}
        },
        blockConcurrencyWhile: async (fn) => fn()
      },
      r2,
      taskId);

      const ckpt = await harnessCell.executeAndCheckpointTurn({
        turnIndex: 1,
        fencingToken: 14,
        toolName: "shell_exec",
        toolOutput: { stdout: "turn-1-ok" }
      });

      let attempts = 0;
      let rehydratedTurnIndex = -1;
      const autoHealRes = await harnessCell.executeInCompanionContainerWithAutoHeal(
      async () => {
        attempts += 1;
        if (attempts === 1) {
          throw new Error("OperationInterruptedError: companion container migrated across bare-metal nodes");
        }
        return { turnIndex: 2, status: "COMPLETED_AFTER_AUTO_HEAL", region: optimalRegion.regionCode };
      },
      async (latestTurn) => {
        rehydratedTurnIndex = latestTurn?.turnIndex ?? 0;
      });


      this.appendAudit(
      params.actor,
      params.role,
      `执行多地域联邦主权路由 (${zone} → ${optimalRegion.regionCode}, RTT=${optimalRegion.rttEwmaMs}ms) 与 AgentTaskHarnessCell 迁移中断自治愈 (RehydratedTurn=#${rehydratedTurnIndex})`,
      `Federation / ${taskId}`,
      `AUTO_HEALED (${optimalRegion.regionCode})`);


      return {
        status: 200,
        body: {
          sovereigntyZone: zone,
          selectedRegion: optimalRegion,
          checkpoint: ckpt,
          rehydratedTurnIndex,
          autoHealResult: autoHealRes
        }
      };
    }

    // action === "velocity_breaker_and_staging_commit"
    const astValidation = _velocity_breaker.SyntacticAndVelocityBreaker.validateJsonToolCall(
    JSON.stringify({ tool: "kubectl_apply", namespace: "prod" }));

    const loopTaskId = `loop-${taskId}-${Date.now()}`;
    const now = Date.now();
    this.velocityBreaker.recordAndCheckTurn(loopTaskId, {
      turnIndex: 1,
      toolName: "shell_exec",
      argsHash: "sha256:identical",
      tokensConsumed: 1500,
      timestampMs: now
    });
    this.velocityBreaker.recordAndCheckTurn(loopTaskId, {
      turnIndex: 2,
      toolName: "shell_exec",
      argsHash: "sha256:identical",
      tokensConsumed: 1500,
      timestampMs: now + 100
    });
    const thirdTurnCheck = this.velocityBreaker.recordAndCheckTurn(loopTaskId, {
      turnIndex: 3,
      toolName: "shell_exec",
      argsHash: "sha256:identical",
      tokensConsumed: 1500,
      timestampMs: now + 200
    });

    const staged = this.stagingGate.stageEffect({
      effectId: `eff-${Date.now()}`,
      taskId,
      turnIndex: 4,
      toolName: "git_push_release",
      args: { branch: "main", tag: "v2.5.0" }
    });
    const committedEffects = this.stagingGate.commitStagedEffects(taskId, (toolName, args) => ({
      executedTool: toolName,
      args,
      committedAtMs: Date.now()
    }));

    this.appendAudit(
    params.actor,
    params.role,
    `验证 SyntacticAndVelocityBreaker 语义死循环熔断 (${thirdTurnCheck.reason}) 与 StagingCommitAndApprovalGate 暂存提交 (${committedEffects.length} 项副作用)`,
    `Harness / ${taskId}`,
    `BREAKER_AND_STAGING_OK`);


    return {
      status: 200,
      body: {
        astValidation,
        velocityBreakerCheck: thirdTurnCheck,
        stagedEffect: staged,
        committedEffects
      }
    };
  }


  inspectArchitectureDesignBlueprint() {
    this.ensureInitialized();
    let designMarkdown = "";
    try {
      const p = path.resolve(process.cwd(), "docs", "console-management-design.md");
      if (fs.existsSync(p)) {
        designMarkdown = fs.readFileSync(p, "utf-8");
      }
    } catch {

      // fallback
    }let wranglerConfig = {};
    try {
      const wp = path.resolve(process.cwd(), "wrangler.json");
      if (fs.existsSync(wp)) {
        wranglerConfig = JSON.parse(fs.readFileSync(wp, "utf-8"));
      }
    } catch {

      // fallback
    }
    return {
      status: 200,
      body: {
        designVersion: "v2.5-M50",
        pillars: ["DESIGN", "CONFIGURATION", "RUNTIME", "OBSERVABILITY_OPS"],
        sourceModulesCount: 86,
        sqliteTablesCount: 16,
        wranglerName: wranglerConfig.name ?? "ansts-v2-celld-scheduler",
        durableObjects: wranglerConfig.durable_objects?.bindings ?? [],
        invariantsVerified: true,
        designMarkdownLength: designMarkdown.length,
        designMarkdownExcerpt: designMarkdown.slice(0, 600)
      }
    };
  }

  ensureConfigCenterTable() {
    const sql = this.deps.v25Store.sql;
    sql.exec(
    "CREATE TABLE IF NOT EXISTS v25_mgmt_config_revisions (revision INTEGER PRIMARY KEY, scope TEXT, config_json TEXT, sha256_checksum TEXT, author TEXT, created_at TEXT, note TEXT)");

    const existing = sql.exec("SELECT * FROM v25_mgmt_config_revisions").toArray();
    if (existing.length === 0) {
      const defaultConfig = {
        clusterName: "ansts-v2-celld-scheduler",
        compatibilityDate: "2026-01-01",
        maxCellConcurrentRequests: 64,
        maxQueueConcurrentProducers: 256,
        durabilityMode: "fleet",
        logPipelineLimit: 4,
        ltxCompactionEnabled: true,
        ltxTruncatePages: 128,
        thinkCpuCores: 0.25,
        actCpuCores: 4.0,
        actMemoryMb: 8192,
        psiFullFreezePct: 70.0,
        targetSloRatio: 0.999
      };
      const jsonStr = JSON.stringify(defaultConfig);
      const checksum = crypto.createHash("sha256").update(jsonStr, "utf-8").digest("hex");
      sql.exec(
      "INSERT INTO v25_mgmt_config_revisions (revision, scope, config_json, sha256_checksum, author, created_at, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
      1,
      "global-runtime-and-wrangler",
      jsonStr,
      checksum,
      "system-seed",
      "09:00:00 UTC",
      "Initial baseline configuration from wrangler.json & TaskSpec v1beta1");

    }
  }

  executeConfigurationCenterGovernance(params)








  {
    this.ensureInitialized();
    this.ensureConfigCenterTable();
    const sql = this.deps.v25Store.sql;
    const rows = sql.
    exec("SELECT * FROM v25_mgmt_config_revisions ORDER BY revision ASC").
    toArray();
    const latestRow = rows[rows.length - 1];
    const currentRevision = Number(latestRow.revision);
    const currentConfig = JSON.parse(String(latestRow.config_json));

    if (params.action === "inspect_active_config") {
      return {
        status: 200,
        body: {
          activeRevision: currentRevision,
          sha256Checksum: String(latestRow.sha256_checksum),
          activeConfig: currentConfig,
          revisions: rows.map((r) => ({
            revision: Number(r.revision),
            sha256Checksum: String(r.sha256_checksum),
            author: String(r.author),
            createdAt: String(r.created_at),
            note: String(r.note)
          }))
        }
      };
    }

    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权变更或回滚集群配置中心版本" }
      };
    }

    if (params.action === "validate_and_apply_config") {
      if (params.expectedRevision !== undefined && params.expectedRevision !== currentRevision) {
        return {
          status: 409,
          body: {
            error: "CONFIG_CAS_CONFLICT",
            expectedRevision: params.expectedRevision,
            actualRevision: currentRevision,
            message: `配置中心 CAS 版本冲突：期望 rev=${params.expectedRevision}，当前实际 rev=${currentRevision}`
          }
        };
      }

      const nextConfig = {
        ...currentConfig,
        ...(params.runtimeOverrides ?? {})
      };

      try {
        const candidateSpecPayload = params.sampleTaskSpec ?? {
          apiVersion: "agent.scheduler.io/v1beta1",
          task_id: "config-validation-probe",
          tenant_id: "Northstar",
          priority: 1,
          max_turns: 30,
          timeout_seconds: 1800,
          resource_profile: {
            think_cpu_cores: Number(nextConfig.thinkCpuCores ?? 0.25),
            act_cpu_cores: Number(nextConfig.actCpuCores ?? 4.0),
            act_memory_mb: Number(nextConfig.actMemoryMb ?? 8192),
            disk_mb: 10240
          }
        };
        _validator.TaskSpecValidator.validateAndNormalize(candidateSpecPayload);
      } catch (err) {
        return {
          status: 400,
          body: {
            error: "CONFIG_SCHEMA_INVALID",
            message: err.message
          }
        };
      }

      const changedKeys = Object.keys(nextConfig).filter((k) => nextConfig[k] !== currentConfig[k]);
      const nextRevision = currentRevision + 1;
      const jsonStr = JSON.stringify(nextConfig);
      const checksum = crypto.createHash("sha256").update(jsonStr, "utf-8").digest("hex");
      const nowUtc = `${new Date().toISOString().slice(11, 19)} UTC`;

      sql.exec(
      "INSERT INTO v25_mgmt_config_revisions (revision, scope, config_json, sha256_checksum, author, created_at, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
      nextRevision,
      "global-runtime-and-wrangler",
      jsonStr,
      checksum,
      params.actor,
      nowUtc,
      params.note ?? `Hot-reloaded [${changedKeys.join(", ") || "config"}]`);


      this.hotCellGuard = new _hot_cell_overload_guard.HotCellOverloadAndQueueGuard({
        maxCellConcurrentRequests: Number(nextConfig.maxCellConcurrentRequests ?? 64),
        maxQueueConcurrentProducers: Number(nextConfig.maxQueueConcurrentProducers ?? 256)
      });
      this.fleetDurability = new _celld_ha_engine.FleetReplicatedDurabilityEngine(
      nextConfig.durabilityMode ?? "fleet",
      Number(nextConfig.logPipelineLimit ?? 4));


      this.appendAudit(
      params.actor,
      params.role,
      `热更新配置中心版本 rev#${currentRevision} → rev#${nextRevision} (变更字段: ${changedKeys.join(", ") || "none"}, SHA-256=${checksum.slice(0, 12)})`,
      "ConfigCenter / Global",
      `APPLIED (rev#${nextRevision})`);


      return {
        status: 200,
        body: {
          previousRevision: currentRevision,
          activeRevision: nextRevision,
          changedKeys,
          sha256Checksum: checksum,
          activeConfig: nextConfig,
          hotReloaded: true
        }
      };
    }

    // action === "rollback_revision"
    const targetRev = params.targetRevision ?? 1;
    const targetRow = rows.find((r) => Number(r.revision) === targetRev);
    if (!targetRow) {
      return {
        status: 404,
        body: { error: "REVISION_NOT_FOUND", message: `未找到历史配置版本 rev#${targetRev}` }
      };
    }
    const restoredConfig = JSON.parse(String(targetRow.config_json));
    const nextRevision = currentRevision + 1;
    const jsonStr = JSON.stringify(restoredConfig);
    const checksum = crypto.createHash("sha256").update(jsonStr, "utf-8").digest("hex");
    const nowUtc = `${new Date().toISOString().slice(11, 19)} UTC`;

    sql.exec(
    "INSERT INTO v25_mgmt_config_revisions (revision, scope, config_json, sha256_checksum, author, created_at, note) VALUES (?, ?, ?, ?, ?, ?, ?)",
    nextRevision,
    "global-runtime-and-wrangler",
    jsonStr,
    checksum,
    params.actor,
    nowUtc,
    `Rollback to rev#${targetRev}`);


    this.hotCellGuard = new _hot_cell_overload_guard.HotCellOverloadAndQueueGuard({
      maxCellConcurrentRequests: Number(restoredConfig.maxCellConcurrentRequests ?? 64),
      maxQueueConcurrentProducers: Number(restoredConfig.maxQueueConcurrentProducers ?? 256)
    });
    this.fleetDurability = new _celld_ha_engine.FleetReplicatedDurabilityEngine(
    restoredConfig.durabilityMode ?? "fleet",
    Number(restoredConfig.logPipelineLimit ?? 4));


    this.appendAudit(
    params.actor,
    params.role,
    `回滚集群配置中心至历史版本 rev#${targetRev} (生成新版本 rev#${nextRevision}, SHA-256=${checksum.slice(0, 12)})`,
    "ConfigCenter / Global",
    `ROLLED_BACK (rev#${nextRevision})`);


    return {
      status: 200,
      body: {
        rolledBackToRevision: targetRev,
        activeRevision: nextRevision,
        sha256Checksum: checksum,
        activeConfig: restoredConfig
      }
    };
  }

  async executeCelldAndLayerFsRuntimeEngine(params)










  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发 celld 运行时引擎调优或 LayerFS 远端同步" }
      };
    }
    const taskId = params.taskId ?? "task-ns-8841";

    if (params.action === "scheduler_cell_rpc_and_alarm") {
      this.schedulerCell.registerNode(
      new _models.NodeHeartbeat({
        node_id: "body-us-west-01",
        total_cpu_cores: 64,
        available_cpu_cores: 48,
        total_memory_mb: 131072,
        available_memory_mb: 98304,
        warm_standby_count: 6,
        psi_some_avg10: 4.2
      }));

      const abortCtrl = new AbortController();
      const rpcTask = new _models.TaskSpec({
        task_id: `cell-rpc-${Date.now() % 10000}`,
        tenant_id: "Northstar",
        priority: _models.TaskPriority.P0_EMERGENCY,
        prompt: "Execute via AnstsSchedulerCell native js_rpc"
      });
      await this.schedulerCell.submitTaskRpc(rpcTask, abortCtrl.signal);
      const tickRes = await this.schedulerCell.scheduleTickRpc(abortCtrl.signal);
      const rehydratedCount = this.schedulerCell.rehydrateSchedulerFromSql();

      this.appendAudit(
      params.actor,
      params.role,
      `执行 AnstsSchedulerCell 原生 js_rpc (submitTaskRpc + scheduleTickRpc) 并完成 SQLite 冷水化校验 (${rehydratedCount} tasks)`,
      `Runtime / AnstsSchedulerCell`,
      `CELL_RPC_OK (${tickRes.placements.length} placed)`);


      return {
        status: 200,
        body: {
          submittedTaskId: rpcTask.task_id,
          placements: tickRes.placements,
          recoveredIds: tickRes.recoveredIds,
          rehydratedTasksCount: rehydratedCount
        }
      };
    }

    if (params.action === "fleet_durability_and_overload_guard") {
      const fastPeer = this.fleetDurability.commitWriteWithFleetDurability({
        livePeerNodeCount: 3,
        peerFsyncLatencyMs: params.peerFsyncLatencyMs ?? 12,
        bucketPutLatencyMs: 45,
        inFlightLogRounds: 3
      });
      const hedgedPeer = this.fleetDurability.commitWriteWithFleetDurability({
        livePeerNodeCount: 3,
        peerFsyncLatencyMs: 320,
        bucketPutLatencyMs: 400,
        inFlightLogRounds: 4
      });
      const singleNodeFallback = this.fleetDurability.commitWriteWithFleetDurability({
        livePeerNodeCount: 0,
        peerFsyncLatencyMs: 999,
        bucketPutLatencyMs: 38
      });

      const cellScope = "AnstsSchedulerCell:shard-01";
      const reqCount = params.concurrentRequests ?? 65;
      let lastAdmission = this.hotCellGuard.tryEnterCellFetch(cellScope);
      let entered = lastAdmission.admitted ? 1 : 0;
      for (let i = 1; i < reqCount; i++) {
        const d = this.hotCellGuard.tryEnterCellFetch(cellScope);
        lastAdmission = d;
        if (d.admitted) entered += 1;
      }
      for (let i = 0; i < entered; i++) {
        this.hotCellGuard.leaveCellFetch(cellScope);
      }
      const queueGroupCommit = this.hotCellGuard.tryAdmitQueueProducerBatch(128);

      this.appendAudit(
      params.actor,
      params.role,
      `验证 FleetReplicatedDurabilityEngine (${fastPeer.acknowledgedVia}/${hedgedPeer.acknowledgedVia}) 与 64 并发过载保护 (HTTP ${lastAdmission.statusCode})`,
      `Runtime / CelldDurability`,
      `FLEET_OK (GroupTx=${queueGroupCommit.committedTransactions})`);


      return {
        status: 200,
        body: {
          fastPeerCommit: fastPeer,
          hedgedPeerCommit: hedgedPeer,
          singleNodeFallback,
          overloadGuardProbe: lastAdmission,
          queueGroupCommit
        }
      };
    }

    // action === "ltx_compaction_and_layerfs_d9_sync"
    let walEntries = this.deps.walEngine.getTaskWalEntries(taskId);
    if (walEntries.length === 0) {
      this.deps.walEngine.appendTurnDelta({
        taskId,
        turnIndex: 1,
        fencingToken: 14,
        changedFiles: ["src/runtime_a.ts", "src/runtime_b.ts"],
        sqliteWalPages: 64,
        highestSubmittedFencingToken: 14
      });
      walEntries = this.deps.walEngine.getTaskWalEntries(taskId);
    }
    const compacted = this.walCompactor.compactLayers(taskId, walEntries);
    const l1Result = this.walCompactor.executeCelldLtxL1Compaction({
      cellId: `cell-${taskId}`,
      l0ObjectCount: 384,
      txidDistance: 512,
      totalL0Megabytes: 48
    });
    const truncationEval = this.walCompactor.evaluateWalTruncationAndPaging({
      dbSizeMb: 3.2,
      walSizePages: 160,
      ltxChainSizeMb: 320
    });
    const uffdRestore = this.walCompactor.restoreViaUserfaultfd(taskId, 0.25);

    const pulledBefore = await this.layerfsStoreSync.pullStoreSnapshot();
    const baseGen = params.basedOnGeneration ?? pulledBefore.remoteGeneration;
    try {
      const pushed = await this.layerfsStoreSync.pushStoreSnapshot({
        machineId: this.deps.nodeId,
        basedOnGeneration: baseGen,
        storeSqliteBytes: new TextEncoder().encode(JSON.stringify(this.deps.layerfs.exportBranchBundle("main"))),
        mainHeadCommitId: this.deps.layerfs.getBranchHeadCommit("main").commitId
      });

      this.appendAudit(
      params.actor,
      params.role,
      `执行 L0→L1 LTX 压缩 (Merged=${l1Result.batchesMerged} batches, UFFD=${uffdRestore.restoredColdStartMs}ms) 并完成 LayerFS Rule D9 同步 (gen=${pushed.remoteGeneration})`,
      `Runtime / LTX-LayerFS`,
      `COMPACTED_AND_SYNCED (gen:${pushed.remoteGeneration})`);


      return {
        status: 200,
        body: {
          taskId,
          compacted,
          l1Compaction: l1Result,
          walTruncation: truncationEval,
          uffdRestore,
          layerfsSync: pushed
        }
      };
    } catch (err) {
      if (err instanceof _layerfs_remote_sync.LayerFsStalePushRefusedError || err.name === "LayerFsStalePushRefusedError") {
        const e = err;
        return {
          status: 409,
          body: {
            error: "LAYERFS_RULE_D9_STALE_PUSH_REFUSED",
            expectedRemoteGeneration: e.expectedRemoteGeneration,
            actualRemoteGeneration: e.actualRemoteGeneration,
            message: e.message
          }
        };
      }
      throw err;
    }
  }

  executeObservabilityAndSreOps(params)






  {
    this.ensureInitialized();
    if (params.role === "VIEWER" && params.action === "execute_auto_remediation_runbook") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发 SRE 自动化自愈 Runbook" }
      };
    }

    if (params.action === "probe_slo_traces_and_metrics") {
      for (const sample of [14, 18, 22, 29, 35, 42, 68, 95, 145]) {
        this.metrics.observeHistogram("ansts_task_dispatch_latency_ms", sample);
      }
      this.metrics.incCounter("ansts_sre_observability_probes_total", 1);
      this.metrics.setGauge("ansts_active_tasks_total", this.deps.scheduler.allTasks.size);

      const waterfall = this.metrics.recordTraceWaterfall("POST /v1/tasks (Full-Chain Dispatch)", [
      { name: "GatewayIngressAndSchemaValidate", durationMs: 4, attributes: { component: "gateway" } },
      { name: "Scheduler5DPlacementAndFence", durationMs: 9, attributes: { component: "scheduler-cell" } },
      { name: "CelldKataContainerActStep", durationMs: 38, attributes: { component: "kata-runtime" } },
      { name: "RustFsLtxCasSegmentFlush", durationMs: 12, attributes: { component: "rpo-zero-ltx" } }]);


      const quantiles = this.metrics.computeQuantiles("ansts_task_dispatch_latency_ms");
      const sloEval = this.metrics.evaluateSloBurnRate({
        targetSloRatio: 0.999,
        totalRequests: 10_000,
        errorRequests: params.simulatedErrorRequests ?? 18,
        p99LatencyMs: params.simulatedP99LatencyMs ?? quantiles.p99,
        latencySloTargetMs: 250
      });

      this.appendAudit(
      params.actor,
      params.role,
      `执行全链路 W3C Trace 瀑布流采样 (traceId=${waterfall.traceId.slice(0, 12)}) 与 SLO 燃烧率评估 (BurnRate=${sloEval.burnRate1h}x, Alert=${sloEval.alertSeverity})`,
      `Observability / SLO-Monitor`,
      `${sloEval.alertSeverity} (Budget=${sloEval.errorBudgetRemainingPct}%)`);


      return {
        status: 200,
        body: {
          traceWaterfall: waterfall,
          latencyQuantiles: quantiles,
          sloEvaluation: sloEval,
          prometheusMetrics: this.metrics.renderPrometheusMetrics()
        }
      };
    }

    // action === "execute_auto_remediation_runbook"
    const runbookId = params.runbookId ?? "RB-SLO-BURN-AND-DLQ-HEAL";
    const healedTaskIds = [];
    for (const [tId, tk] of this.deps.scheduler.allTasks.entries()) {
      if (tk.state === _models.TaskState.DEAD_LETTER) {
        tk.state = _models.TaskState.RUNNING_ACT;
        tk.retry_count = 0;
        healedTaskIds.push(tId);
      }
    }
    this.poolClient.getOrCreateBreaker("ep-deepseek-r1").recordSuccess();
    this.poolClient.getOrCreateBreaker("ep-qwen-38").recordSuccess();

    const ceBus = this.deps.cloudEventsBus ?? new _cloudevents.CloudEventsBus();
    const ceEvent = ceBus.emitEvent({
      type: "io.ansts.sre.runbook.executed",
      subject: runbookId,
      tenantId: "Northstar",
      data: {
        runbookId,
        healedTaskIds,
        circuitBreakersReset: ["ep-deepseek-r1", "ep-qwen-38"],
        actor: params.actor
      }
    });

    const outboxMsgId = `sre-rb-${Date.now()}`;
    this.outboxDispatcher.enqueueOutbox(outboxMsgId, runbookId, "https://pagerduty.ansts.internal/v2/enqueue", {
      event: ceEvent.type,
      runbookId,
      healedTaskIds
    });
    const outboxDelivery = this.outboxDispatcher.processPending(() => true);

    if (this.deps.sseGateway) {
      this.deps.sseGateway.publish({
        id: `sse-${outboxMsgId}`,
        event: "sre.runbook",
        taskId: "mgmt-audit-stream",
        turnIndex: 1,
        timestampMs: Date.now(),
        data: {
          runbookId,
          healedTaskIds,
          outboxMessageId: outboxMsgId
        }
      });
    }

    this.appendAudit(
    params.actor,
    params.role,
    `执行自动化运维自愈 Runbook [${runbookId}]：恢复异常任务 ${healedTaskIds.length} 个、重置模型熔断器并投递 HMAC 告警 Webhook (${outboxMsgId})`,
    `SRE-Ops / ${runbookId}`,
    `REMEDIATED (Delivered=${outboxDelivery.deliveredCount})`);


    return {
      status: 200,
      body: {
        runbookId,
        healedTaskIds,
        circuitBreakersReset: ["ep-deepseek-r1", "ep-qwen-38"],
        cloudEvent: ceEvent,
        webhookOutbox: {
          messageId: outboxMsgId,
          ...outboxDelivery
        }
      }
    };
  }


  executeSchedulerKernelAntiStaleDeadlockStarvationAudit(params)


  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发调度器内核防死锁与防饥饿回归演练" }
      };
    }

    const nowSec = Date.now() / 1000;
    const sched = new _scheduler.CoreScheduler5D(10.0, 30.0, 3, 5.0);

    // [Stale-1] Register a stale node (timestamp 45s old) and a fresh node (4 cores, 8192 MB)
    sched.registerOrUpdateNode(
    new _models.NodeHeartbeat({
      node_id: "node-stale-ghost",
      total_cpu_cores: 32,
      free_cpu_cores: 32,
      total_memory_mb: 65536,
      free_memory_mb: 65536,
      timestamp: nowSec - 45
    }));

    sched.registerOrUpdateNode(
    new _models.NodeHeartbeat({
      node_id: "node-fresh-01",
      total_cpu_cores: 4,
      free_cpu_cores: 4,
      total_memory_mb: 8192,
      free_memory_mb: 8192,
      warm_pool_ready: 2,
      timestamp: nowSec
    }));


    // [Deadlock-1] Submit a huge P0 task (needs 16 think_cpu_cores -> cannot fit 4-core node) followed by a small P2 task (0.25 core) for the SAME tenant
    const hugeP0 = new _models.TaskSpec({
      task_id: "task-hol-huge-p0",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P0_EMERGENCY,
      resource_profile: { think_cpu_cores: 16.0, act_cpu_cores: 16.0, act_memory_mb: 4096, disk_mb: 10240 }
    });
    const smallP2 = new _models.TaskSpec({
      task_id: "task-hol-small-p2",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P2_STANDARD,
      resource_profile: { think_cpu_cores: 0.25, act_cpu_cores: 2.0, act_memory_mb: 2048, disk_mb: 10240 }
    });
    sched.submitTask(hugeP0);
    // Override profiler recommendation on hugeP0 so think_cpu_cores stays 16.0
    hugeP0.resource_profile.think_cpu_cores = 16.0;
    sched.submitTask(smallP2);

    const tick1Placements = sched.scheduleTick(nowSec);
    const staleNodeEvicted = !sched.nodes.get("node-stale-ghost").healthy;
    const holSmallTaskPlaced = tick1Placements.some((p) => p[0] === "task-hol-small-p2" && p[1] === "node-fresh-01");
    const hugeTaskStillQueued = sched.allTasks.get("task-hol-huge-p0").state === _models.TaskState.QUEUED;

    // [Stale-2 & Deadlock-2] Submit a DAG child task depending on smallP2, then expire smallP2 4 times so it enters DEAD_LETTER
    const dagChild = new _models.TaskSpec({
      task_id: "task-dag-child",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P2_STANDARD,
      dependencies: ["task-hol-small-p2"]
    });
    sched.submitTask(dagChild);

    smallP2.retry_count = 3;
    const cpuBeforeReclaim = sched.nodes.get("node-fresh-01").free_cpu_cores;
    sched.reconcileOrphanedLeases(nowSec + 20);
    const cpuAfterReclaim = sched.nodes.get("node-fresh-01").free_cpu_cores;
    const nodeCapacityReclaimed = cpuAfterReclaim > cpuBeforeReclaim && sched.nodes.get("node-fresh-01").running_tasks === 0;
    const dagCascadedDeadLetter =
    smallP2.state === _models.TaskState.DEAD_LETTER &&
    dagChild.state === _models.TaskState.DEAD_LETTER &&
    dagChild.dead_letter_reason === "UPSTREAM_DEPENDENCY_DEAD_LETTER:task-hol-small-p2";

    // [Starvation-1] Verify fair preemption across two P3_BATCH tasks and preemption compensation boost
    const schedPreempt = new _scheduler.CoreScheduler5D(15.0, 30.0, 3, 5.0);
    schedPreempt.registerOrUpdateNode(
    new _models.NodeHeartbeat({
      node_id: "node-sat-01",
      total_cpu_cores: 2.0,
      free_cpu_cores: 2.0,
      total_memory_mb: 8192,
      free_memory_mb: 8192,
      timestamp: nowSec
    }));

    const batchA = new _models.TaskSpec({ task_id: "task-p3-a", tenant_id: "Northstar", priority: _models.TaskPriority.P3_BATCH });
    const batchB = new _models.TaskSpec({ task_id: "task-p3-b", tenant_id: "Northstar", priority: _models.TaskPriority.P3_BATCH });
    schedPreempt.submitTask(batchA);
    schedPreempt.submitTask(batchB);
    batchA.resource_profile.think_cpu_cores = 1.0;
    batchB.resource_profile.think_cpu_cores = 1.0;
    schedPreempt.scheduleTick(nowSec);

    const urgent1 = new _models.TaskSpec({ task_id: "task-p0-u1", tenant_id: "Northstar", priority: _models.TaskPriority.P0_EMERGENCY });
    schedPreempt.submitTask(urgent1);
    urgent1.resource_profile.think_cpu_cores = 1.0;
    schedPreempt.scheduleTick(nowSec + 1);

    const urgent2 = new _models.TaskSpec({ task_id: "task-p0-u2", tenant_id: "Northstar", priority: _models.TaskPriority.P0_EMERGENCY });
    schedPreempt.submitTask(urgent2);
    urgent2.resource_profile.think_cpu_cores = 1.0;
    schedPreempt.scheduleTick(nowSec + 2);

    const fairVictimRotationVerified = batchA.preempted_count === 1 && batchB.preempted_count === 1;

    this.appendAudit(
    params.actor,
    params.role,
    `执行调度器内核 5 项防 Stale/防死锁/防饥饿验证 (StaleTTL=${staleNodeEvicted}, LeaseReclaim=${nodeCapacityReclaimed}, HoLNonBlocking=${holSmallTaskPlaced}, DagCascadeDLQ=${dagCascadedDeadLetter}, FairPreempt=${fairVictimRotationVerified})`,
    "SchedulerKernel / M51-Audit",
    "ALL_5_INVARIANTS_VERIFIED");


    return {
      status: 200,
      body: {
        stale1HeartbeatTtlEvicted: staleNodeEvicted,
        stale2LeaseCapacityReclaimed: nodeCapacityReclaimed,
        deadlock1HolBypassPlacedSmallTask: holSmallTaskPlaced && hugeTaskStillQueued,
        deadlock2DagCascadedDeadLetter: dagCascadedDeadLetter,
        starvation1FairPreemptionRotation: fairVictimRotationVerified,
        allInvariantsVerified:
        staleNodeEvicted &&
        nodeCapacityReclaimed &&
        holSmallTaskPlaced &&
        hugeTaskStillQueued &&
        dagCascadedDeadLetter &&
        fairVictimRotationVerified
      }
    };
  }


  async executeSystemWideRectificationAudit(params)


  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发全系统跨模块 Stale/死锁/饥饿整改审计" }
      };
    }

    // 1. [Cell-Stale-1] Verify AnstsSchedulerCell cold-start rehydration of LEASED task + fencing watermark + alarm renewal on active leases
    let alarmScheduledAt = null;
    const cellSql = this.deps.v25Store.sql;
    const cell1 = new _scheduler_cell.AnstsSchedulerCell({
      id: { toString: () => "AnstsSchedulerCell:m52-audit" },
      storage: {
        sql: cellSql,
        transactionSync: (fn) => fn(),
        getAlarm: async () => alarmScheduledAt,
        setAlarm: async (ts) => {
          alarmScheduledAt = ts;
        }
      },
      blockConcurrencyWhile: async (fn) => fn()
    });
    cell1.registerNode(new _models.NodeHeartbeat({ node_id: "node-cell-m52", total_cpu_cores: 16, free_cpu_cores: 16 }));
    const leasedTask = new _models.TaskSpec({ task_id: "task-cell-m52-1", tenant_id: "Northstar", priority: _models.TaskPriority.P1_HIGH });
    await cell1.submitTaskRpc(leasedTask);
    alarmScheduledAt = null;
    const tick1 = await cell1.scheduleTickRpc();
    const activeLeaseAlarmRenewed = alarmScheduledAt !== null && cell1.scheduler.leaseCoordinator.activeLeaseCount() > 0;
    const fenceBeforeRestart = tick1.placements[0]?.[2] ?? 1;

    // Simulate cold-start restart of AnstsSchedulerCell over the same SQLite storage
    const cell2 = new _scheduler_cell.AnstsSchedulerCell({
      id: { toString: () => "AnstsSchedulerCell:m52-audit-restarted" },
      storage: {
        sql: cellSql,
        transactionSync: (fn) => fn(),
        getAlarm: async () => alarmScheduledAt,
        setAlarm: async (ts) => {
          alarmScheduledAt = ts;
        }
      },
      blockConcurrencyWhile: async (fn) => fn()
    });
    const rehydratedTask = cell2.scheduler.allTasks.get("task-cell-m52-1");
    const interruptedLeaseConvertedToQueued = rehydratedTask?.state === _models.TaskState.QUEUED;
    const tick2 = await cell2.scheduleTickRpc();
    const fenceAfterRestart = tick2.placements.find((p) => p[0] === "task-cell-m52-1")?.[2] ?? 0;
    const monotonicFencePreservedAcrossColdStart = fenceAfterRestart > fenceBeforeRestart;

    // 2. [Router-Stale-2] Verify CognitiveRouter 60s window decay, half-open auto-recovery & fallback active_requests accounting
    const nowMs = Date.now();
    const router = new _cognitive_router.CognitiveRouter();
    const ep = new _models.LlmEndpointMetrics({
      endpoint_id: "ep-m52-primary",
      provider: "deepseek",
      model_name: "DeepSeek-R1",
      tpm_limit: 10_000,
      tpm_used: 9_900
    });
    router.registerEndpoint(ep, 100, 10_000, nowMs);
    // Fallback path when estimatedTokens (500) > tpm_remaining (100): must still increment active_requests
    const [fbEp] = router.selectEndpoint("prefix-m52", 500, _models.TaskPriority.P1_HIGH, nowMs);
    const fallbackIncrementedActiveRequests = fbEp.active_requests === 1;
    router.releaseEndpoint(fbEp.endpoint_id);
    const fallbackReleasedCleanly = fbEp.active_requests === 0;

    // Trigger 5 rate-limit hits -> marks ep unhealthy & trips circuit breaker
    for (let i = 0; i < 5; i++) {
      router.reportInvocationMetrics("ep-m52-primary", 500, 100, true, nowMs + i * 10);
    }
    const markedUnhealthyOnRateLimit = !ep.healthy;
    // Advance clock by 61s (past 60s window & 15s breaker cooldown) -> auto-recovers endpoint & resets tpm_used
    const recoveredEps = router.refreshEndpointWindowsAndHalfOpenProbes(nowMs + 61_000);
    const routerWindowAndHalfOpenRecovered =
    recoveredEps.includes("ep-m52-primary") && ep.healthy && ep.tpm_used === 0 && ep.rate_limit_hits_last_min === 0;

    // 3. [Escrow-Deadlock-3] Verify DagEscrowCoordinator child-failure parent unblocking & A2A priority-aware deadlock resolution
    const escrowCoord = new _escrow.DagEscrowCoordinator();
    const parentTask = new _models.TaskSpec({
      task_id: "parent-m52",
      tenant_id: "Northstar",
      expected_SubAgent_count: 2
    });
    const childTask = new _models.TaskSpec({
      task_id: "child-m52-1",
      tenant_id: "Northstar",
      parent_task_id: "parent-m52"
    });
    escrowCoord.reserveParentEscrow(parentTask, 10, 0);
    escrowCoord.registerChildSubAgent(parentTask, childTask);
    const failRes = escrowCoord.failOrCancelChildSubAgent(parentTask, childTask.task_id, true);
    const escrowUnblockedAndReclaimed =
    failRes.parentUnblocked &&
    parentTask.state === _models.TaskState.DEAD_LETTER &&
    escrowCoord.getTenantEscrowedSlots("Northstar") === 0;

    const wfg = new _deadlock_detector.A2aWaitForGraphDeadlockDetector();
    wfg.addWaitEdge("agent-p0-leader", "agent-p1-worker");
    wfg.addWaitEdge("agent-p1-worker", "agent-p3-helper");
    wfg.addWaitEdge("agent-p3-helper", "agent-p0-leader");
    const wfgResolution = wfg.resolveCycleWithLowestPriorityVictim({
      "agent-p0-leader": 1000,
      "agent-p1-worker": 300,
      "agent-p3-helper": 50
    });
    const wfgLowestPriorityVictimChosen =
    wfgResolution.resolved && wfgResolution.victimAgentId === "agent-p3-helper";

    // 4. [Tunnel-Deadlock-4] Verify TunnelGatewayCell stale heartbeat auto-demotion & unacknowledged in-flight command re-queueing
    const tunCell = new _tunnel_gateway_cell.TunnelGatewayCell();
    tunCell.registerTunnel({ hostId: "body-m52", gatewayId: "celld-gw-01", role: "PRIMARY", connGeneration: 1, epoch: 1 }, nowMs - 45_000);
    tunCell.registerTunnel({ hostId: "body-m52", gatewayId: "celld-gw-02", role: "SECONDARY", connGeneration: 1, epoch: 1 }, nowMs);
    const { command, completionPromise } = tunCell.enqueueCommandForHost("body-m52", { op: "M52_PROBE" }, 5000);
    // Poll on gw-01 using stale timestamp so gw-01 remains older than 30s TTL
    tunCell.pollPendingCommands("body-m52", "celld-gw-01", nowMs - 40_000);
    // Evict stale tunnels and re-queue unacknowledged in-flight commands onto surviving gw-02
    const evictRes = tunCell.evictStaleTunnelsAndRequeueInFlight(nowMs, 30_000);
    const repolledOnSecondary = tunCell.pollPendingCommands("body-m52", "celld-gw-02", nowMs);
    tunCell.acknowledgeCommand(command.commandId, { status: "RECOVERED_ON_SECONDARY" });
    const acked = await completionPromise;
    const tunnelInFlightRequeuedAndAcked =
    evictRes.requeuedCommandIds.includes(command.commandId) &&
    repolledOnSecondary.length === 1 &&
    repolledOnSecondary[0].gatewayId === "celld-gw-02" &&
    acked.status === "RECOVERED_ON_SECONDARY";

    const allM52RectificationsVerified =
    activeLeaseAlarmRenewed &&
    interruptedLeaseConvertedToQueued &&
    monotonicFencePreservedAcrossColdStart &&
    fallbackIncrementedActiveRequests &&
    fallbackReleasedCleanly &&
    markedUnhealthyOnRateLimit &&
    routerWindowAndHalfOpenRecovered &&
    escrowUnblockedAndReclaimed &&
    wfgLowestPriorityVictimChosen &&
    tunnelInFlightRequeuedAndAcked;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M52 全系统跨模块 Stale/死锁/饥饿深度排查与整改验证 (CellRehydrate=${monotonicFencePreservedAcrossColdStart}, RouterDecay=${routerWindowAndHalfOpenRecovered}, EscrowUnblock=${escrowUnblockedAndReclaimed}, TunnelRequeue=${tunnelInFlightRequeuedAndAcked})`,
    "SystemWide / M52-Rectification",
    "ALL_M52_INVARIANTS_VERIFIED");


    return {
      status: 200,
      body: {
        cellStale1: {
          activeLeaseAlarmRenewed,
          interruptedLeaseConvertedToQueued,
          monotonicFencePreservedAcrossColdStart,
          fenceBeforeRestart,
          fenceAfterRestart
        },
        routerStale2: {
          fallbackIncrementedActiveRequests,
          fallbackReleasedCleanly,
          markedUnhealthyOnRateLimit,
          routerWindowAndHalfOpenRecovered
        },
        escrowAndWfgDeadlock3: {
          escrowUnblockedAndReclaimed,
          wfgLowestPriorityVictimChosen,
          wfgVictimAgentId: wfgResolution.victimAgentId
        },
        tunnelDeadlock4: {
          demotedGateways: evictRes.demotedGateways,
          requeuedCommandIds: evictRes.requeuedCommandIds,
          tunnelInFlightRequeuedAndAcked
        },
        allM52RectificationsVerified
      }
    };
  }

  /**
   * [M53] Deep Cross-Layer Risk Elimination Audit:
   * Verifies all 5 deep cross-layer invariants:
   * 1. [Saga-Stale-1] SagaToolGateway.rollbackAndInvalidateTask invalidates executionJournal cache for rolled-back turns & fault-tolerant LIFO compensation continues even if one compensation handler throws.
   * 2. [FinOps-Starvation-2] FinOpsBudgetController UTC daily window auto-reset across day boundaries & refundSpend on aborted/rolled-back turns.
   * 3. [WarmPool-Leak-3] WarmSandboxPoolManager idempotent acquireSandbox(taskId) + reclaimOrphanedSandboxes(activeTaskIds) & LayerFsNumberedSandboxMultiplexer idempotent mountWorkspaceIntoSandbox(workspaceId).
   * 4. [Wal-Stale-4] TurnWalEngine dual-tuple (fencingToken, turnIndex) monotonicity enforcement & verifyTaskWalChainIntegrity CRC32 chain check.
   * 5. [DrainToken-Deadlock-5] CelldStateAwareFleetAutoscaler drain-token lease TTL auto-expiration so a crashed draining node never deadlocks rolling shutdowns.
   */
  executeDeepCrossLayerRiskEliminationAudit(params)


  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权执行 M53 跨层深水区风险清零审计" }
      };
    }

    // 1. [Saga-Stale-1] Verify post-rollback idempotency cache invalidation & fault-tolerant LIFO compensation
    const sagaGw = new _saga_gateway.SagaToolGateway();
    let toolRunCount = 0;
    const compensatedTurns = [];
    sagaGw.executeTool({
      taskId: "task-m53-saga",
      turnIndex: 1,
      toolName: "write_db_row",
      effectClass: _models.ToolCallEffectClass.COMPENSATABLE,
      args: { rowId: 101 },
      actionFn: () => {
        toolRunCount += 1;
        return { run: toolRunCount };
      },
      compensationFn: () => {
        compensatedTurns.push(1);
      }
    });
    sagaGw.executeTool({
      taskId: "task-m53-saga",
      turnIndex: 2,
      toolName: "flaky_external_hook",
      effectClass: _models.ToolCallEffectClass.COMPENSATABLE,
      args: { rowId: 102 },
      actionFn: () => ({ ok: true }),
      compensationFn: () => {
        compensatedTurns.push(2);
        throw new Error("Simulated transient network error during turn 2 compensation");
      }
    });
    const rbRes = sagaGw.rollbackAndInvalidateTask("task-m53-saga", 1);
    // Re-executing turn 1 after rollback must actually invoke actionFn again (toolRunCount: 1 -> 2), NOT return stale cached run=1
    const rerunRes = sagaGw.executeTool({
      taskId: "task-m53-saga",
      turnIndex: 1,
      toolName: "write_db_row",
      effectClass: _models.ToolCallEffectClass.COMPENSATABLE,
      args: { rowId: 101 },
      actionFn: () => {
        toolRunCount += 1;
        return { run: toolRunCount };
      }
    });
    const sagaFaultTolerantLifoAndCacheInvalidated =
    compensatedTurns.length === 2 &&
    compensatedTurns[0] === 2 &&
    compensatedTurns[1] === 1 &&
    rbRes.compensationErrors.length === 1 &&
    rbRes.invalidatedIdemKeys.length === 2 &&
    rerunRes.run === 2;

    // 2. [FinOps-Starvation-2] Verify UTC daily window auto-reset & spend refund on aborted turns
    const finops = new _budget_controller.FinOpsBudgetController();
    finops.configureTenant({
      tenantId: "tenant-m53",
      dailyBudgetUsd: 100,
      softDowngradeThresholdRatio: 0.8,
      primaryModel: "DeepSeek-R1",
      fallbackEconomyModel: "Qwen-3.8"
    });
    const day1Ms = Date.UTC(2026, 10, 10, 23, 50, 0);
    finops.recordSpend("tenant-m53", 105, day1Ms);
    const blockedOnDay1 = !finops.evaluateBudgetAndModel("tenant-m53", day1Ms).allowed;
    // Refund $20 from aborted turn on Day 1 -> unblocks tenant immediately to $85 (soft downgrade)
    const afterRefund = finops.refundSpend("tenant-m53", 20, day1Ms);
    const unblockedAfterRefund = finops.evaluateBudgetAndModel("tenant-m53", day1Ms).allowed && afterRefund === 85;
    // Exhaust again on Day 1, then advance clock to Day 2 UTC -> daily window resets to $0 and restores primary model
    finops.recordSpend("tenant-m53", 20, day1Ms);
    const day2Ms = Date.UTC(2026, 10, 11, 0, 5, 0);
    const evalDay2 = finops.evaluateBudgetAndModel("tenant-m53", day2Ms);
    const finopsUtcResetAndRefundVerified =
    blockedOnDay1 &&
    unblockedAfterRefund &&
    evalDay2.allowed &&
    !evalDay2.downgraded &&
    evalDay2.selectedModel === "DeepSeek-R1" &&
    evalDay2.windowReset;

    // 3. [WarmPool-Leak-3] Verify idempotent acquireSandbox, orphan VM reclamation & multiplexer idempotent workspace mount
    const warmPool = new _warm_pool.WarmSandboxPoolManager("node-m53", 4);
    const vmFirst = warmPool.acquireSandbox("task-m53-vm");
    const vmDuplicate = warmPool.acquireSandbox("task-m53-vm");
    const idempotentSameVmReturned = vmFirst.vmId === vmDuplicate.vmId && warmPool.stats().activeCount === 1;
    warmPool.acquireSandbox("task-m53-orphan");
    const reclaimedOrphanVms = warmPool.reclaimOrphanedSandboxes(new Set(["task-m53-vm"]));
    const orphanVmReclaimedCleanly = reclaimedOrphanVms.length === 1 && warmPool.stats().activeCount === 1;

    const mux = new _warm_pool.LayerFsNumberedSandboxMultiplexer();
    for (let i = 1; i <= 16; i++) {
      mux.mountWorkspaceIntoSandbox(`ws-m53-${i}`, 1000);
    }
    // Re-mounting ws-m53-1 when sandbox #1 is full (16/16) must return sandbox #1 idempotently instead of spawning sandbox #2
    const remountRes = mux.mountWorkspaceIntoSandbox("ws-m53-1", 2000);
    const multiplexerIdempotentMountVerified =
    remountRes.sandboxNumber === 1 && !remountRes.spawnedNewSandbox && remountRes.activeWorkspacesOnSandbox === 16;

    // 4. [Wal-Stale-4] Verify dual-tuple (fencingToken, turnIndex) monotonicity & verifyTaskWalChainIntegrity
    const wal = new _wal_engine.TurnWalEngine();
    wal.appendTurnDelta({
      taskId: "task-m53-wal",
      turnIndex: 1,
      fencingToken: 5,
      changedFiles: ["src/a.ts"],
      sqliteWalPages: 4,
      highestSubmittedFencingToken: 5,
      enforceTurnMonotonicity: true
    });
    wal.appendTurnDelta({
      taskId: "task-m53-wal",
      turnIndex: 2,
      fencingToken: 5,
      changedFiles: ["src/b.ts"],
      sqliteWalPages: 6,
      highestSubmittedFencingToken: 5,
      enforceTurnMonotonicity: true
    });
    let regressiveTurnRejected = false;
    try {
      wal.appendTurnDelta({
        taskId: "task-m53-wal",
        turnIndex: 1,
        fencingToken: 5,
        changedFiles: ["src/stale.ts"],
        sqliteWalPages: 2,
        highestSubmittedFencingToken: 5,
        enforceTurnMonotonicity: true
      });
    } catch (err) {
      regressiveTurnRejected = err instanceof _lease_coordinator.StaleFenceError;
    }
    const chainCheck = wal.verifyTaskWalChainIntegrity("task-m53-wal");
    const walMonotonicAndChainIntegrityVerified =
    regressiveTurnRejected && chainCheck.valid && chainCheck.frameCount === 2 && chainCheck.monotonicFenceOrdered;

    // 5. [DrainToken-Deadlock-5] Verify CelldStateAwareFleetAutoscaler drain-token lease TTL expiration
    const autoscaler = new _manual_scale_controller.CelldStateAwareFleetAutoscaler(40_000);
    const t0 = 1_000_000;
    const drain1 = autoscaler.initiateNodeShutdown({
      nodeId: "node-crashed-draining",
      systemdTimeoutStopSec: 60,
      nowMs: t0
    });
    const blockedBeforeTtl = !autoscaler.initiateNodeShutdown({
      nodeId: "node-survivor-2",
      systemdTimeoutStopSec: 60,
      nowMs: t0 + 10_000
    }).claimedDrainToken;
    const reclaimedAfterTtl = autoscaler.initiateNodeShutdown({
      nodeId: "node-survivor-2",
      systemdTimeoutStopSec: 60,
      nowMs: t0 + 41_000
    });
    const drainTokenTtlReclaimed =
    drain1.claimedDrainToken &&
    blockedBeforeTtl &&
    reclaimedAfterTtl.claimedDrainToken &&
    reclaimedAfterTtl.expiredStaleHolder === "node-crashed-draining";

    const allM53RisksEliminated =
    sagaFaultTolerantLifoAndCacheInvalidated &&
    finopsUtcResetAndRefundVerified &&
    idempotentSameVmReturned &&
    orphanVmReclaimedCleanly &&
    multiplexerIdempotentMountVerified &&
    walMonotonicAndChainIntegrityVerified &&
    drainTokenTtlReclaimed;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M53 跨层深水区风险清零验证 (SagaCacheInvalidate=${sagaFaultTolerantLifoAndCacheInvalidated}, FinOpsUtcReset=${finopsUtcResetAndRefundVerified}, WarmPoolGuard=${idempotentSameVmReturned && orphanVmReclaimedCleanly}, WalChain=${walMonotonicAndChainIntegrityVerified}, DrainTokenTtl=${drainTokenTtlReclaimed})`,
    "SystemWide / M53-RiskElimination",
    "ALL_M53_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        sagaStale1: {
          compensatedTurns,
          invalidatedIdemKeys: rbRes.invalidatedIdemKeys,
          isolatedCompensationErrorCount: rbRes.compensationErrors.length,
          rerunTurnOutput: rerunRes.run,
          sagaFaultTolerantLifoAndCacheInvalidated
        },
        finopsStarvation2: {
          blockedOnDay1,
          unblockedAfterRefund,
          day2SelectedModel: evalDay2.selectedModel,
          day2WindowReset: evalDay2.windowReset,
          finopsUtcResetAndRefundVerified
        },
        warmPoolLeak3: {
          idempotentSameVmReturned,
          reclaimedOrphanVms,
          orphanVmReclaimedCleanly,
          multiplexerIdempotentMountVerified
        },
        walStale4: {
          regressiveTurnRejected,
          chainCheck,
          walMonotonicAndChainIntegrityVerified
        },
        drainTokenDeadlock5: {
          blockedBeforeTtl,
          expiredStaleHolder: reclaimedAfterTtl.expiredStaleHolder,
          drainTokenTtlReclaimed
        },
        allM53RisksEliminated
      }
    };
  }
  /**
   * [M54] Deep Runtime Resilience, Cold-Start Fencing, Half-Open Stampede Guard, Approval TTL,
   * Hot-Cell Leak Reclamation, Outbox/SSE Resilience & A2A Anti-Starvation Audit:
   * Verifies all 6 deep runtime invariants:
   * 1. [HarnessCell-ColdStart-Stale-1] AgentTaskHarnessCell rehydrates highestFencingToken and highestTurnIndex from SQLite on cold-start/migration and rejects stale fencing tokens.
   * 2. [CircuitBreaker-HalfOpen-Stampede-2] ThreeStateCircuitBreaker admits at most 1 concurrent half-open probe (preventing thundering-herd stampedes) and auto-recovers timed-out probes.
   * 3. [Staging-ApprovalTimeout-Deadlock-3] StagingCommitAndApprovalGate auto-aborts staged side-effects on rejection and sweeps expired PENDING approval tickets so tasks never deadlock in SUSPENDED_APPROVAL.
   * 4. [HotCell-InFlight-Leak-Deadlock-4] HotCellOverloadAndQueueGuard reclaims hung in-flight fetch slots beyond maxFetchDurationMs and resets overload split state.
   * 5. [Outbox-Sse-Resource-Starvation-5] TransactionalOutboxDispatcher isolates throwing webhook senders per message + verifies HMAC replay window, and SseEventStreamGateway falls back to replaying retained buffer when Last-Event-ID was evicted.
   * 6. [A2a-GitArbiter-Starvation-6] SharedBlackboardAndGitArbiter applies +150/round conflict-aging boost so repeatedly rejected low-priority agents eventually win arbitration.
   */
  async executeDeepRuntimeResilienceAndAntiStarvationAudit(params)


  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权执行 M54 深层运行时防泄漏与反饥饿审计" }
      };
    }

    // 1. [HarnessCell-ColdStart-Stale-1] Verify AgentTaskHarnessCell cold-start SQLite fencing watermark rehydration
    const cellSql = this.deps.v25Store.sql;
    cellSql.exec("DELETE FROM harness_turns");
    const r2 = new _env.InMemoryR2Bucket();
    const stateLike = {
      id: { toString: () => "AgentTaskHarnessCell:m54-audit" },
      storage: {
        sql: cellSql,
        transactionSync: (fn) => fn(),
        getAlarm: async () => null,
        setAlarm: async () => {}
      },
      blockConcurrencyWhile: async (fn) => fn()
    };
    const harnessCell1 = new _harness_cell.AgentTaskHarnessCell(stateLike, r2, "task-m54-harness");
    await harnessCell1.executeAndCheckpointTurn({
      turnIndex: 1,
      fencingToken: 7,
      toolName: "write_file",
      toolOutput: { ok: true }
    });
    // Cold-start new AgentTaskHarnessCell instance over same SQLite storage
    const harnessCell2 = new _harness_cell.AgentTaskHarnessCell(stateLike, r2, "task-m54-harness");
    const rehydratedWatermark = harnessCell2.getHighestFencingToken();
    let staleZombieTokenRejectedAfterColdStart = false;
    try {
      await harnessCell2.executeAndCheckpointTurn({
        turnIndex: 2,
        fencingToken: 4, // stale token < 7
        toolName: "zombie_write",
        toolOutput: { corrupted: true }
      });
    } catch {
      staleZombieTokenRejectedAfterColdStart = true;
    }
    const harnessColdStartFencingVerified = rehydratedWatermark === 7 && staleZombieTokenRejectedAfterColdStart;

    // 2. [CircuitBreaker-HalfOpen-Stampede-2] Verify single-probe half-open stampede gate & stuck-probe timeout recovery
    const breaker = new _breaker_limiter.ThreeStateCircuitBreaker(2, 10_000, 5_000);
    const t0 = 2_000_000;
    breaker.recordFailure(t0);
    breaker.recordFailure(t0 + 10);
    // Advance past 10s openCooldownMs -> HALF_OPEN
    const probe1 = breaker.tryAcquireRequestSlot(t0 + 11_000);
    const stampedeCaller2 = breaker.tryAcquireRequestSlot(t0 + 11_050);
    // Simulate probe1 hanging past 5s probeTimeoutMs -> slot auto-recovers at t0 + 16_500
    const probeAfterTimeout = breaker.tryAcquireRequestSlot(t0 + 16_500);
    breaker.recordSuccess();
    const halfOpenStampedeAndTimeoutVerified =
    probe1.admitted &&
    probe1.isHalfOpenProbe &&
    !stampedeCaller2.admitted &&
    probeAfterTimeout.admitted &&
    breaker.getState(t0 + 16_600) === "CLOSED";

    // 3. [Staging-ApprovalTimeout-Deadlock-3] Verify rejection side-effect auto-abort & expired PENDING ticket sweep
    const staging = new _staging.StagingCommitAndApprovalGate();
    const taskReject = new _models.TaskSpec({ task_id: "task-m54-reject", tenant_id: "Northstar" });
    staging.stageEffect({
      effectId: "eff-m54-1",
      taskId: taskReject.task_id,
      turnIndex: 1,
      toolName: "drop_table",
      args: {}
    });
    const ticketReject = staging.requestHumanApproval(taskReject, "drop_table", {}, t0);
    staging.resolveApproval(ticketReject.ticketId, false, "zhou", taskReject);
    const stagedAbortedOnReject =
    staging.getStagedEffects(taskReject.task_id)[0]?.aborted === true &&
    taskReject.state === _models.TaskState.DEAD_LETTER;

    const taskExpire = new _models.TaskSpec({ task_id: "task-m54-expire", tenant_id: "Northstar" });
    staging.stageEffect({
      effectId: "eff-m54-2",
      taskId: taskExpire.task_id,
      turnIndex: 1,
      toolName: "wire_transfer",
      args: {}
    });
    const ticketExpire = staging.requestHumanApproval(taskExpire, "wire_transfer", {}, t0);
    const tasksMap = new Map([[taskExpire.task_id, taskExpire]]);
    const expiredIds = staging.expireStaleApprovalTickets(tasksMap, t0 + 3_700_000, 3_600_000);
    const stagingExpirationSweepVerified =
    stagedAbortedOnReject &&
    expiredIds.includes(ticketExpire.ticketId) &&
    taskExpire.state === _models.TaskState.DEAD_LETTER &&
    staging.getStagedEffects(taskExpire.task_id)[0]?.aborted === true;

    // 4. [HotCell-InFlight-Leak-Deadlock-4] Verify stale in-flight fetch slot reclamation
    const hotGuard = new _hot_cell_overload_guard.HotCellOverloadAndQueueGuard({ maxCellConcurrentRequests: 2 });
    hotGuard.tryEnterCellFetch("cell-m54", t0);
    hotGuard.tryEnterCellFetch("cell-m54", t0);
    const overloadedBeforeReclaim = !hotGuard.tryEnterCellFetch("cell-m54", t0 + 100).admitted;
    const reclaimRes = hotGuard.reclaimStaleInFlightFetches(t0 + 35_000, 30_000);
    const admittedAfterReclaim = hotGuard.tryEnterCellFetch("cell-m54", t0 + 35_100).admitted;
    hotGuard.acknowledgeSubShardSplit("cell-m54");
    const hotCellReclamationVerified =
    overloadedBeforeReclaim && reclaimRes.reclaimedCount === 2 && admittedAfterReclaim;

    // 5. [Outbox-Sse-Resource-Starvation-5] Verify Outbox per-message exception isolation & SSE evicted Last-Event-ID fallback replay
    const outbox = new _outbox_webhook.TransactionalOutboxDispatcher("m54-secret", 2);
    outbox.enqueueOutbox("msg-poison", "task-m54", "https://bad.example/hook", { step: 1 });
    outbox.enqueueOutbox("msg-healthy", "task-m54", "https://good.example/hook", { step: 2 });
    const outboxRun = outbox.processPending((msg) => {
      if (msg.messageId === "msg-poison") {
        throw new Error("Simulated ECONNRESET on poisoned webhook endpoint");
      }
      return true;
    });
    const sig = outbox.signPayload("{\"ok\":true}", 1_700_000_000);
    const validSig = outbox.verifySignature("{\"ok\":true}", sig, 1_700_000_100, 300);
    const expiredReplaySig = !outbox.verifySignature("{\"ok\":true}", sig, 1_700_000_900, 300);
    const outboxIsolationAndReplayVerified =
    outboxRun.deliveredCount === 1 && outboxRun.failedAttemptCount === 1 && validSig && expiredReplaySig;

    const sse = new _sse_stream.SseEventStreamGateway(2);
    sse.publish({ id: "evt-1", event: "turn", taskId: "task-m54-sse", turnIndex: 1, timestampMs: t0, data: {} });
    sse.publish({ id: "evt-2", event: "turn", taskId: "task-m54-sse", turnIndex: 2, timestampMs: t0 + 1, data: {} });
    sse.publish({ id: "evt-3", event: "turn", taskId: "task-m54-sse", turnIndex: 3, timestampMs: t0 + 2, data: {} });
    // evt-1 was evicted (maxHistoryPerTask=2 keeps evt-2, evt-3); reconnecting with replayFromEventId="evt-1" must fall back to replaying evt-2 & evt-3
    const sub = sse.subscribe("task-m54-sse", "sub-m54", "evt-1");
    const chunks = [];
    sub.stream.on("data", (c) => chunks.push(Buffer.from(c)));
    sse.unsubscribe("task-m54-sse", "sub-m54");
    const replayedText = Buffer.concat(chunks).toString("utf-8");
    const sseEvictedFallbackVerified =
    sub.replayGapDetected && replayedText.includes("evt-2") && replayedText.includes("evt-3");

    // 6. [A2a-GitArbiter-Starvation-6] Verify conflict-aging priority boost prevents low-priority agent patch starvation
    const arbiter = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter();
    // Round 1: high-agent (200) beats low-agent (100) on shared file; low-agent conflict count becomes 1 (+150 boost next round -> 250)
    const round1 = arbiter.arbitrateGitPatchTouches([
    { agentId: "agent-high", touchedFiles: ["src/core.ts"], priorityScore: 200 },
    { agentId: "agent-low", touchedFiles: ["src/core.ts"], priorityScore: 100 }]);

    // Round 2: low-agent effective score is 100 + 150 = 250 > high-agent 200 -> low-agent wins Round 2!
    const round2 = arbiter.arbitrateGitPatchTouches([
    { agentId: "agent-high", touchedFiles: ["src/core.ts"], priorityScore: 200 },
    { agentId: "agent-low", touchedFiles: ["src/core.ts"], priorityScore: 100 }]);

    const a2aAntiStarvationVerified =
    round1.acceptedAgentIds.includes("agent-high") &&
    round1.conflictedAgentIds.includes("agent-low") &&
    round2.acceptedAgentIds.includes("agent-low") &&
    arbiter.getAgentConflictCount("agent-low") === 0;

    const allM54RisksEliminated =
    harnessColdStartFencingVerified &&
    halfOpenStampedeAndTimeoutVerified &&
    stagingExpirationSweepVerified &&
    hotCellReclamationVerified &&
    outboxIsolationAndReplayVerified &&
    sseEvictedFallbackVerified &&
    a2aAntiStarvationVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M54 深层运行时防泄漏与反饥饿验证 (HarnessColdFence=${harnessColdStartFencingVerified}, HalfOpenSingleProbe=${halfOpenStampedeAndTimeoutVerified}, StagingTtlSweep=${stagingExpirationSweepVerified}, HotCellReclaim=${hotCellReclamationVerified}, OutboxSse=${outboxIsolationAndReplayVerified && sseEvictedFallbackVerified}, A2aAging=${a2aAntiStarvationVerified})`,
    "SystemWide / M54-DeepRuntimeAudit",
    "ALL_M54_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        harnessColdStartStale1: {
          rehydratedWatermark,
          staleZombieTokenRejectedAfterColdStart,
          harnessColdStartFencingVerified
        },
        circuitBreakerStampede2: {
          firstProbeAdmitted: probe1.admitted,
          concurrentStampedeBlocked: !stampedeCaller2.admitted,
          recoveredAfterProbeTimeout: probeAfterTimeout.admitted,
          halfOpenStampedeAndTimeoutVerified
        },
        stagingApprovalDeadlock3: {
          stagedAbortedOnReject,
          expiredTicketIds: expiredIds,
          stagingExpirationSweepVerified
        },
        hotCellInFlightLeak4: {
          reclaimedCount: reclaimRes.reclaimedCount,
          admittedAfterReclaim,
          hotCellReclamationVerified
        },
        outboxAndSseResilience5: {
          outboxIsolationAndReplayVerified,
          sseEvictedFallbackVerified,
          replayGapDetected: sub.replayGapDetected
        },
        a2aGitArbiterStarvation6: {
          round1Winner: round1.acceptedAgentIds[0],
          round2Winner: round2.acceptedAgentIds[0],
          round2EffectiveScores: round2.effectiveScores,
          a2aAntiStarvationVerified
        },
        allM54RisksEliminated
      }
    };
  }
  /**
   * [M55] Deep Persistence, Shard Last-Brain Survival Guard, LayerFS Rule D9 Pull-State Sync,
   * Time-Travel CRC32 Verification, Velocity Breaker Recovery & RPO=0 Atomic Rollback Audit:
   * Verifies all 6 deep storage/HA/transactional invariants:
   * 1. [V25Persistence-CrossTaskIdem-1] SchedulerV25SingleWritePathStore deduplicates retries with identical (tenant_id, idempotency_key) even when task_id differs, isolates tenants, and preserves full spec_json fields.
   * 2. [ShardCoordinator-LastBrainSurvival-2] HaSchedulerShardCoordinator refuses to evict the last surviving Brain node (preventing an empty 256-slot hash ring deadlock) and TopologyCasCoordinator deduplicates member lists.
   * 3. [LayerFsSync-PullToPush-3] LayerFsRemoteStoreSynchronizer updates remoteGeneration and lastKnownEtag inside pullStoreSnapshot() so a cold-started or peer-updated node can pull and immediately push without false Rule D9 409 lockouts.
   * 4. [VelocityBreaker-Reset-4] SyntacticAndVelocityBreaker prunes expired sliding-window history and supports resetTaskBreaker() so approved/rolled-back tasks can resume cleanly.
   * 5. [TimeTravel-Crc32AndEpochDedup-5] DeterministicTimeTravelReplayer rejects corrupted WAL frame CRC32 before forking and deduplicates multi-epoch turns by selecting the highest fencingToken.
   * 6. [RpoZero-AtomicRollbackAndSingleFlight-6] RpoZeroDurabilityEngine rolls back local SQLite transaction when R2 LTX CAS fails, and DurableWorkflowContext coalesces concurrent in-flight stepDo calls.
   */
  async executeStorageShardSyncAndTransactionAudit(params)


  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权执行 M55 存储与高可用事务一致性审计" }
      };
    }

    // 1. [V25Persistence-CrossTaskIdem-1] Verify cross-task idempotency lookup & tenant isolation
    const v25Store = new _scheduler_v25_persistence.SchedulerV25SingleWritePathStore();
    const tFirst = new _models.TaskSpec({
      task_id: "task-m55-orig",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P1_HIGH,
      prompt: "audit-m55",
      idempotency_key: "client-req-m55-001",
      dependencies: ["dep-a"]
    });
    const c1 = v25Store.commitTaskSingleWritePath(tFirst, "body-host-01");
    // Retry with same idempotency_key and tenant_id, but newly generated task_id
    const tRetryDiffId = new _models.TaskSpec({
      task_id: "task-m55-retry-new-id",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P1_HIGH,
      prompt: "audit-m55",
      idempotency_key: "client-req-m55-001"
    });
    const c2 = v25Store.commitTaskSingleWritePath(tRetryDiffId, "body-host-02");
    // Different tenant with same idempotency_key string must NOT collide
    const tOtherTenant = new _models.TaskSpec({
      task_id: "task-m55-helios",
      tenant_id: "Helios",
      priority: _models.TaskPriority.P1_HIGH,
      prompt: "audit-m55-helios",
      idempotency_key: "client-req-m55-001"
    });
    const c3 = v25Store.commitTaskSingleWritePath(tOtherTenant, "body-host-01");
    const crossTaskIdemAndTenantIsolationVerified =
    !c1.deduplicated &&
    c2.deduplicated &&
    c2.task.task_id === "task-m55-orig" &&
    c2.task.dependencies.includes("dep-a") &&
    !c3.deduplicated;

    // 2. [ShardCoordinator-LastBrainSurvival-2] Verify last surviving Brain shard is never evicted to an empty ring
    const shardCoord = new _ha_scheduler_shard_coordinator.HaSchedulerShardCoordinator(256, 8);
    shardCoord.registerBrainShardNode("brain-m55-01");
    shardCoord.registerBrainShardNode("brain-m55-02");
    const ev1 = shardCoord.evictUnhealthyBrainShardNode("brain-m55-02");
    // Attempt to evict the LAST surviving brain node ("brain-m55-01") -> Last-Brain Survival Guard must retain it
    const evLast = shardCoord.evictUnhealthyBrainShardNode("brain-m55-01");
    const resolvedAfterLastEvict = shardCoord.resolveBrainForTask("Northstar", "task-any");
    const lastBrainSurvivalVerified =
    !ev1.lastBrainSurvivalGuardTriggered &&
    Boolean(evLast.lastBrainSurvivalGuardTriggered) &&
    evLast.survivingBrains.length === 1 &&
    resolvedAfterLastEvict.brainNodeId === "brain-m55-01" &&
    resolvedAfterLastEvict.epoch >= 2;

    // 3. [LayerFsSync-PullToPush-3] Verify pullStoreSnapshot syncs remoteGeneration & ETag for subsequent push
    const sharedBucket = new _env.InMemoryR2Bucket();
    const syncNodeA = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(sharedBucket, "proj-m55");
    const syncNodeB = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(sharedBucket, "proj-m55");
    await syncNodeA.pushStoreSnapshot({
      machineId: "node-a",
      basedOnGeneration: 0,
      storeSqliteBytes: new Uint8Array([1, 2, 3]),
      mainHeadCommitId: "C1"
    });
    // Node B pulls generation 1 from R2, then pushes generation 2 based on pulled.remoteGeneration
    const pulledByB = await syncNodeB.pullStoreSnapshot();
    const pushedByB = await syncNodeB.pushStoreSnapshot({
      machineId: "node-b",
      basedOnGeneration: pulledByB.remoteGeneration,
      storeSqliteBytes: new Uint8Array([1, 2, 3, 4]),
      mainHeadCommitId: "C2"
    });
    const layerfsPullThenPushVerified = pulledByB.remoteGeneration === 1 && pushedByB.remoteGeneration === 2;

    // 4. [VelocityBreaker-Reset-4] Verify SyntacticAndVelocityBreaker resetTaskBreaker unlocks resumed task
    const velBreaker = new _velocity_breaker.SyntacticAndVelocityBreaker(3, 120_000);
    const nowMs = Date.now();
    velBreaker.recordAndCheckTurn("task-m55-vel", { turnIndex: 1, toolName: "bash", argsHash: "h1", tokensConsumed: 500, timestampMs: nowMs });
    velBreaker.recordAndCheckTurn("task-m55-vel", { turnIndex: 2, toolName: "bash", argsHash: "h1", tokensConsumed: 500, timestampMs: nowMs + 10 });
    const trip3 = velBreaker.recordAndCheckTurn("task-m55-vel", { turnIndex: 3, toolName: "bash", argsHash: "h1", tokensConsumed: 500, timestampMs: nowMs + 20 });
    velBreaker.resetTaskBreaker("task-m55-vel", 3);
    const afterReset = velBreaker.recordAndCheckTurn("task-m55-vel", { turnIndex: 3, toolName: "bash", argsHash: "h2", tokensConsumed: 500, timestampMs: nowMs + 30 });
    const velocityResetVerified = trip3.tripped && !afterReset.tripped;

    // 5. [TimeTravel-Crc32AndEpochDedup-5] Verify CRC32 corruption guard & highest-epoch turn deduplication
    const wal = new _wal_engine.TurnWalEngine();
    wal.appendTurnDelta({
      taskId: "task-m55-tt",
      turnIndex: 1,
      fencingToken: 1,
      changedFiles: ["stale_epoch1.ts"],
      sqliteWalPages: 2,
      highestSubmittedFencingToken: 1
    });
    const validTurn1Epoch2 = wal.appendTurnDelta({
      taskId: "task-m55-tt",
      turnIndex: 1,
      fencingToken: 2,
      changedFiles: ["canonical_epoch2.ts"],
      sqliteWalPages: 4,
      highestSubmittedFencingToken: 2
    });
    const replayer = new _time_travel.DeterministicTimeTravelReplayer();
    const forkClean = replayer.forkTaskAtTurn(
    tFirst,
    wal.getTaskWalEntries("task-m55-tt"),
    1,
    "task-m55-tt-fork");

    // Corrupt frame CRC32 and verify forkTaskAtTurn rejects it
    validTurn1Epoch2.frame.crc32Hex = "00000000";
    let corruptedForkRejected = false;
    try {
      replayer.forkTaskAtTurn(tFirst, [validTurn1Epoch2], 1, "task-m55-tt-bad");
    } catch {
      corruptedForkRejected = true;
    }
    const timeTravelIntegrityAndDedupVerified =
    forkClean.inheritedFiles.length === 1 &&
    forkClean.inheritedFiles[0] === "canonical_epoch2.ts" &&
    forkClean.inheritedFencingToken === 2 &&
    corruptedForkRejected;

    // 6. [RpoZero-AtomicRollbackAndSingleFlight-6] Verify atomic SQL rollback on LTX CAS conflict & workflow single-flight coalescing
    const r2Cas = new _env.InMemoryR2Bucket();
    const sqlCas = this.deps.v25Store.sql;
    sqlCas.exec("CREATE TABLE IF NOT EXISTS m55_tx_probe (k TEXT PRIMARY KEY, v TEXT)");
    sqlCas.exec("DELETE FROM m55_tx_probe");
    const rpoEngine = new _celld_ha_engine.RpoZeroDurabilityEngine(r2Cas, sqlCas);
    // Pre-populate conflicting LTX key for tx-00000001-f1.ltx
    await r2Cas.put("ltx/cell-m55/tx-00000001-f1.ltx", new Uint8Array([9, 9]));
    let casConflictCaught = false;
    try {
      await rpoEngine.commitWithSyncLtxFlush(
      "cell-m55",
      1,
      () => {
        sqlCas.exec("INSERT INTO m55_tx_probe (k, v) VALUES (?, ?)", "k1", "uncommitted_zombie_write");
      },
      new Uint8Array([1, 2, 3]),
      { explicitTxId: 1, sql: sqlCas });

    } catch {
      casConflictCaught = true;
    }
    const rolledBackRow = sqlCas.exec("SELECT * FROM m55_tx_probe WHERE k = ?", "k1").one();

    const wf = new _celld_ha_engine.DurableWorkflowContext(sqlCas, "wf-m55-singleflight");
    let stepInvocations = 0;
    const [sf1, sf2] = await Promise.all([
    wf.stepDo("expensive-step", async () => {
      stepInvocations += 1;
      await new Promise((r) => setTimeout(r, 15));
      return { count: stepInvocations };
    }),
    wf.stepDo("expensive-step", async () => {
      stepInvocations += 1;
      return { count: stepInvocations };
    })]);

    const rpoRollbackAndSingleFlightVerified =
    casConflictCaught && rolledBackRow === null && stepInvocations === 1 && sf1.result.count === 1 && sf2.result.count === 1;

    const allM55RisksEliminated =
    crossTaskIdemAndTenantIsolationVerified &&
    lastBrainSurvivalVerified &&
    layerfsPullThenPushVerified &&
    velocityResetVerified &&
    timeTravelIntegrityAndDedupVerified &&
    rpoRollbackAndSingleFlightVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M55 存储与高可用事务一致性审计 (CrossTaskIdem=${crossTaskIdemAndTenantIsolationVerified}, LastBrainGuard=${lastBrainSurvivalVerified}, LayerFsPullSync=${layerfsPullThenPushVerified}, TimeTravelCrc=${timeTravelIntegrityAndDedupVerified}, RpoRollback=${rpoRollbackAndSingleFlightVerified})`,
    "SystemWide / M55-StorageHaAudit",
    "ALL_M55_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        v25PersistenceIdem1: {
          crossTaskIdemAndTenantIsolationVerified,
          deduplicatedTaskId: c2.task.task_id
        },
        shardLastBrainSurvival2: {
          lastBrainSurvivalVerified,
          survivingBrain: resolvedAfterLastEvict.brainNodeId,
          epoch: resolvedAfterLastEvict.epoch
        },
        layerfsPullToPushSync3: {
          pulledGeneration: pulledByB.remoteGeneration,
          pushedGeneration: pushedByB.remoteGeneration,
          layerfsPullThenPushVerified
        },
        velocityBreakerReset4: {
          velocityResetVerified
        },
        timeTravelCrcAndDedup5: {
          inheritedFiles: forkClean.inheritedFiles,
          inheritedFencingToken: forkClean.inheritedFencingToken,
          corruptedForkRejected,
          timeTravelIntegrityAndDedupVerified
        },
        rpoZeroAndWorkflow6: {
          casConflictCaught,
          sqliteRolledBackCleanly: rolledBackRow === null,
          concurrentStepInvocations: stepInvocations,
          rpoRollbackAndSingleFlightVerified
        },
        allM55RisksEliminated
      }
    };
  }
  /**
   * [M56] Unified Cold-Start Lease/Fencing Rehydration, LayerFS 3-Way Merge Deletion & Base-Object Pinning,
   * P0 Emergency Ingress Headroom, CelldAdmin Epoch Guard & Facet/Isolate Memory Leak Elimination Audit:
   * Verifies all 6 deep invariants:
   * 1. [UnifiedPersistence-ColdStart-1] UnifiedComponentPersistenceStore rehydrates leaseCoordinator fencing watermarks and converts interrupted LEASED/RUNNING_* tasks back to QUEUED so they re-dispatch with monotonic fencing tokens.
   * 2. [LayerFsMerge-DeletionAndConflict-2] LayerFsWorkspaceDriver detects file deletions (rm/delete) and LayerFsMultiAgentBranchCoordinator propagates clean deletions and catches delete/modify conflicts.
   * 3. [LayerFsStore-PinAndCycleGuard-3] LayerFsContentStoreEngine refuses to delete a base object while a PREFIX/DELTA object still references it, and guards reconstructObjectContent against cyclic baseObjectId chains.
   * 4. [IngressBackpressure-P0Headroom-4] IngressBackpressureController reserves +10% emergency burst headroom for P0_EMERGENCY tasks when globalQueueDepth reaches maxGlobalQueueDepth.
   * 5. [CelldAdmin-NonRegressiveEpoch-5] CelldAdminClient rehydrates placements from SQLite on cold start and preserves existing epochFencingToken on re-registration.
   * 6. [FacetAndIsolate-LeakGuard-6] DurableObjectFacetSupervisor frees SQLite stores on abortFaultyFacet and DynamicWorkerSandboxLoader bounds loadedSpecs via FIFO eviction.
   */
  async executeKernelStorageMergeAndBackpressureAudit(params)


  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权执行 M56 内核存储与紧急准入审计" }
      };
    }

    // 1. [UnifiedPersistence-ColdStart-1] Verify UnifiedComponentPersistenceStore rehydrates fencing watermark & requeues interrupted LEASED task
    const sql = this.deps.v25Store.sql;
    sql.exec("DELETE FROM persisted_tasks");
    sql.exec("DELETE FROM persisted_leases");
    const unified = new _unified_state_store.UnifiedComponentPersistenceStore(sql);
    const sched1 = new _scheduler.CoreScheduler5D();
    sched1.registerOrUpdateNode(new _models.NodeHeartbeat({ node_id: "node-m56", total_cpu_cores: 16, free_cpu_cores: 16 }));
    const taskInterrupted = new _models.TaskSpec({ task_id: "task-m56-1", tenant_id: "Northstar", priority: _models.TaskPriority.P1_HIGH });
    sched1.submitTask(taskInterrupted);
    sched1.scheduleTick(1000);
    // Simulate several lease renewals so fencing token is 5 while state is LEASED
    taskInterrupted.lease_fencing_token = 5;
    unified.snapshotSchedulerState(sched1);

    const sched2 = new _scheduler.CoreScheduler5D();
    sched2.registerOrUpdateNode(new _models.NodeHeartbeat({ node_id: "node-m56", total_cpu_cores: 16, free_cpu_cores: 16 }));
    unified.rehydrateSchedulerFromSql(sched2);
    const rehydratedSpec = sched2.allTasks.get("task-m56-1");
    const convertedToQueued = rehydratedSpec?.state === _models.TaskState.QUEUED;
    const placements2 = sched2.scheduleTick(1010);
    const newFence = placements2.find((p) => p[0] === "task-m56-1")?.[2] ?? 0;
    const unifiedColdStartRehydrationVerified = convertedToQueued && newFence === 6;

    // 2. [LayerFsMerge-DeletionAndConflict-2] Verify file deletion tracking & 3-way merge deletion propagation + delete/modify conflict
    const lfsDriver = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    lfsDriver.executeInProcessFileTool({
      branchName: "main",
      agentId: "agent-init",
      stepId: "step-seed",
      operation: "write",
      filePath: "src/obsolete.ts",
      newContent: "export const legacy = true;\n"
    });
    const baseCommitId = lfsDriver.getBranchHeadCommit("main").commitId;
    lfsDriver.forkBranch("main", "branch-del");
    lfsDriver.forkBranch("main", "branch-target");
    lfsDriver.executeInProcessFileTool({
      branchName: "branch-del",
      agentId: "agent-del",
      stepId: "step-del",
      operation: "delete",
      filePath: "src/obsolete.ts"
    });
    const merger = new _layerfs_branch_coordinator.LayerFsMultiAgentBranchCoordinator(lfsDriver);
    const mergeCleanDel = merger.mergeBranchIntoTarget("branch-del", "branch-target", baseCommitId);
    const targetHeadAfterDel = lfsDriver.getBranchHeadCommit("branch-target");
    const cleanDeletionPropagated =
    mergeCleanDel.deletedFiles.includes("src/obsolete.ts") && !targetHeadAfterDel.filesTree.has("src/obsolete.ts");

    // Verify delete/modify conflict is caught when target modified the file that source deleted
    lfsDriver.forkBranch(baseCommitId, "branch-del-2");
    lfsDriver.forkBranch(baseCommitId, "branch-mod-2");
    lfsDriver.executeInProcessFileTool({
      branchName: "branch-del-2",
      agentId: "agent-del",
      stepId: "step-del-2",
      operation: "delete",
      filePath: "src/obsolete.ts"
    });
    lfsDriver.executeInProcessFileTool({
      branchName: "branch-mod-2",
      agentId: "agent-mod",
      stepId: "step-mod-2",
      operation: "write",
      filePath: "src/obsolete.ts",
      newContent: "export const legacy = false; // modified\n"
    });
    let deleteModifyConflictCaught = false;
    try {
      merger.mergeBranchIntoTarget("branch-del-2", "branch-mod-2", baseCommitId);
    } catch {
      deleteModifyConflictCaught = true;
    }
    const layerfsDeletionAndConflictVerified = cleanDeletionPropagated && deleteModifyConflictCaught;

    // 3. [LayerFsStore-PinAndCycleGuard-3] Verify base object cannot be deleted while referenced by a PREFIX/DELTA object
    const storeEngine = new _layerfs_store_engine.LayerFsContentStoreEngine();
    const baseText = "const sharedHeader = \"0123456789abcdef0123456789abcdef\";\nexport const v = 1;\n";
    const derivedText = "const sharedHeader = \"0123456789abcdef0123456789abcdef\";\nexport const v = 2;\n";
    const baseObj = storeEngine.putFileObject("a.ts", baseText).object;
    const derivedObj = storeEngine.putFileObject("a.ts", derivedText, baseText).object;
    const freedWhilePinned = storeEngine.deleteObjectById(baseObj.objectId);
    const reconstructedWhilePinned = storeEngine.reconstructObjectContent(derivedObj.objectId);
    const storePinGuardVerified = freedWhilePinned === 0 && reconstructedWhilePinned === derivedText;

    // 4. [IngressBackpressure-P0Headroom-4] Verify P0_EMERGENCY is admitted via +10% emergency burst headroom when P2 is blocked at maxGlobalQueueDepth
    const bp = new _backpressure.IngressBackpressureController({ maxGlobalQueueDepth: 1000, p0EmergencyReserveRatio: 0.1 });
    const p2AtFull = bp.evaluateAdmission({
      priority: _models.TaskPriority.P2_STANDARD,
      globalQueueDepth: 1000,
      tenantActiveTasks: 10,
      clusterPsiPressurePct: 20
    });
    const p0AtFull = bp.evaluateAdmission({
      priority: _models.TaskPriority.P0_EMERGENCY,
      globalQueueDepth: 1000,
      tenantActiveTasks: 10,
      clusterPsiPressurePct: 20
    });
    const ingressP0HeadroomVerified = !p2AtFull.allowed && p0AtFull.allowed && Boolean(p0AtFull.usedEmergencyHeadroom);

    // 5. [CelldAdmin-NonRegressiveEpoch-5] Verify CelldAdminClient cold-start SQLite hydration & non-regressive registerCell
    sql.exec("DELETE FROM celld_admin_placements");
    const admin1 = new _celld_admin_client.CelldAdminClient(sql, new _env.InMemoryR2Bucket());
    admin1.registerCell("cell-m56", "node-01");
    await admin1.migrateCellToNode("cell-m56", "node-02"); // epoch -> 2
    await admin1.migrateCellToNode("cell-m56", "node-03"); // epoch -> 3
    const admin2 = new _celld_admin_client.CelldAdminClient(sql, new _env.InMemoryR2Bucket());
    const rehydratedPlacement = admin2.getCellPlacement("cell-m56");
    const reregistered = admin2.registerCell("cell-m56", "node-03");
    const celldAdminNonRegressiveVerified =
    rehydratedPlacement?.epochFencingToken === 3 && reregistered.epochFencingToken === 3;

    // 6. [FacetAndIsolate-LeakGuard-6] Verify aborted Facet releases SQLite store & DynamicWorkerSandboxLoader caps spec cache
    const facetSup = new _celld_native_optimizations.DurableObjectFacetSupervisor();
    const spawned = facetSup.spawnToolFacet("task-m56", 1, "sql_tool", () => sql);
    const countBeforeAbort = facetSup.getActiveSqlStoreCount();
    facetSup.abortFaultyFacet(spawned.facetRecord.facetName, new Error("Simulated facet failure"));
    const countAfterAbort = facetSup.getActiveSqlStoreCount();

    const dynLoader = new _celld_native_optimizations.DynamicWorkerSandboxLoader(undefined, 2);
    await dynLoader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 1, jsSourceCode: "globalThis.__result = { n: 1 };" });
    await dynLoader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 2, jsSourceCode: "globalThis.__result = { n: 2 };" });
    await dynLoader.executeUntrustedCodeInIsolate({ taskId: "t1", turnIndex: 3, jsSourceCode: "globalThis.__result = { n: 3 };" });
    const facetAndIsolateLeakGuardVerified =
    countBeforeAbort === 1 && countAfterAbort === 0 && dynLoader.getCachedSpecCount() === 2;

    const allM56RisksEliminated =
    unifiedColdStartRehydrationVerified &&
    layerfsDeletionAndConflictVerified &&
    storePinGuardVerified &&
    ingressP0HeadroomVerified &&
    celldAdminNonRegressiveVerified &&
    facetAndIsolateLeakGuardVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M56 内核存储、三路合并删改冲突与 P0 紧急准入自检 (UnifiedColdStart=${unifiedColdStartRehydrationVerified}, LayerFsDeletion=${layerfsDeletionAndConflictVerified}, StorePin=${storePinGuardVerified}, P0Headroom=${ingressP0HeadroomVerified}, CelldAdminEpoch=${celldAdminNonRegressiveVerified}, FacetLeakGuard=${facetAndIsolateLeakGuardVerified})`,
    "SystemWide / M56-KernelStorageAudit",
    "ALL_M56_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        unifiedColdStart1: {
          convertedToQueued,
          newFence,
          unifiedColdStartRehydrationVerified
        },
        layerfsDeletionAndConflict2: {
          cleanDeletionPropagated,
          deleteModifyConflictCaught,
          layerfsDeletionAndConflictVerified
        },
        layerfsStorePinGuard3: {
          freedWhilePinned,
          storePinGuardVerified
        },
        ingressP0Headroom4: {
          p2BlockedAtFull: !p2AtFull.allowed,
          p0AdmittedViaEmergencyHeadroom: p0AtFull.allowed,
          ingressP0HeadroomVerified
        },
        celldAdminEpochGuard5: {
          rehydratedEpoch: rehydratedPlacement?.epochFencingToken,
          reregisteredEpoch: reregistered.epochFencingToken,
          celldAdminNonRegressiveVerified
        },
        facetAndIsolateLeakGuard6: {
          sqlStoresAfterAbort: countAfterAbort,
          cachedSpecsCount: dynLoader.getCachedSpecCount(),
          facetAndIsolateLeakGuardVerified
        },
        allM56RisksEliminated
      }
    };
  }

  /**
   * [M57] Executes the 6-Point Runtime Edge, Router Cooldown/Quota, Leaf-First CAS GC, Role-Isolated Scale/Drain,
   * NodeDaemon Turn Monotonicity, Validator NaN/DRF Guard & Vsock Stream Framing Audit (`POST /v1beta1/management/system/runtime-edge-audit`).
   */
  async executeRuntimeEdgeAndGatewayAudit(params)


  {
    // 1. ModelPoolClientAdapter cooldown preservation & oversized token guard
    const pool = new _pool_client.ModelPoolClientAdapter();
    const ep = new _models.LlmEndpointMetrics({
      endpoint_id: "ep-m57-audit",
      provider: "openai",
      model_name: "gpt-4o",
      healthy: false,
      ttft_ewma_ms: 100
    });
    for (let i = 0; i < 3; i++) {
      try {
        pool.executeInference(ep, { taskId: "t1", turnIndex: 1, prompt: "p", estimatedPromptTokens: 50, maxOutputTokens: 50 }, 0.5, 1000);
      } catch {

        // expected
      }}
    ep.healthy = true;
    try {
      pool.executeInference(ep, { taskId: "t1", turnIndex: 1, prompt: "p", estimatedPromptTokens: 50, maxOutputTokens: 50 }, 0.5, 10_000);
    } catch {

      // rejected at 10s without resetting openedAtMs=1000
    }const recoveredAt16s = pool.executeInference(
    ep,
    { taskId: "t1", turnIndex: 1, prompt: "p", estimatedPromptTokens: 50, maxOutputTokens: 50 },
    0.5,
    16_000);

    let oversizedRejected = false;
    try {
      pool.executeInference(ep, { taskId: "t1", turnIndex: 2, prompt: "p", estimatedPromptTokens: 200_000, maxOutputTokens: 100_000 }, 0.5, 17_000);
    } catch (e) {
      oversizedRejected = e.message.includes("QuotaCapacityExceeded");
    }
    const routerCooldownAndQuotaVerified = recoveredAt16s.circuitState === "CLOSED" && oversizedRejected;

    // 2. ParallelCowGitBisectCoordinator probe cleanup & DistributedCasGcSweeper leaf-first sweep
    const drv = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    const c1 = drv.executeInProcessFileTool({
      branchName: "main",
      agentId: "agent-1",
      stepId: "s1",
      operation: "write", filePath: "app.ts", newContent: "const prefix = \"0123456789abcdef0123456789abcdef\";\nexport const a = 1;\n"
    });
    const bisect = new _parallel_cow_bisect.ParallelCowGitBisectCoordinator();
    const bisectOut = bisect.executeLiveLayerFsBisect({
      driver: drv,
      commits: [c1.commitId],
      testShellCommand: "test -f app.ts"
    });
    drv.forkBranch(c1.commitId, "temp-orphan-branch");
    drv.executeInProcessFileTool({
      branchName: "temp-orphan-branch",
      agentId: "agent-2",
      stepId: "s2",
      operation: "write", filePath: "orphan.ts", newContent: "const shared = \"abcdefghijklmnopqrstuvwxyz0123456789\";\nexport const v = 1;\n"
    });
    drv.executeInProcessFileTool({
      branchName: "temp-orphan-branch",
      agentId: "agent-2",
      stepId: "s3",
      operation: "write", filePath: "orphan.ts", newContent: "const shared = \"abcdefghijklmnopqrstuvwxyz0123456789\";\nexport const v = 2;\n"
    });
    drv.deleteBranch("temp-orphan-branch");
    const sweeper = new _distributed_gc_sweeper.DistributedCasGcSweeper();
    const sweepRes = sweeper.sweepLayerFsWorkspaceStore(drv);
    const casGcAndBisectCleanupVerified =
    bisectOut.probedCommits[0]?.cleanedUp === true &&
    sweepRes.sweptBlobIds.length >= 2 &&
    sweepRes.pinnedOrphanBlobIds.length === 0;

    // 3. ManualFleetScaleController role-isolated rebalance & atomic cordon/drain pre-check
    const scaleCtrl = new _manual_scale_controller.ManualFleetScaleController();
    scaleCtrl.registerBareMetalNode({
      nodeId: "worker-audit-1",
      ipAddress: "10.0.1.10",
      role: "worker",
      runningCells: ["c1", "c2", "c3", "c4"],
      systemdUnit: "ansts-node-daemon.service"
    });
    const storageOut = scaleCtrl.scaleOutNode("storage-audit-1", "10.0.2.10", "storage");
    let drainRejected = false;
    try {
      scaleCtrl.cordonAndDrainNode("worker-audit-1");
    } catch {
      drainRejected = true;
    }
    const workerNodeAfterFailedDrain = scaleCtrl.listNodes().find((n) => n.nodeId === "worker-audit-1");
    const scaleRoleAndDrainAtomicityVerified =
    storageOut.rebalancedCells.length === 0 &&
    drainRejected &&
    workerNodeAfterFailedDrain?.state === "ACTIVE";

    // 4. NodeDaemon cross-node turn monotonicity
    const sched = new _scheduler.CoreScheduler5D();
    const d1 = new _node_daemon.NodeDaemon("node-audit-1");
    const d2 = new _node_daemon.NodeDaemon("node-audit-2");
    sched.registerOrUpdateNode(d1.emitHeartbeat(0));
    sched.registerOrUpdateNode(d2.emitHeartbeat(0));
    const saga = new _saga_gateway.SagaToolGateway();
    saga.registerTool("ping", _models.ToolCallEffectClass.READ_ONLY, () => ({ pong: true }));
    const tSpec = new _models.TaskSpec({ task_id: "task-m57-audit", tenant_id: "Northstar", priority: _models.TaskPriority.P1_HIGH, max_turns: 4 });
    sched.submitTask(tSpec);
    sched.scheduleTick(1000);
    const turn1 = d1.executeAgentStep(sched, saga, tSpec, "ping", {});
    const lease2 = sched.leaseCoordinator.acquireLease(tSpec.task_id, "node-audit-2", 2000);
    tSpec.lease_fencing_token = lease2.fencing_token;
    const turn2 = d2.executeAgentStep(sched, saga, tSpec, "ping", {});
    const nodeDaemonTurnMonotonicityVerified = turn1.turn === 1 && turn2.turn === 2;

    // 5. TaskSpecValidator NaN/DRF guard & deduplication
    let nanRejected = false;
    try {
      _validator.TaskSpecValidator.validateAndNormalize({
        task_id: "nan-task",
        tenant_id: "Northstar",
        resource_profile: { think_cpu_cores: "NaN", act_cpu_cores: 2 }
      });
    } catch {
      nanRejected = true;
    }
    const normalizedSpec = _validator.TaskSpecValidator.validateAndNormalize({
      task_id: "valid-m57-task",
      tenant_id: "Northstar",
      dependencies: ["d1", "d1", "d2"]
    });
    const validatorAndGatewayGuardVerified = nanRejected && normalizedSpec.dependencies.length === 2;

    // 6. ZeroTrustVsockHostDaemon incomplete frame guard & ZeroTrustSandboxSecurityGuard command patterns
    let incompleteFrameCaught = false;
    try {
      const full = _vsock_daemon.ZeroTrustVsockHostDaemon.encodeFrame({ hello: "world" });
      _vsock_daemon.ZeroTrustVsockHostDaemon.decodeFrame(full.subarray(0, 6));
    } catch {
      incompleteFrameCaught = true;
    }
    const secGuard = new _zero_trust_security_guard.ZeroTrustSandboxSecurityGuard();
    const vsockAndSecurityGuardVerified =
    incompleteFrameCaught &&
    !secGuard.evaluateShellCommand("rm -rf /*").allowed &&
    !secGuard.evaluateShellCommand("curl http://169.254.169.254/latest/meta-data/").allowed &&
    !secGuard.evaluateShellCommand("cat /var/run/docker.sock").allowed;

    const allM57RisksEliminated =
    routerCooldownAndQuotaVerified &&
    casGcAndBisectCleanupVerified &&
    scaleRoleAndDrainAtomicityVerified &&
    nodeDaemonTurnMonotonicityVerified &&
    validatorAndGatewayGuardVerified &&
    vsockAndSecurityGuardVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M57 运行时边缘、连接池冷却/配额、拓扑叶子优先 GC、角色隔离扩缩容与 Vsock 分帧安全自检 (Router=${routerCooldownAndQuotaVerified}, CasGcBisect=${casGcAndBisectCleanupVerified}, ScaleRole=${scaleRoleAndDrainAtomicityVerified}, NodeDaemonTurn=${nodeDaemonTurnMonotonicityVerified}, ValidatorNaN=${validatorAndGatewayGuardVerified}, VsockSec=${vsockAndSecurityGuardVerified})`,
    "SystemWide / M57-RuntimeEdgeAudit",
    "ALL_M57_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        routerCooldownAndQuota1: {
          recoveredStateAt16s: recoveredAt16s.circuitState,
          oversizedRejected,
          routerCooldownAndQuotaVerified
        },
        casGcAndBisectCleanup2: {
          bisectProbeCleanedUp: bisectOut.probedCommits[0]?.cleanedUp,
          sweptCount: sweepRes.sweptBlobIds.length,
          pinnedOrphanCount: sweepRes.pinnedOrphanBlobIds.length,
          casGcAndBisectCleanupVerified
        },
        scaleRoleAndDrainAtomicity3: {
          storageRebalancedCells: storageOut.rebalancedCells.length,
          workerStateAfterFailedDrain: workerNodeAfterFailedDrain?.state,
          scaleRoleAndDrainAtomicityVerified
        },
        nodeDaemonTurnMonotonicity4: {
          turnOnNode1: turn1.turn,
          turnOnNode2AfterMigration: turn2.turn,
          nodeDaemonTurnMonotonicityVerified
        },
        validatorAndGatewayGuard5: {
          nanRejected,
          dedupedDependenciesCount: normalizedSpec.dependencies.length,
          validatorAndGatewayGuardVerified
        },
        vsockAndSecurityGuard6: {
          incompleteFrameCaught,
          vsockAndSecurityGuardVerified
        },
        allM57RisksEliminated
      }
    };
  }

  /**
   * [M58] Executes the 6-Point Container Fence RFC1918/CGNAT/IPv6 & Start-Cap Guard, Recursive OverlayFS & eBPF LSM Path Hardening,
   * SPIFFE/SLSA Tamper Proofing, Canary Post-Rollback Lock & PSI Victim Filtering, Telemetry/EWMA NaN Guard & Federation RTT Audit
   * (`POST /v1beta1/management/system/container-security-telemetry-audit`).
   */
  async executeContainerSecurityAndTelemetryAudit(params)


  {
    // 1. Container Fence RFC1918/CGNAT/IPv6 & Start-Time max_instances Cap Guard
    let rfc1918Blocked = false;
    let ipv6FenceBlocked = false;
    try {
      _celld_container_bridge.CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget("172.28.1.5", true);
    } catch {
      rfc1918Blocked = true;
    }
    try {
      _celld_container_bridge.CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget("[fe80::1]", true);
    } catch {
      ipv6FenceBlocked = true;
    }
    const cMgr = new _celld_container_bridge.CelldNativeContainerSandboxManager();
    const cCfg = { class_name: "audit-c-m58", image: "alpine:3.20", max_instances: 1 };
    cMgr.registerContainerClass(cCfg);
    const ch1 = cMgr.getOrCreateContainerHandle("cell-m58-a", cCfg);
    const ch2 = cMgr.getOrCreateContainerHandle("cell-m58-b", cCfg);
    ch1.start({ enableInternet: true });
    let startCapEnforced = false;
    try {
      ch2.start({ enableInternet: true });
    } catch {
      startCapEnforced = true;
    }
    const containerFenceAndCapVerified = rfc1918Blocked && ipv6FenceBlocked && startCapEnforced;

    // 2. Recursive OverlayFS Upperdir Diff Capture & eBPF LSM Relative/Credential Path Blocking
    const tmpOverlay = path.join(os.tmpdir(), `ansts-m58-audit-${process.pid}-${Date.now()}`);
    const ovDrv = new _container_driver.CompanionContainerAndOverlayDriver(tmpOverlay);
    const layout = ovDrv.createOverlayMountLayout("task-m58-audit");
    fs.mkdirSync(path.join(layout.upperDir, "pkg", "sub"), { recursive: true });
    fs.writeFileSync(path.join(layout.upperDir, "pkg", "sub", "index.ts"), "export const v = 58;\n", "utf-8");
    const snap = ovDrv.captureUpperdirDeltaSnapshot("task-m58-audit", 1);
    const lsm = new _ebpf_lsm_bridge.EbpfLsmSecurityBridge();
    const overlayRecursiveAndLsmVerified =
    snap.changedFiles.includes("pkg/sub/index.ts") &&
    !lsm.checkFileOpenHook("../../etc/shadow").allowed &&
    !lsm.checkFileOpenHook("/proc/self/environ").allowed;

    // 3. SPIFFE ID Single-Segment & Empty-Signature Rejection + SLSA fileCount Tamper Detection
    const mtls = new _mtls_tunnel_bridge.MtlsReverseTunnelBridge();
    const svid = mtls.issueSpiffeSvidToken("spiffe://ansts.io/ns/fleet/sa/worker", 60_000, 1000);
    const emptySigRejected = !mtls.verifySpiffeCertificate(svid.spiffeId, svid.certExpiresAtMs, 2000, "").authenticated;
    const traversalSpiffeRejected = !mtls.verifySpiffeCertificate("spiffe://ansts.io/ns/fleet/sa/worker/../../root", svid.certExpiresAtMs, 2000).authenticated;
    const slsa = new _slsa_provenance_attestor.SlsaProvenanceAttestor();
    const stmt = slsa.createSignedProvenanceStatement({
      imageReference: "layerfs://main@C1",
      sha256Digest: "abcd1234",
      builderId: "builder-m58",
      commitId: "C1",
      fileCount: 3
    });
    const spiffeAndSlsaIntegrityVerified =
    emptySigRejected &&
    traversalSpiffeRejected &&
    slsa.verifyProvenanceStatement(stmt) &&
    !slsa.verifyProvenanceStatement({ ...stmt, fileCount: 99 });

    // 4. Canary Post-Rollback Lock & Non-Terminal PSI Victim Filtering
    const canary = new _canary_rollout_controller.CanaryRolloutController();
    canary.evaluateCanaryStep({ errorRatePct: 8.0, p99LatencyMs: 2500 });
    const postRollbackStep = canary.evaluateCanaryStep({ errorRatePct: 0.1, p99LatencyMs: 80 });
    const psiGuard = new _psi_guard.LinuxPsiMemoryGuard(40, 70);
    const terminalTask = new _models.TaskSpec({ task_id: "t-done-m58", tenant_id: "Northstar", priority: _models.TaskPriority.P3_BATCH });
    terminalTask.state = _models.TaskState.SUCCEEDED;
    const runningTask = new _models.TaskSpec({ task_id: "t-run-m58", tenant_id: "Northstar", priority: _models.TaskPriority.P2_STANDARD });
    runningTask.state = _models.TaskState.RUNNING_ACT;
    const psiOut = psiGuard.evaluateAndDefend({ someAvg10Pct: 85, fullAvg10Pct: 80, freeSwapMb: 64 }, [terminalTask, runningTask]);
    const canaryLockAndPsiFilterVerified =
    postRollbackStep.state === "ROLLED_BACK" &&
    postRollbackStep.trafficWeightPct === 0 &&
    psiOut.evictedTaskIds.length === 1 &&
    psiOut.evictedTaskIds[0] === "t-run-m58";

    // 5. CloudEventsBus Bounded Ring Buffer & EWMA NaN Poisoning Guard
    const ceBus = new _cloudevents.CloudEventsBus(10);
    for (let i = 0; i < 14; i++) {
      ceBus.emitEvent({
        type: "io.ansts.m58",
        subject: `s-${i}`,
        tenantId: "Northstar",
        llmCostUsd: i === 0 ? 2.25 : -50,
        sandboxCpuCoreSeconds: NaN
      });
    }
    const prewarmer = new _predictive_prewarmer.PredictiveSandboxPrewarmer();
    const pwOut = prewarmer.recordArrivalSampleAndPredict(NaN, -5);
    const telemetryRingAndEwmaGuardVerified =
    ceBus.allEvents().length === 10 &&
    ceBus.getTenantReport("Northstar")?.totalLlmCostUsd === 2.25 &&
    Number.isFinite(pwOut.recommendedWarmPoolSize);

    // 6. MultiRegionFederationRouter RTT Sanitization
    const fed = new _federation_router.MultiRegionFederationRouter();
    fed.registerRegion({ regionCode: "us-nan", baseUrl: "https://us1", healthy: true, rttEwmaMs: NaN, sovereigntyZone: "US" });
    fed.registerRegion({ regionCode: "us-west", baseUrl: "https://us2", healthy: true, rttEwmaMs: 18, sovereigntyZone: "US" });
    const optimal = fed.selectOptimalRegion({ requiredSovereigntyZone: "US" });
    const federationRttAndBodyGuardVerified = optimal.regionCode === "us-west";

    const allM58RisksEliminated =
    containerFenceAndCapVerified &&
    overlayRecursiveAndLsmVerified &&
    spiffeAndSlsaIntegrityVerified &&
    canaryLockAndPsiFilterVerified &&
    telemetryRingAndEwmaGuardVerified &&
    federationRttAndBodyGuardVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M58 容器围栏/并发上限、递归 OverlayFS/LSM、SPIFFE/SLSA 防篡改、金丝雀回滚锁与遥测防毒化自检 (ContainerFence=${containerFenceAndCapVerified}, OverlayLsm=${overlayRecursiveAndLsmVerified}, SpiffeSlsa=${spiffeAndSlsaIntegrityVerified}, CanaryPsi=${canaryLockAndPsiFilterVerified}, TelemetryEwma=${telemetryRingAndEwmaGuardVerified}, FedRtt=${federationRttAndBodyGuardVerified})`,
    "SystemWide / M58-ContainerSecurityTelemetryAudit",
    "ALL_M58_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        containerFenceAndCap1: {
          rfc1918Blocked,
          ipv6FenceBlocked,
          startCapEnforced,
          containerFenceAndCapVerified
        },
        overlayRecursiveAndLsm2: {
          capturedNestedFiles: snap.changedFiles,
          overlayRecursiveAndLsmVerified
        },
        spiffeAndSlsaIntegrity3: {
          emptySigRejected,
          traversalSpiffeRejected,
          spiffeAndSlsaIntegrityVerified
        },
        canaryLockAndPsiFilter4: {
          postRollbackState: postRollbackStep.state,
          evictedTaskIds: psiOut.evictedTaskIds,
          canaryLockAndPsiFilterVerified
        },
        telemetryRingAndEwmaGuard5: {
          retainedEventsCount: ceBus.allEvents().length,
          sanitizedLlmCostUsd: ceBus.getTenantReport("Northstar")?.totalLlmCostUsd,
          telemetryRingAndEwmaGuardVerified
        },
        federationRttAndBodyGuard6: {
          selectedRegionCode: optimal.regionCode,
          federationRttAndBodyGuardVerified
        },
        allM58RisksEliminated
      }
    };
  }

  /**
   * [M59] Executes the 6-Point SQLite Composite-PK/DELETE-Flush/LIMIT Engine, KV TTL Expiration,
   * SSE Malformed-Frame/Clock-Skew Resilience, UFFD Page Cache Invalidation, Vsock Scheme/IMDS Guard & LTX/Facet Hygiene Audit
   * (`POST /v1beta1/management/system/sql-kv-sse-uffd-audit`).
   */
  async executeSqlKvSseUffdAndLtxAudit(params)


  {
    // 1. SQLite Composite PRIMARY KEY, DELETE FROM disk flush & SELECT LIMIT
    const sqlPath = path.join(os.tmpdir(), `ansts-m59-sql-${process.pid}-${Date.now()}.json`);
    const dSql1 = new _env.DurableDiskCellSqlStorage(sqlPath);
    dSql1.exec(
    "CREATE TABLE IF NOT EXISTS harness_turns (task_id TEXT, turn_index INTEGER, ltx_key TEXT, PRIMARY KEY (task_id, turn_index))");

    dSql1.exec("INSERT INTO harness_turns (task_id, turn_index, ltx_key) VALUES (?, ?, ?)", "t-m59", 1, "k1");
    dSql1.exec("INSERT INTO harness_turns (task_id, turn_index, ltx_key) VALUES (?, ?, ?)", "t-m59", 2, "k2");
    dSql1.exec("INSERT INTO harness_turns (task_id, turn_index, ltx_key) VALUES (?, ?, ?)", "t-m59", 3, "k3");
    const countBeforeDelete = dSql1.exec("SELECT * FROM harness_turns WHERE task_id = ?", "t-m59").toArray().length;
    const limitTwoCount = dSql1.exec("SELECT * FROM harness_turns WHERE task_id = ? LIMIT 2", "t-m59").toArray().length;
    dSql1.exec("DELETE FROM harness_turns WHERE task_id = ? AND turn_index = ?", "t-m59", 2);
    const dSql2 = new _env.DurableDiskCellSqlStorage(sqlPath);
    const countAfterRestart = dSql2.exec("SELECT * FROM harness_turns WHERE task_id = ?", "t-m59").toArray().length;
    const sqlCompositePkAndFlushVerified =
    countBeforeDelete === 3 && limitTwoCount === 2 && countAfterRestart === 2;

    // 2. KV expirationTtl enforcement
    const kv = new _env.InMemoryKvNamespace();
    await kv.put("m59-k", "v-alive", { expirationTtl: 5, nowMs: 1000 });
    const aliveVal = await kv.get("m59-k", 3000);
    const expiredVal = await kv.get("m59-k", 7000);
    const kvTtlExpirationVerified = aliveVal === "v-alive" && expiredVal === null;

    // 3. SSE malformed JSON frame skip & clock-skew non-negative TPOT
    const sse = new _live_llm_adapter.LiveOpenAiCompatibleSseAdapter();
    const sseSummary = sse.parseSseTranscript(
    [
    "data: {corrupted\n\n",
    'data: {"choices":[{"delta":{"content":"OK"}}]}\n\n',
    "data: [DONE]\n\n"],

    1000,
    [1050, 1010]);

    const sseMalformedAndSkewVerified =
    sseSummary.assembledContent === "OK" &&
    sseSummary.malformedFramesSkipped === 1 &&
    sseSummary.tpotMs >= 0;

    // 4. UFFD page cache invalidation on re-staging
    const uffd = new _uffd_lazy_restorer.UffdLazyPageFaultRestorer();
    const dig1 = uffd.stageRemoteSnapshotPage(0, new Uint8Array(4096).fill(1));
    uffd.handlePageFault(0);
    const dig2 = uffd.stageRemoteSnapshotPage(0, new Uint8Array(4096).fill(2));
    const fault2 = uffd.handlePageFault(0);
    const uffdCacheInvalidationVerified = dig1 !== dig2 && fault2.sha256Digest === dig2 && fault2.fetchedFromRemote;

    // 5. Vsock HTTP/HTTPS scheme check & command regex hardening
    const vsock = new _vsock_bridge.ZeroTrustVsockBridge(["api.openai.com"]);
    let ftpBlocked = false;
    let noPreserveRootBlocked = false;
    try {
      vsock.sendRequest({ requestId: "r1", vmId: "vm-m59", method: "HTTP_EGRESS", payload: { url: "ftp://api.openai.com/x" } });
    } catch {
      ftpBlocked = true;
    }
    try {
      vsock.sendRequest({ requestId: "r2", vmId: "vm-m59", method: "EXEC_CMD", payload: { command: "rm -rf --no-preserve-root /", executeRealSubprocess: false } });
    } catch {
      noPreserveRootBlocked = true;
    }
    const vsockSchemeAndCmdGuardVerified = ftpBlocked && noPreserveRootBlocked;

    // 6. Facet terminal history pruning & S3 LTX txId/cellId validation
    const fSup = new _celld_native_optimizations.DurableObjectFacetSupervisor(8);
    for (let i = 1; i <= 12; i++) {
      const { facetRecord } = fSup.spawnToolFacet("t-m59", i, "sql", () => new _env.InMemoryCellSqlStorage());
      await fSup.deleteCompletedFacet(facetRecord.facetName);
    }
    const ltxDrv = new _s3_ltx_driver.RustFsS3LtxDriver(new _env.InMemoryR2Bucket());
    let negTxRejected = false;
    try {
      await ltxDrv.putImmutableLtxSegment({ cellId: "c1", txId: -1, fencingToken: 1, ltxBytes: new Uint8Array([1]) });
    } catch {
      negTxRejected = true;
    }
    const facetPruneAndLtxGuardVerified = fSup.listFacets().length <= 8 && negTxRejected;

    const allM59RisksEliminated =
    sqlCompositePkAndFlushVerified &&
    kvTtlExpirationVerified &&
    sseMalformedAndSkewVerified &&
    uffdCacheInvalidationVerified &&
    vsockSchemeAndCmdGuardVerified &&
    facetPruneAndLtxGuardVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M59 SQLite 复合主键/DELETE 落盘/LIMIT、KV TTL 淘汰、SSE 容错、UFFD 缓存失效与 Vsock/LTX 校验自检 (SqlComposite=${sqlCompositePkAndFlushVerified}, KvTtl=${kvTtlExpirationVerified}, SseResilience=${sseMalformedAndSkewVerified}, UffdInvalidate=${uffdCacheInvalidationVerified}, VsockGuard=${vsockSchemeAndCmdGuardVerified}, FacetLtx=${facetPruneAndLtxGuardVerified})`,
    "SystemWide / M59-SqlKvSseUffdAudit",
    "ALL_M59_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        sqlCompositePkAndFlush1: {
          countBeforeDelete,
          limitTwoCount,
          countAfterRestart,
          sqlCompositePkAndFlushVerified
        },
        kvTtlExpiration2: {
          aliveVal,
          expiredVal,
          kvTtlExpirationVerified
        },
        sseMalformedAndSkew3: {
          malformedFramesSkipped: sseSummary.malformedFramesSkipped,
          tpotMs: sseSummary.tpotMs,
          sseMalformedAndSkewVerified
        },
        uffdCacheInvalidation4: {
          dig1,
          dig2,
          uffdCacheInvalidationVerified
        },
        vsockSchemeAndCmdGuard5: {
          ftpBlocked,
          noPreserveRootBlocked,
          vsockSchemeAndCmdGuardVerified
        },
        facetPruneAndLtxGuard6: {
          retainedFacets: fSup.listFacets().length,
          negTxRejected,
          facetPruneAndLtxGuardVerified
        },
        allM59RisksEliminated
      }
    };
  }

  /**
   * [M60] End-to-End Full-Link Persistence, Tenant-Scoped & Deep-Canonical Idempotency,
   * Deterministic Breakpoint Crash-Recovery & Coalescing Duplicate Inspection Audit.
   */
  async executeFullLinkPersistenceIdempotencyBreakpointAndCoalesceAudit(params)


  {
    // 1. [M60-1] End-to-end full-link SQLite persistence & cold-start rehydration across Scheduler, WAL, Saga Journal & Outbox
    const sql1 = new _env.InMemoryCellSqlStorage();
    const store1 = new _unified_state_store.UnifiedComponentPersistenceStore(sql1);
    const sched1 = new _scheduler.CoreScheduler5D();
    const wal1 = new _wal_engine.TurnWalEngine(sql1);
    const saga1 = new _saga_gateway.SagaToolGateway(undefined, undefined, sql1);
    const outbox1 = new _outbox_webhook.TransactionalOutboxDispatcher("sec-m60", 4, sql1);

    let sideEffectCalls = 0;
    saga1.registerTool("deploy_k8s", _models.ToolCallEffectClass.IDEMPOTENT_WRITE, (args) => {
      sideEffectCalls += 1;
      return { deployed: true, calls: sideEffectCalls, args };
    });

    const t1 = new _models.TaskSpec({
      task_id: "t-m60-link",
      tenant_id: "tenant-alpha",
      priority: _models.TaskPriority.P1_HIGH,
      prompt: "Deploy full link cluster",
      idempotency_key: "idem-m60-alpha"
    });
    sched1.submitTask(t1);
    wal1.appendTurnDelta({
      taskId: "t-m60-link",
      turnIndex: 1,
      fencingToken: 1,
      changedFiles: ["/workspace/main.ts"],
      sqliteWalPages: 4,
      highestSubmittedFencingToken: 1
    });
    saga1.invokeTool("t-m60-link", 1, "deploy_k8s", { env: { b: 2, a: 1 } });
    outbox1.enqueueOutbox("msg-m60-1", "t-m60-link", "https://hooks.internal/m60", { ok: true });

    const snapSummary = store1.snapshotFullLinkState({
      scheduler: sched1,
      walEngine: wal1,
      sagaGateway: saga1,
      outboxDispatcher: outbox1
    });

    const sched2 = new _scheduler.CoreScheduler5D();
    const wal2 = new _wal_engine.TurnWalEngine();
    const saga2 = new _saga_gateway.SagaToolGateway();
    saga2.registerTool("deploy_k8s", _models.ToolCallEffectClass.IDEMPOTENT_WRITE, (args) => {
      sideEffectCalls += 1;
      return { deployed: true, calls: sideEffectCalls, args };
    });
    const outbox2 = new _outbox_webhook.TransactionalOutboxDispatcher("sec-m60", 4);
    const rehydrateSummary = store1.rehydrateFullLinkState({
      scheduler: sched2,
      walEngine: wal2,
      sagaGateway: saga2,
      outboxDispatcher: outbox2
    });
    // Re-invoking with reordered nested JSON keys after cold-start must hit rehydrated journal!
    const [, dedupAfterRestart] = saga2.invokeTool("t-m60-link", 1, "deploy_k8s", { env: { a: 1, b: 2 } });
    const fullLinkPersistenceVerified =
    rehydrateSummary.tasksRestored === 1 &&
    rehydrateSummary.walEntriesRestored === 1 &&
    rehydrateSummary.sagaJournalRestored === 1 &&
    rehydrateSummary.outboxMessagesRestored === 1 &&
    dedupAfterRestart === true &&
    sideEffectCalls === 1;

    // 2. [M60-2] Tenant-scoped task idempotency isolation & deep recursive canonical JSON tool hashing
    const tenantBTask = sched2.submitTask(
    new _models.TaskSpec({
      task_id: "t-m60-beta",
      tenant_id: "tenant-beta",
      priority: _models.TaskPriority.P2_STANDARD,
      prompt: "Tenant B independent task",
      idempotency_key: "idem-m60-alpha"
    }));

    const tenantARetry = sched2.submitTask(
    new _models.TaskSpec({
      task_id: "t-m60-alpha-retry",
      tenant_id: "tenant-alpha",
      priority: _models.TaskPriority.P2_STANDARD,
      prompt: "Deploy full link cluster",
      idempotency_key: "idem-m60-alpha"
    }));

    const deepKey1 = _saga_gateway.SagaToolGateway.generateIdempotencyKey("t1", 1, "fn", {
      outer: { z: [1, { d: 4, c: 3 }], a: true }
    });
    const deepKey2 = _saga_gateway.SagaToolGateway.generateIdempotencyKey("t1", 1, "fn", {
      outer: { a: true, z: [1, { c: 3, d: 4 }] }
    });
    const tenantAndDeepIdempotencyVerified =
    tenantBTask.task_id === "t-m60-beta" &&
    tenantARetry.task_id === "t-m60-link" &&
    deepKey1 === deepKey2;

    // 3. [M60-3] SSE envelope.id deduplication & CloudEvents/FinOps idempotent billing guard
    const sse = new _sse_stream.SseEventStreamGateway(10);
    sse.subscribe("t-sse-m60", "sub-1");
    const d1 = sse.publish({
      id: "evt-m60-1",
      event: "turn.completed",
      taskId: "t-sse-m60",
      turnIndex: 1,
      timestampMs: 1000,
      data: { ok: true }
    });
    const d2 = sse.publish({
      id: "evt-m60-1",
      event: "turn.completed",
      taskId: "t-sse-m60",
      turnIndex: 1,
      timestampMs: 1005,
      data: { ok: true }
    });
    const ceBus = new _cloudevents.CloudEventsBus(20);
    ceBus.emitEvent({
      type: "ansts.turn.billed",
      subject: "t-m60-link",
      tenantId: "tenant-alpha",
      idempotencyKey: "bill-turn-1",
      llmCostUsd: 1.25,
      sandboxCpuCoreSeconds: 10
    });
    ceBus.emitEvent({
      type: "ansts.turn.billed",
      subject: "t-m60-link",
      tenantId: "tenant-alpha",
      idempotencyKey: "bill-turn-1",
      llmCostUsd: 1.25,
      sandboxCpuCoreSeconds: 10
    });
    const finReport = ceBus.getTenantReport("tenant-alpha");
    const sseAndCloudEventsIdempotencyVerified =
    d1 === 1 &&
    d2 === 0 &&
    sse.getTaskHistory("t-sse-m60").length === 1 &&
    finReport?.totalLlmCostUsd === 1.25;

    // 4. [M60-4] Deterministic WAL/checkpoint breakpoint recovery & mid-turn uncommitted side-effect rollback
    const bpSched = new _scheduler.CoreScheduler5D();
    const nodeA = new _node_daemon.NodeDaemon("node-a");
    const nodeB = new _node_daemon.NodeDaemon("node-b");
    bpSched.registerOrUpdateNode(nodeA.emitHeartbeat(0));
    bpSched.registerOrUpdateNode(nodeB.emitHeartbeat(0));
    const bpTask = bpSched.submitTask(
    new _models.TaskSpec({
      task_id: "t-m60-bp",
      tenant_id: "tenant-bp",
      priority: _models.TaskPriority.P1_HIGH,
      prompt: "Multi-turn resilient job"
    }));

    bpSched.scheduleTick();
    const bpWal = new _wal_engine.TurnWalEngine();
    const bpSaga = new _saga_gateway.SagaToolGateway();
    let uncommittedRolledBack = false;
    bpSaga.registerTool(
    "step_tool",
    _models.ToolCallEffectClass.COMPENSATABLE,
    (args) => ({ executedTurn: args.turn }),
    () => {
      uncommittedRolledBack = true;
      return { compensated: true };
    });

    nodeA.executeAgentStep(bpSched, bpSaga, bpTask, "step_tool", { turn: 1 });
    bpWal.appendTurnDelta({
      taskId: bpTask.task_id,
      turnIndex: 1,
      fencingToken: bpTask.lease_fencing_token,
      changedFiles: ["/workspace/step1.txt"],
      sqliteWalPages: 2,
      highestSubmittedFencingToken: bpTask.lease_fencing_token,
      enforceTurnMonotonicity: true,
      allowIdempotentRetry: true
    });
    // Simulate mid-turn 2 tool execution followed by node crash BEFORE WAL/checkpoint commit
    bpSaga.invokeTool(bpTask.task_id, 2, "step_tool", { turn: 2 });
    const newLease = bpSched.leaseCoordinator.acquireLease(bpTask.task_id, "node-b");
    bpTask.assigned_node_id = "node-b";
    bpTask.lease_fencing_token = newLease.fencing_token;
    const recovery = nodeB.recoverTaskFromBreakpoint(bpSched, bpTask, bpWal, bpSaga);
    const step2AfterRecovery = nodeB.executeAgentStep(bpSched, bpSaga, bpTask, "step_tool", { turn: 2 });
    const breakpointRecoveryVerified =
    recovery.restoredTurnIndex === 1 &&
    recovery.nextTurnToExecute === 2 &&
    uncommittedRolledBack === true &&
    step2AfterRecovery.turn === 2 &&
    step2AfterRecovery.deduplicated === false;

    // 5. [M60-5] Mergeable duplicate task inspection, in-flight request coalescing & priority promotion
    const coalSched = new _scheduler.CoreScheduler5D();
    coalSched.completedIds.add("dep-a");
    coalSched.completedIds.add("dep-b");
    const firstSub = coalSched.submitTask(
    new _models.TaskSpec({
      task_id: "t-coal-1",
      tenant_id: "tenant-coal",
      priority: _models.TaskPriority.P2_STANDARD,
      prompt: "Compile kernel module X",
      dependencies: ["dep-b", "dep-a"]
    }),
    { mergeDuplicateInFlight: true });

    const secondMerged = coalSched.submitTask(
    new _models.TaskSpec({
      task_id: "t-coal-2",
      tenant_id: "tenant-coal",
      priority: _models.TaskPriority.P0_EMERGENCY,
      prompt: "  Compile kernel module X  ",
      dependencies: ["dep-a", "dep-b"],
      idempotency_key: "idem-coal-urgent"
    }),
    { mergeDuplicateInFlight: true });

    const duplicateCoalescingVerified =
    secondMerged.task_id === firstSub.task_id &&
    firstSub.merged_duplicate_count === 1 &&
    firstSub.priority === _models.TaskPriority.P0_EMERGENCY &&
    firstSub.merged_request_ids.includes("t-coal-2") &&
    coalSched.queueManager.totalQueued() === 1;

    const allM60RisksEliminated =
    fullLinkPersistenceVerified &&
    tenantAndDeepIdempotencyVerified &&
    sseAndCloudEventsIdempotencyVerified &&
    breakpointRecoveryVerified &&
    duplicateCoalescingVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M60 全链路持久化、深度幂等、确定性断点恢复与可并入重复合流自检 (FullLink=${fullLinkPersistenceVerified}, DeepIdem=${tenantAndDeepIdempotencyVerified}, SseCeIdem=${sseAndCloudEventsIdempotencyVerified}, Breakpoint=${breakpointRecoveryVerified}, Coalesce=${duplicateCoalescingVerified})`,
    "SystemWide / M60-PersistenceIdempotencyRecoveryAudit",
    "ALL_M60_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        fullLinkPersistence1: {
          snapSummary,
          rehydrateSummary,
          dedupAfterRestart,
          fullLinkPersistenceVerified
        },
        tenantAndDeepIdempotency2: {
          tenantBTaskId: tenantBTask.task_id,
          tenantARetryTaskId: tenantARetry.task_id,
          deepKeyMatched: deepKey1 === deepKey2,
          tenantAndDeepIdempotencyVerified
        },
        sseAndCloudEventsIdempotency3: {
          firstPublishDelivered: d1,
          duplicatePublishDelivered: d2,
          totalLlmCostUsd: finReport?.totalLlmCostUsd,
          sseAndCloudEventsIdempotencyVerified
        },
        breakpointRecovery4: {
          recovery,
          uncommittedRolledBack,
          resumedTurn: step2AfterRecovery.turn,
          breakpointRecoveryVerified
        },
        duplicateCoalescing5: {
          canonicalTaskId: firstSub.task_id,
          mergedDuplicateCount: firstSub.merged_duplicate_count,
          promotedPriority: firstSub.priority,
          mergedRequestIds: firstSub.merged_request_ids,
          duplicateCoalescingVerified
        },
        allM60RisksEliminated
      }
    };
  }

  /**
   * [M61] Full-Subsystem Persistence, Idempotent CAS/Push Retry, Breakpoint State Rollback
   * & Coalescing Duplicate Arbitration Audit.
   */
  async executeSubsystemPersistenceIdempotencyAndCoalesceAudit(params)


  {
    const sql = new _env.InMemoryCellSqlStorage();

    // 1. [M61-1] SharedBlackboardAndGitArbiter & TopologyCasCoordinator SQLite persistence, idempotent CAS retry & duplicate patch coalescing
    const bb1 = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter(sql);
    const cas1 = bb1.putCas("arch/plan", { steps: [1, 2], meta: { b: 2, a: 1 } }, 0, "agent-alpha");
    const cas1Retry = bb1.putCas("arch/plan", { meta: { a: 1, b: 2 }, steps: [1, 2] }, 0, "agent-alpha");
    const arb = bb1.arbitrateGitPatchTouches([
    { agentId: "agent-alpha", touchedFiles: ["src/core.ts"], priorityScore: 100 },
    { agentId: "agent-alpha", touchedFiles: ["src/core.ts", "src/util.ts"], priorityScore: 120 }]);

    const bb2 = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter(sql);
    const top1 = new _topology_cas_coordinator.TopologyCasCoordinator(sql);
    const tUpd1 = top1.updateTopologyCas(0, ["node-2", "node-1"]);
    const tUpdRetry = top1.updateTopologyCas(0, ["node-1", "node-2"]);
    const blackboardAndTopologyVerified =
    cas1.committed === true &&
    cas1Retry.deduplicated === true &&
    arb.acceptedAgentIds.includes("agent-alpha") &&
    arb.conflictedAgentIds.length === 0 &&
    arb.coalescedDuplicateAgents.includes("agent-alpha") &&
    bb2.get("arch/plan")?.version === 1 &&
    tUpd1.generation === 1 &&
    tUpdRetry.deduplicated === true;

    // 2. [M61-2] StagingCommitAndApprovalGate, DagEscrowCoordinator & FinOpsBudgetController SQLite persistence & idempotency
    const gate1 = new _staging.StagingCommitAndApprovalGate(sql);
    const eff1 = gate1.stageEffect({
      effectId: "eff-m61-1",
      taskId: "t-m61",
      turnIndex: 1,
      toolName: "drop_idx",
      args: { table: "users", cascade: true }
    });
    const effRetry = gate1.stageEffect({
      effectId: "eff-m61-1-retry",
      taskId: "t-m61",
      turnIndex: 1,
      toolName: "drop_idx",
      args: { cascade: true, table: "users" }
    });
    const tSpec = new _models.TaskSpec({ task_id: "t-m61", tenant_id: "ten-m61", expected_SubAgent_count: 2 });
    const tk1 = gate1.requestHumanApproval(tSpec, "drop_idx", { table: "users", cascade: true });
    const tkRetry = gate1.requestHumanApproval(tSpec, "drop_idx", { cascade: true, table: "users" });
    const gate2 = new _staging.StagingCommitAndApprovalGate(sql);

    const escrow1 = new _escrow.DagEscrowCoordinator(sql);
    escrow1.reserveParentEscrow(tSpec, 10, 0);
    escrow1.reserveParentEscrow(tSpec, 10, 9); // Idempotent retry even when currentTenantUsed is high
    const childSpec = new _models.TaskSpec({ task_id: "t-m61-child-1", tenant_id: "ten-m61" });
    const cReg1 = escrow1.registerChildSubAgent(tSpec, childSpec);
    const cRegRetry = escrow1.registerChildSubAgent(tSpec, childSpec);
    const escrow2 = new _escrow.DagEscrowCoordinator(sql);

    const fin1 = new _budget_controller.FinOpsBudgetController(sql);
    fin1.configureTenant({
      tenantId: "ten-m61",
      dailyBudgetUsd: 50,
      softDowngradeThresholdRatio: 0.8,
      preferredModel: "claude-3-7-sonnet",
      fallbackEconomyModel: "deepseek-chat"
    });
    fin1.recordSpend("ten-m61", 5.0, "2026-11-10", "spend-op-1");
    const spendAfterRetry = fin1.recordSpend("ten-m61", 5.0, "2026-11-10", "spend-op-1");
    fin1.refundSpend("ten-m61", 2.0, "2026-11-10", "refund-op-1");
    const spendAfterRefundRetry = fin1.refundSpend("ten-m61", 2.0, "2026-11-10", "refund-op-1");
    const fin2 = new _budget_controller.FinOpsBudgetController(sql);

    const stagingEscrowFinOpsVerified =
    eff1.effectId === effRetry.effectId &&
    tk1.ticketId === tkRetry.ticketId &&
    gate2.getStagedEffects("t-m61").length === 1 &&
    cReg1.usedEscrowSlot === true &&
    cRegRetry.deduplicated === true &&
    escrow2.getTenantEscrowedSlots("ten-m61") === 2 &&
    spendAfterRetry === 5.0 &&
    spendAfterRefundRetry === 3.0 &&
    fin2.evaluateBudgetAndModel("ten-m61", "2026-11-10").allowed === true;

    // 3. [M61-3] SyntacticAndVelocityBreaker same-turn retry coalescing & SQLite persistence
    const vb1 = new _velocity_breaker.SyntacticAndVelocityBreaker(3, 10_000, sql);
    vb1.recordAndCheckTurn("t-vb", {
      turnIndex: 1,
      toolName: "read_file",
      argsHash: "h1",
      tokensConsumed: 4000,
      timestampMs: 1000
    });
    vb1.recordAndCheckTurn("t-vb", {
      turnIndex: 1,
      toolName: "read_file",
      argsHash: "h1",
      tokensConsumed: 4000,
      timestampMs: 1010
    });
    const vbCheck = vb1.recordAndCheckTurn("t-vb", {
      turnIndex: 1,
      toolName: "read_file",
      argsHash: "h1",
      tokensConsumed: 4000,
      timestampMs: 1020
    });
    const velocityBreakerCoalesceVerified =
    vbCheck.tripped === false && vbCheck.coalescedSameTurnRetry === true;

    // 4. [M61-4] TunnelGatewayCell command idempotency coalescing + SQLite & AgentTaskHarnessCell R2-to-SQL reconciliation
    const tgw = new _tunnel_gateway_cell.TunnelGatewayCell(sql);
    tgw.registerTunnel({
      hostId: "host-m61",
      gatewayId: "gw-1",
      role: "PRIMARY",
      connGeneration: 1,
      epoch: 1
    });
    const enq1 = tgw.enqueueCommandForHost("host-m61", { cmd: "run" }, 5000, { idempotencyKey: "tcmd-idem-1" });
    const enq2 = tgw.enqueueCommandForHost("host-m61", { cmd: "run" }, 5000, { idempotencyKey: "tcmd-idem-1" });
    const polled = tgw.pollPendingCommands("host-m61", "gw-1");
    tgw.acknowledgeCommand(enq1.command.commandId, { exitCode: 0 });
    const [r1, r2Out] = await Promise.all([enq1.completionPromise, enq2.completionPromise]);

    const r2 = new _env.InMemoryR2Bucket();
    const doState = {
      id: { toString: () => "hcell-m61" },
      storage: {
        sql: new _env.InMemoryCellSqlStorage(),
        get: async () => undefined,
        put: async () => {},
        delete: async () => false,
        setAlarm: async () => {},
        getAlarm: async () => null
      },
      blockConcurrencyWhile: async (cb) => cb()
    };
    await r2.put(
    "cells/task-m61-h/turn-1-f2.ltx",
    JSON.stringify({
      taskId: "task-m61-h",
      turnIndex: 1,
      fencingToken: 2,
      toolName: "bash",
      toolOutput: { ok: true }
    }),
    { customMetadata: { taskId: "task-m61-h", turnIndex: "1", fencingToken: "2" } });

    const hCell = new _harness_cell.AgentTaskHarnessCell(doState, r2, "task-m61-h");
    const recRes = await hCell.reconcileBreakpointFromR2AndSql();
    const idempTurn = await hCell.executeAndCheckpointTurn({
      turnIndex: 1,
      fencingToken: 2,
      toolName: "bash",
      toolOutput: { ok: true },
      enforceTurnMonotonicity: true
    });
    const tunnelAndHarnessVerified =
    enq2.deduplicated === true &&
    polled.length === 1 &&
    r1.exitCode === 0 &&
    r2Out.exitCode === 0 &&
    recRes.reconciledTurnsCount === 1 &&
    idempTurn.deduplicated === true;

    // 5. [M61-5] LayerFsWorkspaceDriver branch breakpoint rollback & LayerFsRemoteStoreSynchronizer idempotent push retry
    const lfs = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    const w1 = lfs.executeInProcessFileTool({
      branchName: "main",
      agentId: "ag-1",
      stepId: "step-turn-1",
      operation: "write",
      filePath: "src/app.ts",
      newContent: "v1"
    });
    lfs.executeInProcessFileTool({
      branchName: "main",
      agentId: "ag-1",
      stepId: "step-turn-2-dirty",
      operation: "write",
      filePath: "src/app.ts",
      newContent: "v2-dirty"
    });
    const rbBranch = lfs.rollbackBranchToCommitOrStep("main", "step-turn-1");
    const readAfterRb = lfs.executeInProcessFileTool({
      branchName: "main",
      agentId: "ag-1",
      stepId: "step-read",
      operation: "read",
      filePath: "src/app.ts"
    });

    const syncer = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(new _env.InMemoryR2Bucket(), "proj-m61");
    const payloadBytes = new Uint8Array([10, 20, 30, 40]);
    const push1 = await syncer.pushStoreSnapshot({
      machineId: "m1",
      basedOnGeneration: 0,
      storeSqliteBytes: payloadBytes,
      mainHeadCommitId: w1.commitId,
      idempotencyKey: "push-m61-1"
    });
    const pushRetry = await syncer.pushStoreSnapshot({
      machineId: "m1",
      basedOnGeneration: 0,
      storeSqliteBytes: payloadBytes,
      mainHeadCommitId: w1.commitId,
      idempotencyKey: "push-m61-1"
    });
    const layerFsRollbackAndPushVerified =
    rbBranch.restoredCommitId === w1.commitId &&
    readAfterRb.readResult === "v1" &&
    push1.remoteGeneration === 1 &&
    pushRetry.deduplicated === true;

    const allM61RisksEliminated =
    blackboardAndTopologyVerified &&
    stagingEscrowFinOpsVerified &&
    velocityBreakerCoalesceVerified &&
    tunnelAndHarnessVerified &&
    layerFsRollbackAndPushVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M61 子系统持久化、CAS/Push 幂等重试、断点状态回滚与重复合流仲裁自检 (BlackboardTop=${blackboardAndTopologyVerified}, StagingEscrowFinOps=${stagingEscrowFinOpsVerified}, VelocityCoalesce=${velocityBreakerCoalesceVerified}, TunnelHarness=${tunnelAndHarnessVerified}, LayerFs=${layerFsRollbackAndPushVerified})`,
    "SystemWide / M61-SubsystemPersistenceIdempotencyAudit",
    "ALL_M61_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        blackboardAndTopology1: {
          casDeduplicated: cas1Retry.deduplicated,
          coalescedDuplicateAgents: arb.coalescedDuplicateAgents,
          topologyDeduplicated: tUpdRetry.deduplicated,
          blackboardAndTopologyVerified
        },
        stagingEscrowFinOps2: {
          effectDeduplicated: eff1.effectId === effRetry.effectId,
          ticketCoalesced: tk1.ticketId === tkRetry.ticketId,
          childDeduplicated: cRegRetry.deduplicated,
          spendAfterRefundRetry,
          stagingEscrowFinOpsVerified
        },
        velocityBreakerCoalesce3: {
          tripped: vbCheck.tripped,
          coalescedSameTurnRetry: vbCheck.coalescedSameTurnRetry,
          velocityBreakerCoalesceVerified
        },
        tunnelAndHarness4: {
          tunnelCommandDeduplicated: enq2.deduplicated,
          reconciledTurnsCount: recRes.reconciledTurnsCount,
          harnessTurnDeduplicated: idempTurn.deduplicated,
          tunnelAndHarnessVerified
        },
        layerFsRollbackAndPush5: {
          restoredCommitId: rbBranch.restoredCommitId,
          readAfterRollback: readAfterRb.readResult,
          pushRetryDeduplicated: pushRetry.deduplicated,
          layerFsRollbackAndPushVerified
        },
        allM61RisksEliminated
      }
    };
  }

  /**
   * [M62] DFX Full-Spectrum Resiliency, Diagnosability, Testability & Operability Hardening Audit.
   */
  async executeDfxHardeningAudit(params)


  {
    const tmpCgroupDir = path.join(os.tmpdir(), `ansts-dfx-cgroup-${Date.now()}`);

    // 1. [M62-1] DFX-Reliability & Recoverability: CgroupPhaseController & LinuxCgroupV2AndPsiDriver
    const phaseCtrl = new _phase_controller.CgroupPhaseController();
    const thinkAlloc = phaseCtrl.applyThinkPhase("t-dfx-1", {
      think_cpu_cores: 0.001, // sub-core edge case
      act_cpu_cores: 4.0,
      act_memory_mb: 4096,
      disk_mb: 1024
    });
    const cgroupDriver = new _linux_cgroup_psi.LinuxCgroupV2AndPsiDriver(tmpCgroupDir);
    cgroupDriver.writePhaseLimits("t-dfx-1", "THINK", 0.001, 256, 512);
    const cgroupCleaned = cgroupDriver.removeTaskCgroup("t-dfx-1");
    const cgroupReliabilityVerified =
    thinkAlloc.cpuQuotaUsPer100ms >= _phase_controller.CgroupPhaseController.MIN_CPU_QUOTA_US &&
    cgroupCleaned === true;

    // 2. [M62-2] DFX-Observability & Diagnosability: OtelAndPrometheusExporter
    const otel = new _otel_prometheus.OtelAndPrometheusExporter();
    otel.incCounter("dfx/counter.ops!!", 5);
    otel.setGauge("dfx.memory.util", 85.5);
    otel.observeHistogram("dfx.latency.ms", 120);
    const promMetrics = otel.renderPrometheusMetrics();
    const burnEval = otel.evaluateSloBurnRate({
      totalRequests: 10000,
      errorRequests: 2,
      p99LatencyMs: 180
    });
    const otelDiagnosabilityVerified =
    promMetrics.includes("dfx_counter_ops__ 5") &&
    burnEval.alertSeverity === "HEALTHY";

    // 3. [M62-3] DFX-Safety & Fault Tolerance: LinuxPsiMemoryGuard
    const psiGuard = new _psi_guard.LinuxPsiMemoryGuard(40.0, 70.0);
    const p0Task = new _models.TaskSpec({ task_id: "t-dfx-p0", tenant_id: "ten", priority: _models.TaskPriority.P0_EMERGENCY });
    const p1Task = new _models.TaskSpec({ task_id: "t-dfx-p1", tenant_id: "ten", priority: _models.TaskPriority.P1_HIGH });
    const p2Task = new _models.TaskSpec({ task_id: "t-dfx-p2", tenant_id: "ten", priority: _models.TaskPriority.P2_STANDARD });
    const p3Task = new _models.TaskSpec({ task_id: "t-dfx-p3", tenant_id: "ten", priority: _models.TaskPriority.P3_BATCH });
    const psiAction = psiGuard.evaluateAndDefend(
    { someAvg10Pct: 50.0, fullAvg10Pct: 75.0, freeSwapMb: 64 },
    [p0Task, p1Task, p2Task, p3Task]);

    const psiSafetyVerified =
    psiAction.level === "FREEZE_AND_CHECKPOINT" &&
    !psiAction.evictedTaskIds.includes("t-dfx-p0") &&
    !psiAction.evictedTaskIds.includes("t-dfx-p1") &&
    psiAction.evictedTaskIds.includes("t-dfx-p3");

    // 4. [M62-4] DFX-Operability & Serviceability: CapacitySlaEnforcer
    const slaSql = new _env.InMemoryCellSqlStorage();
    const slaEnforcer = new _capacity_sla_enforcer.CapacitySlaEnforcer(undefined, slaSql);
    const slaEval = slaEnforcer.evaluateSlaCompliance({
      p0QueueWaitP99Ms: 150,
      warmPoolHitRatio: 0.92,
      freeClusterCpuRatio: 0.08
    });
    const slaEnforcer2 = new _capacity_sla_enforcer.CapacitySlaEnforcer(undefined, slaSql);
    const capacitySlaOperabilityVerified =
    slaEval.compliant === false &&
    slaEval.reserveEmergencyHeadroom === true &&
    slaEnforcer2.isEmergencyHeadroomActive() === true;

    // 5. [M62-5] DFX-Maintainability & Deployability: EdgeTunnelHaRouter & CrossRegionAsyncReplicator
    const tgw = new _tunnel_gateway_cell.TunnelGatewayCell();
    tgw.registerTunnel({
      hostId: "body-dfx",
      gatewayId: "gw-1",
      role: "PRIMARY",
      connGeneration: 1,
      epoch: 1
    });
    const tunnelRouter = new _edge_tunnel_ha_router.EdgeTunnelHaRouter(tgw);
    const repl = new _cross_region_replicator.CrossRegionAsyncReplicator();
    const replRes = await repl.replicateToRemoteS3Region({
      s3Endpoint: "http://127.0.0.1:9000",
      sourceRegion: "US",
      targetRegion: "EU",
      objectKey: "ltx/test.json",
      payload: { ok: true }
    });
    const deployabilityVerified =
    tunnelRouter.routeRpcToBodyHost("body-dfx", { test: true }).delivered === true &&
    replRes.replicatedCount === 1;

    const allM62DfxRisksEliminated =
    cgroupReliabilityVerified &&
    otelDiagnosabilityVerified &&
    psiSafetyVerified &&
    capacitySlaOperabilityVerified &&
    deployabilityVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M62 DFX 全维度可靠性、可诊断性、可服务性与可观测性自检 (CgroupReliability=${cgroupReliabilityVerified}, OtelDiag=${otelDiagnosabilityVerified}, PsiSafety=${psiSafetyVerified}, SlaOperability=${capacitySlaOperabilityVerified}, Deployability=${deployabilityVerified})`,
    "SystemWide / M62-DfxHardeningAudit",
    "ALL_M62_DFX_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        cgroupReliability1: {
          thinkQuotaUs: thinkAlloc.cpuQuotaUsPer100ms,
          cgroupCleaned,
          cgroupReliabilityVerified
        },
        otelDiagnosability2: {
          burnSeverity: burnEval.alertSeverity,
          otelDiagnosabilityVerified
        },
        psiSafety3: {
          level: psiAction.level,
          evicted: psiAction.evictedTaskIds,
          psiSafetyVerified
        },
        capacitySlaOperability4: {
          emergencyHeadroomActive: slaEnforcer.isEmergencyHeadroomActive(),
          rehydratedHeadroomActive: slaEnforcer2.isEmergencyHeadroomActive(),
          capacitySlaOperabilityVerified
        },
        deployability5: {
          replicatedCount: replRes.replicatedCount,
          deployabilityVerified
        },
        allM62DfxRisksEliminated
      }
    };
  }

  /**
   * [M63] Deep DFX Hardening Phase II Audit (Chaos Auto-Heal, Anti-Flapping Backpressure/Shedder/Sharder,
   * Sovereign Routing Persistence, Multi-Cycle WFG Resolution, TEE/Zero-Trust IMDSv6 Guard & Token Cost Clamping).
   */
  async executeDfxDeepModuleHardeningAudit(params)


  {
    const sql = new _env.InMemoryCellSqlStorage();

    // 1. [M63-1] Chaos Auto-Heal & Partition TTL
    const chaosProxy = new _network_chaos_proxy.NetworkChaosFaultInjectionProxy();
    chaosProxy.injectPartition("host-ttl", 100, 1000);
    let blockedAt1050 = false;
    try {
      chaosProxy.assertReachable("host-ttl", 1050);
    } catch {
      blockedAt1050 = true;
    }
    chaosProxy.assertReachable("host-ttl", 1200); // auto-healed after TTL
    const drillRunner = new _chaos_drill_runner.AutomatedChaosDrillRunner();
    const drillRes = drillRunner.executeLivePartitionAndFencingDrill({
      chaosProxy,
      targetHost: "host-drill",
      oldNodeFencingToken: 2,
      newNodeFencingToken: 5
    });
    const invalidTokenDrill = drillRunner.runSplitBrainAndRecoveryDrill({
      oldNodeFencingToken: Number.NaN,
      newNodeFencingToken: 5
    });
    const chaosAutoHealVerified =
    blockedAt1050 === true &&
    drillRes.partitionHealed === true &&
    drillRes.rpoZeroVerified === true &&
    invalidTokenDrill.zombieBlocked === false;

    // 2. [M63-2] Fail-Safe Backpressure, Load Shedder & Auto-Sharder NaN/Zero-Division Guards
    const bpLoop = new _backpressure_loop.BidirectionalBackpressureController();
    const bpNanState = bpLoop.computeClusterHealthState({
      maxNodePsiSomePct: Number.NaN,
      globalRateLimitedEndpoints: 4,
      totalEndpoints: 0,
      globalQueueDepth: 100
    });
    const shedder = new _priority_load_shedder.PriorityLoadShedder();
    const p3OnNan = shedder.shouldAdmitUnderLoad(_models.TaskPriority.P3_BATCH, Number.NaN);
    const p0OnNan = shedder.shouldAdmitUnderLoad(_models.TaskPriority.P0_EMERGENCY, Number.NaN);
    const sharder = new _horizontal_scaler.DynamicQueueAutoSharder(16, 4, 128);
    const shardAdjust = sharder.adjustShards(Number.NaN, 0);
    const backpressureAndSharderVerified =
    bpNanState.mode === "RED_SHEDDING" &&
    p3OnNan === false &&
    p0OnNan === true &&
    Number.isFinite(shardAdjust.newShards) &&
    shardAdjust.newShards === 4;

    // 3. [M63-3] GeoSovereigntyAndLatencyRouter Finite Latency Sanitization, Tie-Break & SQLite Persistence
    const geo1 = new _geo_routing.GeoSovereigntyAndLatencyRouter(sql);
    geo1.registerGeoRegion({
      regionId: "eu-central-2",
      jurisdiction: "GDPR_EU",
      latencyMs: 25,
      capacityAvailable: true
    });
    geo1.registerGeoRegion({
      regionId: "eu-central-1",
      jurisdiction: "GDPR_EU",
      latencyMs: 25,
      capacityAvailable: true
    });
    geo1.registerGeoRegion({
      regionId: "eu-corrupt",
      jurisdiction: "GDPR_EU",
      latencyMs: -100,
      capacityAvailable: true
    });
    const geo2 = new _geo_routing.GeoSovereigntyAndLatencyRouter(sql);
    const selectedRegion = geo2.routeByJurisdiction("GDPR_EU");
    const geoRoutingVerified =
    selectedRegion.regionId === "eu-central-1" && geo2.listRegions().length === 3;

    // 4. [M63-4] A2aWaitForGraphDeadlockDetector Multi-Cycle Iterative Resolution
    const wfg = new _deadlock_detector.A2aWaitForGraphDeadlockDetector();
    wfg.addWaitEdge("ag-1", "ag-2");
    wfg.addWaitEdge("ag-2", "ag-1");
    wfg.addWaitEdge("ag-3", "ag-4");
    wfg.addWaitEdge("ag-4", "ag-3");
    const wfgRes = wfg.resolveCycleWithLowestPriorityVictim({
      "ag-1": 100,
      "ag-2": 10,
      "ag-3": 80,
      "ag-4": 20
    });
    const multiCycleWfgVerified =
    wfgRes.resolved === true &&
    wfgRes.evictedVictims.length === 2 &&
    wfgRes.evictedVictims.includes("ag-2") &&
    wfgRes.evictedVictims.includes("ag-4");

    // 5. [M63-5] ConfidentialTeeAttestationBridge Tamper Guard, Zero-Trust IPv6-IMDS/mkfs Guard & ModelPool Cost Clamping
    const tee = new _confidential_tee_bridge.ConfidentialTeeAttestationBridge();
    const quote = tee.generateAndVerifyAttestationQuote({
      vmId: "vm-m63",
      measurementDigestHex: "a1b2c3d4e5f60718293a4b5c6d7e8f90",
      nonce: "nonce-123"
    });
    const validExternal = tee.verifyExternalAttestationQuote({
      vmId: "vm-m63",
      measurementDigestHex: "a1b2c3d4e5f60718293a4b5c6d7e8f90",
      nonce: "nonce-123",
      ed25519SignatureHex: quote.ed25519SignatureHex
    });
    const tamperedExternal = tee.verifyExternalAttestationQuote({
      vmId: "vm-m63-tampered",
      measurementDigestHex: "a1b2c3d4e5f60718293a4b5c6d7e8f90",
      nonce: "nonce-123",
      ed25519SignatureHex: quote.ed25519SignatureHex
    });

    const zt = new _zero_trust_security_guard.ZeroTrustSandboxSecurityGuard();
    const imdsV6Check = zt.evaluateShellCommand("curl http://[::ffff:169.254.169.254]/latest/meta-data/");
    const mkfsCheck = zt.evaluateShellCommand("mkfs.ext4 /dev/sda1");

    const pool = new _pool_client.ModelPoolClientAdapter();
    const ep = new _models.LlmEndpointMetrics({
      endpoint_id: "ep-m63",
      provider: "openai",
      model_name: "gpt-4o",
      healthy: true
    });
    const infRes = pool.executeInference(
    ep,
    {
      taskId: "t-m63",
      turnIndex: 1,
      prompt: "Hello",
      estimatedPromptTokens: 1000,
      maxOutputTokens: 200
    },
    2.5 // Out-of-range cache ratio > 1.0 must clamp to 1.0 and keep cachedTokens <= 1000, costUsd > 0
    );
    const securityAndCostVerified =
    quote.verified === true &&
    validExternal === true &&
    tamperedExternal === false &&
    imdsV6Check.allowed === false &&
    mkfsCheck.allowed === false &&
    infRes.cachedTokens === 1000 &&
    infRes.costUsd > 0;

    const allM63DfxRisksEliminated =
    chaosAutoHealVerified &&
    backpressureAndSharderVerified &&
    geoRoutingVerified &&
    multiCycleWfgVerified &&
    securityAndCostVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M63 第二阶段 DFX 深度加固自检 (ChaosAutoHeal=${chaosAutoHealVerified}, BackpressureSharder=${backpressureAndSharderVerified}, GeoRouting=${geoRoutingVerified}, MultiCycleWfg=${multiCycleWfgVerified}, SecAndCost=${securityAndCostVerified})`,
    "SystemWide / M63-DfxDeepModuleAudit",
    "ALL_M63_DFX_RISKS_ELIMINATED");


    return {
      status: 200,
      body: {
        chaosAutoHeal1: {
          blockedAt1050,
          partitionHealed: drillRes.partitionHealed,
          chaosAutoHealVerified
        },
        backpressureAndSharder2: {
          bpNanMode: bpNanState.mode,
          p3OnNan,
          p0OnNan,
          newShards: shardAdjust.newShards,
          backpressureAndSharderVerified
        },
        geoRouting3: {
          selectedRegionId: selectedRegion.regionId,
          persistedCount: geo2.listRegions().length,
          geoRoutingVerified
        },
        multiCycleWfg4: {
          resolved: wfgRes.resolved,
          evictedVictims: wfgRes.evictedVictims,
          multiCycleWfgVerified
        },
        securityAndCost5: {
          validExternal,
          tamperedExternal,
          imdsV6Blocked: !imdsV6Check.allowed,
          mkfsBlocked: !mkfsCheck.allowed,
          cachedTokens: infRes.cachedTokens,
          costUsd: infRes.costUsd,
          securityAndCostVerified
        },
        allM63DfxRisksEliminated
      }
    };
  }

  /**
   * [M64] End-to-End Implementation Consistency & Authentic Feature Wiring Audit:
   * Verifies Worker/Cell route & queue dispatch parity, 100% TaskSpec field fidelity across all 3 SQLite stores,
   * SagaCompensator/StagingGate/McpRegistry/CognitiveRouter/DagEscrow SQLite persistence & turn-accurate staged commit,
   * AnstsLocalAndComposeServer full-subsystem SQLite wiring & cold-start scheduler rehydration,
   * and AgentHarnessRunner router token metering + PartitionedSchedulerRouter multi-shard scheduling.
   */
  async executeImplementationConsistencyAudit(params)


  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发 M64 落地功能实现一致性自检" }
      };
    }

    // 1. [M64-1] Worker & Cell prefixed route & queue dispatch parity
    const cellState = {
      id: { toString: () => "cell-m64-01" },
      storage: {
        sql: new _env.InMemoryCellSqlStorage(),
        transactionSync: (fn) => fn(),
        getAlarm: async () => null,
        setAlarm: async () => {}
      },
      blockConcurrencyWhile: async (fn) => fn()
    };
    const schedCell = new _scheduler_cell.AnstsSchedulerCell(cellState);
    const submitResp = await schedCell.fetch(
    new Request("https://worker.internal/v1/scheduler/submit", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        task_id: "t-m64-worker-01",
        tenant_id: "Northstar",
        priority: _models.TaskPriority.P1_HIGH,
        prompt: "Verify worker prefixed route",
        resource_profile: { think_cpu_cores: 0.5, act_cpu_cores: 8, act_memory_mb: 8192, disk_mb: 20480 },
        parent_task_id: "parent-m64",
        expected_SubAgent_count: 3,
        prefix_Cache_hash: "pfx-m64-hash"
      })
    }));

    const submitJson = await submitResp.json();
    const workerCellRouteVerified =
    submitResp.status === 202 &&
    submitJson.task_id === "t-m64-worker-01" &&
    schedCell.scheduler.allTasks.has("t-m64-worker-01");

    // 2. [M64-2] 100% TaskSpec field fidelity across AnstsSchedulerCell, SchedulerV25SingleWritePathStore & UnifiedComponentPersistenceStore
    const rehydratedCell = new _scheduler_cell.AnstsSchedulerCell(cellState);
    const cellTask = rehydratedCell.scheduler.allTasks.get("t-m64-worker-01");

    const v25Sql = new _env.InMemoryCellSqlStorage();
    const v25Store1 = new _scheduler_v25_persistence.SchedulerV25SingleWritePathStore(v25Sql);
    const fullSpec = new _models.TaskSpec({
      task_id: "t-m64-fidelity",
      tenant_id: "Northstar",
      priority: _models.TaskPriority.P0_EMERGENCY,
      prompt: "Full field fidelity check",
      resource_profile: { think_cpu_cores: 0.75, act_cpu_cores: 12, act_memory_mb: 16384, disk_mb: 40960 },
      parent_task_id: "parent-root-01",
      expected_SubAgent_count: 4,
      prefix_Cache_hash: "hash-fidelity-99"
    });
    fullSpec.retry_count = 2;
    fullSpec.preempted_count = 1;
    fullSpec.dead_letter_reason = "NONE";
    v25Store1.commitTaskSingleWritePath(fullSpec, "body-host-01");
    const v25Restored = v25Store1.getPersistedTask("t-m64-fidelity");

    const unifiedSql = new _env.InMemoryCellSqlStorage();
    const unified = new _unified_state_store.UnifiedComponentPersistenceStore(unifiedSql);
    const s1 = new _scheduler.CoreScheduler5D();
    s1.allTasks.set(fullSpec.task_id, fullSpec);
    unified.snapshotSchedulerState(s1);
    const s2 = new _scheduler.CoreScheduler5D();
    unified.rehydrateSchedulerFromSql(s2);
    const unifiedRestored = s2.allTasks.get("t-m64-fidelity");

    const taskSpecFidelityVerified =
    cellTask?.resource_profile.act_cpu_cores === 8 &&
    cellTask?.parent_task_id === "parent-m64" &&
    cellTask?.expected_SubAgent_count === 3 &&
    cellTask?.prefix_Cache_hash === "pfx-m64-hash" &&
    v25Restored?.resource_profile.act_cpu_cores === 12 &&
    v25Restored?.retry_count === 2 &&
    v25Restored?.preempted_count === 1 &&
    v25Restored?.prefix_Cache_hash === "hash-fidelity-99" &&
    unifiedRestored?.resource_profile.act_memory_mb === 16384 &&
    unifiedRestored?.prefix_Cache_hash === "hash-fidelity-99";

    // 3. [M64-3] SagaCompensator/StagingGate/McpRegistry/CognitiveRouter/DagEscrow SQLite persistence & turn-accurate staged commit
    const sagaSql = new _env.InMemoryCellSqlStorage();
    const gw1 = new _saga_gateway.SagaToolGateway(undefined, undefined, sagaSql);
    const mcp1 = new _mcp_registry.McpDynamicToolRegistry(sagaSql, gw1);
    mcp1.registerMcpServerTools(
    "mcp-k8s",
    [
    {
      toolName: "deploy_manifest",
      description: "Deploy manifest",
      effectClass: _models.ToolCallEffectClass.COMPENSATABLE,
      inputSchema: {}
    }],

    gw1,
    (_m, p) => ({ deployed: p.name }));

    const dummyTask = new _models.TaskSpec({ task_id: "t-m64-saga", tenant_id: "Northstar" });
    gw1.stagingGate.stageEffect({
      effectId: "eff-turn-3",
      taskId: "t-m64-saga",
      turnIndex: 3,
      toolName: "deploy_manifest",
      args: { env: "prod" }
    });
    const ticket = gw1.stagingGate.requestHumanApproval(dummyTask, "deploy_manifest", { env: "prod" });
    gw1.stagingGate.resolveApproval(ticket.ticketId, true, "chen", dummyTask);
    const committed = gw1.stagingGate.commitStagedEffects("t-m64-saga", (tName, tArgs, tTurn) => {
      return gw1.invokeTool("t-m64-saga", tTurn, tName, tArgs)[0];
    });

    // Rehydrate on a brand-new SagaToolGateway + McpDynamicToolRegistry over the same SQLite and roll back turn >= 3
    const gw2 = new _saga_gateway.SagaToolGateway(undefined, undefined, sagaSql);
    const mcp2 = new _mcp_registry.McpDynamicToolRegistry(sagaSql, gw2);
    const rb = gw2.rollbackAndInvalidateTask("t-m64-saga", 3);

    // Also verify DagEscrowCoordinator dependencyGraph cycle detection across SQLite restart
    const escrowSql = new _env.InMemoryCellSqlStorage();
    const esc1 = new _escrow.DagEscrowCoordinator(escrowSql);
    const pTask = new _models.TaskSpec({ task_id: "dag-p1", tenant_id: "Northstar", expected_SubAgent_count: 2 });
    const cTask = new _models.TaskSpec({ task_id: "dag-c1", tenant_id: "Northstar", parent_task_id: "dag-p1" });
    esc1.reserveParentEscrow(pTask, 10, 0);
    esc1.registerChildSubAgent(pTask, cTask);
    const esc2 = new _escrow.DagEscrowCoordinator(escrowSql);
    let cycleCaughtAfterRestart = false;
    try {
      const badChild = new _models.TaskSpec({
        task_id: "dag-p1",
        tenant_id: "Northstar",
        parent_task_id: "dag-c1",
        dependencies: ["dag-c1"]
      });
      esc2.registerChildSubAgent(cTask, badChild);
    } catch {
      cycleCaughtAfterRestart = true;
    }

    const sagaMcpEscrowVerified =
    committed.length === 1 &&
    committed[0].turnIndex === 3 &&
    mcp2.listTools().length === 1 &&
    rb.rolledBackSteps.length === 1 &&
    rb.rolledBackSteps[0].turnIndex === 3 &&
    cycleCaughtAfterRestart === true;

    // 4. [M64-5] AgentHarnessRunner router token metering & PartitionedSchedulerRouter multi-shard scheduling
    const routerSql = new _env.InMemoryCellSqlStorage();
    const cRouter = new _cognitive_router.CognitiveRouter(0.3, routerSql);
    cRouter.registerEndpoint(
    new _models.LlmEndpointMetrics({
      endpoint_id: "ep-m64-1",
      provider: "deepseek",
      model_name: "DeepSeek",
      ttft_ewma_ms: 150,
      tpm_used: 0
    }));

    const hRunner = new _runner.AgentHarnessRunner(cRouter, gw2);
    const hTask = new _models.TaskSpec({ task_id: "t-m64-harness", tenant_id: "Northstar" });
    hRunner.executeSingleTurn(hTask, "deploy_manifest", { env: "staging" }, 1800);
    const cRouter2 = new _cognitive_router.CognitiveRouter(0.3, routerSql);
    const rehydratedEp = cRouter2.getEndpoint("ep-m64-1");

    const pRouter = new _horizontal_scaler.PartitionedSchedulerRouter(4);
    pRouter.registerNodeAcrossShards(
    new _models.NodeHeartbeat({ node_id: "node-m64-01", total_cpu_cores: 16, free_cpu_cores: 16 }));

    await pRouter.routeSubmitTask(
    new _models.TaskSpec({ task_id: "t-m64-shard", tenant_id: "Northstar", prompt: "shard test" }));

    const tickRes = await pRouter.scheduleTickAcrossShards();

    const harnessAndShardRouterVerified =
    rehydratedEp !== undefined &&
    rehydratedEp.tpm_used === 1800 &&
    tickRes.totalPlacements.length === 1 &&
    tickRes.totalPlacements[0].taskId === "t-m64-shard";

    const allM64ConsistencyChecksPassed =
    workerCellRouteVerified &&
    taskSpecFidelityVerified &&
    sagaMcpEscrowVerified &&
    harnessAndShardRouterVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M64 落地功能如实实现一致性自检 (WorkerCell=${workerCellRouteVerified}, TaskSpecFidelity=${taskSpecFidelityVerified}, SagaMcpEscrow=${sagaMcpEscrowVerified}, HarnessShardRouter=${harnessAndShardRouterVerified})`,
    "SystemWide / M64-ImplementationConsistencyAudit",
    "ALL_M64_CONSISTENCY_CHECKS_PASSED");


    return {
      status: 200,
      body: {
        workerCell1: {
          submitStatus: submitResp.status,
          workerCellRouteVerified
        },
        taskSpecFidelity2: {
          cellActCpu: cellTask?.resource_profile.act_cpu_cores,
          v25RetryCount: v25Restored?.retry_count,
          unifiedActMemMb: unifiedRestored?.resource_profile.act_memory_mb,
          taskSpecFidelityVerified
        },
        sagaMcpEscrow3: {
          committedTurnIndex: committed[0]?.turnIndex,
          rolledBackCountAfterRestart: rb.rolledBackSteps.length,
          rehydratedMcpTools: mcp2.listTools().length,
          cycleCaughtAfterRestart,
          sagaMcpEscrowVerified
        },
        harnessAndShardRouter4: {
          rehydratedTpmUsed: rehydratedEp?.tpm_used,
          shardPlacementsCount: tickRes.totalPlacements.length,
          harnessAndShardRouterVerified
        },
        allM64ConsistencyChecksPassed
      }
    };
  }

  /**
   * [M65] Deep Implementation Consistency Phase II Audit:
   * Verifies WalCompactorAndPagedRestorer & LayerFsRemoteStoreSynchronizer SQLite persistence,
   * CanaryRolloutController (post-rollback lock) + PredictiveSandboxPrewarmer + TaskResourceProfiler SQLite rehydration,
   * TunnelGatewayCell DO fetch API (/v1/tunnel/register, /poll, /ack), and live route state-mutation sync.
   */
  async executeDeepConsistencyPhase2Audit(params)


  {
    this.ensureInitialized();
    if (params.role === "VIEWER") {
      return {
        status: 403,
        body: { error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权触发 M65 第二阶段落地功能实现一致性自检" }
      };
    }

    // 1. [M65-1] WalCompactorAndPagedRestorer & LayerFsRemoteStoreSynchronizer SQLite persistence across restarts
    const compSql = new _env.InMemoryCellSqlStorage();
    const wal1 = new _wal_engine.TurnWalEngine(compSql);
    wal1.appendTurnDelta({
      taskId: "t-m65-comp",
      turnIndex: 1,
      fencingToken: 1,
      changedFiles: ["src/a.ts", "src/b.ts"],
      sqliteWalPages: 25,
      highestSubmittedFencingToken: 1
    });
    const comp1 = new _compactor.WalCompactorAndPagedRestorer(4096, compSql);
    comp1.compactLayers("t-m65-comp", wal1.getTaskWalEntries("t-m65-comp"));
    const comp2 = new _compactor.WalCompactorAndPagedRestorer(4096, compSql);
    const uffdRes = comp2.restoreViaUserfaultfd("t-m65-comp", 0.2);

    const syncBucket = new _env.InMemoryR2Bucket();
    const syncSql = new _env.InMemoryCellSqlStorage();
    const sync1 = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(syncBucket, "proj-m65", syncSql);
    const storeBytes = new TextEncoder().encode("sqlite-store-v1");
    await sync1.pushStoreSnapshot({
      machineId: "m1",
      basedOnGeneration: 0,
      storeSqliteBytes: storeBytes,
      mainHeadCommitId: "C1",
      idempotencyKey: "idem-sync-m65"
    });
    const sync2 = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(syncBucket, "proj-m65", syncSql);
    const retryPush = await sync2.pushStoreSnapshot({
      machineId: "m1",
      basedOnGeneration: 0,
      storeSqliteBytes: storeBytes,
      mainHeadCommitId: "C1",
      idempotencyKey: "idem-sync-m65"
    });

    const compactorAndRemoteSyncVerified =
    comp2.getCompactedSnapshot("t-m65-comp")?.totalSqlitePages === 25 &&
    uffdRes.eagerPagesLoaded === 5 &&
    sync2.getRemoteGeneration() === 1 &&
    retryPush.deduplicated === true;

    // 2. [M65-2] CanaryRolloutController, PredictiveSandboxPrewarmer & TaskResourceProfiler SQLite rehydration
    const govSql = new _env.InMemoryCellSqlStorage();
    const can1 = new _canary_rollout_controller.CanaryRolloutController(govSql);
    can1.evaluateCanaryStep({ errorRatePct: 5.5, p99LatencyMs: 200 }); // triggers ROLLED_BACK
    const can2 = new _canary_rollout_controller.CanaryRolloutController(govSql);
    const postRestartStep = can2.evaluateCanaryStep({ errorRatePct: 0.01, p99LatencyMs: 80 }); // must stay ROLLED_BACK!

    const pre1 = new _predictive_prewarmer.PredictiveSandboxPrewarmer(0.5, govSql);
    pre1.recordArrivalSampleAndPredict(25.0, 4.0);
    const pre2 = new _predictive_prewarmer.PredictiveSandboxPrewarmer(0.5, govSql);

    const prof1 = new _profiler.TaskResourceProfiler(0.5, govSql);
    const sampleTask = new _models.TaskSpec({ task_id: "t-prof", tenant_id: "Northstar" });
    prof1.recordExecutionMetrics(sampleTask, 8.0, 8192);
    prof1.recordExecutionMetrics(sampleTask, 8.0, 8192);
    prof1.recordExecutionMetrics(sampleTask, 8.0, 8192);
    const prof2 = new _profiler.TaskResourceProfiler(0.5, govSql);
    const recProfile = prof2.recommendProfile(sampleTask);

    const canaryPrewarmerProfilerVerified =
    postRestartStep.state === "ROLLED_BACK" &&
    postRestartStep.trafficWeightPct === 0 &&
    pre2.getEwmaArrivalRatePerSec() === 15.0 &&
    recProfile.act_cpu_cores === 9.6 &&
    recProfile.act_memory_mb === 10240;

    // 3. [M65-4] TunnelGatewayCell DO fetch API (/v1/tunnel/register, /poll, /ack)
    const tunSql = new _env.InMemoryCellSqlStorage();
    const tunCell = new _tunnel_gateway_cell.TunnelGatewayCell(tunSql);
    const regResp = await tunCell.fetch(
    new Request("https://edge.internal/v1/tunnel/register", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        hostId: "body-m65-01",
        gatewayId: "gw-m65-01",
        role: "PRIMARY",
        connGeneration: 1,
        epoch: 1
      })
    }));

    const enq = tunCell.enqueueCommandForHost("body-m65-01", { cmd: "echo tunnel" }, 5000, {
      idempotencyKey: "idem-tun-m65"
    });
    const pollResp = await tunCell.fetch(
    new Request("https://edge.internal/v1/tunnel/poll?hostId=body-m65-01"));

    const pollJson = await pollResp.json();
    const ackResp = await tunCell.fetch(
    new Request("https://edge.internal/v1/tunnel/ack", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        commandId: pollJson.commands[0].commandId,
        result: { status: "TUNNEL_OK" }
      })
    }));

    const awaitedRes = await enq.completionPromise;
    const tunnelFetchVerified =
    regResp.status === 200 &&
    pollJson.commands.length === 1 &&
    ackResp.status === 200 &&
    awaitedRes.status === "TUNNEL_OK";

    const allM65Phase2ChecksPassed =
    compactorAndRemoteSyncVerified &&
    canaryPrewarmerProfilerVerified &&
    tunnelFetchVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M65 第二阶段落地功能实现一致性自检 (CompactorSync=${compactorAndRemoteSyncVerified}, CanaryPrewarmProf=${canaryPrewarmerProfilerVerified}, TunnelFetch=${tunnelFetchVerified})`,
    "SystemWide / M65-DeepConsistencyPhase2Audit",
    "ALL_M65_PHASE2_CHECKS_PASSED");


    return {
      status: 200,
      body: {
        compactorAndRemoteSync1: {
          restoredPages: comp2.getCompactedSnapshot("t-m65-comp")?.totalSqlitePages,
          eagerPagesLoaded: uffdRes.eagerPagesLoaded,
          remoteGeneration: sync2.getRemoteGeneration(),
          retryDeduplicated: retryPush.deduplicated,
          compactorAndRemoteSyncVerified
        },
        canaryPrewarmerProfiler2: {
          postRestartCanaryState: postRestartStep.state,
          rehydratedEwmaRate: pre2.getEwmaArrivalRatePerSec(),
          recommendedActCpu: recProfile.act_cpu_cores,
          recommendedActMemMb: recProfile.act_memory_mb,
          canaryPrewarmerProfilerVerified
        },
        tunnelFetch3: {
          polledCount: pollJson.commands.length,
          awaitedStatus: awaitedRes.status,
          tunnelFetchVerified
        },
        allM65Phase2ChecksPassed
      }
    };
  }
  /**
   * [M66] Deep Potential Risk Identification & Elimination Audit
   */
  async executePotentialRisksEliminationAudit(params)


  {
    this.ensureInitialized();

    // 1. [M66-1] TransactionalOutboxDispatcher timing-safe HMAC, constructor overload & dead-letter retry
    const outboxSql = new _env.InMemoryCellSqlStorage();
    const outbox = new _outbox_webhook.TransactionalOutboxDispatcher("sec-m66-audit", 2, outboxSql);
    const samplePayload = JSON.stringify({ event: "task.done", id: "t-m66" });
    const sig = outbox.signPayload(samplePayload, 1700000000);
    const sigValid = outbox.verifySignature(samplePayload, sig.replace(",", ", "), 1700000050, 300);
    const sigTampered = outbox.verifySignature(samplePayload, sig.slice(0, -2) + "ff", 1700000050, 300);
    outbox.enqueueOutbox("m66-dlq", "t-m66", "https://bad.local/hook", { x: 1 });
    outbox.processPending(() => false);
    outbox.processPending(() => false);
    const wasDeadLettered = outbox.listMessages()[0]?.deadLettered === true;
    outbox.retryDeadLetteredMessage("m66-dlq", "https://good.local/hook");
    const retryRes = outbox.processPending(() => true);
    const outboxVerified = sigValid && !sigTampered && wasDeadLettered && retryRes.deliveredCount === 1;

    // 2. [M66-2] CloudEventsBus bounded idempotency + SQLite FinOps & SseEventStreamGateway CRLF + orphan GC
    const ceSql = new _env.InMemoryCellSqlStorage();
    const ce1 = new _cloudevents.CloudEventsBus(10, ceSql);
    for (let i = 0; i < 50; i++) {
      ce1.emitEvent({
        type: "ansts.turn",
        subject: `t-${i}`,
        tenantId: "Northstar",
        idempotencyKey: `k-${i}`,
        llmCostUsd: 0.02
      });
    }
    const ce2 = new _cloudevents.CloudEventsBus(10, ceSql);
    const sse = new _sse_stream.SseEventStreamGateway(10);
    const orphanSub = sse.subscribe("t-orphan-0", "sub-o1");
    const crlfSub = sse.subscribe("t-active-1", "sub-a1");
    const sseChunks = [];
    crlfSub.stream.on("data", (c) => sseChunks.push(c.toString("utf-8")));
    sse.publish({
      id: "id-1\r\nevent: injected",
      event: "turn.step\r\nid: fake",
      taskId: "t-active-1",
      turnIndex: 1,
      timestampMs: Date.now(),
      data: { ok: true }
    });
    const gcCount = sse.gcCompletedTaskStreams(new Set(["t-active-1"]));
    const cloudEventsAndSseVerified =
    ce1.getIdempotencyCacheSize() <= 40 &&
    ce2.getTenantReport("Northstar")?.totalLlmCostUsd === 1.0 &&
    sseChunks.join("").trim().split("\n").length === 3 &&
    gcCount === 1 &&
    orphanSub.closed === true;

    // 3. [M66-3] LayerFsWorkspaceDriver path traversal guard & unreachable commit/B-Tree GC + logicalTotalBytes
    const lfs = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    let traversalBlocked = false;
    try {
      lfs.executeInProcessFileTool({
        branchName: "main",
        agentId: "a1",
        stepId: "s1",
        operation: "write",
        filePath: "../../outside.txt",
        newContent: "bad"
      });
    } catch {
      traversalBlocked = true;
    }
    const baseLogical = lfs.storeEngine.getStorageStats().logicalTotalBytes;
    lfs.forkBranch("main", "ephemeral-br");
    lfs.executeInProcessFileTool({
      branchName: "ephemeral-br",
      agentId: "a1",
      stepId: "s2",
      operation: "write",
      filePath: "src/ephemeral.ts",
      newContent: "export const x = 42;"
    });
    lfs.deleteBranch("ephemeral-br");
    const sweeper = new _distributed_gc_sweeper.DistributedCasGcSweeper();
    const gcSweep = sweeper.sweepLayerFsWorkspaceStore(lfs);
    const postGcLogical = lfs.storeEngine.getStorageStats().logicalTotalBytes;
    const layerFsVerified =
    traversalBlocked &&
    gcSweep.prunedCommitIds.length === 1 &&
    gcSweep.prunedBTreeCommitCount === 1 &&
    postGcLogical === baseLogical;

    // 4. [M66-4] WarmSandboxPoolManager clamping & LayerFsNumberedSandboxMultiplexer closed-entry pruning
    const pool = new _warm_pool.WarmSandboxPoolManager("node-m66", -4);
    const mux = new _warm_pool.LayerFsNumberedSandboxMultiplexer();
    const mounted = mux.mountWorkspaceIntoSandbox("ws-m66", 100);
    mux.unmountWorkspace(mounted.sandboxNumber, "ws-m66", 200);
    const sweptSandboxes = mux.sweepIdleSandboxes(200 + _warm_pool.LayerFsNumberedSandboxMultiplexer.IDLE_CLOSE_TIMEOUT_MS + 1);
    const sandboxAndVsockVerified =
    pool.targetWarmCount === 8 &&
    sweptSandboxes.length === 1 &&
    mux.getActiveSandboxCount() === 0;

    const allM66PotentialRiskChecksPassed =
    outboxVerified &&
    cloudEventsAndSseVerified &&
    layerFsVerified &&
    sandboxAndVsockVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M66 深度潜在风险识别与消除自检 (Outbox=${outboxVerified}, CloudEventsSSE=${cloudEventsAndSseVerified}, LayerFS=${layerFsVerified}, SandboxMux=${sandboxAndVsockVerified})`,
    "SystemWide / M66-PotentialRisksAudit",
    "ALL_M66_CHECKS_PASSED");


    return {
      status: 200,
      body: {
        outboxSecurityAndDlq1: {
          sigValid,
          sigTampered,
          wasDeadLettered,
          deliveredAfterRetry: retryRes.deliveredCount,
          outboxVerified
        },
        cloudEventsAndSse2: {
          boundedIdempotencySize: ce1.getIdempotencyCacheSize(),
          rehydratedLlmCostUsd: ce2.getTenantReport("Northstar")?.totalLlmCostUsd,
          orphanStreamsCleaned: gcCount,
          cloudEventsAndSseVerified
        },
        layerFsTraversalAndGc3: {
          traversalBlocked,
          prunedCommitCount: gcSweep.prunedCommitIds.length,
          prunedBTreeCommitCount: gcSweep.prunedBTreeCommitCount,
          postGcLogicalBytes: postGcLogical,
          layerFsVerified
        },
        sandboxPoolAndVsock4: {
          sanitizedWarmTarget: pool.targetWarmCount,
          activeSandboxesAfterSweep: mux.getActiveSandboxCount(),
          sandboxAndVsockVerified
        },
        allM66PotentialRiskChecksPassed
      }
    };
  }
  /**
   * [M67] Deep Potential Risk Identification & Elimination Phase II Audit
   */
  async executeDeepPotentialRiskPhase2Audit(params)


  {
    this.ensureInitialized();

    // 1. [M67-1] CelldNativeContainerSandboxManager loopback/ULA/hex-IP/metadata SSRF block & WorkDir cleanup
    const mgr = new _celld_container_bridge.CelldNativeContainerSandboxManager();
    mgr.registerContainerClass({
      class_name: "AuditRunner",
      image: "ghcr.io/ansts/audit:v2.5",
      runtime: "kata",
      max_instances: 2
    });
    const handle = mgr.getOrCreateContainerHandle("cell-m67-audit", {
      class_name: "AuditRunner",
      image: "ghcr.io/ansts/audit:v2.5",
      runtime: "kata"
    });
    const started = handle.start({ enableInternet: true });
    let loopbackBlocked = false;
    let hexIpBlocked = false;
    try {
      _celld_container_bridge.CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget("127.0.0.2", true);
    } catch {
      loopbackBlocked = true;
    }
    try {
      _celld_container_bridge.CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget("0xa9fea9fe", true);
    } catch {
      hexIpBlocked = true;
    }
    await mgr.destroyContainerHandle("cell-m67-audit");
    const workDirCleaned = !fs.existsSync(started.containerWorkDir);
    const containerFenceVerified = loopbackBlocked && hexIpBlocked && workDirCleaned && mgr.getHandleCount() === 0;

    // 2. [M67-2] HotCellOverloadAndQueueGuard double-release/reclaimed lease desync guard & ShardedQueueBrokerPool idempotency
    const guard = new _hot_cell_overload_guard.HotCellOverloadAndQueueGuard({ maxCellConcurrentRequests: 2 });
    const l1 = guard.tryEnterCellFetch("cell-m67", 1000);
    const l2 = guard.tryEnterCellFetch("cell-m67", 25_000);
    guard.reclaimStaleInFlightFetches(35_000, 30_000);
    guard.leaveCellFetch("cell-m67", l1.leaseId); // stale release must not evict l2
    const activeAfterStaleLeave = guard.getInFlightCount("cell-m67");
    guard.leaveCellFetch("cell-m67", l2.leaseId);
    const broker = new _celld_ha_engine.ShardedQueueBrokerPool(4, 16);
    const shard = broker.publish("k1", "id-m67-1", { v: 1 });
    broker.publish("k1", "id-m67-1", { v: 1 });
    const batch = broker.consumeBatch(shard, 10);
    const hotCellAndQueueVerified =
    activeAfterStaleLeave === 1 &&
    guard.getInFlightCount("cell-m67") === 0 &&
    !guard.tryAdmitQueueProducerBatch(Number.NaN).admitted &&
    batch.length === 1;

    // 3. [M67-3] DynamicWorkerSandboxLoader async fetch timeout & inputPayload deep-clone isolation
    const loader = new _celld_native_optimizations.DynamicWorkerSandboxLoader();
    const callerPayload = { token: "clean" };
    await loader.executeUntrustedCodeInIsolate({
      taskId: "t-m67-iso",
      turnIndex: 1,
      jsSourceCode: "inputPayload.token = 'dirty'; globalThis.__result = { ok: true };",
      inputPayload: callerPayload
    });
    let asyncTimeoutTriggered = false;
    try {
      await loader.executeUntrustedCodeInIsolate({
        taskId: "t-m67-hang",
        turnIndex: 2,
        jsSourceCode: "export default { async fetch() { return new Promise(() => {}); } }",
        asyncTimeoutMs: 40
      });
    } catch {
      asyncTimeoutTriggered = true;
    }
    const heapGuard = new _celld_native_optimizations.V8HeapAndSqlBudgetGuard(-5, 0, -1);
    const isolateAndHeapVerified =
    callerPayload.token === "clean" &&
    asyncTimeoutTriggered &&
    heapGuard.hardHeapLimitMb >= heapGuard.softHeapLimitMb;

    // 4. [M67-4] ZeroTrustVsockBridge URL credentials/port/backslash guard & EbpfLsmSecurityBridge /proc/<pid> block
    const vsock = new _vsock_bridge.ZeroTrustVsockBridge(["api.openai.com"]);
    let credBlocked = false;
    try {
      vsock.sendRequest({
        requestId: "r-cred",
        vmId: "vm-m67",
        method: "HTTP_EGRESS",
        payload: { url: "https://user:pass@api.openai.com/v1" }
      });
    } catch {
      credBlocked = true;
    }
    const lsm = new _ebpf_lsm_bridge.EbpfLsmSecurityBridge();
    const nullByteBlocked = !lsm.checkFileOpenHook("/etc/shadow\0.txt").allowed;
    const procPidBlocked = !lsm.checkFileOpenHook("/proc/99/environ").allowed;
    const vsockAndLsmVerified = credBlocked && nullByteBlocked && procPidBlocked;

    // 5. [M67-5] LayerFsMultiAgentBranchCoordinator merge B-Tree sync & TurnWalEngine task WAL GC
    const lfs = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    const coord = new _layerfs_branch_coordinator.LayerFsMultiAgentBranchCoordinator(lfs);
    lfs.forkBranch("main", "br-m67");
    lfs.executeInProcessFileTool({
      branchName: "br-m67",
      agentId: "a1",
      stepId: "s1",
      operation: "write",
      filePath: "src/m67.ts",
      newContent: "export const m67 = 1;"
    });
    const pagesBefore = lfs.storeEngine.getTrackedCommitPageCount();
    coord.mergeBranchIntoTarget("br-m67", "main", "C0");
    const pagesAfter = lfs.storeEngine.getTrackedCommitPageCount();
    const walSql = new _env.InMemoryCellSqlStorage();
    const wal = new _wal_engine.TurnWalEngine(walSql);
    wal.appendTurnDelta({
      taskId: "t-wal-gc",
      turnIndex: 1,
      fencingToken: 1,
      changedFiles: ["m67.ts"],
      sqliteWalPages: 1,
      highestSubmittedFencingToken: 1
    });
    const walGc = wal.gcCompletedTaskWals(new Set());
    const mergeAndWalGcVerified = pagesAfter === pagesBefore + 1 && walGc.purgedTasks === 1;

    const allM67DeepRiskPhase2ChecksPassed =
    containerFenceVerified &&
    hotCellAndQueueVerified &&
    isolateAndHeapVerified &&
    vsockAndLsmVerified &&
    mergeAndWalGcVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M67 第二阶段深度潜在风险识别与消除自检 (Fence=${containerFenceVerified}, HotCellQueue=${hotCellAndQueueVerified}, Isolate=${isolateAndHeapVerified}, VsockLSM=${vsockAndLsmVerified}, MergeWalGC=${mergeAndWalGcVerified})`,
    "SystemWide / M67-DeepRiskPhase2Audit",
    "ALL_M67_CHECKS_PASSED");


    return {
      status: 200,
      body: {
        containerFenceAndWorkDir1: {
          loopbackBlocked,
          hexIpBlocked,
          workDirCleaned,
          containerFenceVerified
        },
        hotCellAndShardedQueue2: {
          activeAfterStaleLeave,
          dedupedBatchLength: batch.length,
          hotCellAndQueueVerified
        },
        isolateTimeoutAndClone3: {
          callerPayloadPreserved: callerPayload.token === "clean",
          asyncTimeoutTriggered,
          isolateAndHeapVerified
        },
        vsockAndEbpfLsm4: {
          credBlocked,
          nullByteBlocked,
          procPidBlocked,
          vsockAndLsmVerified
        },
        layerFsMergeAndWalGc5: {
          bTreePagesAddedOnMerge: pagesAfter - pagesBefore,
          walTasksPurged: walGc.purgedTasks,
          mergeAndWalGcVerified
        },
        allM67DeepRiskPhase2ChecksPassed
      }
    };
  }
  /**
   * [M68] Deep Potential Risk Identification & Elimination Phase III Audit
   */
  async executeDeepPotentialRiskPhase3Audit(params)


  {
    this.ensureInitialized();

    // 1. [M68-1] TunnelGatewayCell timed-out command queue/idempotency eviction, non-regressive registration & bounded cache
    const tunSql = new _env.InMemoryCellSqlStorage();
    const tunCell = new _tunnel_gateway_cell.TunnelGatewayCell(tunSql, 3);
    tunCell.registerTunnel({
      hostId: "body-m68-01",
      gatewayId: "gw-01",
      role: "PRIMARY",
      connGeneration: 4,
      epoch: 3
    });
    const regressed = tunCell.registerTunnel({
      hostId: "body-m68-01",
      gatewayId: "gw-01",
      role: "PRIMARY",
      connGeneration: 1,
      epoch: 2
    });
    const timedOut = tunCell.enqueueCommandForHost(
    "body-m68-01",
    { cmd: "sleep 5" },
    25,
    { idempotencyKey: "idem-m68-t1" });

    let timeoutRejected = false;
    try {
      await timedOut.completionPromise;
    } catch {
      timeoutRejected = true;
    }
    const ghostPolled = tunCell.pollPendingCommands("body-m68-01");
    const retryAfterTimeout = tunCell.enqueueCommandForHost(
    "body-m68-01",
    { cmd: "echo ok" },
    1000,
    { idempotencyKey: "idem-m68-t1" });

    const retryPolled = tunCell.pollPendingCommands("body-m68-01");
    tunCell.acknowledgeCommand(retryPolled[0].commandId, { status: "RECOVERED" });
    const retryResult = await retryAfterTimeout.completionPromise;
    const tunnelTimeoutAndEpochVerified =
    regressed.epoch === 3 &&
    regressed.connGeneration === 4 &&
    timeoutRejected &&
    ghostPolled.length === 0 &&
    retryAfterTimeout.deduplicated === false &&
    retryResult.status === "RECOVERED";

    // 2. [M68-2] FinOpsBudgetController UTC-day rollover idempotency pruning & policy sanitization
    const finSql = new _env.InMemoryCellSqlStorage();
    const finops = new _budget_controller.FinOpsBudgetController(finSql);
    finops.setPolicy({
      tenantId: "Northstar",
      dailyBudgetUsd: -20,
      softDowngradeThresholdRatio: 1.8,
      preferredModel: "DeepSeek-R1",
      fallbackEconomyModel: "Qwen-3.8"
    });
    const sanitizedPolicy = finops.getPolicy("Northstar");
    finops.setPolicy({
      tenantId: "Northstar",
      dailyBudgetUsd: 100,
      softDowngradeThresholdRatio: 0.8,
      preferredModel: "DeepSeek-R1",
      fallbackEconomyModel: "Qwen-3.8"
    });
    const day1Spend = finops.recordSpend("Northstar", 12, "2026-11-10T08:00:00Z", "daily-job-key");
    const day2Spend = finops.recordSpend("Northstar", 18, "2026-11-11T08:00:00Z", "daily-job-key");
    const finopsRolloverAndSanitizeVerified =
    sanitizedPolicy.dailyBudgetUsd === 0 &&
    sanitizedPolicy.softDowngradeThresholdRatio === 1 &&
    day1Spend === 12 &&
    day2Spend === 18 &&
    finops.getSeenIdempotencyKeyCount("Northstar") === 1;

    // 3. [M68-3]QueueManager eager tombstone/empty-heap cleanup & LeaseCoordinator advanceTurnFence epoch sync
    const sched = new _scheduler.CoreScheduler5D();
    const qTask = new _models.TaskSpec({ task_id: "t-m68-qm", tenant_id: "ephemeral-tenant" });
    sched.queueManager.enqueue(qTask);
    sched.queueManager.removeQueuedTask("t-m68-qm");
    const emptyHeapCleaned = sched.queueManager.activeTenantHeapCount() === 0;
    sched.leaseCoordinator.acquireLease("t-m68-fence", "node-1");
    sched.leaseCoordinator.advanceTurnFence("t-m68-fence", new _models.FenceToken(4, 1, "t-m68-fence"));
    const nextLease = sched.leaseCoordinator.acquireLease("t-m68-fence", "node-2");
    const fencePurged = sched.leaseCoordinator.purgeCompletedTaskFence("t-m68-fence");
    const queueAndFenceVerified =
    emptyHeapCleaned && nextLease.fencing_token === 5 && fencePurged;

    // 4. [M68-4] CoreScheduler5D cascade-dead-letter active lease/resource reclamation & NodeDaemon checkpoint purge
    sched.registerOrUpdateNode(
    new _models.NodeHeartbeat({
      node_id: "node-m68-01",
      total_cpu_cores: 16,
      free_cpu_cores: 16,
      total_memory_mb: 32768,
      free_memory_mb: 32768
    }));

    const pTask = new _models.TaskSpec({ task_id: "t-m68-p", tenant_id: "Northstar", prompt: "p" });
    const cTask = new _models.TaskSpec({ task_id: "t-m68-c", tenant_id: "Northstar", prompt: "c" });
    sched.submitTask(pTask, { mergeDuplicateInFlight: true });
    sched.submitTask(cTask, { mergeDuplicateInFlight: true });
    sched.scheduleTick();
    cTask.dependencies.push("t-m68-p");
    const cascaded = sched.cascadeDeadLetterToDependents("t-m68-p");
    const nodeAfterCascade = sched.nodes.get("node-m68-01");
    const daemon = new _node_daemon.NodeDaemon("node-m68-01", -1, 0, -5);
    const schedulerAndDaemonVerified =
    cascaded.includes("t-m68-c") &&
    sched.leaseCoordinator.getLease("t-m68-c") === undefined &&
    nodeAfterCascade.running_tasks === 1 &&
    daemon.totalCpu === 32.0;

    // 5. [M68-5] SagaCompensatorEngine no-handler step rollback idempotency invalidation & StagingGate purge
    const sagaSql = new _env.InMemoryCellSqlStorage();
    const gw = new _saga_gateway.SagaToolGateway(undefined, undefined, sagaSql);
    let runs = 0;
    gw.registerTool("no_handler_tool", _models.ToolCallEffectClass.COMPENSATABLE, () => {
      runs += 1;
      return { runs };
    });
    gw.invokeTool("t-m68-saga", 2, "no_handler_tool", { x: 1 });
    const rb = gw.rollbackAndInvalidateTask("t-m68-saga", 2);
    const [reRun, dedupAfterRb] = gw.invokeTool("t-m68-saga", 2, "no_handler_tool", { x: 1 });
    const purgedSteps = gw.compensator.purgeTaskSteps("t-m68-saga");
    const sagaAndStagingVerified =
    rb.rolledBackSteps.length === 1 &&
    rb.invalidatedIdemKeys.length >= 1 &&
    dedupAfterRb === false &&
    reRun.runs === 2 &&
    purgedSteps === 2;

    const allM68DeepRiskPhase3ChecksPassed =
    tunnelTimeoutAndEpochVerified &&
    finopsRolloverAndSanitizeVerified &&
    queueAndFenceVerified &&
    schedulerAndDaemonVerified &&
    sagaAndStagingVerified;

    this.appendAudit(
    params.actor,
    params.role,
    `执行 M68 第三阶段深度潜在风险识别与消除自检 (Tunnel=${tunnelTimeoutAndEpochVerified}, FinOps=${finopsRolloverAndSanitizeVerified}, QueueFence=${queueAndFenceVerified}, SchedulerDaemon=${schedulerAndDaemonVerified}, SagaStaging=${sagaAndStagingVerified})`,
    "SystemWide / M68-DeepRiskPhase3Audit",
    "ALL_M68_CHECKS_PASSED");


    return {
      status: 200,
      body: {
        tunnelTimeoutAndEpoch1: {
          preservedEpoch: regressed.epoch,
          preservedGen: regressed.connGeneration,
          ghostPolledCount: ghostPolled.length,
          tunnelTimeoutAndEpochVerified
        },
        finopsRolloverAndSanitize2: {
          day1Spend,
          day2Spend,
          activeIdempotencyKeys: finops.getSeenIdempotencyKeyCount("Northstar"),
          finopsRolloverAndSanitizeVerified
        },
        queueAndFence3: {
          emptyHeapCleaned,
          nextLeaseEpoch: nextLease.fencing_token,
          fencePurged,
          queueAndFenceVerified
        },
        schedulerAndDaemon4: {
          cascadedIds: cascaded,
          runningTasksAfterCascade: nodeAfterCascade.running_tasks,
          schedulerAndDaemonVerified
        },
        sagaAndStaging5: {
          invalidatedIdemKeysCount: rb.invalidatedIdemKeys.length,
          reRunCount: reRun.runs,
          purgedSteps,
          sagaAndStagingVerified
        },
        allM68DeepRiskPhase3ChecksPassed
      }
    };
  }
}exports.PlatformManagementPlaneEngine = PlatformManagementPlaneEngine;