"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.AnstsLocalAndComposeServer = void 0;






var http = _interopRequireWildcard(require("node:http"));
var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));
var _scheduler = require("./scheduler.js");
var _node_daemon = require("./node_daemon.js");
var _validator = require("./gateway/validator.js");
var _backpressure = require("./gateway/backpressure.js");
var _sse_stream = require("./gateway/sse_stream.js");
var _server = require("./dashboard/server.js");
var _otel_prometheus = require("./telemetry/otel_prometheus.js");
var _layerfs_workspace_driver = require("./runtime/layerfs_workspace_driver.js");
var _manual_scale_controller = require("./cluster/manual_scale_controller.js");
var _cognitive_router = require("./cognitive_router.js");
var _models = require("./models.js");
var _tunnel_gateway_cell = require("./cloudflare/tunnel_gateway_cell.js");
var _compensator = require("./saga/compensator.js");
var _staging = require("./saga/staging.js");
var _saga_gateway = require("./saga_gateway.js");
var _ha_scheduler_shard_coordinator = require("./cloudflare/ha_scheduler_shard_coordinator.js");
var _scheduler_v25_persistence = require("./cloudflare/scheduler_v25_persistence.js");
var _edge_tunnel_ha_router = require("./runtime/edge_tunnel_ha_router.js");
var _env = require("./cloudflare/env.js");
var _blackboard_and_git_arbiter = require("./a2a/blackboard_and_git_arbiter.js");
var _deadlock_detector = require("./a2a/deadlock_detector.js");
var _parallel_cow_bisect = require("./a2a/parallel_cow_bisect.js");
var _distributed_gc_sweeper = require("./storage/distributed_gc_sweeper.js");
var _layerfs_branch_coordinator = require("./dag/layerfs_branch_coordinator.js");
var _slsa_provenance_attestor = require("./runtime/slsa_provenance_attestor.js");
var _confidential_tee_bridge = require("./runtime/confidential_tee_bridge.js");
var _zero_trust_security_guard = require("./runtime/zero_trust_security_guard.js");
var _mtls_tunnel_bridge = require("./runtime/mtls_tunnel_bridge.js");
var _capacity_sla_enforcer = require("./cluster/capacity_sla_enforcer.js");
var _priority_load_shedder = require("./cluster/priority_load_shedder.js");
var _canary_rollout_controller = require("./cluster/canary_rollout_controller.js");
var _predictive_prewarmer = require("./runtime/predictive_prewarmer.js");
var _geo_routing = require("./cluster/geo_routing.js");
var _cross_region_replicator = require("./cluster/cross_region_replicator.js");
var _chaos_drill_runner = require("./cluster/chaos_drill_runner.js");
var _network_chaos_proxy = require("./runtime/network_chaos_proxy.js");
var _preflight_gate = require("./runtime/preflight_gate.js");
var _sandbox_branch_forker = require("./runtime/sandbox_branch_forker.js");
var _budget_controller = require("./finops/budget_controller.js");
var _mcp_registry = require("./saga/mcp_registry.js");
var _wal_engine = require("./checkpoint/wal_engine.js");
var _time_travel = require("./checkpoint/time_travel.js");
var _live_migration = require("./runtime/live_migration.js");

var _celld_native_optimizations = require("./cloudflare/celld_native_optimizations.js");





var _hot_cell_overload_guard = require("./cloudflare/hot_cell_overload_guard.js");
var _compactor = require("./checkpoint/compactor.js");
var _layerfs_remote_sync = require("./checkpoint/layerfs_remote_sync.js");

var _lease_coordinator = require("./lease_coordinator.js");
var _runner = require("./harness/runner.js");
var _velocity_breaker = require("./harness/velocity_breaker.js");
var _escrow = require("./dag/escrow.js");
var _phase_controller = require("./runtime/phase_controller.js");
var _psi_guard = require("./runtime/psi_guard.js");
var _cloudevents = require("./telemetry/cloudevents.js");
var _outbox_webhook = require("./telemetry/outbox_webhook.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M18 & v2.5 Unified Standalone, Docker Compose & Multi-Node Cluster Service Entrypoint (`src/server_main.ts`).
 * Hardened with:
 * - [P0] Durable Disk-Backed SQL Storage (`DurableDiskCellSqlStorage`) + Cold-Start Rehydration (`RPO=0`) & RustFS S3 LTX Sync
 * - [P0] Strict Single-Write-Path (`SchedulerV25SingleWritePathStore` before in-memory index) & Stateless Gateway Fan-Out
 * - [P1] Cross-Node LayerFS Snapshot Sync (`Body-01 <-> RustFS (:9000) <-> Body-02`) & Auto Dual-Homed Reverse Tunnel Registration
 */












function parseJsonEnvMap(raw) {
  if (!raw || !raw.trim()) return {};
  try {
    const parsed = JSON.parse(raw);
    const out = {};
    for (const [k, v] of Object.entries(parsed)) {
      if (typeof v === "string") out[k] = v;
    }
    return out;
  } catch {
    return {};
  }
}

class AnstsLocalAndComposeServer {
  maxBodyBytes;
  nodeId;
  nodeRole;
  storageDir;
  s3Endpoint;
  primaryGatewayUrl;
  secondaryGatewayUrl;
  peerBrainUrls;
  peerBodyUrls;
  scheduler;
  backpressure;
  sseGateway;
  metrics;
  layerfs;
  fleetController;
  router;
  sagaGateway;
  shardCoordinator;
  v25Store;
  tunnelRouter;
  blackboard;
  deadlockDetector;
  bisectCoordinator;
  gcSweeper;
  branchCoordinator;
  slsaAttestor;
  teeBridge;
  securityGuard;
  mtlsBridge;
  slaEnforcer;
  loadShedder;
  canaryController;
  prewarmer;
  geoRouter;
  crossRegionReplicator;
  chaosDrillRunner;
  chaosProxy;
  preflightGate;
  branchForker;
  finopsController;
  mcpRegistry;
  walEngine;
  timeTravelReplayer;
  heavyMigrator;
  harnessRunner;
  velocityBreaker;
  dagEscrow;
  phaseController;
  psiGuard;
  cloudEventsBus;
  peerTasksCache = null;
  peerTasksInFlight = null;
  outboxDispatcher;
  dashboardServer;
  managementPlane;
  facetSupervisor;
  dynamicWorkerLoader;
  kvOptimizer;
  heapBudgetGuard;
  hotCellGuard;
  walCompactor;
  layerfsStoreSync;
  apiServer = null;
  tickTimer = null;
  tunnelPollTimer = null;
  host;
  defaultApiPort;
  defaultDashboardPort;
  tickIntervalMs;
  draining = false;

  constructor(options = {}) {
    this.host = options.host ?? process.env.ANSTS_HOST ?? "0.0.0.0";
    this.defaultApiPort = options.apiPort ?? Number(process.env.ANSTS_LISTEN_PORT ?? process.env.ANSTS_PORT ?? 8080);
    this.defaultDashboardPort = options.dashboardPort ?? Number(process.env.ANSTS_DASHBOARD_PORT ?? 0);
    this.maxBodyBytes = options.maxBodyBytes ?? 4 * 1024 * 1024;
    this.nodeId = options.nodeId ?? process.env.ANSTS_NODE_ID ?? "ansts-dev-node-01";
    this.nodeRole =
    (options.nodeRole ?? process.env.ANSTS_NODE_ROLE) || "gateway";
    this.storageDir =
    options.storageDir ??
    process.env.ANSTS_STORAGE_DIR ??
    `/tmp/ansts/${this.nodeId}-${process.pid}`;
    this.s3Endpoint = options.s3Endpoint ?? process.env.S3_ENDPOINT ?? null;
    this.primaryGatewayUrl = options.primaryGatewayUrl ?? process.env.ANSTS_PRIMARY_GATEWAY_URL ?? null;
    this.secondaryGatewayUrl = options.secondaryGatewayUrl ?? process.env.ANSTS_SECONDARY_GATEWAY_URL ?? null;
    this.peerBrainUrls = options.peerBrainUrls ?? parseJsonEnvMap(process.env.ANSTS_PEER_BRAIN_URLS);
    this.peerBodyUrls = options.peerBodyUrls ?? parseJsonEnvMap(process.env.ANSTS_PEER_BODY_URLS);

    this.tickIntervalMs = options.tickIntervalMs ?? Number(process.env.ANSTS_TICK_INTERVAL_MS ?? 1000);
    this.scheduler = new _scheduler.CoreScheduler5D(15.0);
    this.backpressure = new _backpressure.IngressBackpressureController();
    this.sseGateway = new _sse_stream.SseEventStreamGateway();
    this.metrics = new _otel_prometheus.OtelAndPrometheusExporter();
    this.layerfs = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    this.fleetController = new _manual_scale_controller.ManualFleetScaleController();
    // Ensure storageDir exists and bind DurableDiskCellSqlStorage for RPO=0 persistence across all subsystems
    try {
      fs.mkdirSync(this.storageDir, { recursive: true });
    } catch {

      // ignore if read-only in restricted environments
    }const sqlFilePath = path.join(this.storageDir, "v25_scheduler.sql.json");
    const durableSql = new _env.DurableDiskCellSqlStorage(sqlFilePath);
    this.v25Store = new _scheduler_v25_persistence.SchedulerV25SingleWritePathStore(durableSql);
    this.router = new _cognitive_router.CognitiveRouter(0.3, durableSql);
    this.sagaGateway = new _saga_gateway.SagaToolGateway(
    new _compensator.SagaCompensatorEngine(durableSql),
    new _staging.StagingCommitAndApprovalGate(durableSql),
    durableSql);

    this.tunnelRouter = new _edge_tunnel_ha_router.EdgeTunnelHaRouter(new _tunnel_gateway_cell.TunnelGatewayCell(durableSql));
    this.blackboard = new _blackboard_and_git_arbiter.SharedBlackboardAndGitArbiter(durableSql);
    this.deadlockDetector = new _deadlock_detector.A2aWaitForGraphDeadlockDetector();
    this.bisectCoordinator = new _parallel_cow_bisect.ParallelCowGitBisectCoordinator();
    this.gcSweeper = new _distributed_gc_sweeper.DistributedCasGcSweeper();
    this.branchCoordinator = new _layerfs_branch_coordinator.LayerFsMultiAgentBranchCoordinator(this.layerfs);
    this.slsaAttestor = new _slsa_provenance_attestor.SlsaProvenanceAttestor();
    this.teeBridge = new _confidential_tee_bridge.ConfidentialTeeAttestationBridge();
    this.securityGuard = new _zero_trust_security_guard.ZeroTrustSandboxSecurityGuard();
    this.mtlsBridge = new _mtls_tunnel_bridge.MtlsReverseTunnelBridge();
    this.slaEnforcer = new _capacity_sla_enforcer.CapacitySlaEnforcer({ sql: durableSql });
    this.loadShedder = new _priority_load_shedder.PriorityLoadShedder();
    this.canaryController = new _canary_rollout_controller.CanaryRolloutController(durableSql);
    this.prewarmer = new _predictive_prewarmer.PredictiveSandboxPrewarmer(0.4, durableSql);
    this.geoRouter = new _geo_routing.GeoSovereigntyAndLatencyRouter(durableSql);
    if (this.geoRouter.listRegions().length === 0) {
      this.geoRouter.registerGeoRegion({ regionId: "eu-central-1", jurisdiction: "GDPR_EU", latencyMs: 18, capacityAvailable: true });
      this.geoRouter.registerGeoRegion({ regionId: "eu-west-1", jurisdiction: "GDPR_EU", latencyMs: 26, capacityAvailable: true });
      this.geoRouter.registerGeoRegion({ regionId: "us-west-2", jurisdiction: "US_FED", latencyMs: 12, capacityAvailable: true });
      this.geoRouter.registerGeoRegion({ regionId: "ap-southeast-1", jurisdiction: "APAC_SG", latencyMs: 31, capacityAvailable: true });
    }
    this.crossRegionReplicator = new _cross_region_replicator.CrossRegionAsyncReplicator();
    this.chaosDrillRunner = new _chaos_drill_runner.AutomatedChaosDrillRunner();
    this.chaosProxy = new _network_chaos_proxy.NetworkChaosFaultInjectionProxy();
    this.preflightGate = new _preflight_gate.SandboxPreflightReadinessGate();
    this.branchForker = new _sandbox_branch_forker.CoWSandboxBranchForker(this.layerfs);
    this.finopsController = new _budget_controller.FinOpsBudgetController(durableSql);
    this.mcpRegistry = new _mcp_registry.McpDynamicToolRegistry(durableSql, this.sagaGateway);
    this.walEngine = new _wal_engine.TurnWalEngine(durableSql);
    this.timeTravelReplayer = new _time_travel.DeterministicTimeTravelReplayer();
    this.heavyMigrator = new _live_migration.EphemeralHeavyExecutorMigrator(new _env.InMemoryR2Bucket());
    this.velocityBreaker = new _velocity_breaker.SyntacticAndVelocityBreaker(3, 120_000, durableSql);
    this.harnessRunner = new _runner.AgentHarnessRunner(this.router, this.sagaGateway, this.velocityBreaker);
    this.dagEscrow = new _escrow.DagEscrowCoordinator(durableSql);
    this.phaseController = new _phase_controller.CgroupPhaseController();
    this.psiGuard = new _psi_guard.LinuxPsiMemoryGuard();
    this.cloudEventsBus = new _cloudevents.CloudEventsBus(1000, durableSql);
    this.outboxDispatcher = new _outbox_webhook.TransactionalOutboxDispatcher(undefined, 4, durableSql);

    const brainNodes =
    Object.keys(this.peerBrainUrls).length > 0 ?
    Object.keys(this.peerBrainUrls) :
    ["brain-node-01", "brain-node-02", "brain-node-03"];
    this.shardCoordinator = new _ha_scheduler_shard_coordinator.HaSchedulerShardCoordinator(256, 8, durableSql);
    if (this.shardCoordinator.topologyCas.getMembers().length === 0) {
      for (const bId of brainNodes) {
        this.shardCoordinator.registerBrainShardNode(bId);
      }
    }

    // Register default LLM endpoints (only if not already rehydrated from SQLite) & Saga tools
    if (!this.router.getEndpoint("ep-deepseek-r1")) {
      this.router.registerEndpoint(
      new _models.LlmEndpointMetrics({
        endpoint_id: "ep-deepseek-r1",
        provider: "deepseek",
        model_name: "DeepSeek",
        ttft_ewma_ms: 180
      }));

    }
    if (!this.router.getEndpoint("ep-qwen-38")) {
      this.router.registerEndpoint(
      new _models.LlmEndpointMetrics({
        endpoint_id: "ep-qwen-38",
        provider: "qwen",
        model_name: "Qwen-3.8",
        ttft_ewma_ms: 150
      }));

    }
    this.sagaGateway.registerTool(
    "shell_exec",
    _models.ToolCallEffectClass.IDEMPOTENT_WRITE,
    (args) => ({
      executed: args.cmd ?? "echo ok",
      status: "SUCCESS"
    }),
    (args) => ({
      rolledBackCmd: args.cmd ?? "echo ok",
      status: "COMPENSATED"
    }));


    // Bootstrap worker nodes
    const configuredBodies = Object.keys(this.peerBodyUrls);
    if (configuredBodies.length > 0) {
      configuredBodies.forEach((bodyId, idx) => {
        const daemon = new _node_daemon.NodeDaemon(bodyId, 32, 65536, 8);
        this.scheduler.registerOrUpdateNode(daemon.emitHeartbeat(0));
        this.fleetController.registerBareMetalNode({
          nodeId: bodyId,
          ipAddress: `10.240.0.${20 + idx}`,
          role: "worker",
          systemdUnit: "ansts-body-daemon.service",
          runningCells: [`BodySandboxHost:${bodyId}`]
        });
      });
    } else {
      const workerCount = options.workerNodeCount ?? Number(process.env.ANSTS_WORKER_NODES ?? 3);
      for (let i = 1; i <= workerCount; i++) {
        const nodeId = `ansts-worker-${String(i).padStart(2, "0")}`;
        const daemon = new _node_daemon.NodeDaemon(nodeId, 32, 65536, 8);
        this.scheduler.registerOrUpdateNode(daemon.emitHeartbeat(0));
        this.fleetController.registerBareMetalNode({
          nodeId,
          ipAddress: `10.240.0.${10 + i}`,
          role: "worker",
          systemdUnit: "ansts-node-daemon.service",
          runningCells: [`AnstsSchedulerCell:shard-${i}`]
        });
      }
    }

    // [M64-4] Rehydrate full in-memory scheduler state (tasks, fencing watermarks, idempotency, fingerprint, queue) from durable v25Store on cold start
    for (const persistedTask of this.v25Store.listPersistedTasks()) {
      if (persistedTask.lease_fencing_token > 0) {
        this.scheduler.leaseCoordinator.rehydrateFencingWatermark(
        persistedTask.task_id,
        persistedTask.lease_fencing_token);

      }
      this.scheduler.allTasks.set(persistedTask.task_id, persistedTask);
      if (persistedTask.idempotency_key) {
        this.scheduler.idempotencyIndex.set(
        _scheduler.CoreScheduler5D.scopeIdempotencyKey(persistedTask.tenant_id, persistedTask.idempotency_key),
        persistedTask.task_id);

      }
      for (const mKey of persistedTask.merged_idempotency_keys) {
        this.scheduler.idempotencyIndex.set(
        _scheduler.CoreScheduler5D.scopeIdempotencyKey(persistedTask.tenant_id, mKey),
        persistedTask.task_id);

      }
      if (!persistedTask.content_fingerprint) {
        persistedTask.content_fingerprint = _scheduler.CoreScheduler5D.computeTaskContentFingerprint(persistedTask);
      }
      this.scheduler.contentFingerprintIndex.set(persistedTask.content_fingerprint, persistedTask.task_id);
      if (persistedTask.state === _models.TaskState.QUEUED) {
        this.scheduler.queueManager.enqueue(persistedTask);
      } else if (persistedTask.state === _models.TaskState.COMPLETED) {
        this.scheduler.completedIds.add(persistedTask.task_id);
      } else if (persistedTask.state === _models.TaskState.DEAD_LETTER) {
        this.scheduler.deadLetterIds.add(persistedTask.task_id);
      }
    }

    const bridge = new _server.DashboardBridgeService(this.scheduler, this.fleetController);
    this.dashboardServer = new _server.DashboardBackendServer(bridge, this.host);
    this.facetSupervisor = new _celld_native_optimizations.DurableObjectFacetSupervisor();
    this.managementPlane = new _server.PlatformManagementPlaneEngine({
      nodeId: this.nodeId,
      scheduler: this.scheduler,
      v25Store: this.v25Store,
      shardCoordinator: this.shardCoordinator,
      fleetController: this.fleetController,
      finopsController: this.finopsController,
      dagEscrow: this.dagEscrow,
      walEngine: this.walEngine,
      timeTravelReplayer: this.timeTravelReplayer,
      layerfs: this.layerfs,
      sagaGateway: this.sagaGateway,
      slaEnforcer: this.slaEnforcer,
      canaryController: this.canaryController,
      crossRegionReplicator: this.crossRegionReplicator,
      loadShedder: this.loadShedder,
      prewarmer: this.prewarmer,
      geoRouter: this.geoRouter,
      heavyMigrator: this.heavyMigrator,
      tunnelRouter: this.tunnelRouter,
      cloudEventsBus: this.cloudEventsBus,
      sseGateway: this.sseGateway,
      blackboard: this.blackboard,
      deadlockDetector: this.deadlockDetector,
      bisectCoordinator: this.bisectCoordinator,
      gcSweeper: this.gcSweeper,
      metrics: this.metrics,
      outboxDispatcher: this.outboxDispatcher,
      harnessRunner: this.harnessRunner,
      phaseController: this.phaseController,
      mcpRegistry: this.mcpRegistry,
      facetSupervisor: this.facetSupervisor,
      peerBrainUrls: this.peerBrainUrls,
      peerBodyUrls: this.peerBodyUrls,
      s3Endpoint: this.s3Endpoint
    });
    this.dynamicWorkerLoader = new _celld_native_optimizations.DynamicWorkerSandboxLoader();
    this.kvOptimizer = new _celld_native_optimizations.CelldKvOptimizerAndGuard(new _env.InMemoryKvNamespace(), 32);
    this.heapBudgetGuard = new _celld_native_optimizations.V8HeapAndSqlBudgetGuard(96, 128, 5000);
    this.hotCellGuard = new _hot_cell_overload_guard.HotCellOverloadAndQueueGuard({
      maxCellConcurrentRequests: 64,
      maxQueueConcurrentProducers: 256
    });
    this.walCompactor = new _compactor.WalCompactorAndPagedRestorer(4096, durableSql);
    this.layerfsStoreSync = new _layerfs_remote_sync.LayerFsRemoteStoreSynchronizer(new _env.InMemoryR2Bucket(), "ansts-fleet", durableSql);
  }

  async start(apiPort, dashboardPort) {
    const targetApiPort = apiPort ?? this.defaultApiPort;
    const targetDashPort = dashboardPort ?? this.defaultDashboardPort;

    const boundApiPort = await new Promise((resolve, reject) => {
      this.apiServer = http.createServer((req, res) => {
        this.handleHttpRequest(req, res).catch((err) => {
          const msg = err.message ?? String(err);
          const status = msg.startsWith("PAYLOAD_TOO_LARGE:") ? 413 : 500;
          if (!res.headersSent) {
            res.writeHead(status, { "content-type": "application/json" });
          }
          res.end(JSON.stringify({ error: msg }));
        });
      });
      this.apiServer.on("error", reject);
      this.apiServer.listen(targetApiPort, this.host, () => {
        const addr = this.apiServer.address();
        resolve(typeof addr === "object" && addr ? addr.port : targetApiPort);
      });
    });

    const boundDashPort = await this.dashboardServer.listen(targetDashPort);

    // If this node is a `body` daemon and gateway URLs are configured, proactively register dual-homed reverse tunnels
    if (this.nodeRole === "body") {
      await this.registerOutboundReverseTunnels();
      this.tunnelPollTimer = setInterval(() => {
        if (!this.draining) {
          this.pollAndExecuteOutboundTunnelCommands().catch(() => {});
        }
      }, 40);
    }

    this.tickTimer = setInterval(() => {
      if (!this.draining) {
        const recovered = this.scheduler.reconcileOrphanedLeases();
        const placements = this.scheduler.scheduleTick();
        if (recovered.length > 0 || placements.length > 0) {
          for (const t of this.scheduler.allTasks.values()) {
            this.syncTaskToV25Store(t);
          }
        }
        this.metrics.setGauge("ansts_queued_tasks", this.scheduler.queueManager.totalQueued());
        this.metrics.setGauge("ansts_Registered_nodes", this.scheduler.nodes.size);
      }
    }, this.tickIntervalMs);

    return { apiPort: boundApiPort, dashboardPort: boundDashPort };
  }

  syncTaskToV25Store(task) {
    const idemKey = task.idempotency_key ?? `idem:${task.tenant_id}:${task.task_id}`;
    this.v25Store.sql.exec(
    "INSERT OR REPLACE INTO v25_scheduler_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, idem_key, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    task.task_id,
    task.tenant_id,
    task.priority,
    task.state,
    task.lease_fencing_token,
    task.assigned_node_id ?? "",
    idemKey,
    JSON.stringify(task));

  }

  async registerOutboundReverseTunnels() {
    const targets = [];
    if (this.primaryGatewayUrl) {
      targets.push({ url: this.primaryGatewayUrl, role: "PRIMARY", gwId: "celld-gw-01" });
    }
    if (this.secondaryGatewayUrl) {
      targets.push({ url: this.secondaryGatewayUrl, role: "SECONDARY", gwId: "celld-gw-02" });
    }
    for (const t of targets) {
      try {
        await fetch(`${t.url.replace(/\/$/, "")}/internal/v1beta1/tunnel/register`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            hostId: this.nodeId,
            gatewayId: t.gwId,
            role: t.role,
            connGeneration: 1,
            epoch: 1
          })
        });
      } catch {

        // ignore if gateway not yet up
      }}
  }


  async pollAndExecuteOutboundTunnelCommands() {
    const gatewayUrls = [this.primaryGatewayUrl, this.secondaryGatewayUrl].filter(
    (u) => Boolean(u && u.trim()));

    for (const gwUrl of gatewayUrls) {
      try {
        const baseUrl = gwUrl.replace(/\/$/, "");
        const pollResp = await fetch(
        `${baseUrl}/internal/v1beta1/tunnel/poll?hostId=${encodeURIComponent(this.nodeId)}`);

        if (!pollResp.ok) continue;
        const { commands } = await pollResp.json();


        for (const cmd of commands ?? []) {
          const taskId = String(cmd.payload.taskId ?? `task-${Date.now()}`);
          const branchName = String(cmd.payload.branchName ?? `task-${taskId}`);
          if (this.s3Endpoint) {
            await this.layerfs.pullBranchFromRemoteS3(branchName, this.s3Endpoint);
          }
          try {
            this.layerfs.getBranchHeadCommit(branchName);
          } catch {
            this.layerfs.forkBranch("main", branchName);
          }
          const execRes = this.layerfs.executeEphemeralBashWorkspace({
            branchName,
            agentId: String(cmd.payload.tenantId ?? "tunnel-agent"),
            stepId: `tunnel-exec-${taskId}`,
            command: `printf "%s\\n" ${JSON.stringify(String(cmd.payload.promptText ?? "ok"))} > .ansts_tunnel_trace.txt`
          });
          let s3Synced = false;
          if (this.s3Endpoint) {
            s3Synced = await this.layerfs.pushBranchToRemoteS3(branchName, this.s3Endpoint);
          }
          await fetch(`${baseUrl}/internal/v1beta1/tunnel/ack`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              commandId: cmd.commandId,
              result: {
                executedByBodyId: this.nodeId,
                taskId,
                branchName,
                commitHash: execRes.commitId,
                commitId: execRes.commitId,
                committedNewSnapshot: execRes.committedNewSnapshot,
                mountLatencyMs: execRes.mountLatencyMs,
                s3Synced,
                deliveredViaReverseTunnel: true,
                status: "COMPLETED"
              }
            })
          });
        }
      } catch {

        // ignore transient gateway poll error
      }}
  }

  /**
  * [M76-P2] Single-flight & TTL-cached peer Brain task aggregation with concurrency cap (max 4 concurrent requests)
  * and 1500ms AbortSignal timeout so polling /v1beta1/console/tasks or /v1beta1/console/overview never amplifies backend load.
  */
  invalidatePeerTasksCache() {
    this.peerTasksCache = null;
  }

  async fetchPeerBrainTasksCached(ttlMs = 1500) {
    const now = Date.now();
    if (this.peerTasksCache && now < this.peerTasksCache.expiresAtMs) {
      return this.peerTasksCache.tasks;
    }
    if (this.peerTasksInFlight) {
      return this.peerTasksInFlight;
    }
    const brainEntries = Object.entries(this.peerBrainUrls);
    if (brainEntries.length === 0) {
      return [];
    }
    this.peerTasksInFlight = (async () => {
      const collected = [];
      const concurrencyLimit = 4;
      for (let i = 0; i < brainEntries.length; i += concurrencyLimit) {
        const batch = brainEntries.slice(i, i + concurrencyLimit);
        await Promise.all(
        batch.map(async ([, bUrl]) => {
          const ctrl = new AbortController();
          const tid = setTimeout(() => ctrl.abort(), 1500);
          try {
            const resp = await fetch(`${bUrl}/internal/v1beta1/brain/tasks`, {
              signal: ctrl.signal
            });
            if (resp.ok) {
              const data = await resp.json();
              for (const t of data.tasks ?? []) {
                collected.push(t);
              }
            }
          } catch {

            // ignore unreachable brain
          } finally {clearTimeout(tid);
          }
        }));

      }
      this.peerTasksCache = { expiresAtMs: Date.now() + ttlMs, tasks: collected };
      this.peerTasksInFlight = null;
      return collected;
    })();
    return this.peerTasksInFlight;
  }

  resolveAuthPrincipal(
  req,
  body,
  requireExplicitIdentity = false)
  {
    const authHeader = req.headers["authorization"];
    return this.managementPlane.rbacGuard.resolveRequestPrincipal({
      authHeader,
      body,
      requireExplicitIdentity
    });
  }

  async readJsonBody(req) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      let totalBytes = 0;
      let rejectedTooLarge = false;
      req.on("data", (c) => {
        if (rejectedTooLarge) return;
        totalBytes += c.byteLength;
        if (totalBytes > this.maxBodyBytes) {
          rejectedTooLarge = true;
          reject(new Error(`PAYLOAD_TOO_LARGE: request body exceeded ${this.maxBodyBytes} bytes`));
          return;
        }
        chunks.push(c);
      });
      req.on("end", () => {
        if (rejectedTooLarge) return;
        try {
          const raw = Buffer.concat(chunks).toString("utf-8").trim();
          resolve(raw ? JSON.parse(raw) : {});
        } catch (err) {
          reject(err);
        }
      });
    });
  }

  async handleHttpRequest(req, res) {
    const method = req.method ?? "GET";
    const urlObj = new URL(req.url ?? "/", "http://127.0.0.1");
    const pathname = urlObj.pathname;

    if (
    method === "GET" && (
    pathname === "/healthz" ||
    pathname === "/health" ||
    pathname === "/readyz" ||
    pathname === "/.well-known/celld/health"))
    {
      if (this.draining) {
        res.writeHead(503, { "content-type": "application/json", connection: "close" });
        res.end(JSON.stringify({ status: "draining", healthy: false, nodeId: this.nodeId, nodeRole: this.nodeRole }));
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        status: "ok",
        healthy: true,
        nodeId: this.nodeId,
        nodeRole: this.nodeRole,
        queuedTasks: this.scheduler.queueManager.totalQueued(),
        totalTasks: this.scheduler.allTasks.size,
        activeNodes: this.scheduler.nodes.size,
        layerfsStats: this.layerfs.storeEngine.getStorageStats()
      }));

      return;
    }

    if (method === "GET" && pathname === "/state") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        nodeId: this.nodeId,
        nodeRole: this.nodeRole,
        node_load: {
          capacity_waiting: 0,
          activation_waiting: 0,
          restoring: 0,
          owned_cells: this.scheduler.allTasks.size + 1,
          resident_cells: this.scheduler.allTasks.size + 1,
          pressured: false,
          memory_headroom: true,
          shed_cells: 0
        },
        deployment: {
          isolates: { live: this.scheduler.nodes.size, live_empty: 0, retiring: 0 }
        }
      }));

      return;
    }

    if (method === "GET" && pathname === "/metrics") {
      this.metrics.setGauge("ansts_queued_tasks", this.scheduler.queueManager.totalQueued());
      this.metrics.setGauge("ansts_total_tasks", this.scheduler.allTasks.size);
      res.writeHead(200, { "content-type": "text/plain; version=0.0.4" });
      res.end(this.metrics.renderPrometheusMetrics());
      return;
    }

    if (method === "POST" && pathname === "/v1/tasks") {
      try {
        const body = await this.readJsonBody(req);
        const spec = _validator.TaskSpecValidator.validateAndNormalize(body);
        const mergeDuplicateInFlight = Boolean(
        body.merge_duplicate ?? body.deduplicate_by_fingerprint ?? false);

        // [M64-4] Pre-check duplicate/mergeable task BEFORE backpressure rejection (consistent with ApiGatewayServer)
        const dupCheck = this.scheduler.inspectAndMergeDuplicateTask(spec, {
          mergeDuplicateInFlight
        });
        if (dupCheck.deduplicated) {
          const canonical = dupCheck.canonicalTask;
          this.v25Store.commitTaskSingleWritePath(spec, canonical.assigned_node_id, {
            mergeDuplicateInFlight
          });
          res.writeHead(202, {
            "content-type": "application/json",
            "X-Ansts-Deduplicated": dupCheck.mergedBy
          });
          res.end(
          JSON.stringify({
            task_id: canonical.task_id,
            state: canonical.state,
            assigned_node_id: canonical.assigned_node_id ?? null,
            lease_fencing_token: canonical.lease_fencing_token,
            deduplicated: true,
            merged_by: dupCheck.mergedBy,
            priority_promoted: dupCheck.priorityPromoted,
            merged_duplicate_count: canonical.merged_duplicate_count,
            placements: []
          }));

          return;
        }

        const decision = this.backpressure.evaluateAdmission({
          priority: spec.priority,
          globalQueueDepth: this.scheduler.queueManager.totalQueued(),
          tenantActiveTasks: this.scheduler.queueManager.getTenantActiveCount(spec.tenant_id),
          clusterPsiPressurePct: 5.0
        });
        if (!decision.allowed) {
          res.writeHead(decision.statusCode, {
            "content-type": "application/json",
            "Retry-After": String(decision.retryAfterSeconds ?? 15)
          });
          res.end(JSON.stringify({ error: "TOO_MANY_REQUESTS", reason: decision.reason }));
          return;
        }
        // Strict Single-Write-Path: check existing persisted task first, then schedule and commit atomically
        const existingPersisted = this.v25Store.getPersistedTask(spec.task_id);
        if (!existingPersisted) {
          const submitted = this.scheduler.submitTask(spec, { mergeDuplicateInFlight });
          const placements = this.scheduler.scheduleTick();
          this.v25Store.commitTaskSingleWritePath(submitted, submitted.assigned_node_id, {
            mergeDuplicateInFlight
          });
          if (this.s3Endpoint) {
            await this.v25Store.flushLtxSnapshotToS3(this.s3Endpoint, this.nodeId);
          }
          this.invalidatePeerTasksCache();
          this.metrics.incCounter("ansts_tasks_submitted_total", 1);
          res.writeHead(202, { "content-type": "application/json" });
          res.end(
          JSON.stringify({
            task_id: submitted.task_id,
            state: submitted.state,
            assigned_node_id: submitted.assigned_node_id ?? null,
            lease_fencing_token: submitted.lease_fencing_token,
            deduplicated: false,
            merged_duplicate_count: submitted.merged_duplicate_count,
            placements
          }));

          return;
        }
        const preCommitted = this.v25Store.commitTaskSingleWritePath(spec, undefined, {
          mergeDuplicateInFlight
        });
        res.writeHead(202, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          task_id: preCommitted.task.task_id,
          state: preCommitted.task.state,
          assigned_node_id: preCommitted.task.assigned_node_id ?? null,
          lease_fencing_token: preCommitted.fencingToken,
          deduplicated: true,
          merged_duplicate_count: preCommitted.task.merged_duplicate_count,
          placements: []
        }));

      } catch (err) {
        if (err instanceof _validator.TaskSpecValidationError) {
          res.writeHead(400, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "VALIDATION_ERROR", field: err.field, message: err.message }));
          return;
        }
        throw err;
      }
      return;
    }

    if (method === "GET" && pathname.startsWith("/v1/tasks/")) {
      const taskId = decodeURIComponent(pathname.slice("/v1/tasks/".length));
      const localTask = this.scheduler.allTasks.get(taskId) ?? this.v25Store.getPersistedTask(taskId);
      if (localTask) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(localTask));
        return;
      }
      // Stateless Gateway Fan-out across peerBrainUrls if not cached locally
      for (const brainUrl of Object.values(this.peerBrainUrls)) {
        try {
          const resp = await fetch(`${brainUrl}/internal/v1beta1/brain/tasks/${encodeURIComponent(taskId)}`);
          if (resp.ok) {
            const remoteTask = await resp.json();
            res.writeHead(200, { "content-type": "application/json" });
            res.end(JSON.stringify(remoteTask));
            return;
          }
        } catch {

          // continue probing remaining brain shards
        }}
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "TASK_NOT_FOUND", task_id: taskId }));
      return;
    }

    if (method === "POST" && pathname === "/v1/layerfs/exec") {
      const body = await this.readJsonBody(req);
      const branchName = String(body.branch ?? "main");
      const command = String(body.command ?? "echo layerfs-ok");
      const secDecision = this.securityGuard.evaluateAction({
        shellCommand: command,
        filePath: body.filePath ? String(body.filePath) : undefined
      });
      if (!secDecision.allowed) {
        res.writeHead(403, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "ZERO_TRUST_POLICY_VIOLATION",
          reason: secDecision.reason ?? "BLOCKED_BY_SECURITY_GUARD"
        }));

        return;
      }
      const stepId = String(body.step_id ?? `step-${Date.now()}`);
      if (this.s3Endpoint) {
        await this.layerfs.pullBranchFromRemoteS3(branchName, this.s3Endpoint);
      }
      if (branchName !== "main") {
        try {
          this.layerfs.getBranchHeadCommit(branchName);
        } catch {
          this.layerfs.forkBranch("main", branchName);
        }
      }
      const result = this.layerfs.executeEphemeralBashWorkspace({
        branchName,
        agentId: String(body.agent_id ?? "http-agent"),
        stepId,
        command
      });
      let s3Synced = false;
      if (this.s3Endpoint) {
        s3Synced = await this.layerfs.pushBranchToRemoteS3(branchName, this.s3Endpoint);
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ...result, executedByNodeId: this.nodeId, nodeRole: this.nodeRole, s3Synced }));
      return;
    }

    // [M46-2] Internal Body LayerFS Live Migration Bundle Import (`POST /internal/v1beta1/body/layerfs-import`)
    if (method === "POST" && pathname === "/internal/v1beta1/body/layerfs-import") {
      const body = await this.readJsonBody(req);
      const bundle = body.branchBundle;
      if (bundle && Array.isArray(bundle.commits)) {
        this.layerfs.importBranchBundle(bundle);
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        importedByNodeId: this.nodeId,
        branchName: bundle?.branchName ?? "unknown",
        headCommitId: bundle?.headCommitId ?? "C0"
      }));

      return;
    }

    // Internal Brain Task Query endpoints (`GET /internal/v1beta1/brain/tasks` & `/:id`)
    if (method === "GET" && pathname === "/internal/v1beta1/brain/tasks") {
      const tasks = this.v25Store.listPersistedTasks();
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ brainNodeId: this.nodeId, totalTasks: tasks.length, tasks }));
      return;
    }

    if (method === "GET" && pathname.startsWith("/internal/v1beta1/brain/tasks/")) {
      const taskId = decodeURIComponent(pathname.slice("/internal/v1beta1/brain/tasks/".length));
      const task = this.v25Store.getPersistedTask(taskId) ?? this.scheduler.allTasks.get(taskId);
      if (!task) {
        res.writeHead(404, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "TASK_NOT_FOUND", task_id: taskId }));
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(task));
      return;
    }

    // Internal Brain RPC endpoint (`POST /internal/v1beta1/brain/schedule`) - Strict Single-Write-Path
    if (method === "POST" && pathname === "/internal/v1beta1/brain/schedule") {
      const body = await this.readJsonBody(req);
      let recoveredFromFailedBrainCount = 0;
      if (body.failoverFromBrainId && this.s3Endpoint) {
        recoveredFromFailedBrainCount = await this.v25Store.restoreLtxSnapshotFromS3(
        this.s3Endpoint,
        String(body.failoverFromBrainId));

        for (const t of this.v25Store.listPersistedTasks()) {
          this.scheduler.allTasks.set(t.task_id, t);
        }
      }
      const taskId = String(body.taskId ?? `task-${Date.now()}`);
      const tenantId = String(body.tenantId ?? "tenant-default");
      const slotInfo = this.shardCoordinator.resolveBrainForTask(tenantId, taskId);
      const spec = _validator.TaskSpecValidator.validateAndNormalize({
        task_id: taskId,
        tenant_id: tenantId,
        priority: 1,
        prompt: String(body.promptText ?? ""),
        idempotency_key: body.idempotencyKey ? String(body.idempotencyKey) : `idem:${tenantId}:${taskId}`
      });

      // Step 1: Check / Commit in durable v25Store first (Single-Write-Path)
      const firstCommit = this.v25Store.commitTaskSingleWritePath(spec);
      let placements = [];
      let finalTask = firstCommit.task;

      if (!firstCommit.deduplicated) {
        const submitted = this.scheduler.submitTask(spec);
        placements = this.scheduler.scheduleTick();
        const targetHost = String(body.targetBodyHostId ?? submitted.assigned_node_id ?? "body-host-01");
        submitted.assigned_node_id = targetHost;
        // Persist updated state & assigned node into durable SQL table
        this.v25Store.sql.exec(
        "INSERT OR REPLACE INTO v25_scheduler_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, idem_key, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        submitted.task_id,
        submitted.tenant_id,
        submitted.priority,
        submitted.state,
        submitted.lease_fencing_token,
        targetHost,
        spec.idempotency_key,
        JSON.stringify(submitted));

        finalTask = submitted;
        if (this.s3Endpoint) {
          await this.v25Store.flushLtxSnapshotToS3(this.s3Endpoint, this.nodeId);
        }
      }

      this.metrics.incCounter("ansts_brain_schedules_total", 1);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        scheduledByBrainId: this.nodeId,
        recoveredFromFailedBrainCount,
        slot: slotInfo.slotIndex,
        taskId: finalTask.task_id,
        state: finalTask.state,
        deduplicated: firstCommit.deduplicated,
        fencingToken: finalTask.lease_fencing_token,
        assignedBodyHostId: String(body.targetBodyHostId ?? finalTask.assigned_node_id ?? "body-host-01"),
        placements
      }));

      return;
    }

    // Internal Body RPC endpoint (`POST /internal/v1beta1/body/execute`)
    if (method === "POST" && pathname === "/internal/v1beta1/body/execute") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? `task-${Date.now()}`);
      const branchName = String(body.branchName ?? `task-${taskId}`);
      if (this.s3Endpoint) {
        await this.layerfs.pullBranchFromRemoteS3(branchName, this.s3Endpoint);
      }
      try {
        this.layerfs.getBranchHeadCommit(branchName);
      } catch {
        this.layerfs.forkBranch("main", branchName);
      }
      const execResult = this.layerfs.executeEphemeralBashWorkspace({
        branchName,
        agentId: String(body.tenantId ?? "distributed-agent"),
        stepId: `exec-${taskId}`,
        command: `printf '%s\\n' ${JSON.stringify(String(body.promptText ?? "ok"))} > .ansts_task_trace.txt`
      });
      let s3Synced = false;
      if (this.s3Endpoint) {
        s3Synced = await this.layerfs.pushBranchToRemoteS3(branchName, this.s3Endpoint);
      }
      this.metrics.incCounter("ansts_body_executions_total", 1);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        executedByBodyId: this.nodeId,
        taskId,
        branchName,
        commitHash: execResult.commitId,
        commitId: execResult.commitId,
        committedNewSnapshot: execResult.committedNewSnapshot,
        mountLatencyMs: execResult.mountLatencyMs,
        s3Synced,
        status: "COMPLETED"
      }));

      return;
    }

    // Dual-Homed Reverse Tunnel registration & health mark (`POST /internal/v1beta1/tunnel/register` & `/health`)
    if (method === "POST" && pathname === "/internal/v1beta1/tunnel/register") {
      const body = await this.readJsonBody(req);
      const hostId = String(body.hostId ?? "body-host-01");
      const gatewayId = String(body.gatewayId ?? this.nodeId);
      const role = body.role === "SECONDARY" ? "SECONDARY" : "PRIMARY";
      const connGeneration = Number(body.connGeneration ?? 1);
      const epoch = Number(body.epoch ?? 1);

      this.tunnelRouter.tunnelCell.registerTunnel({
        hostId,
        gatewayId,
        role,
        connGeneration,
        epoch
      });

      const activeBinding = this.tunnelRouter.tunnelCell.selectActiveTunnel(hostId);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        registered: true,
        hostId,
        gatewayId,
        role,
        connGeneration,
        epoch,
        activeGatewayId: activeBinding.gatewayId
      }));

      return;
    }


    if (method === "GET" && pathname === "/internal/v1beta1/tunnel/poll") {
      const hostId = urlObj.searchParams.get("hostId") ?? "body-host-01";
      const gatewayId = urlObj.searchParams.get("gatewayId") ?? undefined;
      const commands = this.tunnelRouter.tunnelCell.pollPendingCommands(hostId, gatewayId);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ hostId, commands }));
      return;
    }

    if (method === "POST" && pathname === "/internal/v1beta1/tunnel/ack") {
      const body = await this.readJsonBody(req);
      const commandId = String(body.commandId ?? "");
      const result = body.result ?? {};
      const acked = this.tunnelRouter.tunnelCell.acknowledgeCommand(commandId, result);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ acknowledged: acked, commandId }));
      return;
    }

    if (method === "POST" && pathname === "/internal/v1beta1/tunnel/health") {
      const body = await this.readJsonBody(req);
      const hostId = String(body.hostId ?? "body-host-01");
      const gatewayId = String(body.gatewayId ?? this.nodeId);
      const healthy = Boolean(body.healthy ?? true);
      this.tunnelRouter.tunnelCell.markGatewayHealth(hostId, gatewayId, healthy);
      const activeBinding = this.tunnelRouter.tunnelCell.selectActiveTunnel(hostId);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ hostId, activeGatewayId: activeBinding.gatewayId, activeRole: activeBinding.role }));
      return;
    }

    // Distributed Task Dispatch (`POST /v1beta1/tasks/dispatch`)
    if (method === "POST" && pathname === "/v1beta1/tasks/dispatch") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? `task-${Date.now()}`);
      const tenantId = String(body.tenantId ?? "tenant-default");
      const taskPriority = Number(body.priority ?? 1);
      if (body.enforceQuotaGate || this.managementPlane.getScenario() === "quota") {
        await this.managementPlane.pullManagementStateFromS3();
        const quotaAdm = this.managementPlane.checkTenantDispatchAdmission(
        tenantId,
        taskPriority,
        Boolean(body.enforceQuotaGate));

        if (!quotaAdm.allowed) {
          res.writeHead(429, { "content-type": "application/json", "Retry-After": "30" });
          res.end(
          JSON.stringify({
            error: quotaAdm.reason ?? "TENANT_QUOTA_EXHAUSTED",
            tenantId,
            usedSlots: quotaAdm.usedSlots,
            maxSlots: quotaAdm.maxSlots
          }));

          return;
        }
      }
      const promptText = String(body.promptText ?? "");
      const slotResolution = this.shardCoordinator.resolveBrainForTask(tenantId, taskId);
      const ownerBrainId = slotResolution.brainNodeId ?? "brain-node-01";
      const targetBodyHostId = String(body.targetBodyHostId ?? "body-host-01");
      this.invalidatePeerTasksCache();
      this.metrics.incCounter("ansts_tasks_dispatched_total", 1);

      // Forward to live peer Brain shard owner; if unreachable, automatically evict & failover to surviving Brain shard!
      let remoteBrainResult = null;
      let failoverEvent = null;
      const brainBaseUrl = this.peerBrainUrls[ownerBrainId];
      if (brainBaseUrl) {
        try {
          const resp = await fetch(`${brainBaseUrl}/internal/v1beta1/brain/schedule`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              taskId,
              tenantId,
              promptText,
              targetBodyHostId,
              slot: slotResolution.slotIndex
            })
          });
          if (resp.ok) {
            remoteBrainResult = await resp.json();
          }
        } catch {
          // Primary Brain shard is unreachable! Evict from 256-slot ring and failover to surviving Brain
          const eviction = this.shardCoordinator.evictUnhealthyBrainShardNode(ownerBrainId);
          const nextResolution = this.shardCoordinator.resolveBrainForTask(tenantId, taskId);
          const nextBrainId = nextResolution.brainNodeId;
          if (nextBrainId && this.peerBrainUrls[nextBrainId]) {
            try {
              const failResp = await fetch(`${this.peerBrainUrls[nextBrainId]}/internal/v1beta1/brain/schedule`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({
                  taskId,
                  tenantId,
                  promptText,
                  targetBodyHostId,
                  slot: nextResolution.slotIndex,
                  failoverFromBrainId: ownerBrainId
                })
              });
              if (failResp.ok) {
                remoteBrainResult = await failResp.json();
                failoverEvent = {
                  evictedBrainNodeId: ownerBrainId,
                  newOwnerBrainNodeId: nextBrainId,
                  newTopologyGeneration: eviction.newTopologyGeneration
                };
              }
            } catch {

              // ignore if all brains unreachable
            }}
        }
      }

      // Fallback to local schedule only when no peerBrainUrls are configured/reachable
      let localState = "LEASED";
      let localFencingToken = 1;
      let localPlacements = [[taskId, targetBodyHostId, 1]];
      if (!remoteBrainResult) {
        const spec = _validator.TaskSpecValidator.validateAndNormalize({
          task_id: taskId,
          tenant_id: tenantId,
          priority: 1,
          prompt: promptText
        });
        const submitted = this.scheduler.submitTask(spec);
        localPlacements = this.scheduler.scheduleTick();
        this.v25Store.commitTaskSingleWritePath(submitted, targetBodyHostId);
        localState = submitted.state;
        localFencingToken = submitted.lease_fencing_token;
      }

      let remoteBodyResult = null;
      const useReverseTunnel = body.dispatchMode === "reverse_tunnel" || !this.peerBodyUrls[targetBodyHostId];
      if (!useReverseTunnel && this.peerBodyUrls[targetBodyHostId]) {
        try {
          const resp = await fetch(`${this.peerBodyUrls[targetBodyHostId]}/internal/v1beta1/body/execute`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ taskId, tenantId, promptText })
          });
          if (resp.ok) {
            remoteBodyResult = await resp.json();
          }
        } catch {

          // fallback to reverse tunnel below
        }}
      if (!remoteBodyResult) {
        try {
          const tunnelDispatch = await this.tunnelRouter.dispatchOverReverseTunnel(
          targetBodyHostId,
          { taskId, tenantId, promptText },
          3000);

          remoteBodyResult = {
            ...tunnelDispatch.executionResult,
            routedViaGatewayId: tunnelDispatch.routedViaGatewayId,
            tunnelRole: tunnelDispatch.role
          };
        } catch {

          // ignore if no reverse tunnel active
        }}

      res.writeHead(202, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        dispatched: true,
        gatewayNodeId: this.nodeId,
        taskId,
        tenantId,
        shardSlot: slotResolution.slotIndex,
        routedBrainNodeId: remoteBrainResult?.scheduledByBrainId ?? ownerBrainId,
        brainFailover: failoverEvent,
        deduplicated: remoteBrainResult?.deduplicated ?? false,
        fencingToken: remoteBrainResult?.fencingToken ?? localFencingToken,
        state: remoteBrainResult?.state ?? localState,
        targetBodyHostId,
        bodyExecution: remoteBodyResult,
        placements: remoteBrainResult?.placements ?? localPlacements
      }));

      return;
    }

    if (method === "GET" && pathname === "/v1beta1/console/overview") {
      // Stateless fan-out across all peerBrainUrls so both Gateway-01 and Gateway-02 return identical global counts
      const globalTaskIds = new Set(this.scheduler.allTasks.keys());
      for (const t of this.v25Store.listPersistedTasks()) {
        globalTaskIds.add(t.task_id);
      }
      const peerTasks = await this.fetchPeerBrainTasksCached();
      for (const t of peerTasks) {
        globalTaskIds.add(t.task_id);
      }

      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        region: urlObj.searchParams.get("region") ?? "us-west-2",
        gatewayNodeId: this.nodeId,
        totalTasks: globalTaskIds.size,
        activeNodes: this.scheduler.nodes.size,
        configuredBrains: Object.keys(this.peerBrainUrls),
        configuredBodies: Object.keys(this.peerBodyUrls),
        status: "HEALTHY"
      }));

      return;
    }

    if (method === "GET" && pathname === "/v1beta1/console/shards") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        totalShards: 64,
        virtualSlots: 256,
        brains:
        Object.keys(this.peerBrainUrls).length > 0 ?
        Object.keys(this.peerBrainUrls) :
        ["brain-node-01", "brain-node-02", "brain-node-03"],
        sampleSlotOwners: {
          slot0: this.shardCoordinator.ring.getOwnerNodeForSlot(0),
          slot85: this.shardCoordinator.ring.getOwnerNodeForSlot(85),
          slot170: this.shardCoordinator.ring.getOwnerNodeForSlot(170)
        }
      }));

      return;
    }

    // [M40] TanStack Console BFF: Live 11-Service Topology & 256-Slot Ring Map (`GET /v1beta1/console/topology`)
    if (method === "GET" && pathname === "/v1beta1/console/topology") {
      const slotOwners = Array.from({ length: 256 }, (_, idx) =>
      this.shardCoordinator.ring.getOwnerNodeForSlot(idx) ?? "brain-node-01");

      const slotDistribution = {};
      for (const owner of slotOwners) {
        slotDistribution[owner] = (slotDistribution[owner] ?? 0) + 1;
      }
      const brainKeys =
      Object.keys(this.peerBrainUrls).length > 0 ?
      Object.keys(this.peerBrainUrls) :
      ["brain-node-01", "brain-node-02", "brain-node-03"];
      const bodyKeys =
      Object.keys(this.peerBodyUrls).length > 0 ?
      Object.keys(this.peerBodyUrls) :
      ["body-host-01", "body-host-02"];

      const brains = brainKeys.map((bId, idx) => ({
        nodeId: bId,
        url: this.peerBrainUrls[bId] ?? `http://127.0.0.1:${18082 + idx}`,
        ownedSlots: slotDistribution[bId] ?? 0,
        status: (slotDistribution[bId] ?? 0) > 0 ? "HEALTHY" : "EVICTED"
      }));

      const bodies = bodyKeys.map((bdId, idx) => {
        let activeGatewayId = "celld-gw-01";
        let activeRole = "PRIMARY";
        try {
          const active = this.tunnelRouter.tunnelCell.selectActiveTunnel(bdId);
          activeGatewayId = active.gatewayId;
          activeRole = active.role;
        } catch {

          // default primary tunnel
        }return {
          nodeId: bdId,
          url: this.peerBodyUrls[bdId] ?? `http://127.0.0.1:${19501 + idx}`,
          status: "HEALTHY",
          activeGatewayId,
          activeRole
        };
      });

      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        cluster: "ansts-v25-dev-cluster",
        gatewayNodeId: this.nodeId,
        virtualSlots: 256,
        slotOwners,
        slotDistribution,
        brains,
        bodies,
        servicesCount: 11
      }));

      return;
    }

    // [M40 & M76] TanStack Console BFF: Interactive Brain Shard Eviction & Rebalance (`POST /v1beta1/console/shards/rebalance`)
    if (method === "POST" && pathname === "/v1beta1/console/shards/rebalance") {
      const body = await this.readJsonBody(req);
      const auth = this.resolveAuthPrincipal(req, body, false);
      if (!auth.ok) {
        res.writeHead(auth.status, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: auth.error, message: auth.message }));
        return;
      }
      if (auth.role === "VIEWER") {
        res.writeHead(403, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "RBAC_FORBIDDEN", message: "VIEWER 角色无权执行分片驱逐或重平衡" }));
        return;
      }
      if (
      typeof body.action !== "string" ||
      !["evict", "register"].includes(body.action) ||
      typeof body.brainNodeId !== "string" ||
      body.brainNodeId.trim().length === 0)
      {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "MISSING_REQUIRED_PARAMETERS",
          message: "Required parameters: action ('evict' | 'register') and non-empty brainNodeId"
        }));

        return;
      }
      const action = body.action;
      const brainNodeId = body.brainNodeId.trim();
      let event = {};
      if (action === "evict") {
        event = this.shardCoordinator.evictUnhealthyBrainShardNode(brainNodeId);
      } else {
        const epoch = this.shardCoordinator.registerBrainShardNode(brainNodeId);
        event = { registeredNodeId: brainNodeId, epoch };
      }
      const slotOwners = Array.from({ length: 256 }, (_, idx) =>
      this.shardCoordinator.ring.getOwnerNodeForSlot(idx) ?? "brain-node-01");

      const slotDistribution = {};
      for (const owner of slotOwners) {
        slotDistribution[owner] = (slotDistribution[owner] ?? 0) + 1;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ action, brainNodeId, event, slotDistribution, slotOwners }));
      return;
    }

    // [M40] TanStack Console BFF: Global Stateless Task Table Fan-Out (`GET /v1beta1/console/tasks`)
    if (method === "GET" && pathname === "/v1beta1/console/tasks") {
      const taskMap = new Map();
      for (const [id, t] of this.scheduler.allTasks.entries()) {
        taskMap.set(id, t);
      }
      for (const t of this.v25Store.listPersistedTasks()) {
        taskMap.set(t.task_id, t);
      }
      const peerTasks = await this.fetchPeerBrainTasksCached();
      for (const t of peerTasks) {
        taskMap.set(t.task_id, t);
      }
      const tasks = Array.from(taskMap.values()).map((t) => {
        const resSlot = this.shardCoordinator.resolveBrainForTask(t.tenant_id, t.task_id);
        return {
          task_id: t.task_id,
          tenant_id: t.tenant_id,
          priority: t.priority,
          state: t.state,
          lease_fencing_token: t.lease_fencing_token,
          assigned_node_id: t.assigned_node_id ?? "body-host-01",
          shardSlot: resSlot.slotIndex,
          routedBrainNodeId: resSlot.brainNodeId ?? "brain-node-01",
          prompt: t.prompt
        };
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ totalTasks: tasks.length, tasks }));
      return;
    }

    // [M40] TanStack Console BFF: LayerFS Branch & Line-Blame Explorer (`GET /v1beta1/console/layerfs`)
    if (method === "GET" && pathname === "/v1beta1/console/layerfs") {
      const branch = urlObj.searchParams.get("branch");
      const file = urlObj.searchParams.get("file");
      let blame = null;
      if (branch && file) {
        try {
          blame = this.layerfs.blameFile(branch, file);
        } catch {
          blame = [];
        }
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        nodeId: this.nodeId,
        branches: this.layerfs.listBranchesSummary(),
        storageStats: this.layerfs.storeEngine.getStorageStats(),
        blame
      }));

      return;
    }

    // [M35] Delete LayerFS Branch (`DELETE /v1/layerfs/branches/:branch`)
    if (method === "DELETE" && pathname.startsWith("/v1/layerfs/branches/")) {
      const branchName = decodeURIComponent(pathname.slice("/v1/layerfs/branches/".length));
      const deleted = this.layerfs.deleteBranch(branchName);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ deleted, branchName }));
      return;
    }

    // [M35] Distributed CAS Garbage Collection (`POST /v1beta1/storage/gc`)
    if (method === "POST" && pathname === "/v1beta1/storage/gc") {
      const gcResult = this.gcSweeper.sweepLayerFsWorkspaceStore(this.layerfs);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        nodeId: this.nodeId,
        ...gcResult,
        remainingStats: this.layerfs.storeEngine.getStorageStats()
      }));

      return;
    }

    // [M35] A2A Shared Blackboard, Git Patch Arbiter & Wait-For-Graph Deadlock Detector (`POST /v1beta1/a2a/blackboard`)
    if (method === "POST" && pathname === "/v1beta1/a2a/blackboard") {
      const body = await this.readJsonBody(req);
      const action = String(body.action ?? "get");
      if (action === "putCas") {
        try {
          const out = this.blackboard.putCas(
          String(body.key ?? "default"),
          body.value,
          Number(body.expectedVersion ?? 0),
          String(body.authorAgentId ?? "agent-01"));

          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify(out));
        } catch (err) {
          res.writeHead(409, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "CAS_CONFLICT", message: err.message }));
        }
        return;
      }
      if (action === "arbitratePatches") {
        const patches = body.patches ?? [];
        const out = this.blackboard.arbitrateGitPatchTouches(patches);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(out));
        return;
      }
      if (action === "addWaitEdge") {
        this.deadlockDetector.addWaitEdge(String(body.waiterAgentId), String(body.holderAgentId));
        const cycle = this.deadlockDetector.detectCycle();
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(cycle));
        return;
      }
      const entry = this.blackboard.get(String(body.key ?? "default"));
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ entry: entry ?? null }));
      return;
    }

    // [M35] A2A Parallel CoW Git Bisect over HTTP (`POST /v1beta1/a2a/bisect`)
    if (method === "POST" && pathname === "/v1beta1/a2a/bisect") {
      const body = await this.readJsonBody(req);
      const commits = body.commits ?? [];
      const testShellCommand = String(body.testShellCommand ?? "true");
      const out = this.bisectCoordinator.executeLiveLayerFsBisect({
        driver: this.layerfs,
        commits,
        testShellCommand
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(out));
      return;
    }


    // [M36] Live Cross-Node Multi-Agent LayerFS 3-Way Branch Merge (`POST /v1/layerfs/merge`)
    if (method === "POST" && pathname === "/v1/layerfs/merge") {
      const body = await this.readJsonBody(req);
      const sourceBranch = String(body.sourceBranch ?? "feat-agent-a");
      const targetBranch = String(body.targetBranch ?? "main");
      const baseCommitId = String(body.baseCommitId ?? "C0");
      if (this.s3Endpoint) {
        await this.layerfs.pullBranchFromRemoteS3(sourceBranch, this.s3Endpoint);
        await this.layerfs.pullBranchFromRemoteS3(targetBranch, this.s3Endpoint);
      }
      try {
        const mergeResult = this.branchCoordinator.mergeBranchIntoTarget(
        sourceBranch,
        targetBranch,
        baseCommitId);

        let s3Synced = false;
        if (this.s3Endpoint) {
          s3Synced = await this.layerfs.pushBranchToRemoteS3(targetBranch, this.s3Endpoint);
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ...mergeResult, targetBranch, s3Synced }));
      } catch (err) {
        if (err instanceof _layerfs_branch_coordinator.LayerFsMergeConflictError) {
          res.writeHead(409, { "content-type": "application/json" });
          res.end(
          JSON.stringify({
            error: "LAYERFS_MERGE_CONFLICT",
            conflictingFiles: err.conflictingFiles,
            message: err.message
          }));

          return;
        }
        throw err;
      }
      return;
    }

    // [M36] Cryptographic SLSA v1.0 Provenance + TEE Attestation + SPIFFE mTLS Verification (`POST /v1beta1/security/attest`)
    if (method === "POST" && pathname === "/v1beta1/security/attest") {
      const body = await this.readJsonBody(req);
      const branchOrCommit = String(body.branchOrCommit ?? "main");
      const spiffeId = String(body.spiffeId ?? `spiffe://ansts.io/ns/fleet/sa/${this.nodeRole}`);
      const certExpiresAtMs = Number(body.certExpiresAtMs ?? Date.now() + 3600_000);

      const mtlsCheck = this.mtlsBridge.verifySpiffeCertificate(spiffeId, certExpiresAtMs);
      if (!mtlsCheck.authenticated) {
        res.writeHead(401, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "SPIFFE_MTLS_UNAUTHENTICATED", spiffeId }));
        return;
      }

      if (this.s3Endpoint) {
        await this.layerfs.pullBranchFromRemoteS3(branchOrCommit, this.s3Endpoint);
      }
      const slsaStatement = this.slsaAttestor.attestLayerFsCommit(this.layerfs, branchOrCommit);
      const slsaVerified = this.slsaAttestor.verifyProvenanceStatement(slsaStatement);
      const teeQuote = this.teeBridge.generateAndVerifyAttestationQuote({
        vmId: this.nodeId,
        measurementDigestHex: slsaStatement.subjectDigest,
        nonce: String(body.nonce ?? "ansts-v25-nonce")
      });

      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        nodeId: this.nodeId,
        mtlsCheck,
        slsaStatement,
        slsaVerified,
        teeQuote
      }));

      return;
    }

    // [M36] Closed-Loop Cluster Governance: SLA Enforcer, Priority Load Shedder, Canary & Prewarmer (`POST /v1beta1/cluster/governance`)
    if (method === "POST" && pathname === "/v1beta1/cluster/governance") {
      const body = await this.readJsonBody(req);
      const action = String(body.action ?? "evaluateSlaAndPrewarm");
      if (action === "evaluateCanary") {
        const canaryRes = this.canaryController.evaluateCanaryStep({
          errorRatePct: Number(body.errorRatePct ?? 0.1),
          p99LatencyMs: Number(body.p99LatencyMs ?? 120),
          maxErrorRatePct: body.maxErrorRatePct !== undefined ? Number(body.maxErrorRatePct) : undefined,
          maxP99LatencyMs: body.maxP99LatencyMs !== undefined ? Number(body.maxP99LatencyMs) : undefined
        });
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(canaryRes));
        return;
      }
      const utilizationRatio = Number(body.utilizationRatio ?? 0.5);
      const slaCompliance = this.slaEnforcer.evaluateSlaCompliance({
        p0QueueWaitP99Ms: Number(body.p0QueueWaitP99Ms ?? 80),
        warmPoolHitRatio: Number(body.warmPoolHitRatio ?? 0.94),
        freeClusterCpuRatio: Number(body.freeClusterCpuRatio ?? 1 - utilizationRatio)
      });
      const prewarmPrediction = this.prewarmer.recordArrivalSampleAndPredict(
      Number(body.observedArrivalsPerSec ?? 12.0),
      Number(body.avgSandboxHoldSec ?? 4.0));

      const priorityAdmission = {
        P0_EMERGENCY: this.loadShedder.shouldAdmitUnderLoad(0, utilizationRatio),
        P1_HIGH: this.loadShedder.shouldAdmitUnderLoad(1, utilizationRatio),
        P2_STANDARD: this.loadShedder.shouldAdmitUnderLoad(2, utilizationRatio),
        P3_BATCH: this.loadShedder.shouldAdmitUnderLoad(3, utilizationRatio)
      };
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        nodeId: this.nodeId,
        utilizationRatio,
        slaCompliance,
        prewarmPrediction,
        priorityAdmission
      }));

      return;
    }


    // [M37] Geo-Sovereignty Routing & Cross-Region S3 LTX Replication (`POST /v1beta1/cluster/geo-route`)
    if (method === "POST" && pathname === "/v1beta1/cluster/geo-route") {
      const body = await this.readJsonBody(req);
      if (body.action === "setRegionAvailability") {
        const updated = this.geoRouter.setRegionAvailability(
        String(body.regionId ?? "eu-central-1"),
        Boolean(body.capacityAvailable ?? true),
        body.latencyMs !== undefined ? Number(body.latencyMs) : undefined);

        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ updated, regions: this.geoRouter.listRegions() }));
        return;
      }
      const jurisdiction = body.jurisdiction ?? "GDPR_EU";
      try {
        const selectedRegion = this.geoRouter.routeByJurisdiction(jurisdiction);
        let replication = null;
        if (body.replicateObjectKey) {
          if (this.s3Endpoint) {
            replication = await this.crossRegionReplicator.replicateToRemoteS3Region({
              s3Endpoint: this.s3Endpoint,
              sourceRegion: selectedRegion.regionId,
              targetRegion: String(body.drTargetRegion ?? "eu-west-1"),
              objectKey: String(body.replicateObjectKey),
              payload: body.payload ?? { jurisdiction }
            });
          } else {
            replication = this.crossRegionReplicator.replicateLtxObject(
            String(body.drTargetRegion ?? "eu-west-1"),
            String(body.replicateObjectKey));

          }
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ selectedRegion, replication }));
      } catch (err) {
        res.writeHead(451, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "DATA_SOVEREIGNTY_LOCK",
          jurisdiction,
          message: err.message
        }));

      }
      return;
    }

    // [M37] Live Chaos Drill & Host Preflight Readiness Gate (`POST /v1beta1/cluster/chaos-drill`)
    if (method === "POST" && pathname === "/v1beta1/cluster/chaos-drill") {
      const body = await this.readJsonBody(req);
      const preflight = this.preflightGate.probeLiveHostReadiness(Number(body.minDiskGb ?? 1));
      const drill = this.chaosDrillRunner.executeLivePartitionAndFencingDrill({
        chaosProxy: this.chaosProxy,
        targetHost: String(body.targetHost ?? "body-host-01"),
        oldNodeFencingToken: Number(body.oldNodeFencingToken ?? 3),
        newNodeFencingToken: Number(body.newNodeFencingToken ?? 4)
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ nodeId: this.nodeId, preflight, drill }));
      return;
    }

    // [M37] Deterministic Execution Replay Envelope & Parallel CoW Sandbox Branch Forker (`POST /v1beta1/runtime/deterministic-fork`)
    if (method === "POST" && pathname === "/v1beta1/runtime/deterministic-fork") {
      const body = await this.readJsonBody(req);
      const sourceBranch = String(body.sourceBranch ?? "main");
      const branches =
      body.branches ?? [];
      const results = this.branchForker.executeDeterministicSpeculativeBranches({
        sourceBranch,
        branches
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ nodeId: this.nodeId, sourceBranch, results }));
      return;
    }


    // [M38-1] Cognitive LLM Router + FinOps Budget Auto-Downgrade & 402 Gate (`POST /v1beta1/router/completions`)
    if (method === "POST" && pathname === "/v1beta1/router/completions") {
      const body = await this.readJsonBody(req);
      if (body.action === "setBudgetPolicy") {
        this.finopsController.setPolicy({
          tenantId: String(body.tenantId ?? "tenant-default"),
          dailyBudgetUsd: Number(body.dailyBudgetUsd ?? 10.0),
          softDowngradeThresholdRatio: Number(body.softDowngradeThresholdRatio ?? 0.8),
          preferredModel: String(body.preferredModel ?? "DeepSeek"),
          fallbackEconomyModel: String(body.fallbackEconomyModel ?? "Qwen-3.8")
        });
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ policyUpdated: true, tenantId: body.tenantId }));
        return;
      }
      const tenantId = String(body.tenantId ?? "tenant-default");
      const budgetDecision = this.finopsController.evaluateBudgetAndModel(tenantId);
      if (!budgetDecision.allowed) {
        res.writeHead(402, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "FINOPS_BUDGET_EXCEEDED",
          tenantId,
          ...budgetDecision
        }));

        return;
      }
      const costUsd = Number(body.costUsd ?? 0.5);
      const totalSpentUsd = this.finopsController.recordSpend(tenantId, costUsd);
      const prefixHash = _cognitive_router.CognitiveRouter.computePrefixHash(
      String(body.systemPrompt ?? "You are ANSTS Agent"),
      String(body.toolsSchema ?? "default-tools"));

      const [endpoint, prefixCacheHit] = this.router.selectEndpoint(
      prefixHash,
      Number(body.estimatedTokens ?? 512),
      Number(body.priority ?? _models.TaskPriority.P1_HIGH));

      this.router.releaseEndpoint(endpoint.endpoint_id);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        tenantId,
        selectedModel: budgetDecision.selectedModel,
        downgraded: budgetDecision.downgraded,
        utilizationRatio: budgetDecision.utilizationRatio,
        totalSpentUsd,
        endpointId: endpoint.endpoint_id,
        prefixCacheHit,
        prefixHash
      }));

      return;
    }

    // [M38-2] Dynamic MCP Tool Hub Registration (`POST /v1beta1/saga/mcp/register`)
    if (method === "POST" && pathname === "/v1beta1/saga/mcp/register") {
      const body = await this.readJsonBody(req);
      const serverName = String(body.serverName ?? "mcp-github");
      const rawTools = body.tools ?? [];
      const descriptors = rawTools.map((t) => ({
        toolName: String(t.toolName ?? "mcp_tool"),
        description: String(t.description ?? "MCP dynamic tool"),
        effectClass: t.effectClass ?? _models.ToolCallEffectClass.COMPENSATABLE_WRITE,
        inputSchema: t.inputSchema ?? {}
      }));
      const count = this.mcpRegistry.registerMcpServerTools(
      serverName,
      descriptors,
      this.sagaGateway,
      (rpcMethod, rpcParams) => ({
        jsonrpc: "2.0",
        method: rpcMethod,
        serverName,
        invokedTool: rpcParams.name,
        argsEcho: rpcParams.arguments,
        status: "MCP_OK"
      }));

      // Also attach compensation handlers for registered COMPENSATABLE_WRITE MCP tools
      for (const d of descriptors) {
        if (d.effectClass) {
          this.sagaGateway.registerTool(
          d.toolName,
          d.effectClass,
          (args) => ({
            jsonrpc: "2.0",
            serverName,
            invokedTool: d.toolName,
            argsEcho: args,
            status: "MCP_OK"
          }),
          (args) => ({
            compensatedTool: d.toolName,
            argsEcho: args,
            status: "MCP_COMPENSATED"
          }));

        }
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ registeredCount: count, totalMcpTools: this.mcpRegistry.listTools().length }));
      return;
    }

    // [M38-2] Saga Idempotent Tool Execution & Human Approval Staging (`POST /v1beta1/saga/execute`)
    if (method === "POST" && pathname === "/v1beta1/saga/execute") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? "task-saga-01");
      const turnIndex = Number(body.turnIndex ?? 1);
      const toolName = String(body.toolName ?? "shell_exec");
      const args = body.args ?? {};

      let task = this.scheduler.allTasks.get(taskId);
      if (!task) {
        task = new _models.TaskSpec({
          task_id: taskId,
          tenant_id: String(body.tenantId ?? "tenant-default"),
          priority: _models.TaskPriority.P1_HIGH,
          prompt: "saga task"
        });
        this.scheduler.allTasks.set(taskId, task);
      }

      if (body.requireApproval) {
        const staged = this.sagaGateway.stagingGate.stageEffect({
          effectId: `eff-${taskId}-${turnIndex}`,
          taskId,
          turnIndex,
          toolName,
          args
        });
        const ticket = this.sagaGateway.stagingGate.requestHumanApproval(task, toolName, args);
        this.syncTaskToV25Store(task);
        res.writeHead(202, { "content-type": "application/json" });
        res.end(JSON.stringify({ status: "PENDING_APPROVAL", stagedEffectId: staged.effectId, ticket }));
        return;
      }

      const [output, deduplicated] = this.sagaGateway.invokeTool(taskId, turnIndex, toolName, args);
      this.syncTaskToV25Store(task);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "EXECUTED", taskId, turnIndex, toolName, deduplicated, output }));
      return;
    }

    // [M38-2] Human-in-the-Loop Approval Resolution (`POST /v1beta1/saga/approve`)
    if (method === "POST" && pathname === "/v1beta1/saga/approve") {
      const body = await this.readJsonBody(req);
      const ticketId = String(body.ticketId ?? "");
      const taskId = String(body.taskId ?? "task-saga-01");
      const approved = Boolean(body.approved ?? true);
      const decidedBy = String(body.decidedBy ?? "secops-reviewer");
      let task = this.scheduler.allTasks.get(taskId);
      if (!task) {
        task = new _models.TaskSpec({
          task_id: taskId,
          tenant_id: "tenant-default",
          priority: _models.TaskPriority.P1_HIGH,
          prompt: "saga approval task"
        });
        this.scheduler.allTasks.set(taskId, task);
      }
      const ticket = this.sagaGateway.stagingGate.resolveApproval(ticketId, approved, decidedBy, task);
      let committedEffects = [];
      if (approved) {
        committedEffects = this.sagaGateway.stagingGate.commitStagedEffects(
        taskId,
        (tName, tArgs, tTurnIndex) => {
          return this.sagaGateway.invokeTool(taskId, tTurnIndex, tName, tArgs)[0];
        });

      } else {
        this.sagaGateway.stagingGate.abortStagedEffects(taskId);
      }
      this.syncTaskToV25Store(task);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ticket, taskState: task.state, committedEffects }));
      return;
    }

    // [M38-2 & M64-3] Reverse-Order LIFO Saga Compensation Rollback & Idempotency Journal Invalidation (`POST /v1beta1/saga/rollback`)
    if (method === "POST" && pathname === "/v1beta1/saga/rollback") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? "task-saga-01");
      const upToTurnInclusive = Number(body.upToTurnInclusive ?? 0);
      const rb = this.sagaGateway.rollbackAndInvalidateTask(taskId, upToTurnInclusive);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        taskId,
        rolledBackCount: rb.rolledBackSteps.length,
        invalidatedIdemKeysCount: rb.invalidatedIdemKeys.length,
        rolledBackSteps: rb.rolledBackSteps.map((s) => ({
          stepId: s.stepId,
          turnIndex: s.turnIndex,
          toolName: s.toolName,
          compensationResult: s.compensationResult
        }))
      }));

      return;
    }

    // [M38-3] Turn WAL Append & Deterministic Time-Travel Task/LayerFS Forking (`POST /v1beta1/checkpoint/time-travel`)
    if (method === "POST" && pathname === "/v1beta1/checkpoint/time-travel") {
      const body = await this.readJsonBody(req);
      const action = String(body.action ?? "appendTurnWal");
      const taskId = String(body.taskId ?? "task-wal-01");

      if (action === "appendTurnWal") {
        try {
          const entry = this.walEngine.appendTurnDelta({
            taskId,
            turnIndex: Number(body.turnIndex ?? 1),
            fencingToken: Number(body.fencingToken ?? 1),
            changedFiles: body.changedFiles ?? ["app.ts"],
            sqliteWalPages: Number(body.sqliteWalPages ?? 4),
            highestSubmittedFencingToken: Number(body.highestSubmittedFencingToken ?? 1)
          });
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ appended: true, entry }));
        } catch (err) {
          if (err instanceof _lease_coordinator.StaleFenceError || err.name === "StaleFenceError" || err.message.includes("Stale")) {
            res.writeHead(409, { "content-type": "application/json" });
            res.end(JSON.stringify({ error: "STALE_FENCE_TOKEN", message: err.message }));
            return;
          }
          throw err;
        }
        return;
      }

      // action === "forkAtTurn"
      let originalTask = this.scheduler.allTasks.get(taskId) ?? this.v25Store.getPersistedTask(taskId);
      if (!originalTask) {
        originalTask = new _models.TaskSpec({
          task_id: taskId,
          tenant_id: String(body.tenantId ?? "tenant-default"),
          priority: _models.TaskPriority.P1_HIGH,
          prompt: "original task prompt"
        });
        this.scheduler.allTasks.set(taskId, originalTask);
      }
      const walEntries = this.walEngine.getTaskWalEntries(taskId);
      const forked = this.timeTravelReplayer.forkTaskAtTurn(
      originalTask,
      walEntries,
      Number(body.targetTurnIndex ?? 1),
      String(body.newTaskId ?? `${taskId}-fork`),
      body.overriddenPrompt ? String(body.overriddenPrompt) : undefined);

      this.scheduler.allTasks.set(forked.forkedTask.task_id, forked.forkedTask);
      this.v25Store.commitTaskSingleWritePath(forked.forkedTask);

      let layerfsFork = null;
      if (body.sourceCommitId) {
        layerfsFork = this.timeTravelReplayer.forkLayerFsCommitZeroCopy(
        this.layerfs,
        String(body.sourceCommitId),
        String(body.newBranchName ?? `branch-${forked.forkedTask.task_id}`));

      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ forked, layerfsFork }));
      return;
    }

    // [M38-3] Heavy Sandbox Cross-Node Live Migration (`POST /v1beta1/runtime/migrate`)
    if (method === "POST" && pathname === "/v1beta1/runtime/migrate") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? "task-heavy-01");
      const branchName = String(body.branchName ?? "main");
      const payloadBytes = new TextEncoder().encode(
      JSON.stringify(this.layerfs.exportBranchBundle(branchName)));

      const migration = await this.heavyMigrator.migrateHeavyTaskAcrossNodes({
        taskId,
        turnIndex: Number(body.turnIndex ?? 1),
        sourceNodeId: String(body.sourceNodeId ?? this.nodeId),
        targetNodeId: String(body.targetNodeId ?? "body-host-02"),
        newFencingToken: Number(body.newFencingToken ?? 2),
        upperdirSnapshotBytes: payloadBytes
      });
      let s3Synced = false;
      if (this.s3Endpoint) {
        s3Synced = await this.layerfs.pushBranchToRemoteS3(branchName, this.s3Endpoint);
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ...migration, branchName, s3Synced }));
      return;
    }


    // [M39-1] Multi-Turn Agent Harness Runner + Velocity Breaker + Cgroup Phase + PSI Guard (`POST /v1beta1/harness/run-turn`)
    if (method === "POST" && pathname === "/v1beta1/harness/run-turn") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? "task-harness-01");
      const tenantId = String(body.tenantId ?? "tenant-default");
      const toolName = String(body.toolName ?? "shell_exec");
      const toolArgs = body.toolArgs ?? { cmd: "echo ok" };
      const estimatedTokens = Number(body.estimatedTokens ?? 1200);

      let task = this.scheduler.allTasks.get(taskId);
      if (!task) {
        task = new _models.TaskSpec({
          task_id: taskId,
          tenant_id: tenantId,
          priority: _models.TaskPriority.P1_HIGH,
          prompt: String(body.prompt ?? "harness turn")
        });
        this.scheduler.allTasks.set(taskId, task);
      }

      const thinkCgroup = this.phaseController.applyThinkPhase(taskId, task.resource_profile);
      try {
        const turnSummary = this.harnessRunner.executeSingleTurn(
        task,
        toolName,
        toolArgs,
        estimatedTokens);

        const actCgroup = this.phaseController.applyActPhase(taskId, task.resource_profile);
        this.syncTaskToV25Store(task);
        try {
          const fTok = Math.max(1, task.lease_fencing_token);
          this.walEngine.appendTurnDelta({
            taskId,
            turnIndex: turnSummary.turnIndex,
            fencingToken: fTok,
            changedFiles: [`harness/${taskId}-turn-${turnSummary.turnIndex}.json`],
            sqliteWalPages: 4,
            highestSubmittedFencingToken: fTok
          });
        } catch {

          // ignore if WAL already has this turn
        }
        let psiDefense = null;
        if (body.psiSample) {
          const sample = body.psiSample;
          psiDefense = this.psiGuard.evaluateAndDefend(sample, Array.from(this.scheduler.allTasks.values()));
        }

        const sseEvtId = `evt-${taskId}-t${turnSummary.turnIndex}`;
        this.sseGateway.publish({
          id: sseEvtId,
          event: "turn.completed",
          taskId,
          turnIndex: turnSummary.turnIndex,
          timestampMs: Date.now(),
          data: { toolName, turnIndex: turnSummary.turnIndex, endpointId: turnSummary.endpointId }
        });

        const cloudEvent = this.cloudEventsBus.emitEvent({
          type: "io.ansts.harness.turn.completed",
          subject: taskId,
          tenantId,
          llmCostUsd: Number(body.llmCostUsd ?? 0.02),
          sandboxCpuCoreSeconds: 1.5,
          data: { turnIndex: turnSummary.turnIndex, toolName }
        });

        res.writeHead(200, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          turnSummary,
          thinkCgroup,
          actCgroup,
          psiDefense,
          sseEvtId,
          cloudEventId: cloudEvent.id
        }));

      } catch (err) {
        const msg = err.message;
        if (msg.includes("Velocity breaker tripped")) {
          this.syncTaskToV25Store(task);
          res.writeHead(429, { "content-type": "application/json" });
          res.end(
          JSON.stringify({
            error: "VELOCITY_BREAKER_TRIPPED",
            taskState: task.state,
            message: msg
          }));

          return;
        }
        throw err;
      }
      return;
    }

    // [M39-1] Parent-Child Sub-Agent DAG Quota Escrow & Cycle Guard (`POST /v1beta1/dag/escrow`)
    if (method === "POST" && pathname === "/v1beta1/dag/escrow") {
      const body = await this.readJsonBody(req);
      const action = String(body.action ?? "reserveParent");
      const parentId = String(body.parentTaskId ?? "task-parent-01");
      const tenantId = String(body.tenantId ?? "tenant-dag");

      let parentTask = this.scheduler.allTasks.get(parentId);
      if (!parentTask) {
        parentTask = new _models.TaskSpec({
          task_id: parentId,
          tenant_id: tenantId,
          priority: _models.TaskPriority.P1_HIGH,
          prompt: "parent task",
          expected_SubAgent_count: Number(body.expectedSubAgentCount ?? 2)
        });
        this.scheduler.allTasks.set(parentId, parentTask);
      }

      if (action === "reserveParent") {
        const record = this.dagEscrow.reserveParentEscrow(
        parentTask,
        Number(body.maxTenantQuota ?? 10),
        Number(body.currentTenantUsed ?? 1));

        this.syncTaskToV25Store(parentTask);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          parentTaskId: record.parentTaskId,
          escrowedSlots: record.escrowedSlots,
          usedSlots: record.usedSlots
        }));

        return;
      }

      if (action === "registerChild") {
        const childId = String(body.childTaskId ?? "task-child-01");
        const deps = body.dependencies ?? [];
        const childTask = new _models.TaskSpec({
          task_id: childId,
          tenant_id: tenantId,
          priority: _models.TaskPriority.P1_HIGH,
          prompt: "child task",
          dependencies: deps,
          parent_task_id: parentId
        });
        try {
          const out = this.dagEscrow.registerChildSubAgent(parentTask, childTask);
          this.scheduler.allTasks.set(childId, childTask);
          this.syncTaskToV25Store(parentTask);
          this.syncTaskToV25Store(childTask);
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ ...out, parentState: parentTask.state, childTaskId: childId }));
        } catch (err) {
          if (err.message.includes("DAG cycle detected")) {
            res.writeHead(400, { "content-type": "application/json" });
            res.end(JSON.stringify({ error: "DAG_CYCLE_DETECTED", message: err.message }));
            return;
          }
          throw err;
        }
        return;
      }

      if (action === "completeChild") {
        const childId = String(body.childTaskId ?? "task-child-01");
        const allChildrenDone = this.dagEscrow.completeChildSubAgent(parentTask, childId);
        this.syncTaskToV25Store(parentTask);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ allChildrenDone, parentState: parentTask.state }));
        return;
      }

      if (action === "failChild") {
        const childId = String(body.childTaskId ?? "task-child-01");
        const abortParent = body.abortParentOnChildFailure !== undefined ? Boolean(body.abortParentOnChildFailure) : true;
        const failRes = this.dagEscrow.failOrCancelChildSubAgent(parentTask, childId, abortParent);
        this.syncTaskToV25Store(parentTask);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(failRes));
        return;
      }
    }

    // [M39-2] CloudEvents v1.0 Telemetry Bus & HMAC-SHA256 Transactional Outbox Webhook (`POST /v1beta1/telemetry/events`)
    if (method === "POST" && pathname === "/v1beta1/telemetry/events") {
      const body = await this.readJsonBody(req);
      const tenantId = String(body.tenantId ?? "tenant-telemetry");
      const taskId = String(body.taskId ?? "task-telemetry-01");
      const evt = this.cloudEventsBus.emitEvent({
        type: String(body.type ?? "io.ansts.task.completed"),
        subject: taskId,
        tenantId,
        llmCostUsd: Number(body.llmCostUsd ?? 0.15),
        sandboxCpuCoreSeconds: Number(body.sandboxCpuCoreSeconds ?? 12.5),
        data: body.data ?? {}
      });

      let webhookDelivery = null;
      if (body.webhookUrl) {
        const msgId = `outbox-${evt.id}`;
        const msg = this.outboxDispatcher.enqueueOutbox(msgId, taskId, String(body.webhookUrl), {
          cloudEvent: evt
        });
        const sig = this.outboxDispatcher.signPayload(JSON.stringify(msg.payload));
        msg.signatureHeader = sig;
        let delivered = false;
        try {
          const whResp = await fetch(String(body.webhookUrl), {
            method: "POST",
            headers: {
              "content-type": "application/json",
              "x-ansts-signature": sig
            },
            body: JSON.stringify(msg.payload)
          });
          delivered = whResp.ok;
          msg.delivered = delivered;
        } catch {
          delivered = false;
        }
        webhookDelivery = { messageId: msgId, delivered, signatureHeader: sig };
      }

      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        cloudEvent: evt,
        tenantFinOpsReport: this.cloudEventsBus.getTenantReport(tenantId),
        webhookDelivery
      }));

      return;
    }

    // [M39-2] SSE Stream with `Last-Event-ID` Historical Replay (`GET /v1/events/stream`)
    if (method === "GET" && pathname === "/v1/events/stream") {
      const taskId = urlObj.searchParams.get("taskId") ?? "task-harness-01";
      const lastEventId =
      req.headers["last-event-id"] ??
      urlObj.searchParams.get("lastEventId") ??
      undefined;
      const subId = `sub-${Date.now()}`;
      const sub = this.sseGateway.subscribe(taskId, subId, lastEventId);
      const chunks = [];
      sub.stream.on("data", (c) => chunks.push(Buffer.from(c)));
      this.sseGateway.unsubscribe(taskId, subId);
      const ssePayload = Buffer.concat(chunks).toString("utf-8");
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache"
      });
      res.end(ssePayload);
      return;
    }

    if (method === "POST" && pathname === "/shutdown") {
      this.draining = true;
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "draining_started", handoff: urlObj.searchParams.get("handoff") ?? "release" }));
      return;
    }

    // [M41 & M42] Platform Management Plane BFF Endpoints (`/v1beta1/management/*`) — Backed by real PlatformManagementPlaneEngine + Cross-Gateway S3 Sync
    if (pathname.startsWith("/v1beta1/management/")) {
      await this.managementPlane.pullManagementStateFromS3();
    }

    if (method === "GET" && pathname === "/v1beta1/management/state") {
      if (this.managementPlane.getScenario() === "offline" && urlObj.searchParams.get("recover") !== "1") {
        res.writeHead(503, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "MANAGEMENT_API_UNAVAILABLE",
          scenario: "offline",
          message: "Management BFF endpoint temporarily unavailable (503)"
        }));

        return;
      }
      if (urlObj.searchParams.get("recover") === "1") {
        this.managementPlane.setScenario("normal", "chen");
        await this.managementPlane.pushManagementStateToS3();
      }
      const statePayload = await this.managementPlane.buildFullManagementState({
        region: urlObj.searchParams.get("region") ?? undefined,
        tenant: urlObj.searchParams.get("tenant") ?? undefined,
        timeRange: urlObj.searchParams.get("timeRange") ?? undefined
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(statePayload));
      return;
    }

    if (method === "GET" && pathname.startsWith("/v1beta1/management/tasks/") && pathname.endsWith("/turns")) {
      const taskId = decodeURIComponent(
      pathname.slice("/v1beta1/management/tasks/".length, pathname.length - "/turns".length));

      const detail = this.managementPlane.getTaskTurnsDetail(taskId);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(detail));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/scenario") {
      const body = await this.readJsonBody(req);
      if (body.action === "reset") {
        this.managementPlane.resetToBaseline();
        await this.managementPlane.pushManagementStateToS3();
        const resetState = await this.managementPlane.buildFullManagementState();
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ reset: true, state: resetState }));
        return;
      }
      const nextScenario = body.scenario ?? "normal";
      const actor = String(body.actor ?? "chen");
      this.managementPlane.setScenario(nextScenario, actor);
      await this.managementPlane.pushManagementStateToS3();
      const statePayload = await this.managementPlane.buildFullManagementState();
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ scenario: nextScenario, state: statePayload }));
      return;
    }

    // [M76] Console RBAC JWT Token Issuance & Audit Chain Verification (`POST /v1beta1/management/auth/token`)
    if (method === "POST" && pathname === "/v1beta1/management/auth/token") {
      const body = await this.readJsonBody(req);
      const principalId = typeof body.principalId === "string" ? body.principalId.trim() : "chen";
      const token = this.managementPlane.rbacGuard.issuePrincipalToken(principalId);
      const verified = this.managementPlane.rbacGuard.verifyAuthorizationHeader(`Bearer ${token}`);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        principalId,
        token,
        claims: verified.claims,
        auditChainIntegrity: this.managementPlane.rbacGuard.verifyAuditChainIntegrity()
      }));

      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/tenants/config") {
      const body = await this.readJsonBody(req);
      const auth = this.resolveAuthPrincipal(req, body, true);
      if (!auth.ok) {
        res.writeHead(auth.status, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: auth.error, message: auth.message }));
        return;
      }
      if (typeof body.tenantId !== "string" || body.tenantId.trim().length === 0 || body.expectedVersion === undefined) {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "MISSING_REQUIRED_PARAMETERS",
          message: "Required parameters: tenantId and expectedVersion"
        }));

        return;
      }
      const result = this.managementPlane.updateTenantConfig({
        tenantId: body.tenantId.trim(),
        maxSlots: Number(body.maxSlots ?? 160),
        dailyBudgetUsd: Number(body.dailyBudgetUsd ?? 1200),
        expectedVersion: Number(body.expectedVersion),
        actor: auth.actor,
        role: auth.role
      });
      if (result.status === 200) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/nodes/batch") {
      const body = await this.readJsonBody(req);
      const auth = this.resolveAuthPrincipal(req, body, true);
      if (!auth.ok) {
        res.writeHead(auth.status, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: auth.error, message: auth.message }));
        return;
      }
      if (
      typeof body.operation !== "string" ||
      !["drain", "cordon", "uncordon", "prewarm"].includes(body.operation) ||
      !Array.isArray(body.nodeIds) ||
      body.nodeIds.length === 0)
      {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "MISSING_REQUIRED_PARAMETERS",
          message: "Required parameters: operation ('drain'|'cordon'|'uncordon'|'prewarm') and non-empty nodeIds array"
        }));

        return;
      }
      const result = this.managementPlane.executeBatchNodeOperation({
        operation: body.operation,
        nodeIds: body.nodeIds.map((id) => String(id).trim()),
        forceMigrateLeases: Boolean(body.forceMigrateLeases ?? false),
        actor: auth.actor,
        role: auth.role
      });
      await this.managementPlane.pushManagementStateToS3();
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/tasks/cancel") {
      const body = await this.readJsonBody(req);
      const auth = this.resolveAuthPrincipal(req, body, true);
      if (!auth.ok) {
        res.writeHead(auth.status, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: auth.error, message: auth.message }));
        return;
      }
      if (
      typeof body.taskId !== "string" ||
      body.taskId.trim().length === 0 ||
      typeof body.mode !== "string" ||
      !["graceful", "force_fence"].includes(body.mode))
      {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "MISSING_REQUIRED_PARAMETERS",
          message: "Required parameters: non-empty taskId and mode ('graceful' | 'force_fence')"
        }));

        return;
      }
      const result = this.managementPlane.cancelOrFenceTask({
        taskId: body.taskId.trim(),
        mode: body.mode,
        actor: auth.actor,
        role: auth.role
      });
      await this.managementPlane.pushManagementStateToS3();
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/tasks/time-travel") {
      const body = await this.readJsonBody(req);
      const auth = this.resolveAuthPrincipal(req, body, true);
      if (!auth.ok) {
        res.writeHead(auth.status, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: auth.error, message: auth.message }));
        return;
      }
      if (typeof body.taskId !== "string" || body.taskId.trim().length === 0 || body.targetTurnIndex === undefined) {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "MISSING_REQUIRED_PARAMETERS",
          message: "Required parameters: taskId and targetTurnIndex"
        }));

        return;
      }
      const result = this.managementPlane.inspectAndForkTaskTurn({
        taskId: body.taskId.trim(),
        targetTurnIndex: Number(body.targetTurnIndex),
        actor: auth.actor,
        role: auth.role
      });
      if (result.status === 200) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/approvals/create") {
      const body = await this.readJsonBody(req);
      const auth = this.resolveAuthPrincipal(req, body, true);
      if (!auth.ok) {
        res.writeHead(auth.status, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: auth.error, message: auth.message }));
        return;
      }
      if (
      typeof body.type !== "string" ||
      body.type.trim().length === 0 ||
      typeof body.target !== "string" ||
      body.target.trim().length === 0)
      {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "MISSING_REQUIRED_PARAMETERS",
          message: "Required parameters: non-empty type and target"
        }));

        return;
      }
      const result = this.managementPlane.createApprovalTicket({
        type: body.type.trim(),
        target: body.target.trim(),
        region: String(body.region ?? "us-west-2"),
        tenant: String(body.tenant ?? "Northstar"),
        initiator: auth.actor,
        role: auth.role,
        risk: body.risk ? String(body.risk) : undefined
      });
      if (result.status === 201) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/approvals/decide") {
      const body = await this.readJsonBody(req);
      const auth = this.resolveAuthPrincipal(req, body, true);
      if (!auth.ok) {
        res.writeHead(auth.status, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: auth.error, message: auth.message }));
        return;
      }
      if (
      typeof body.approvalId !== "string" ||
      body.approvalId.trim().length === 0 ||
      typeof body.decision !== "string" ||
      !["approve", "reject"].includes(body.decision))
      {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "MISSING_REQUIRED_PARAMETERS",
          message: "Required parameters: approvalId and decision ('approve' | 'reject')"
        }));

        return;
      }
      const result = this.managementPlane.decideApprovalTicket({
        approvalId: body.approvalId.trim(),
        initiator: body.initiator ? String(body.initiator) : undefined,
        approver: auth.actor,
        role: auth.role,
        decision: body.decision
      });
      if (result.status === 200) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/nodes/scale") {
      const body = await this.readJsonBody(req);
      const auth = this.resolveAuthPrincipal(req, body, true);
      if (!auth.ok) {
        res.writeHead(auth.status, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: auth.error, message: auth.message }));
        return;
      }
      if (
      typeof body.action !== "string" ||
      !["scale_out", "decommission"].includes(body.action) ||
      typeof body.nodeId !== "string" ||
      body.nodeId.trim().length === 0)
      {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "MISSING_REQUIRED_PARAMETERS",
          message: "Required parameters: action ('scale_out' | 'decommission') and non-empty nodeId"
        }));

        return;
      }
      const result = this.managementPlane.scaleOutOrDecommissionNode({
        action: body.action,
        nodeId: body.nodeId.trim(),
        region: String(body.region ?? "us-west-2"),
        role: body.role === "brain" ? "brain" : "body",
        ipAddress: body.ipAddress ? String(body.ipAddress) : undefined,
        actor: auth.actor,
        actorRole: auth.role
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/tenants/finops-escrow") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.mutateTenantFinOpsOrEscrow({
        action: body.action ?? "record_spend",
        tenantId: String(body.tenantId ?? "Northstar"),
        spendDeltaUsd: body.spendDeltaUsd !== undefined ? Number(body.spendDeltaUsd) : undefined,
        escrowDeltaSlots: body.escrowDeltaSlots !== undefined ? Number(body.escrowDeltaSlots) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/tasks/step") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeTaskTurnStep({
        taskId: String(body.taskId ?? "task-ns-8841"),
        toolName: body.toolName ? String(body.toolName) : undefined,
        shellCommand: body.shellCommand ? String(body.shellCommand) : undefined,
        executeRealBash: Boolean(body.executeRealBash ?? false),
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/canary") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.evaluateCanaryOrGeoRegion({
        action: body.action ?? "promote_step",
        regionId: body.regionId ? String(body.regionId) : undefined,
        capacityAvailable: body.capacityAvailable !== undefined ? Boolean(body.capacityAvailable) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "GET" && pathname === "/v1beta1/management/audits") {
      const result = this.managementPlane.queryAuditLogs({
        q: urlObj.searchParams.get("q") ?? undefined,
        actor: urlObj.searchParams.get("actor") ?? undefined,
        role: urlObj.searchParams.get("role") ?? undefined
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(result));
      return;
    }

    // [M70-1] DLQ Dead-Letter Task & Outbox Webhook Replay Workbench (`POST /v1beta1/management/ops/dlq-replay`)
    if (method === "POST" && pathname === "/v1beta1/management/ops/dlq-replay") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeDlqAndOutboxReplayWorkbench({
        action: body.action ?? "inspect_dlq",




        taskId: body.taskId ? String(body.taskId) : undefined,
        messageId: body.messageId ? String(body.messageId) : undefined,
        newTargetUrl: body.newTargetUrl ? String(body.newTargetUrl) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M70-2] Task Breakpoint Crash-Recovery & Duplicate Coalescing Inspector (`POST /v1beta1/management/tasks/breakpoint-recover` & `/coalesce-inspect`)
    if (
    method === "POST" && (
    pathname === "/v1beta1/management/tasks/breakpoint-recover" ||
    pathname === "/v1beta1/management/tasks/coalesce-inspect"))
    {
      const body = await this.readJsonBody(req);
      const defaultAction =
      pathname === "/v1beta1/management/tasks/coalesce-inspect" ?
      "coalesce_duplicate" :
      "recover_breakpoint";
      const result = this.managementPlane.executeTaskBreakpointRecoveryAndCoalesce({
        action: body.action ?? defaultAction,
        taskId: body.taskId ? String(body.taskId) : undefined,
        tenantId: body.tenantId ? String(body.tenantId) : undefined,
        prompt: body.prompt ? String(body.prompt) : undefined,
        priority: body.priority !== undefined ? Number(body.priority) : undefined,
        idempotencyKey: body.idempotencyKey ? String(body.idempotencyKey) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M70-3] M57-M69 One-Click Full-Link Health Scorecard (`POST /v1beta1/management/ops/health-scorecard`)
    if (method === "POST" && pathname === "/v1beta1/management/ops/health-scorecard") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeFullLinkHealthScorecard({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M75] Phase 2 eBPF Network Tracing, Byzantine Slashing & Erasure Coding Suite (`POST /v1beta1/management/ops/phase2-network-slashing-erasure`)
    if (method === "POST" && pathname === "/v1beta1/management/ops/phase2-network-slashing-erasure") {
      const body = await this.readJsonBody(req);
      const out = this.managementPlane.executePhase2NetworkSlashingErasureSuite({
        action: body.action,





        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(out.status, { "content-type": "application/json" });
      res.end(JSON.stringify(out.body));
      return;
    }

    // [M74] Multi-Agent Swarm Governance & Causal Consistency Suite (`POST /v1beta1/management/ops/phase1-swarm-governance`)
    if (method === "POST" && pathname === "/v1beta1/management/ops/phase1-swarm-governance") {
      const body = await this.readJsonBody(req);
      const out = this.managementPlane.executePhase1SwarmGovernanceSuite({
        action: body.action,





        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(out.status, { "content-type": "application/json" });
      res.end(JSON.stringify(out.body));
      return;
    }

    // [M70-4] Day-2 Governance Suite (`POST /v1beta1/management/ops/day2-governance`)
    if (method === "POST" && pathname === "/v1beta1/management/ops/phase3-memory-spot-market") {
      const body = await this.readJsonBody(req);
      const out = this.managementPlane.executePhase3MemoryAndSpotMarketSuite({
        action: body.action,




        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(out.status, { "content-type": "application/json" });
      res.end(JSON.stringify(out.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/ops/phase2-memory-guardrail-replay") {
      const body = await this.readJsonBody(req);
      const out = this.managementPlane.executePhase2MemoryGuardrailReplaySuite({
        action: body.action,





        queryText: typeof body.queryText === "string" ? body.queryText : undefined,
        repoSignature: typeof body.repoSignature === "string" ? body.repoSignature : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(out.status, { "content-type": "application/json" });
      res.end(JSON.stringify(out.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/ops/phase1-cognitive-acceleration") {
      const body = await this.readJsonBody(req);
      const out = this.managementPlane.executePhase1CognitiveAccelerationSuite({
        action: body.action,





        parentTaskId: typeof body.parentTaskId === "string" ? body.parentTaskId : undefined,
        sourceBranch: typeof body.sourceBranch === "string" ? body.sourceBranch : undefined,
        promptText: typeof body.promptText === "string" ? body.promptText : undefined,
        confidenceThreshold:
        typeof body.confidenceThreshold === "number" ? body.confidenceThreshold : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(out.status, { "content-type": "application/json" });
      res.end(JSON.stringify(out.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/ops/day2-governance") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeDay2GovernanceOperation({
        action: body.action ?? "security_fence_alerts",




        tenantId: body.tenantId ? String(body.tenantId) : undefined,
        candidateOverrides:
        body.candidateOverrides && typeof body.candidateOverrides === "object" ?
        body.candidateOverrides :
        undefined,
        newWebhookSecret: body.newWebhookSecret ? String(body.newWebhookSecret) : undefined,
        graceWindowSec: body.graceWindowSec !== undefined ? Number(body.graceWindowSec) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M69] Final Deep Edge-Case Sweep & Zero-Residual-Defect Closure Audit (`POST /v1beta1/management/system/final-closure-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/final-closure-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeFinalZeroDefectClosureAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M68] Deep Potential Risk Identification & Elimination Phase III Audit (`POST /v1beta1/management/system/deep-risk-phase3-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/deep-risk-phase3-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeDeepPotentialRiskPhase3Audit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M67] Deep Potential Risk Identification & Elimination Phase II Audit (`POST /v1beta1/management/system/deep-risk-phase2-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/deep-risk-phase2-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeDeepPotentialRiskPhase2Audit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M66] Deep Potential Risk Identification & Elimination Audit (`POST /v1beta1/management/system/potential-risks-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/potential-risks-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executePotentialRisksEliminationAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M65] Deep Implementation Consistency Phase II Audit (`POST /v1beta1/management/system/deep-consistency-phase2-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/deep-consistency-phase2-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeDeepConsistencyPhase2Audit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M64] End-to-End Implementation Consistency & Authentic Feature Wiring Audit (`POST /v1beta1/management/system/implementation-consistency-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/implementation-consistency-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeImplementationConsistencyAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M63] Deep DFX Hardening Phase II Audit (`POST /v1beta1/management/system/dfx-deep-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/dfx-deep-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeDfxDeepModuleHardeningAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M62] DFX Full-Spectrum Resiliency, Diagnosability, Testability & Operability Hardening Audit (`POST /v1beta1/management/system/dfx-hardening-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/dfx-hardening-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeDfxHardeningAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M61] Full-Subsystem Persistence, Idempotent CAS/Push Retry, Breakpoint State Rollback & Coalescing Duplicate Arbitration Audit (`POST /v1beta1/management/system/subsystem-persistence-idempotency-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/subsystem-persistence-idempotency-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeSubsystemPersistenceIdempotencyAndCoalesceAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M60] End-to-End Full-Link Persistence, Tenant-Scoped & Deep-Canonical Idempotency, Deterministic Breakpoint Crash-Recovery & Coalescing Duplicate Inspection Audit (`POST /v1beta1/management/system/persistence-idempotency-recovery-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/persistence-idempotency-recovery-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeFullLinkPersistenceIdempotencyBreakpointAndCoalesceAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M59] SQLite Composite-PK/DELETE-Flush/LIMIT Engine Hardening, KV TTL Expiration, SSE Malformed-Frame/Clock-Skew Resilience, UFFD Page Cache Invalidation, Vsock Scheme/IMDS Guard & LTX/Facet Metadata Hygiene Audit (`POST /v1beta1/management/system/sql-kv-sse-uffd-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/sql-kv-sse-uffd-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeSqlKvSseUffdAndLtxAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M58] Container Fence RFC1918/CGNAT/IPv6 & Start-Cap Guard, Recursive OverlayFS & eBPF LSM Path Hardening, SPIFFE/SLSA Tamper Proofing, Canary Post-Rollback Lock & PSI Victim Filtering, Telemetry/EWMA NaN Guard & Federation RTT Audit (`POST /v1beta1/management/system/container-security-telemetry-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/container-security-telemetry-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeContainerSecurityAndTelemetryAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M57] Runtime Edge, Router Cooldown/Quota, Leaf-First CAS GC, Role-Isolated Scale/Drain, NodeDaemon Turn Monotonicity, Validator NaN/DRF Guard & Vsock Framing Audit (`POST /v1beta1/management/system/runtime-edge-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/runtime-edge-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeRuntimeEdgeAndGatewayAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M56] Unified Cold-Start Lease/Fencing Rehydration, LayerFS 3-Way Merge Deletion & Base-Object Pinning, P0 Emergency Ingress Headroom, CelldAdmin Epoch Guard & Facet/Isolate Memory Leak Elimination Audit (`POST /v1beta1/management/system/kernel-storage-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/kernel-storage-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeKernelStorageMergeAndBackpressureAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M55] Deep Persistence, Shard Last-Brain Survival Guard, LayerFS Rule D9 Pull-State Sync, Time-Travel CRC32 Verification, Velocity Breaker Recovery & RPO=0 Atomic Rollback Audit (`POST /v1beta1/management/system/storage-ha-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/storage-ha-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeStorageShardSyncAndTransactionAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M54] Deep Runtime Resilience, Cold-Start Fencing, Half-Open Stampede Guard, Approval TTL, Hot-Cell Leak Reclamation, Outbox/SSE & A2A Anti-Starvation Audit (`POST /v1beta1/management/system/deep-runtime-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/deep-runtime-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeDeepRuntimeResilienceAndAntiStarvationAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M53] Deep Cross-Layer Risk Elimination Audit (`POST /v1beta1/management/system/risk-elimination-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/risk-elimination-audit") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeDeepCrossLayerRiskEliminationAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M52] System-Wide Stale, Deadlock, Starvation & State-Consistency Rectification Audit (`POST /v1beta1/management/system/deep-rectification-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/system/deep-rectification-audit") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeSystemWideRectificationAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M51] Scheduler Kernel Anti-Stale, Deadlock-Free & Starvation-Proof Audit (`POST /v1beta1/management/scheduler/kernel-audit`)
    if (method === "POST" && pathname === "/v1beta1/management/scheduler/kernel-audit") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeSchedulerKernelAntiStaleDeadlockStarvationAudit({
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M50-1] Architecture Design Blueprint & Contract Verification (`GET /v1beta1/management/design/blueprint`)
    if (method === "GET" && pathname === "/v1beta1/management/design/blueprint") {
      const result = this.managementPlane.inspectArchitectureDesignBlueprint();
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M50-2] Versioned Dynamic Configuration Center (`GET /v1beta1/management/config/center` & `POST /v1beta1/management/config/center`)
    if (pathname === "/v1beta1/management/config/center" && (method === "GET" || method === "POST")) {
      if (method === "GET") {
        const result = this.managementPlane.executeConfigurationCenterGovernance({
          action: "inspect_active_config",
          actor: "chen",
          role: "SRE_ADMIN"
        });
        res.writeHead(result.status, { "content-type": "application/json" });
        res.end(JSON.stringify(result.body));
        return;
      }
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeConfigurationCenterGovernance({
        action: body.action ?? "validate_and_apply_config",



        expectedRevision: body.expectedRevision !== undefined ? Number(body.expectedRevision) : undefined,
        targetRevision: body.targetRevision !== undefined ? Number(body.targetRevision) : undefined,
        runtimeOverrides: body.runtimeOverrides ? body.runtimeOverrides : undefined,
        sampleTaskSpec: body.sampleTaskSpec ? body.sampleTaskSpec : undefined,
        note: body.note ? String(body.note) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M50-3] Deep Runtime Engine Optimization (`POST /v1beta1/management/runtime/celld-engine`)
    if (method === "POST" && pathname === "/v1beta1/management/runtime/celld-engine") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeCelldAndLayerFsRuntimeEngine({
        action: body.action ?? "scheduler_cell_rpc_and_alarm",



        taskId: body.taskId ? String(body.taskId) : undefined,
        concurrentRequests: body.concurrentRequests !== undefined ? Number(body.concurrentRequests) : undefined,
        peerFsyncLatencyMs: body.peerFsyncLatencyMs !== undefined ? Number(body.peerFsyncLatencyMs) : undefined,
        basedOnGeneration: body.basedOnGeneration !== undefined ? Number(body.basedOnGeneration) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M50-4] Closed-Loop SRE Observability, SLO Burn-Rate Alerting & Self-Healing Runbooks (`POST /v1beta1/management/ops/observability`)
    if (method === "POST" && pathname === "/v1beta1/management/ops/observability") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeObservabilityAndSreOps({
        action: body.action ?? "probe_slo_traces_and_metrics",


        simulatedErrorRequests: body.simulatedErrorRequests !== undefined ? Number(body.simulatedErrorRequests) : undefined,
        simulatedP99LatencyMs: body.simulatedP99LatencyMs !== undefined ? Number(body.simulatedP99LatencyMs) : undefined,
        runbookId: body.runbookId ? String(body.runbookId) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M49-1] Cognitive Router Full-Chain Inference Pipeline, Circuit Breaker & Token Bucket (`POST /v1beta1/management/router/inference-pipeline`)
    if (method === "POST" && pathname === "/v1beta1/management/router/inference-pipeline") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeRouterInferencePipeline({
        action: body.action ?? "execute_sse_inference",



        tenantId: body.tenantId ? String(body.tenantId) : undefined,
        taskId: body.taskId ? String(body.taskId) : undefined,
        endpointId: body.endpointId ? String(body.endpointId) : undefined,
        promptTokens: body.promptTokens !== undefined ? Number(body.promptTokens) : undefined,
        maxOutputTokens: body.maxOutputTokens !== undefined ? Number(body.maxOutputTokens) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M49-2] Live Unix Socket Vsock Daemon, eBPF LSM, Ed25519 SLSA Provenance & UFFD Page Restorer (`POST /v1beta1/management/security/kernel-attestation`)
    if (method === "POST" && pathname === "/v1beta1/management/security/kernel-attestation") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeKernelSecurityAndAttestation({
        action: body.action ?? "vsock_unix_daemon_probe",



        taskId: body.taskId ? String(body.taskId) : undefined,
        branchName: body.branchName ? String(body.branchName) : undefined,
        allowedHostname: body.allowedHostname ? String(body.allowedHostname) : undefined,
        blockedHostname: body.blockedHostname ? String(body.blockedHostname) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M49-3] Automated Split-Brain / Network Partition Chaos Drill, Host Preflight & Topology CAS (`POST /v1beta1/management/chaos/drill`)
    if (method === "POST" && pathname === "/v1beta1/management/chaos/drill") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeChaosAndTopologyCasDrill({
        action: body.action ?? "run_partition_and_split_brain_drill",


        targetHost: body.targetHost ? String(body.targetHost) : undefined,
        expectedGeneration: body.expectedGeneration !== undefined ? Number(body.expectedGeneration) : undefined,
        nextMembers: Array.isArray(body.nextMembers) ? body.nextMembers : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M49-4] Dual-Homed Reverse Tunnel Failover, Multi-Region Federation, Harness Auto-Heal & Velocity/Staging Gate (`POST /v1beta1/management/federation/tunnel-harness`)
    if (method === "POST" && pathname === "/v1beta1/management/federation/tunnel-harness") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeFederationTunnelAndHarnessGovernance({
        action: body.action ?? "tunnel_command_and_failover",



        hostId: body.hostId ? String(body.hostId) : undefined,
        sovereigntyZone: body.sovereigntyZone ? String(body.sovereigntyZone) : undefined,
        taskId: body.taskId ? String(body.taskId) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M48-1] Unified 8-Table Full-Cluster State Snapshot & Cold-Start Rehydration (`POST /v1beta1/management/persistence/snapshot`)
    if (method === "POST" && pathname === "/v1beta1/management/persistence/snapshot") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeUnifiedPersistenceSnapshot({
        action: body.action ?? "snapshot_all",
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M48-2] Native denoland/celld ctx.container + celld-fence & DurableWorkflowContext (`POST /v1beta1/management/celld/container-workflow`)
    if (method === "POST" && pathname === "/v1beta1/management/celld/container-workflow") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeCelldContainerAndWorkflow({
        action: body.action ?? "start_and_probe_container",
        cellId: body.cellId ? String(body.cellId) : undefined,
        runtime: body.runtime ? String(body.runtime) : undefined,
        workflowId: body.workflowId ? String(body.workflowId) : undefined,
        stepName: body.stepName ? String(body.stepName) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M48-3] Speculative Deterministic Multi-Branch CoW Execution (`POST /v1beta1/management/layerfs/speculative`)
    if (method === "POST" && pathname === "/v1beta1/management/layerfs/speculative") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeSpeculativeDeterministicBranches({
        taskId: body.taskId ? String(body.taskId) : undefined,
        sourceBranch: body.sourceBranch ? String(body.sourceBranch) : undefined,
        mergeWinningBranch: body.mergeWinningBranch !== undefined ? Boolean(body.mergeWinningBranch) : true,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M48-4] Closed-Loop Linux PSI Memory Guard, Bidirectional Backpressure & Dynamic Queue/Container Auto-Scaler (`POST /v1beta1/management/cluster/autoscale-psi`)
    if (method === "POST" && pathname === "/v1beta1/management/cluster/autoscale-psi") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeAutoscaleAndPsiLoop({
        action: body.action ?? "probe_and_autoscale",
        simulatedPsiFullPct: body.simulatedPsiFullPct !== undefined ? Number(body.simulatedPsiFullPct) : undefined,
        incomingWriteRps: body.incomingWriteRps !== undefined ? Number(body.incomingWriteRps) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M47-3] Physical Linux Cgroup v2, OverlayFS Upperdir, Vsock RPC, Ed25519 SPIFFE/TEE & CelldAdmin Cell Migration (`POST /v1beta1/management/runtime/sandbox`)
    if (method === "POST" && pathname === "/v1beta1/management/runtime/sandbox") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.executeRuntimeSandboxLifecycle({
        action: body.action ?? "allocate_and_mount",




        taskId: body.taskId ? String(body.taskId) : undefined,
        command: body.command ? String(body.command) : undefined,
        filePath: body.filePath ? String(body.filePath) : undefined,
        fileContent: body.fileContent ? String(body.fileContent) : undefined,
        targetNodeId: body.targetNodeId ? String(body.targetNodeId) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M46-3] Dynamic MCP Tool Registry & Saga LIFO Rollback (`POST /v1beta1/management/mcp`)
    if (method === "POST" && pathname === "/v1beta1/management/mcp") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeMcpAndSagaGovernance({
        action: body.action ?? "invoke_mcp_tool",
        serverName: body.serverName ? String(body.serverName) : undefined,
        toolName: body.toolName ? String(body.toolName) : undefined,
        description: body.description ? String(body.description) : undefined,
        taskId: body.taskId ? String(body.taskId) : undefined,
        turnIndex: body.turnIndex !== undefined ? Number(body.turnIndex) : undefined,
        toolArgs: body.toolArgs ? body.toolArgs : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M45-1] Multi-Agent A2A Governance Engine (`POST /v1beta1/management/a2a`)
    if (method === "POST" && pathname === "/v1beta1/management/a2a") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeA2aGovernanceAction({
        action: body.action ?? "put_blackboard_cas",





        key: body.key ? String(body.key) : undefined,
        value: body.value,
        expectedVersion: body.expectedVersion !== undefined ? Number(body.expectedVersion) : undefined,
        authorAgentId: body.authorAgentId ? String(body.authorAgentId) : undefined,
        patches: Array.isArray(body.patches) ? body.patches : undefined,
        victimAgentId: body.victimAgentId ? String(body.victimAgentId) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M45-2] Live Heavy Task Cross-Node CRIU/Upperdir Migration (`POST /v1beta1/management/tasks/migrate`)
    if (method === "POST" && pathname === "/v1beta1/management/tasks/migrate") {
      const body = await this.readJsonBody(req);
      const result = await this.managementPlane.migrateTaskAcrossBodyNodes({
        taskId: String(body.taskId ?? "task-ns-8841"),
        targetNodeId: body.targetNodeId ? String(body.targetNodeId) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M45-2] LayerFS File Line-Level Blame Inspector (`GET /v1beta1/management/layerfs/blame`)
    if (method === "GET" && pathname === "/v1beta1/management/layerfs/blame") {
      const result = this.managementPlane.inspectLayerFsFileBlame({
        taskId: urlObj.searchParams.get("taskId") ?? undefined,
        branchOrCommit: urlObj.searchParams.get("branch") ?? undefined,
        filePath: urlObj.searchParams.get("filePath") ?? undefined
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M45-3] Distributed CAS Mark-and-Sweep GC & celld Epoch GC Audit (`POST /v1beta1/management/storage/gc-sweep`)
    if (method === "POST" && pathname === "/v1beta1/management/storage/gc-sweep") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.executeStorageGcAndEpochAudit({
        action: body.action ?? "sweep_cas",
        graceSecs: body.graceSecs !== undefined ? Number(body.graceSecs) : undefined,
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      if (result.status < 400) {
        await this.managementPlane.pushManagementStateToS3();
      }
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M45-4] Rolling-Upgrade Fleet Diagnostics Probe (`POST /v1beta1/management/diagnostics/probe`)
    if (method === "POST" && pathname === "/v1beta1/management/diagnostics/probe") {
      const body = await this.readJsonBody(req);
      const result = this.managementPlane.runFleetRollingUpgradeDiagnostics({
        expectedProtocol: body.expectedProtocol ? String(body.expectedProtocol) : undefined,
        enqueueWebhookTest: Boolean(body.enqueueWebhookTest ?? true),
        actor: String(body.actor ?? "chen"),
        role: String(body.role ?? "SRE_ADMIN")
      });
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/management/capacity/action") {
      const body = await this.readJsonBody(req);
      const auth = this.resolveAuthPrincipal(req, body, true);
      if (!auth.ok) {
        res.writeHead(auth.status, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: auth.error, message: auth.message }));
        return;
      }
      if (
      typeof body.action !== "string" ||
      !["mitigate_capacity", "rebalance_verification", "apply_budget_fallback"].includes(body.action))
      {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          error: "MISSING_REQUIRED_PARAMETERS",
          message:
          "Required parameter: action ('mitigate_capacity' | 'rebalance_verification' | 'apply_budget_fallback')"
        }));

        return;
      }
      const result = this.managementPlane.executeCapacityOrRebalanceAction({
        action: body.action,
        actor: auth.actor,
        role: auth.role
      });
      await this.managementPlane.pushManagementStateToS3();
      res.writeHead(result.status, { "content-type": "application/json" });
      res.end(JSON.stringify(result.body));
      return;
    }

    // [M43-1] denoland/celld v0.6.3 Dynamic Worker V8 Isolate Sandbox + Heap/SQL Budget Guard (`POST /v1beta1/celld/isolate`)
    if (method === "POST" && pathname === "/v1beta1/celld/isolate") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? "task-isolate-01");
      const turnIndex = Number(body.turnIndex ?? 1);
      const jsSourceCode = String(
      body.jsSourceCode ??
      "globalThis.__result = { computed: (inputPayload.a || 21) * 2, isolateEnv: env.TASK_ID };");

      const allowInternetEgress = Boolean(body.allowInternetEgress ?? false);
      const inputPayload = body.inputPayload ?? { a: 21 };

      try {
        const isolateRes = await this.dynamicWorkerLoader.executeUntrustedCodeInIsolate({
          taskId,
          turnIndex,
          jsSourceCode,
          allowInternetEgress,
          inputPayload
        });
        const heapStatus = this.heapBudgetGuard.evaluateHeapPressure(
        body.simulatedHeapMb !== undefined ? Number(body.simulatedHeapMb) : undefined);

        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ nodeId: this.nodeId, ...isolateRes, heapStatus }));
      } catch (err) {
        const msg = err.message;
        if (msg.includes("DynamicWorker Egress Blocked")) {
          res.writeHead(403, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "ISOLATE_EGRESS_BLOCKED", message: msg }));
          return;
        }
        throw err;
      }
      return;
    }

    // [M43-1] denoland/celld v0.6.3 Durable Object Sub-Actor Facets & 32-Shard KV Optimizer (`POST /v1beta1/celld/facets`)
    if (method === "POST" && pathname === "/v1beta1/celld/facets") {
      const body = await this.readJsonBody(req);
      const action = String(body.action ?? "spawnAndShardKv");
      const taskId = String(body.taskId ?? "task-facet-01");
      const turnIndex = Number(body.turnIndex ?? 1);
      const toolName = String(body.toolName ?? "sql_sandbox");

      if (action === "abortFacet") {
        const facetName = String(body.facetName ?? `facet-${taskId}-t${turnIndex}-${toolName}`);
        const aborted = this.facetSupervisor.abortFaultyFacet(facetName, new Error(String(body.reason ?? "tool_timeout")));
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ aborted, activeFacets: this.facetSupervisor.listFacets() }));
        return;
      }

      const { facetRecord, facetSql } = this.facetSupervisor.spawnToolFacet(
      taskId,
      turnIndex,
      toolName,
      () => new _env.InMemoryCellSqlStorage());

      const { elapsedMs } = this.heapBudgetGuard.execWithBudget(
      facetSql,
      "CREATE TABLE IF NOT EXISTS facet_scratch (k TEXT PRIMARY KEY, v TEXT)");

      const kvShard = await this.kvOptimizer.putSafe(
      "idem-ns",
      `${taskId}:t${turnIndex}`,
      JSON.stringify({ facetName: facetRecord.facetName, status: "OK" }));

      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        facetRecord,
        sqlBudgetElapsedMs: elapsedMs,
        kvShard,
        activeFacetsCount: this.facetSupervisor.listFacets().length
      }));

      return;
    }

    // [M43-1] denoland/celld v0.6.3 Hot-Cell 64-Fetch Overload Guard & 256-Producer Group-Commit (`POST /v1beta1/celld/overload-guard`)
    if (method === "POST" && pathname === "/v1beta1/celld/overload-guard") {
      const body = await this.readJsonBody(req);
      const cellScope = String(body.cellScope ?? "AnstsSchedulerCell:shard-01");
      const concurrentRequests = Number(body.concurrentRequests ?? 1);
      const producerCount = Number(body.producerCount ?? 32);

      let lastDecision = this.hotCellGuard.tryEnterCellFetch(cellScope);
      let entered = lastDecision.admitted ? 1 : 0;
      for (let i = 1; i < concurrentRequests; i++) {
        const d = this.hotCellGuard.tryEnterCellFetch(cellScope);
        lastDecision = d;
        if (d.admitted) entered += 1;
      }
      for (let i = 0; i < entered; i++) {
        this.hotCellGuard.leaveCellFetch(cellScope);
      }

      const queueBatch = this.hotCellGuard.tryAdmitQueueProducerBatch(producerCount);
      res.writeHead(lastDecision.statusCode, {
        "content-type": "application/json",
        ...lastDecision.headers
      });
      res.end(
      JSON.stringify({
        cellScope,
        cellAdmission: lastDecision,
        queueBatch
      }));

      return;
    }

    // [M43-1] denoland/celld v0.6.3 L0->L1 Additive LTX Compaction, 128-Page WAL Truncation & UFFD Restore (`POST /v1beta1/celld/ltx-compaction`)
    if (method === "POST" && pathname === "/v1beta1/celld/ltx-compaction") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? "task-ns-8841");
      this.managementPlane.ensureInitialized();
      let walEntries = this.walEngine.getTaskWalEntries(taskId);
      if (walEntries.length === 0) {
        this.walEngine.appendTurnDelta({
          taskId,
          turnIndex: 1,
          fencingToken: 1,
          changedFiles: ["src/a.ts", "src/b.ts"],
          sqliteWalPages: 64,
          highestSubmittedFencingToken: 1
        });
        walEntries = this.walEngine.getTaskWalEntries(taskId);
      }

      const compactedSnapshot = this.walCompactor.compactLayers(taskId, walEntries);
      const l1Compaction = this.walCompactor.executeCelldLtxL1Compaction({
        cellId: `cell-${taskId}`,
        l0ObjectCount: Number(body.l0ObjectCount ?? 320),
        txidDistance: Number(body.txidDistance ?? 512),
        totalL0Megabytes: Number(body.totalL0Megabytes ?? 48)
      });
      const walTruncation = this.walCompactor.evaluateWalTruncationAndPaging({
        dbSizeMb: Number(body.dbSizeMb ?? 3.2),
        walSizePages: Number(body.walSizePages ?? 160),
        ltxChainSizeMb: Number(body.ltxChainSizeMb ?? 300)
      });
      const uffdRestore = this.walCompactor.restoreViaUserfaultfd(taskId, 0.2);

      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        taskId,
        compactedSnapshot,
        l1Compaction,
        walTruncation,
        uffdRestore
      }));

      return;
    }

    // [M43-2] LayerFS 2.0 Remote Store Rule D9 Synchronizer (`POST /v1beta1/layerfs/store-sync`)
    if (method === "POST" && pathname === "/v1beta1/layerfs/store-sync") {
      const body = await this.readJsonBody(req);
      const action = String(body.action ?? "push");
      if (action === "pull") {
        const pulled = await this.layerfsStoreSync.pullStoreSnapshot();
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(pulled));
        return;
      }
      try {
        const pushed = await this.layerfsStoreSync.pushStoreSnapshot({
          machineId: String(body.machineId ?? this.nodeId),
          basedOnGeneration: Number(body.basedOnGeneration ?? 0),
          storeSqliteBytes: new TextEncoder().encode(JSON.stringify(this.layerfs.exportBranchBundle("main"))),
          mainHeadCommitId: this.layerfs.getBranchHeadCommit("main").commitId
        });
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(pushed));
      } catch (err) {
        if (err instanceof _layerfs_remote_sync.LayerFsStalePushRefusedError || err.name === "LayerFsStalePushRefusedError") {
          const e = err;
          res.writeHead(409, { "content-type": "application/json" });
          res.end(
          JSON.stringify({
            error: "LAYERFS_RULE_D9_STALE_PUSH_REFUSED",
            expectedRemoteGeneration: e.expectedRemoteGeneration,
            actualRemoteGeneration: e.actualRemoteGeneration,
            message: e.message
          }));

          return;
        }
        throw err;
      }
      return;
    }

    // [M43-3] Real-Time Management Audit SSE Stream with Last-Event-ID Replay (`GET /v1beta1/management/events/stream`)
    if (method === "GET" && pathname === "/v1beta1/management/events/stream") {
      this.managementPlane.ensureInitialized();
      const lastEventId =
      req.headers["last-event-id"] ??
      urlObj.searchParams.get("lastEventId") ??
      undefined;
      const subId = `mgmt-sub-${Date.now()}`;
      const sub = this.sseGateway.subscribe("mgmt-audit-stream", subId, lastEventId);
      const chunks = [];
      sub.stream.on("data", (c) => chunks.push(Buffer.from(c)));
      this.sseGateway.unsubscribe("mgmt-audit-stream", subId);
      const ssePayload = Buffer.concat(chunks).toString("utf-8");
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache"
      });
      res.end(ssePayload);
      return;
    }

    // Serve static console & management plane docs
    if (method === "GET") {
      const staticMap = {
        "/": { relPath: "index.html", contentType: "text/html; charset=utf-8" },
        "/index.html": { relPath: "index.html", contentType: "text/html; charset=utf-8" },
        "/ansts_console_live.html": { relPath: "index.html", contentType: "text/html; charset=utf-8" },
        "/ansts_console_management.html": { relPath: "ansts_console_management.html", contentType: "text/html; charset=utf-8" },
        "/management": { relPath: "ansts_console_management.html", contentType: "text/html; charset=utf-8" },
        "/console-management/prototype.css": { relPath: "console-management/prototype.css", contentType: "text/css; charset=utf-8" },
        "/console-management/prototype.js": { relPath: "console-management/prototype.js", contentType: "application/javascript; charset=utf-8" },
        "/console-management-design.md": { relPath: "console-management-design.md", contentType: "text/markdown; charset=utf-8" }
      };
      const matched = staticMap[pathname];
      if (matched) {
        const candidatePath = path.resolve(process.cwd(), "docs", matched.relPath);
        if (fs.existsSync(candidatePath)) {
          res.writeHead(200, { "content-type": matched.contentType });
          res.end(fs.readFileSync(candidatePath, "utf-8"));
          return;
        }
      }
    }

    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "NOT_FOUND", path: pathname }));
  }

  async stop() {
    this.draining = true;
    if (this.tickTimer) {
      clearInterval(this.tickTimer);
      this.tickTimer = null;
    }
    if (this.tunnelPollTimer) {
      clearInterval(this.tunnelPollTimer);
      this.tunnelPollTimer = null;
    }
    await this.dashboardServer.close();
    if (this.apiServer) {
      await new Promise((resolve) => {
        this.apiServer.closeAllConnections?.();
        this.apiServer.close(() => {
          this.apiServer = null;
          resolve();
        });
      });
    }
  }
}

// Execute server if invoked directly via `node dist/src/server_main.js` or `npm start`
exports.AnstsLocalAndComposeServer = AnstsLocalAndComposeServer;const isMainModule =
typeof process !== "undefined" &&
Array.isArray(process.argv) &&
process.argv[1] && (
process.argv[1].endsWith("server_main.js") || process.argv[1].endsWith("server_main.ts"));

if (isMainModule) {
  const server = new AnstsLocalAndComposeServer();
  server.start().then(({ apiPort, dashboardPort }) => {
    console.log(
    JSON.stringify({
      service: "ANSTS v2.5 Control Plane & LayerFS Runtime",
      nodeId: server.nodeId,
      nodeRole: server.nodeRole,
      status: "LISTENING",
      apiGatewayUrl: `http://127.0.0.1:${apiPort}`,
      healthzUrl: `http://127.0.0.1:${apiPort}/healthz`,
      metricsUrl: `http://127.0.0.1:${apiPort}/metrics`,
      dashboardUrl: `http://127.0.0.1:${dashboardPort}`
    }));

  });

  const shutdown = () => {
    server.stop().then(() => process.exit(0));
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}