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
var _saga_gateway = require("./saga_gateway.js");
var _ha_scheduler_shard_coordinator = require("./cloudflare/ha_scheduler_shard_coordinator.js");
var _scheduler_v25_persistence = require("./cloudflare/scheduler_v25_persistence.js");
var _edge_tunnel_ha_router = require("./runtime/edge_tunnel_ha_router.js");
var _env = require("./cloudflare/env.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M18 & v2.5 Unified Standalone, Docker Compose & Multi-Node Cluster Service Entrypoint (`src/server_main.ts`).
 * Supports single-stack mode (`:8080` API + `:8090` Dashboard) and `feat/celld-v25` Multi-Node Cluster mode
 * (`gateway`, `brain`, `body`) with cross-node HTTP RPC forwarding (`gateway -> brain -> body`),
 * Dual-Homed Reverse Tunnel registration (`EdgeTunnelHaRouter`), 256-slot `HaSchedulerShardCoordinator`,
 * `SchedulerV25SingleWritePathStore`, LayerFS Dual-Path Workspace Store, and Prometheus `/metrics`.
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
  nodeId;
  nodeRole;
  storageDir;
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
  dashboardServer;
  apiServer = null;
  tickTimer = null;
  host;
  tickIntervalMs;
  draining = false;

  constructor(options = {}) {
    this.host = options.host ?? process.env.ANSTS_HOST ?? "0.0.0.0";
    this.nodeId = options.nodeId ?? process.env.ANSTS_NODE_ID ?? "ansts-dev-node-01";
    this.nodeRole =
    (options.nodeRole ?? process.env.ANSTS_NODE_ROLE) || "gateway";
    this.storageDir = options.storageDir ?? process.env.ANSTS_STORAGE_DIR ?? `/tmp/ansts/${this.nodeId}`;
    this.peerBrainUrls = options.peerBrainUrls ?? parseJsonEnvMap(process.env.ANSTS_PEER_BRAIN_URLS);
    this.peerBodyUrls = options.peerBodyUrls ?? parseJsonEnvMap(process.env.ANSTS_PEER_BODY_URLS);

    this.tickIntervalMs = options.tickIntervalMs ?? Number(process.env.ANSTS_TICK_INTERVAL_MS ?? 1000);
    this.scheduler = new _scheduler.CoreScheduler5D(15.0);
    this.backpressure = new _backpressure.IngressBackpressureController();
    this.sseGateway = new _sse_stream.SseEventStreamGateway();
    this.metrics = new _otel_prometheus.OtelAndPrometheusExporter();
    this.layerfs = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
    this.fleetController = new _manual_scale_controller.ManualFleetScaleController();
    this.router = new _cognitive_router.CognitiveRouter();
    this.sagaGateway = new _saga_gateway.SagaToolGateway();
    this.v25Store = new _scheduler_v25_persistence.SchedulerV25SingleWritePathStore(new _env.InMemoryCellSqlStorage());
    this.tunnelRouter = new _edge_tunnel_ha_router.EdgeTunnelHaRouter();

    const brainNodes =
    Object.keys(this.peerBrainUrls).length > 0 ?
    Object.keys(this.peerBrainUrls) :
    ["brain-node-01", "brain-node-02", "brain-node-03"];
    this.shardCoordinator = new _ha_scheduler_shard_coordinator.HaSchedulerShardCoordinator(256, 8);
    for (const bId of brainNodes) {
      this.shardCoordinator.registerBrainShardNode(bId);
    }

    // Ensure storageDir exists for state persistence metadata
    try {
      fs.mkdirSync(this.storageDir, { recursive: true });
    } catch {

      // ignore if read-only in restricted environments
    }
    // Register default LLM endpoints & Saga tools
    this.router.registerEndpoint(
    new _models.LlmEndpointMetrics({
      endpoint_id: "ep-deepseek-r1",
      provider: "deepseek",
      model_name: "DeepSeek",
      ttft_ewma_ms: 180
    }));

    this.router.registerEndpoint(
    new _models.LlmEndpointMetrics({
      endpoint_id: "ep-qwen-38",
      provider: "qwen",
      model_name: "Qwen-3.8",
      ttft_ewma_ms: 150
    }));

    this.sagaGateway.registerTool("shell_exec", _models.ToolCallEffectClass.IDEMPOTENT_WRITE, (args) => ({
      executed: args.cmd ?? "echo ok",
      status: "SUCCESS"
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

    const bridge = new _server.DashboardBridgeService(this.scheduler, this.fleetController);
    this.dashboardServer = new _server.DashboardBackendServer(bridge, this.host);
  }

  async start(apiPort, dashboardPort) {
    const targetApiPort = apiPort ?? Number(process.env.ANSTS_LISTEN_PORT ?? process.env.ANSTS_PORT ?? 8080);
    const targetDashPort = dashboardPort ?? Number(process.env.ANSTS_DASHBOARD_PORT ?? 0);

    const boundApiPort = await new Promise((resolve, reject) => {
      this.apiServer = http.createServer((req, res) => {
        this.handleHttpRequest(req, res).catch((err) => {
          res.writeHead(500, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: err.message }));
        });
      });
      this.apiServer.on("error", reject);
      this.apiServer.listen(targetApiPort, this.host, () => {
        const addr = this.apiServer.address();
        resolve(typeof addr === "object" && addr ? addr.port : targetApiPort);
      });
    });

    const boundDashPort = await this.dashboardServer.listen(targetDashPort);

    this.tickTimer = setInterval(() => {
      if (!this.draining) {
        this.scheduler.reconcileOrphanedLeases();
        this.scheduler.scheduleTick();
        this.metrics.setGauge("ansts_queued_tasks", this.scheduler.queueManager.totalQueued());
        this.metrics.setGauge("ansts_Registered_nodes", this.scheduler.nodes.size);
      }
    }, this.tickIntervalMs);

    return { apiPort: boundApiPort, dashboardPort: boundDashPort };
  }

  async readJsonBody(req) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      req.on("data", (c) => chunks.push(c));
      req.on("end", () => {
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
        const submitted = this.scheduler.submitTask(spec);
        const placements = this.scheduler.scheduleTick();
        this.metrics.incCounter("ansts_tasks_submitted_total", 1);
        res.writeHead(202, { "content-type": "application/json" });
        res.end(
        JSON.stringify({
          task_id: submitted.task_id,
          state: submitted.state,
          assigned_node_id: submitted.assigned_node_id ?? null,
          lease_fencing_token: submitted.lease_fencing_token,
          placements
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
      const task = this.scheduler.allTasks.get(taskId);
      if (!task) {
        res.writeHead(404, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "TASK_NOT_FOUND", task_id: taskId }));
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(task));
      return;
    }

    if (method === "POST" && pathname === "/v1/layerfs/exec") {
      const body = await this.readJsonBody(req);
      const branchName = String(body.branch ?? "main");
      const command = String(body.command ?? "echo layerfs-ok");
      const stepId = String(body.step_id ?? `step-${Date.now()}`);
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
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ...result, executedByNodeId: this.nodeId, nodeRole: this.nodeRole }));
      return;
    }

    // Internal Brain RPC endpoint (`POST /internal/v1beta1/brain/schedule`)
    if (method === "POST" && pathname === "/internal/v1beta1/brain/schedule") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? `task-${Date.now()}`);
      const tenantId = String(body.tenantId ?? "tenant-default");
      const slotInfo = this.shardCoordinator.resolveBrainForTask(tenantId, taskId);
      const spec = _validator.TaskSpecValidator.validateAndNormalize({
        task_id: taskId,
        tenant_id: tenantId,
        priority: 1,
        prompt: String(body.promptText ?? "")
      });
      const submitted = this.scheduler.submitTask(spec);
      const placements = this.scheduler.scheduleTick();
      const persisted = this.v25Store.commitTaskSingleWritePath(submitted);
      this.metrics.incCounter("ansts_brain_schedules_total", 1);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        scheduledByBrainId: this.nodeId,
        slot: slotInfo.slotIndex,
        taskId: submitted.task_id,
        state: submitted.state,
        deduplicated: persisted.deduplicated,
        fencingToken: submitted.lease_fencing_token,
        assignedBodyHostId: String(body.targetBodyHostId ?? submitted.assigned_node_id ?? "body-host-01"),
        placements
      }));

      return;
    }

    // Internal Body RPC endpoint (`POST /internal/v1beta1/body/execute`)
    if (method === "POST" && pathname === "/internal/v1beta1/body/execute") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? `task-${Date.now()}`);
      const branchName = `task-${taskId}`;
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
        status: "COMPLETED"
      }));

      return;
    }

    // Dual-Homed Reverse Tunnel registration (`POST /internal/v1beta1/tunnel/register`)
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

    // Distributed Task Dispatch (`POST /v1beta1/tasks/dispatch`)
    if (method === "POST" && pathname === "/v1beta1/tasks/dispatch") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? `task-${Date.now()}`);
      const tenantId = String(body.tenantId ?? "tenant-default");
      const promptText = String(body.promptText ?? "");
      const slotResolution = this.shardCoordinator.resolveBrainForTask(tenantId, taskId);
      const ownerBrainId = slotResolution.brainNodeId ?? "brain-node-01";

      const spec = _validator.TaskSpecValidator.validateAndNormalize({
        task_id: taskId,
        tenant_id: tenantId,
        priority: 1,
        prompt: promptText
      });
      const submitted = this.scheduler.submitTask(spec);
      const placements = this.scheduler.scheduleTick();
      const targetBodyHostId = String(body.targetBodyHostId ?? submitted.assigned_node_id ?? "body-host-01");
      this.metrics.incCounter("ansts_tasks_dispatched_total", 1);

      // Forward to live peer Brain & Body nodes if configured in ANSTS_PEER_BRAIN_URLS / ANSTS_PEER_BODY_URLS
      let remoteBrainResult = null;
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

          // fallback to local schedule if peer brain is unreachable
        }}

      let remoteBodyResult = null;
      const bodyBaseUrl = this.peerBodyUrls[targetBodyHostId];
      if (bodyBaseUrl) {
        try {
          const resp = await fetch(`${bodyBaseUrl}/internal/v1beta1/body/execute`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ taskId, tenantId, promptText })
          });
          if (resp.ok) {
            remoteBodyResult = await resp.json();
          }
        } catch {

          // fallback if peer body is unreachable
        }}

      res.writeHead(202, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        dispatched: true,
        gatewayNodeId: this.nodeId,
        taskId: submitted.task_id,
        tenantId,
        shardSlot: slotResolution.slotIndex,
        routedBrainNodeId: remoteBrainResult?.scheduledByBrainId ?? ownerBrainId,
        fencingToken: remoteBrainResult?.fencingToken ?? submitted.lease_fencing_token,
        state: submitted.state,
        targetBodyHostId,
        bodyExecution: remoteBodyResult,
        placements
      }));

      return;
    }

    if (method === "GET" && pathname === "/v1beta1/console/overview") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
      JSON.stringify({
        region: urlObj.searchParams.get("region") ?? "us-west-2",
        gatewayNodeId: this.nodeId,
        totalTasks: this.scheduler.allTasks.size,
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

    if (method === "POST" && pathname === "/shutdown") {
      this.draining = true;
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "draining_started", handoff: urlObj.searchParams.get("handoff") ?? "release" }));
      return;
    }

    // Serve static console docs if requested on root `/`
    if (method === "GET" && (pathname === "/" || pathname === "/index.html" || pathname === "/ansts_console_live.html")) {
      const docFile = pathname === "/ansts_console_live.html" ? "ansts_console_live.html" : "index.html";
      const candidatePath = path.resolve(process.cwd(), "docs", docFile);
      if (fs.existsSync(candidatePath)) {
        res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
        res.end(fs.readFileSync(candidatePath, "utf-8"));
        return;
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