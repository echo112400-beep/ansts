/**
 * @fileoverview M18 & v2.5 Unified Standalone, Docker Compose & Multi-Node Cluster Service Entrypoint (`src/server_main.ts`).
 * Supports single-stack mode (`:8080` API + `:8090` Dashboard) and `feat/celld-v25` Multi-Node Cluster mode
 * (`gateway`, `brain`, `body`) with cross-node HTTP RPC forwarding (`gateway -> brain -> body`),
 * Dual-Homed Reverse Tunnel registration (`EdgeTunnelHaRouter`), 256-slot `HaSchedulerShardCoordinator`,
 * `SchedulerV25SingleWritePathStore`, LayerFS Dual-Path Workspace Store, and Prometheus `/metrics`.
 */
import * as http from "node:http";
import * as fs from "node:fs";
import * as path from "node:path";
import { CoreScheduler5D } from "./scheduler.js";
import { NodeDaemon } from "./node_daemon.js";
import { TaskSpecValidator, TaskSpecValidationError } from "./gateway/validator.js";
import { IngressBackpressureController } from "./gateway/backpressure.js";
import { SseEventStreamGateway } from "./gateway/sse_stream.js";
import { DashboardBridgeService, DashboardBackendServer } from "./dashboard/server.js";
import { OtelAndPrometheusExporter } from "./telemetry/otel_prometheus.js";
import { LayerFsWorkspaceDriver } from "./runtime/layerfs_workspace_driver.js";
import { ManualFleetScaleController } from "./cluster/manual_scale_controller.js";
import { CognitiveRouter } from "./cognitive_router.js";
import { LlmEndpointMetrics, ToolCallEffectClass } from "./models.js";
import { SagaToolGateway } from "./saga_gateway.js";
import { HaSchedulerShardCoordinator } from "./cloudflare/ha_scheduler_shard_coordinator.js";
import { SchedulerV25SingleWritePathStore } from "./cloudflare/scheduler_v25_persistence.js";
import { EdgeTunnelHaRouter } from "./runtime/edge_tunnel_ha_router.js";
import { InMemoryCellSqlStorage } from "./cloudflare/env.js";

export interface AnstsServiceRuntimeOptions {
  host?: string;
  apiPort?: number;
  dashboardPort?: number;
  workerNodeCount?: number;
  tickIntervalMs?: number;
  nodeId?: string;
  nodeRole?: "brain" | "body" | "gateway";
  peerBrainUrls?: Record<string, string>;
  peerBodyUrls?: Record<string, string>;
  storageDir?: string;
}

function parseJsonEnvMap(raw: string | undefined): Record<string, string> {
  if (!raw || !raw.trim()) return {};
  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsed)) {
      if (typeof v === "string") out[k] = v;
    }
    return out;
  } catch {
    return {};
  }
}

export class AnstsLocalAndComposeServer {
  public readonly nodeId: string;
  public readonly nodeRole: "brain" | "body" | "gateway";
  public readonly storageDir: string;
  public readonly peerBrainUrls: Record<string, string>;
  public readonly peerBodyUrls: Record<string, string>;
  public readonly scheduler: CoreScheduler5D;
  public readonly backpressure: IngressBackpressureController;
  public readonly sseGateway: SseEventStreamGateway;
  public readonly metrics: OtelAndPrometheusExporter;
  public readonly layerfs: LayerFsWorkspaceDriver;
  public readonly fleetController: ManualFleetScaleController;
  public readonly router: CognitiveRouter;
  public readonly sagaGateway: SagaToolGateway;
  public readonly shardCoordinator: HaSchedulerShardCoordinator;
  public readonly v25Store: SchedulerV25SingleWritePathStore;
  public readonly tunnelRouter: EdgeTunnelHaRouter;
  public readonly dashboardServer: DashboardBackendServer;
  private apiServer: http.Server | null = null;
  private tickTimer: ReturnType<typeof setInterval> | null = null;
  private readonly host: string;
  private readonly tickIntervalMs: number;
  private draining = false;

  constructor(options: AnstsServiceRuntimeOptions = {}) {
    this.host = options.host ?? process.env.ANSTS_HOST ?? "0.0.0.0";
    this.nodeId = options.nodeId ?? process.env.ANSTS_NODE_ID ?? "ansts-dev-node-01";
    this.nodeRole =
      (options.nodeRole ?? (process.env.ANSTS_NODE_ROLE as "brain" | "body" | "gateway")) || "gateway";
    this.storageDir = options.storageDir ?? process.env.ANSTS_STORAGE_DIR ?? `/tmp/ansts/${this.nodeId}`;
    this.peerBrainUrls = options.peerBrainUrls ?? parseJsonEnvMap(process.env.ANSTS_PEER_BRAIN_URLS);
    this.peerBodyUrls = options.peerBodyUrls ?? parseJsonEnvMap(process.env.ANSTS_PEER_BODY_URLS);

    this.tickIntervalMs = options.tickIntervalMs ?? Number(process.env.ANSTS_TICK_INTERVAL_MS ?? 1000);
    this.scheduler = new CoreScheduler5D(15.0);
    this.backpressure = new IngressBackpressureController();
    this.sseGateway = new SseEventStreamGateway();
    this.metrics = new OtelAndPrometheusExporter();
    this.layerfs = new LayerFsWorkspaceDriver();
    this.fleetController = new ManualFleetScaleController();
    this.router = new CognitiveRouter();
    this.sagaGateway = new SagaToolGateway();
    this.v25Store = new SchedulerV25SingleWritePathStore(new InMemoryCellSqlStorage());
    this.tunnelRouter = new EdgeTunnelHaRouter();

    const brainNodes =
      Object.keys(this.peerBrainUrls).length > 0
        ? Object.keys(this.peerBrainUrls)
        : ["brain-node-01", "brain-node-02", "brain-node-03"];
    this.shardCoordinator = new HaSchedulerShardCoordinator(256, 8);
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
      new LlmEndpointMetrics({
        endpoint_id: "ep-deepseek-r1",
        provider: "deepseek",
        model_name: "DeepSeek",
        ttft_ewma_ms: 180,
      })
    );
    this.router.registerEndpoint(
      new LlmEndpointMetrics({
        endpoint_id: "ep-qwen-38",
        provider: "qwen",
        model_name: "Qwen-3.8",
        ttft_ewma_ms: 150,
      })
    );
    this.sagaGateway.registerTool("shell_exec", ToolCallEffectClass.IDEMPOTENT_WRITE, (args) => ({
      executed: args.cmd ?? "echo ok",
      status: "SUCCESS",
    }));

    // Bootstrap worker nodes
    const configuredBodies = Object.keys(this.peerBodyUrls);
    if (configuredBodies.length > 0) {
      configuredBodies.forEach((bodyId, idx) => {
        const daemon = new NodeDaemon(bodyId, 32, 65536, 8);
        this.scheduler.registerOrUpdateNode(daemon.emitHeartbeat(0));
        this.fleetController.registerBareMetalNode({
          nodeId: bodyId,
          ipAddress: `10.240.0.${20 + idx}`,
          role: "worker",
          systemdUnit: "ansts-body-daemon.service",
          runningCells: [`BodySandboxHost:${bodyId}`],
        });
      });
    } else {
      const workerCount = options.workerNodeCount ?? Number(process.env.ANSTS_WORKER_NODES ?? 3);
      for (let i = 1; i <= workerCount; i++) {
        const nodeId = `ansts-worker-${String(i).padStart(2, "0")}`;
        const daemon = new NodeDaemon(nodeId, 32, 65536, 8);
        this.scheduler.registerOrUpdateNode(daemon.emitHeartbeat(0));
        this.fleetController.registerBareMetalNode({
          nodeId,
          ipAddress: `10.240.0.${10 + i}`,
          role: "worker",
          systemdUnit: "ansts-node-daemon.service",
          runningCells: [`AnstsSchedulerCell:shard-${i}`],
        });
      }
    }

    const bridge = new DashboardBridgeService(this.scheduler, this.fleetController);
    this.dashboardServer = new DashboardBackendServer(bridge, this.host);
  }

  public async start(apiPort?: number, dashboardPort?: number): Promise<{ apiPort: number; dashboardPort: number }> {
    const targetApiPort = apiPort ?? Number(process.env.ANSTS_LISTEN_PORT ?? process.env.ANSTS_PORT ?? 8080);
    const targetDashPort = dashboardPort ?? Number(process.env.ANSTS_DASHBOARD_PORT ?? 0);

    const boundApiPort = await new Promise<number>((resolve, reject) => {
      this.apiServer = http.createServer((req, res) => {
        this.handleHttpRequest(req, res).catch((err: unknown) => {
          res.writeHead(500, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: (err as Error).message }));
        });
      });
      this.apiServer.on("error", reject);
      this.apiServer.listen(targetApiPort, this.host, () => {
        const addr = this.apiServer!.address();
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

  private async readJsonBody(req: http.IncomingMessage): Promise<Record<string, unknown>> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => {
        try {
          const raw = Buffer.concat(chunks).toString("utf-8").trim();
          resolve(raw ? (JSON.parse(raw) as Record<string, unknown>) : {});
        } catch (err) {
          reject(err);
        }
      });
    });
  }

  private async handleHttpRequest(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const method = req.method ?? "GET";
    const urlObj = new URL(req.url ?? "/", "http://127.0.0.1");
    const pathname = urlObj.pathname;

    if (
      method === "GET" &&
      (pathname === "/healthz" ||
        pathname === "/health" ||
        pathname === "/readyz" ||
        pathname === "/.well-known/celld/health")
    ) {
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
          layerfsStats: this.layerfs.storeEngine.getStorageStats(),
        })
      );
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
            shed_cells: 0,
          },
          deployment: {
            isolates: { live: this.scheduler.nodes.size, live_empty: 0, retiring: 0 },
          },
        })
      );
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
        const spec = TaskSpecValidator.validateAndNormalize(body);
        const decision = this.backpressure.evaluateAdmission({
          priority: spec.priority,
          globalQueueDepth: this.scheduler.queueManager.totalQueued(),
          tenantActiveTasks: this.scheduler.queueManager.getTenantActiveCount(spec.tenant_id),
          clusterPsiPressurePct: 5.0,
        });
        if (!decision.allowed) {
          res.writeHead(decision.statusCode, {
            "content-type": "application/json",
            "Retry-After": String(decision.retryAfterSeconds ?? 15),
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
            placements,
          })
        );
      } catch (err) {
        if (err instanceof TaskSpecValidationError) {
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
        command,
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
      const spec = TaskSpecValidator.validateAndNormalize({
        task_id: taskId,
        tenant_id: tenantId,
        priority: 1,
        prompt: String(body.promptText ?? ""),
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
          placements,
        })
      );
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
        command: `printf '%s\\n' ${JSON.stringify(String(body.promptText ?? "ok"))} > .ansts_task_trace.txt`,
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
          status: "COMPLETED",
        })
      );
      return;
    }

    // Dual-Homed Reverse Tunnel registration (`POST /internal/v1beta1/tunnel/register`)
    if (method === "POST" && pathname === "/internal/v1beta1/tunnel/register") {
      const body = await this.readJsonBody(req);
      const hostId = String(body.hostId ?? "body-host-01");
      const gatewayId = String(body.gatewayId ?? this.nodeId);
      const role = (body.role === "SECONDARY" ? "SECONDARY" : "PRIMARY") as "PRIMARY" | "SECONDARY";
      const connGeneration = Number(body.connGeneration ?? 1);
      const epoch = Number(body.epoch ?? 1);

      this.tunnelRouter.tunnelCell.registerTunnel({
        hostId,
        gatewayId,
        role,
        connGeneration,
        epoch,
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
          activeGatewayId: activeBinding.gatewayId,
        })
      );
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

      const spec = TaskSpecValidator.validateAndNormalize({
        task_id: taskId,
        tenant_id: tenantId,
        priority: 1,
        prompt: promptText,
      });
      const submitted = this.scheduler.submitTask(spec);
      const placements = this.scheduler.scheduleTick();
      const targetBodyHostId = String(body.targetBodyHostId ?? submitted.assigned_node_id ?? "body-host-01");
      this.metrics.incCounter("ansts_tasks_dispatched_total", 1);

      // Forward to live peer Brain & Body nodes if configured in ANSTS_PEER_BRAIN_URLS / ANSTS_PEER_BODY_URLS
      let remoteBrainResult: Record<string, unknown> | null = null;
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
              slot: slotResolution.slotIndex,
            }),
          });
          if (resp.ok) {
            remoteBrainResult = (await resp.json()) as Record<string, unknown>;
          }
        } catch {
          // fallback to local schedule if peer brain is unreachable
        }
      }

      let remoteBodyResult: Record<string, unknown> | null = null;
      const bodyBaseUrl = this.peerBodyUrls[targetBodyHostId];
      if (bodyBaseUrl) {
        try {
          const resp = await fetch(`${bodyBaseUrl}/internal/v1beta1/body/execute`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ taskId, tenantId, promptText }),
          });
          if (resp.ok) {
            remoteBodyResult = (await resp.json()) as Record<string, unknown>;
          }
        } catch {
          // fallback if peer body is unreachable
        }
      }

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
          placements,
        })
      );
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
          status: "HEALTHY",
        })
      );
      return;
    }

    if (method === "GET" && pathname === "/v1beta1/console/shards") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          totalShards: 64,
          virtualSlots: 256,
          brains:
            Object.keys(this.peerBrainUrls).length > 0
              ? Object.keys(this.peerBrainUrls)
              : ["brain-node-01", "brain-node-02", "brain-node-03"],
          sampleSlotOwners: {
            slot0: this.shardCoordinator.ring.getOwnerNodeForSlot(0),
            slot85: this.shardCoordinator.ring.getOwnerNodeForSlot(85),
            slot170: this.shardCoordinator.ring.getOwnerNodeForSlot(170),
          },
        })
      );
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

  public async stop(): Promise<void> {
    this.draining = true;
    if (this.tickTimer) {
      clearInterval(this.tickTimer);
      this.tickTimer = null;
    }
    await this.dashboardServer.close();
    if (this.apiServer) {
      await new Promise<void>((resolve) => {
        (this.apiServer as unknown as { closeAllConnections?: () => void }).closeAllConnections?.();
        this.apiServer!.close(() => {
          this.apiServer = null;
          resolve();
        });
      });
    }
  }
}

// Execute server if invoked directly via `node dist/src/server_main.js` or `npm start`
const isMainModule =
  typeof process !== "undefined" &&
  Array.isArray(process.argv) &&
  process.argv[1] &&
  (process.argv[1].endsWith("server_main.js") || process.argv[1].endsWith("server_main.ts"));

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
        dashboardUrl: `http://127.0.0.1:${dashboardPort}`,
      })
    );
  });

  const shutdown = () => {
    server.stop().then(() => process.exit(0));
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}
