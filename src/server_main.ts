/**
 * @fileoverview M18 Unified Standalone & Docker Compose Service Entrypoint (`src/server_main.ts`).
 * Starts the ANSTS v2.2 Control Plane HTTP Gateway (`:8080`), Operational Web Dashboard (`:8090`),
 * Pre-warmed NodeDaemon Worker Pool, LayerFS Dual-Path Workspace Store, Prometheus `/metrics`,
 * and `denoland/celld` compatible `/healthz`, `/.well-known/celld/health`, and `/state` endpoints.
 */
import * as http from "node:http";
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

export interface AnstsServiceRuntimeOptions {
  host?: string;
  apiPort?: number;
  dashboardPort?: number;
  workerNodeCount?: number;
  tickIntervalMs?: number;
}

export class AnstsLocalAndComposeServer {
  public readonly scheduler: CoreScheduler5D;
  public readonly backpressure: IngressBackpressureController;
  public readonly sseGateway: SseEventStreamGateway;
  public readonly metrics: OtelAndPrometheusExporter;
  public readonly layerfs: LayerFsWorkspaceDriver;
  public readonly fleetController: ManualFleetScaleController;
  public readonly router: CognitiveRouter;
  public readonly sagaGateway: SagaToolGateway;
  public readonly dashboardServer: DashboardBackendServer;
  private apiServer: http.Server | null = null;
  private tickTimer: ReturnType<typeof setInterval> | null = null;
  private readonly host: string;
  private readonly tickIntervalMs: number;
  private draining = false;

  constructor(options: AnstsServiceRuntimeOptions = {}) {
    this.host = options.host ?? process.env.ANSTS_HOST ?? "0.0.0.0";
    this.tickIntervalMs = options.tickIntervalMs ?? Number(process.env.ANSTS_TICK_INTERVAL_MS ?? 1000);
    this.scheduler = new CoreScheduler5D(15.0);
    this.backpressure = new IngressBackpressureController();
    this.sseGateway = new SseEventStreamGateway();
    this.metrics = new OtelAndPrometheusExporter();
    this.layerfs = new LayerFsWorkspaceDriver();
    this.fleetController = new ManualFleetScaleController();
    this.router = new CognitiveRouter();
    this.sagaGateway = new SagaToolGateway();

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

    // Bootstrap local/container worker nodes
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

    const bridge = new DashboardBridgeService(this.scheduler, this.fleetController);
    this.dashboardServer = new DashboardBackendServer(bridge, this.host);
  }

  public async start(apiPort?: number, dashboardPort?: number): Promise<{ apiPort: number; dashboardPort: number }> {
    const targetApiPort = apiPort ?? Number(process.env.ANSTS_LISTEN_PORT ?? process.env.ANSTS_PORT ?? 8080);
    const targetDashPort = dashboardPort ?? Number(process.env.ANSTS_DASHBOARD_PORT ?? 8090);

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

    if (method === "GET" && (pathname === "/healthz" || pathname === "/health" || pathname === "/.well-known/celld/health")) {
      if (this.draining) {
        res.writeHead(503, { "content-type": "application/json", connection: "close" });
        res.end(JSON.stringify({ status: "draining", healthy: false }));
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          status: "ok",
          healthy: true,
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
      res.end(JSON.stringify(result));
      return;
    }

    if (method === "POST" && pathname === "/internal/v1beta1/tunnel/register") {
      const body = await this.readJsonBody(req);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          registered: true,
          hostId: body.hostId ?? "body-host-01",
          gatewayId: body.gatewayId ?? (process.env.ANSTS_NODE_ID ?? "celld-gw-01"),
          role: body.role ?? "PRIMARY",
          epoch: body.epoch ?? 1,
        })
      );
      return;
    }

    if (method === "POST" && pathname === "/v1beta1/tasks/dispatch") {
      const body = await this.readJsonBody(req);
      const spec = TaskSpecValidator.validateAndNormalize({
        task_id: String(body.taskId ?? `task-${Date.now()}`),
        tenant_id: String(body.tenantId ?? "tenant-default"),
        priority: 1,
        prompt: String(body.promptText ?? ""),
      });
      const submitted = this.scheduler.submitTask(spec);
      const placements = this.scheduler.scheduleTick();
      res.writeHead(202, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          dispatched: true,
          taskId: submitted.task_id,
          state: submitted.state,
          targetBodyHostId: body.targetBodyHostId ?? submitted.assigned_node_id,
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
          totalTasks: this.scheduler.allTasks.size,
          activeNodes: this.scheduler.nodes.size,
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
          virtualSlots: 1024,
          brains: ["brain-node-01", "brain-node-02", "brain-node-03"],
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
        service: "ANSTS v2.2 Control Plane & LayerFS Runtime",
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
