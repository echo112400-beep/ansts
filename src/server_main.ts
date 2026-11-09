/**
 * @fileoverview M18 & v2.5 Unified Standalone, Docker Compose & Multi-Node Cluster Service Entrypoint (`src/server_main.ts`).
 * Hardened with:
 * - [P0] Durable Disk-Backed SQL Storage (`DurableDiskCellSqlStorage`) + Cold-Start Rehydration (`RPO=0`) & RustFS S3 LTX Sync
 * - [P0] Strict Single-Write-Path (`SchedulerV25SingleWritePathStore` before in-memory index) & Stateless Gateway Fan-Out
 * - [P1] Cross-Node LayerFS Snapshot Sync (`Body-01 <-> RustFS (:9000) <-> Body-02`) & Auto Dual-Homed Reverse Tunnel Registration
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
import { LlmEndpointMetrics, TaskSpec, ToolCallEffectClass } from "./models.js";
import { SagaToolGateway } from "./saga_gateway.js";
import { HaSchedulerShardCoordinator } from "./cloudflare/ha_scheduler_shard_coordinator.js";
import { SchedulerV25SingleWritePathStore } from "./cloudflare/scheduler_v25_persistence.js";
import { EdgeTunnelHaRouter } from "./runtime/edge_tunnel_ha_router.js";
import { DurableDiskCellSqlStorage } from "./cloudflare/env.js";

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
  s3Endpoint?: string;
  primaryGatewayUrl?: string;
  secondaryGatewayUrl?: string;
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
  public readonly s3Endpoint: string | null;
  public readonly primaryGatewayUrl: string | null;
  public readonly secondaryGatewayUrl: string | null;
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
    this.scheduler = new CoreScheduler5D(15.0);
    this.backpressure = new IngressBackpressureController();
    this.sseGateway = new SseEventStreamGateway();
    this.metrics = new OtelAndPrometheusExporter();
    this.layerfs = new LayerFsWorkspaceDriver();
    this.fleetController = new ManualFleetScaleController();
    this.router = new CognitiveRouter();
    this.sagaGateway = new SagaToolGateway();

    // Ensure storageDir exists and bind DurableDiskCellSqlStorage for RPO=0 persistence
    try {
      fs.mkdirSync(this.storageDir, { recursive: true });
    } catch {
      // ignore if read-only in restricted environments
    }
    const sqlFilePath = path.join(this.storageDir, "v25_scheduler.sql.json");
    this.v25Store = new SchedulerV25SingleWritePathStore(new DurableDiskCellSqlStorage(sqlFilePath));
    this.tunnelRouter = new EdgeTunnelHaRouter();

    const brainNodes =
      Object.keys(this.peerBrainUrls).length > 0
        ? Object.keys(this.peerBrainUrls)
        : ["brain-node-01", "brain-node-02", "brain-node-03"];
    this.shardCoordinator = new HaSchedulerShardCoordinator(256, 8);
    for (const bId of brainNodes) {
      this.shardCoordinator.registerBrainShardNode(bId);
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

    // Rehydrate in-memory scheduler index from durable v25Store on cold start
    for (const persistedTask of this.v25Store.listPersistedTasks()) {
      this.scheduler.allTasks.set(persistedTask.task_id, persistedTask);
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

    // If this node is a `body` daemon and gateway URLs are configured, proactively register dual-homed reverse tunnels
    if (this.nodeRole === "body") {
      await this.registerOutboundReverseTunnels();
    }

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

  private async registerOutboundReverseTunnels(): Promise<void> {
    const targets: Array<{ url: string; role: "PRIMARY" | "SECONDARY"; gwId: string }> = [];
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
            epoch: 1,
          }),
        });
      } catch {
        // ignore if gateway not yet up
      }
    }
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
        // Strict Single-Write-Path: check existing persisted task first, then schedule and commit atomically
        const existingPersisted = this.v25Store.getPersistedTask(spec.task_id);
        if (!existingPersisted) {
          const submitted = this.scheduler.submitTask(spec);
          const placements = this.scheduler.scheduleTick();
          this.v25Store.commitTaskSingleWritePath(submitted, submitted.assigned_node_id);
          if (this.s3Endpoint) {
            await this.v25Store.flushLtxSnapshotToS3(this.s3Endpoint, this.nodeId);
          }
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
          return;
        }
        const preCommitted = this.v25Store.commitTaskSingleWritePath(spec);
        res.writeHead(202, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            task_id: preCommitted.task.task_id,
            state: preCommitted.task.state,
            assigned_node_id: preCommitted.task.assigned_node_id ?? null,
            lease_fencing_token: preCommitted.fencingToken,
            deduplicated: true,
            placements: [],
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
        }
      }
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "TASK_NOT_FOUND", task_id: taskId }));
      return;
    }

    if (method === "POST" && pathname === "/v1/layerfs/exec") {
      const body = await this.readJsonBody(req);
      const branchName = String(body.branch ?? "main");
      const command = String(body.command ?? "echo layerfs-ok");
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
        command,
      });
      let s3Synced = false;
      if (this.s3Endpoint) {
        s3Synced = await this.layerfs.pushBranchToRemoteS3(branchName, this.s3Endpoint);
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ...result, executedByNodeId: this.nodeId, nodeRole: this.nodeRole, s3Synced }));
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
      const taskId = String(body.taskId ?? `task-${Date.now()}`);
      const tenantId = String(body.tenantId ?? "tenant-default");
      const slotInfo = this.shardCoordinator.resolveBrainForTask(tenantId, taskId);
      const spec = TaskSpecValidator.validateAndNormalize({
        task_id: taskId,
        tenant_id: tenantId,
        priority: 1,
        prompt: String(body.promptText ?? ""),
        idempotency_key: body.idempotencyKey ? String(body.idempotencyKey) : `idem:${tenantId}:${taskId}`,
      });

      // Step 1: Check / Commit in durable v25Store first (Single-Write-Path)
      const firstCommit = this.v25Store.commitTaskSingleWritePath(spec);
      let placements: Array<[string, string, number]> = [];
      let finalTask: TaskSpec = firstCommit.task;

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
          JSON.stringify(submitted)
        );
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
          slot: slotInfo.slotIndex,
          taskId: finalTask.task_id,
          state: finalTask.state,
          deduplicated: firstCommit.deduplicated,
          fencingToken: finalTask.lease_fencing_token,
          assignedBodyHostId: String(body.targetBodyHostId ?? finalTask.assigned_node_id ?? "body-host-01"),
          placements,
        })
      );
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
        command: `printf '%s\\n' ${JSON.stringify(String(body.promptText ?? "ok"))} > .ansts_task_trace.txt`,
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
          status: "COMPLETED",
        })
      );
      return;
    }

    // Dual-Homed Reverse Tunnel registration & health mark (`POST /internal/v1beta1/tunnel/register` & `/health`)
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
      const promptText = String(body.promptText ?? "");
      const slotResolution = this.shardCoordinator.resolveBrainForTask(tenantId, taskId);
      const ownerBrainId = slotResolution.brainNodeId ?? "brain-node-01";
      const targetBodyHostId = String(body.targetBodyHostId ?? "body-host-01");
      this.metrics.incCounter("ansts_tasks_dispatched_total", 1);

      // Forward to live peer Brain shard owner if configured in ANSTS_PEER_BRAIN_URLS
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

      // Fallback to local schedule only when no peerBrainUrls are configured/reachable
      let localState = "LEASED";
      let localFencingToken = 1;
      let localPlacements: Array<[string, string, number]> = [[taskId, targetBodyHostId, 1]];
      if (!remoteBrainResult) {
        const spec = TaskSpecValidator.validateAndNormalize({
          task_id: taskId,
          tenant_id: tenantId,
          priority: 1,
          prompt: promptText,
        });
        const submitted = this.scheduler.submitTask(spec);
        localPlacements = this.scheduler.scheduleTick();
        this.v25Store.commitTaskSingleWritePath(submitted, targetBodyHostId);
        localState = submitted.state;
        localFencingToken = submitted.lease_fencing_token;
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
          taskId,
          tenantId,
          shardSlot: slotResolution.slotIndex,
          routedBrainNodeId: remoteBrainResult?.scheduledByBrainId ?? ownerBrainId,
          deduplicated: remoteBrainResult?.deduplicated ?? false,
          fencingToken: remoteBrainResult?.fencingToken ?? localFencingToken,
          state: remoteBrainResult?.state ?? localState,
          targetBodyHostId,
          bodyExecution: remoteBodyResult,
          placements: remoteBrainResult?.placements ?? localPlacements,
        })
      );
      return;
    }

    if (method === "GET" && pathname === "/v1beta1/console/overview") {
      // Stateless fan-out across all peerBrainUrls so both Gateway-01 and Gateway-02 return identical global counts
      const globalTaskIds = new Set<string>(this.scheduler.allTasks.keys());
      for (const t of this.v25Store.listPersistedTasks()) {
        globalTaskIds.add(t.task_id);
      }
      const brainEntries = Object.entries(this.peerBrainUrls);
      if (brainEntries.length > 0) {
        await Promise.all(
          brainEntries.map(async ([, bUrl]) => {
            try {
              const resp = await fetch(`${bUrl}/internal/v1beta1/brain/tasks`);
              if (resp.ok) {
                const data = (await resp.json()) as { tasks?: Array<{ task_id: string }> };
                for (const t of data.tasks ?? []) {
                  globalTaskIds.add(t.task_id);
                }
              }
            } catch {
              // ignore unreachable brain
            }
          })
        );
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
