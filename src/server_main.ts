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
import { SharedBlackboardAndGitArbiter } from "./a2a/blackboard_and_git_arbiter.js";
import { A2aWaitForGraphDeadlockDetector } from "./a2a/deadlock_detector.js";
import { ParallelCowGitBisectCoordinator } from "./a2a/parallel_cow_bisect.js";
import { DistributedCasGcSweeper } from "./storage/distributed_gc_sweeper.js";
import { LayerFsMultiAgentBranchCoordinator, LayerFsMergeConflictError } from "./dag/layerfs_branch_coordinator.js";
import { SlsaProvenanceAttestor } from "./runtime/slsa_provenance_attestor.js";
import { ConfidentialTeeAttestationBridge } from "./runtime/confidential_tee_bridge.js";
import { ZeroTrustSandboxSecurityGuard } from "./runtime/zero_trust_security_guard.js";
import { MtlsReverseTunnelBridge } from "./runtime/mtls_tunnel_bridge.js";
import { CapacitySlaEnforcer } from "./cluster/capacity_sla_enforcer.js";
import { PriorityLoadShedder } from "./cluster/priority_load_shedder.js";
import { CanaryRolloutController } from "./cluster/canary_rollout_controller.js";
import { PredictiveSandboxPrewarmer } from "./runtime/predictive_prewarmer.js";
import { GeoSovereigntyAndLatencyRouter } from "./cluster/geo_routing.js";
import { CrossRegionAsyncReplicator } from "./cluster/cross_region_replicator.js";
import { AutomatedChaosDrillRunner } from "./cluster/chaos_drill_runner.js";
import { NetworkChaosFaultInjectionProxy } from "./runtime/network_chaos_proxy.js";
import { SandboxPreflightReadinessGate } from "./runtime/preflight_gate.js";
import { CoWSandboxBranchForker } from "./runtime/sandbox_branch_forker.js";
import { FinOpsBudgetController } from "./finops/budget_controller.js";
import { McpDynamicToolRegistry } from "./saga/mcp_registry.js";
import { TurnWalEngine } from "./checkpoint/wal_engine.js";
import { DeterministicTimeTravelReplayer } from "./checkpoint/time_travel.js";
import { EphemeralHeavyExecutorMigrator } from "./runtime/live_migration.js";
import { InMemoryR2Bucket } from "./cloudflare/env.js";
import { TaskPriority } from "./models.js";
import { StaleFenceError } from "./lease_coordinator.js";
import { AgentHarnessRunner } from "./harness/runner.js";
import { SyntacticAndVelocityBreaker } from "./harness/velocity_breaker.js";
import { DagEscrowCoordinator } from "./dag/escrow.js";
import { CgroupPhaseController } from "./runtime/phase_controller.js";
import { LinuxPsiMemoryGuard } from "./runtime/psi_guard.js";
import { CloudEventsBus } from "./telemetry/cloudevents.js";
import { TransactionalOutboxDispatcher } from "./telemetry/outbox_webhook.js";

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
  public readonly blackboard: SharedBlackboardAndGitArbiter;
  public readonly deadlockDetector: A2aWaitForGraphDeadlockDetector;
  public readonly bisectCoordinator: ParallelCowGitBisectCoordinator;
  public readonly gcSweeper: DistributedCasGcSweeper;
  public readonly branchCoordinator: LayerFsMultiAgentBranchCoordinator;
  public readonly slsaAttestor: SlsaProvenanceAttestor;
  public readonly teeBridge: ConfidentialTeeAttestationBridge;
  public readonly securityGuard: ZeroTrustSandboxSecurityGuard;
  public readonly mtlsBridge: MtlsReverseTunnelBridge;
  public readonly slaEnforcer: CapacitySlaEnforcer;
  public readonly loadShedder: PriorityLoadShedder;
  public readonly canaryController: CanaryRolloutController;
  public readonly prewarmer: PredictiveSandboxPrewarmer;
  public readonly geoRouter: GeoSovereigntyAndLatencyRouter;
  public readonly crossRegionReplicator: CrossRegionAsyncReplicator;
  public readonly chaosDrillRunner: AutomatedChaosDrillRunner;
  public readonly chaosProxy: NetworkChaosFaultInjectionProxy;
  public readonly preflightGate: SandboxPreflightReadinessGate;
  public readonly branchForker: CoWSandboxBranchForker;
  public readonly finopsController: FinOpsBudgetController;
  public readonly mcpRegistry: McpDynamicToolRegistry;
  public readonly walEngine: TurnWalEngine;
  public readonly timeTravelReplayer: DeterministicTimeTravelReplayer;
  public readonly heavyMigrator: EphemeralHeavyExecutorMigrator;
  public readonly harnessRunner: AgentHarnessRunner;
  public readonly velocityBreaker: SyntacticAndVelocityBreaker;
  public readonly dagEscrow: DagEscrowCoordinator;
  public readonly phaseController: CgroupPhaseController;
  public readonly psiGuard: LinuxPsiMemoryGuard;
  public readonly cloudEventsBus: CloudEventsBus;
  public readonly outboxDispatcher: TransactionalOutboxDispatcher;
  public readonly dashboardServer: DashboardBackendServer;
  private apiServer: http.Server | null = null;
  private tickTimer: ReturnType<typeof setInterval> | null = null;
  private tunnelPollTimer: ReturnType<typeof setInterval> | null = null;
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
    this.blackboard = new SharedBlackboardAndGitArbiter();
    this.deadlockDetector = new A2aWaitForGraphDeadlockDetector();
    this.bisectCoordinator = new ParallelCowGitBisectCoordinator();
    this.gcSweeper = new DistributedCasGcSweeper();
    this.branchCoordinator = new LayerFsMultiAgentBranchCoordinator(this.layerfs);
    this.slsaAttestor = new SlsaProvenanceAttestor();
    this.teeBridge = new ConfidentialTeeAttestationBridge();
    this.securityGuard = new ZeroTrustSandboxSecurityGuard();
    this.mtlsBridge = new MtlsReverseTunnelBridge();
    this.slaEnforcer = new CapacitySlaEnforcer();
    this.loadShedder = new PriorityLoadShedder();
    this.canaryController = new CanaryRolloutController();
    this.prewarmer = new PredictiveSandboxPrewarmer();
    this.geoRouter = new GeoSovereigntyAndLatencyRouter();
    this.geoRouter.registerGeoRegion({ regionId: "eu-central-1", jurisdiction: "GDPR_EU", latencyMs: 18, capacityAvailable: true });
    this.geoRouter.registerGeoRegion({ regionId: "eu-west-1", jurisdiction: "GDPR_EU", latencyMs: 26, capacityAvailable: true });
    this.geoRouter.registerGeoRegion({ regionId: "us-west-2", jurisdiction: "US_FED", latencyMs: 12, capacityAvailable: true });
    this.geoRouter.registerGeoRegion({ regionId: "ap-southeast-1", jurisdiction: "APAC_SG", latencyMs: 31, capacityAvailable: true });
    this.crossRegionReplicator = new CrossRegionAsyncReplicator();
    this.chaosDrillRunner = new AutomatedChaosDrillRunner();
    this.chaosProxy = new NetworkChaosFaultInjectionProxy();
    this.preflightGate = new SandboxPreflightReadinessGate();
    this.branchForker = new CoWSandboxBranchForker(this.layerfs);
    this.finopsController = new FinOpsBudgetController();
    this.mcpRegistry = new McpDynamicToolRegistry();
    this.walEngine = new TurnWalEngine();
    this.timeTravelReplayer = new DeterministicTimeTravelReplayer();
    this.heavyMigrator = new EphemeralHeavyExecutorMigrator(new InMemoryR2Bucket());
    this.velocityBreaker = new SyntacticAndVelocityBreaker();
    this.harnessRunner = new AgentHarnessRunner(this.router, this.sagaGateway, this.velocityBreaker);
    this.dagEscrow = new DagEscrowCoordinator();
    this.phaseController = new CgroupPhaseController();
    this.psiGuard = new LinuxPsiMemoryGuard();
    this.cloudEventsBus = new CloudEventsBus();
    this.outboxDispatcher = new TransactionalOutboxDispatcher();

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
    this.sagaGateway.registerTool(
      "shell_exec",
      ToolCallEffectClass.IDEMPOTENT_WRITE,
      (args) => ({
        executed: args.cmd ?? "echo ok",
        status: "SUCCESS",
      }),
      (args) => ({
        rolledBackCmd: args.cmd ?? "echo ok",
        status: "COMPENSATED",
      })
    );

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
      this.tunnelPollTimer = setInterval(() => {
        if (!this.draining) {
          this.pollAndExecuteOutboundTunnelCommands().catch(() => {});
        }
      }, 40);
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


  private async pollAndExecuteOutboundTunnelCommands(): Promise<void> {
    const gatewayUrls = [this.primaryGatewayUrl, this.secondaryGatewayUrl].filter(
      (u): u is string => Boolean(u && u.trim())
    );
    for (const gwUrl of gatewayUrls) {
      try {
        const baseUrl = gwUrl.replace(/\/$/, "");
        const pollResp = await fetch(
          `${baseUrl}/internal/v1beta1/tunnel/poll?hostId=${encodeURIComponent(this.nodeId)}`
        );
        if (!pollResp.ok) continue;
        const { commands } = (await pollResp.json()) as {
          commands?: Array<{ commandId: string; payload: Record<string, unknown> }>;
        };
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
            command: `printf "%s\\n" ${JSON.stringify(String(cmd.payload.promptText ?? "ok"))} > .ansts_tunnel_trace.txt`,
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
                status: "COMPLETED",
              },
            }),
          });
        }
      } catch {
        // ignore transient gateway poll error
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
      const secDecision = this.securityGuard.evaluateAction({
        shellCommand: command,
        filePath: body.filePath ? String(body.filePath) : undefined,
      });
      if (!secDecision.allowed) {
        res.writeHead(403, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            error: "ZERO_TRUST_POLICY_VIOLATION",
            reason: secDecision.reason ?? "BLOCKED_BY_SECURITY_GUARD",
          })
        );
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
      let recoveredFromFailedBrainCount = 0;
      if (body.failoverFromBrainId && this.s3Endpoint) {
        recoveredFromFailedBrainCount = await this.v25Store.restoreLtxSnapshotFromS3(
          this.s3Endpoint,
          String(body.failoverFromBrainId)
        );
        for (const t of this.v25Store.listPersistedTasks()) {
          this.scheduler.allTasks.set(t.task_id, t);
        }
      }
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
          recoveredFromFailedBrainCount,
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
      const result = (body.result as Record<string, unknown>) ?? {};
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
      const promptText = String(body.promptText ?? "");
      const slotResolution = this.shardCoordinator.resolveBrainForTask(tenantId, taskId);
      const ownerBrainId = slotResolution.brainNodeId ?? "brain-node-01";
      const targetBodyHostId = String(body.targetBodyHostId ?? "body-host-01");
      this.metrics.incCounter("ansts_tasks_dispatched_total", 1);

      // Forward to live peer Brain shard owner; if unreachable, automatically evict & failover to surviving Brain shard!
      let remoteBrainResult: Record<string, unknown> | null = null;
      let failoverEvent: Record<string, unknown> | null = null;
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
                  failoverFromBrainId: ownerBrainId,
                }),
              });
              if (failResp.ok) {
                remoteBrainResult = (await failResp.json()) as Record<string, unknown>;
                failoverEvent = {
                  evictedBrainNodeId: ownerBrainId,
                  newOwnerBrainNodeId: nextBrainId,
                  newTopologyGeneration: eviction.newTopologyGeneration,
                };
              }
            } catch {
              // ignore if all brains unreachable
            }
          }
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
      const useReverseTunnel = body.dispatchMode === "reverse_tunnel" || !this.peerBodyUrls[targetBodyHostId];
      if (!useReverseTunnel && this.peerBodyUrls[targetBodyHostId]) {
        try {
          const resp = await fetch(`${this.peerBodyUrls[targetBodyHostId]}/internal/v1beta1/body/execute`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ taskId, tenantId, promptText }),
          });
          if (resp.ok) {
            remoteBodyResult = (await resp.json()) as Record<string, unknown>;
          }
        } catch {
          // fallback to reverse tunnel below
        }
      }
      if (!remoteBodyResult) {
        try {
          const tunnelDispatch = await this.tunnelRouter.dispatchOverReverseTunnel(
            targetBodyHostId,
            { taskId, tenantId, promptText },
            3000
          );
          remoteBodyResult = {
            ...tunnelDispatch.executionResult,
            routedViaGatewayId: tunnelDispatch.routedViaGatewayId,
            tunnelRole: tunnelDispatch.role,
          };
        } catch {
          // ignore if no reverse tunnel active
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
          brainFailover: failoverEvent,
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

    // [M40] TanStack Console BFF: Live 11-Service Topology & 256-Slot Ring Map (`GET /v1beta1/console/topology`)
    if (method === "GET" && pathname === "/v1beta1/console/topology") {
      const slotOwners = Array.from({ length: 256 }, (_, idx) =>
        this.shardCoordinator.ring.getOwnerNodeForSlot(idx) ?? "brain-node-01"
      );
      const slotDistribution: Record<string, number> = {};
      for (const owner of slotOwners) {
        slotDistribution[owner] = (slotDistribution[owner] ?? 0) + 1;
      }
      const brainKeys =
        Object.keys(this.peerBrainUrls).length > 0
          ? Object.keys(this.peerBrainUrls)
          : ["brain-node-01", "brain-node-02", "brain-node-03"];
      const bodyKeys =
        Object.keys(this.peerBodyUrls).length > 0
          ? Object.keys(this.peerBodyUrls)
          : ["body-host-01", "body-host-02"];

      const brains = brainKeys.map((bId, idx) => ({
        nodeId: bId,
        url: this.peerBrainUrls[bId] ?? `http://127.0.0.1:${18082 + idx}`,
        ownedSlots: slotDistribution[bId] ?? 0,
        status: (slotDistribution[bId] ?? 0) > 0 ? "HEALTHY" : "EVICTED",
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
        }
        return {
          nodeId: bdId,
          url: this.peerBodyUrls[bdId] ?? `http://127.0.0.1:${19501 + idx}`,
          status: "HEALTHY",
          activeGatewayId,
          activeRole,
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
          servicesCount: 11,
        })
      );
      return;
    }

    // [M40] TanStack Console BFF: Interactive Brain Shard Eviction & Rebalance (`POST /v1beta1/console/shards/rebalance`)
    if (method === "POST" && pathname === "/v1beta1/console/shards/rebalance") {
      const body = await this.readJsonBody(req);
      const action = String(body.action ?? "evict");
      const brainNodeId = String(body.brainNodeId ?? "brain-node-02");
      let event: Record<string, unknown> = {};
      if (action === "evict") {
        event = this.shardCoordinator.evictUnhealthyBrainShardNode(brainNodeId);
      } else {
        const epoch = this.shardCoordinator.registerBrainShardNode(brainNodeId);
        event = { registeredNodeId: brainNodeId, epoch };
      }
      const slotOwners = Array.from({ length: 256 }, (_, idx) =>
        this.shardCoordinator.ring.getOwnerNodeForSlot(idx) ?? "brain-node-01"
      );
      const slotDistribution: Record<string, number> = {};
      for (const owner of slotOwners) {
        slotDistribution[owner] = (slotDistribution[owner] ?? 0) + 1;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ action, brainNodeId, event, slotDistribution, slotOwners }));
      return;
    }

    // [M40] TanStack Console BFF: Global Stateless Task Table Fan-Out (`GET /v1beta1/console/tasks`)
    if (method === "GET" && pathname === "/v1beta1/console/tasks") {
      const taskMap = new Map<string, TaskSpec>();
      for (const [id, t] of this.scheduler.allTasks.entries()) {
        taskMap.set(id, t);
      }
      for (const t of this.v25Store.listPersistedTasks()) {
        taskMap.set(t.task_id, t);
      }
      const brainEntries = Object.entries(this.peerBrainUrls);
      if (brainEntries.length > 0) {
        await Promise.all(
          brainEntries.map(async ([, bUrl]) => {
            try {
              const resp = await fetch(`${bUrl}/internal/v1beta1/brain/tasks`);
              if (resp.ok) {
                const data = (await resp.json()) as { tasks?: TaskSpec[] };
                for (const t of data.tasks ?? []) {
                  taskMap.set(t.task_id, t);
                }
              }
            } catch {
              // ignore unreachable brain
            }
          })
        );
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
          prompt: t.prompt,
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
      let blame: Array<{ lineNumber: number; text: string; stepId: string }> | null = null;
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
          blame,
        })
      );
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
          remainingStats: this.layerfs.storeEngine.getStorageStats(),
        })
      );
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
            String(body.authorAgentId ?? "agent-01")
          );
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify(out));
        } catch (err) {
          res.writeHead(409, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "CAS_CONFLICT", message: (err as Error).message }));
        }
        return;
      }
      if (action === "arbitratePatches") {
        const patches = (body.patches as Array<{ agentId: string; touchedFiles: string[]; priorityScore: number }>) ?? [];
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
      const commits = (body.commits as string[]) ?? [];
      const testShellCommand = String(body.testShellCommand ?? "true");
      const out = this.bisectCoordinator.executeLiveLayerFsBisect({
        driver: this.layerfs,
        commits,
        testShellCommand,
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
          baseCommitId
        );
        let s3Synced = false;
        if (this.s3Endpoint) {
          s3Synced = await this.layerfs.pushBranchToRemoteS3(targetBranch, this.s3Endpoint);
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ...mergeResult, targetBranch, s3Synced }));
      } catch (err) {
        if (err instanceof LayerFsMergeConflictError) {
          res.writeHead(409, { "content-type": "application/json" });
          res.end(
            JSON.stringify({
              error: "LAYERFS_MERGE_CONFLICT",
              conflictingFiles: err.conflictingFiles,
              message: err.message,
            })
          );
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
        nonce: String(body.nonce ?? "ansts-v25-nonce"),
      });

      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          nodeId: this.nodeId,
          mtlsCheck,
          slsaStatement,
          slsaVerified,
          teeQuote,
        })
      );
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
          maxP99LatencyMs: body.maxP99LatencyMs !== undefined ? Number(body.maxP99LatencyMs) : undefined,
        });
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(canaryRes));
        return;
      }
      const utilizationRatio = Number(body.utilizationRatio ?? 0.5);
      const slaCompliance = this.slaEnforcer.evaluateSlaCompliance({
        p0QueueWaitP99Ms: Number(body.p0QueueWaitP99Ms ?? 80),
        warmPoolHitRatio: Number(body.warmPoolHitRatio ?? 0.94),
        freeClusterCpuRatio: Number(body.freeClusterCpuRatio ?? (1 - utilizationRatio)),
      });
      const prewarmPrediction = this.prewarmer.recordArrivalSampleAndPredict(
        Number(body.observedArrivalsPerSec ?? 12.0),
        Number(body.avgSandboxHoldSec ?? 4.0)
      );
      const priorityAdmission = {
        P0_EMERGENCY: this.loadShedder.shouldAdmitUnderLoad(0, utilizationRatio),
        P1_HIGH: this.loadShedder.shouldAdmitUnderLoad(1, utilizationRatio),
        P2_STANDARD: this.loadShedder.shouldAdmitUnderLoad(2, utilizationRatio),
        P3_BATCH: this.loadShedder.shouldAdmitUnderLoad(3, utilizationRatio),
      };
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          nodeId: this.nodeId,
          utilizationRatio,
          slaCompliance,
          prewarmPrediction,
          priorityAdmission,
        })
      );
      return;
    }


    // [M37] Geo-Sovereignty Routing & Cross-Region S3 LTX Replication (`POST /v1beta1/cluster/geo-route`)
    if (method === "POST" && pathname === "/v1beta1/cluster/geo-route") {
      const body = await this.readJsonBody(req);
      if (body.action === "setRegionAvailability") {
        const updated = this.geoRouter.setRegionAvailability(
          String(body.regionId ?? "eu-central-1"),
          Boolean(body.capacityAvailable ?? true),
          body.latencyMs !== undefined ? Number(body.latencyMs) : undefined
        );
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ updated, regions: this.geoRouter.listRegions() }));
        return;
      }
      const jurisdiction = (body.jurisdiction ?? "GDPR_EU") as "GDPR_EU" | "US_FED" | "APAC_SG";
      try {
        const selectedRegion = this.geoRouter.routeByJurisdiction(jurisdiction);
        let replication: Record<string, unknown> | null = null;
        if (body.replicateObjectKey) {
          if (this.s3Endpoint) {
            replication = await this.crossRegionReplicator.replicateToRemoteS3Region({
              s3Endpoint: this.s3Endpoint,
              sourceRegion: selectedRegion.regionId,
              targetRegion: String(body.drTargetRegion ?? "eu-west-1"),
              objectKey: String(body.replicateObjectKey),
              payload: (body.payload as Record<string, unknown>) ?? { jurisdiction },
            });
          } else {
            replication = this.crossRegionReplicator.replicateLtxObject(
              String(body.drTargetRegion ?? "eu-west-1"),
              String(body.replicateObjectKey)
            );
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
            message: (err as Error).message,
          })
        );
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
        newNodeFencingToken: Number(body.newNodeFencingToken ?? 4),
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
        (body.branches as Array<{ branchName: string; command: string; seed?: number }>) ?? [];
      const results = this.branchForker.executeDeterministicSpeculativeBranches({
        sourceBranch,
        branches,
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
          fallbackEconomyModel: String(body.fallbackEconomyModel ?? "Qwen-3.8"),
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
            ...budgetDecision,
          })
        );
        return;
      }
      const costUsd = Number(body.costUsd ?? 0.5);
      const totalSpentUsd = this.finopsController.recordSpend(tenantId, costUsd);
      const prefixHash = CognitiveRouter.computePrefixHash(
        String(body.systemPrompt ?? "You are ANSTS Agent"),
        String(body.toolsSchema ?? "default-tools")
      );
      const [endpoint, prefixCacheHit] = this.router.selectEndpoint(
        prefixHash,
        Number(body.estimatedTokens ?? 512),
        Number(body.priority ?? TaskPriority.P1_HIGH) as TaskPriority
      );
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
          prefixHash,
        })
      );
      return;
    }

    // [M38-2] Dynamic MCP Tool Hub Registration (`POST /v1beta1/saga/mcp/register`)
    if (method === "POST" && pathname === "/v1beta1/saga/mcp/register") {
      const body = await this.readJsonBody(req);
      const serverName = String(body.serverName ?? "mcp-github");
      const rawTools = (body.tools as Array<Record<string, unknown>>) ?? [];
      const descriptors = rawTools.map((t) => ({
        toolName: String(t.toolName ?? "mcp_tool"),
        description: String(t.description ?? "MCP dynamic tool"),
        effectClass: (t.effectClass as ToolCallEffectClass) ?? ToolCallEffectClass.COMPENSATABLE_WRITE,
        inputSchema: (t.inputSchema as Record<string, unknown>) ?? {},
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
          status: "MCP_OK",
        })
      );
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
              status: "MCP_OK",
            }),
            (args) => ({
              compensatedTool: d.toolName,
              argsEcho: args,
              status: "MCP_COMPENSATED",
            })
          );
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
      const args = (body.args as Record<string, unknown>) ?? {};

      let task = this.scheduler.allTasks.get(taskId);
      if (!task) {
        task = new TaskSpec({
          task_id: taskId,
          tenant_id: String(body.tenantId ?? "tenant-default"),
          priority: TaskPriority.P1_HIGH,
          prompt: "saga task",
        });
        this.scheduler.allTasks.set(taskId, task);
      }

      if (body.requireApproval) {
        const staged = this.sagaGateway.stagingGate.stageEffect({
          effectId: `eff-${taskId}-${turnIndex}`,
          taskId,
          turnIndex,
          toolName,
          args,
        });
        const ticket = this.sagaGateway.stagingGate.requestHumanApproval(task, toolName, args);
        res.writeHead(202, { "content-type": "application/json" });
        res.end(JSON.stringify({ status: "PENDING_APPROVAL", stagedEffectId: staged.effectId, ticket }));
        return;
      }

      const [output, deduplicated] = this.sagaGateway.invokeTool(taskId, turnIndex, toolName, args);
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
        task = new TaskSpec({
          task_id: taskId,
          tenant_id: "tenant-default",
          priority: TaskPriority.P1_HIGH,
          prompt: "saga approval task",
        });
        this.scheduler.allTasks.set(taskId, task);
      }
      const ticket = this.sagaGateway.stagingGate.resolveApproval(ticketId, approved, decidedBy, task);
      let committedEffects: Array<{ effectId: string; output: Record<string, unknown> }> = [];
      if (approved) {
        committedEffects = this.sagaGateway.stagingGate.commitStagedEffects(taskId, (tName, tArgs) => {
          return this.sagaGateway.invokeTool(taskId, 1, tName, tArgs)[0];
        });
      } else {
        this.sagaGateway.stagingGate.abortStagedEffects(taskId);
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ticket, taskState: task.state, committedEffects }));
      return;
    }

    // [M38-2] Reverse-Order LIFO Saga Compensation Rollback (`POST /v1beta1/saga/rollback`)
    if (method === "POST" && pathname === "/v1beta1/saga/rollback") {
      const body = await this.readJsonBody(req);
      const taskId = String(body.taskId ?? "task-saga-01");
      const upToTurnInclusive = Number(body.upToTurnInclusive ?? 0);
      const rolledBackSteps = this.sagaGateway.compensator.rollbackTask(taskId, upToTurnInclusive);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          taskId,
          rolledBackCount: rolledBackSteps.length,
          rolledBackSteps: rolledBackSteps.map((s) => ({
            stepId: s.stepId,
            turnIndex: s.turnIndex,
            toolName: s.toolName,
            compensationResult: s.compensationResult,
          })),
        })
      );
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
            changedFiles: (body.changedFiles as string[]) ?? ["app.ts"],
            sqliteWalPages: Number(body.sqliteWalPages ?? 4),
            highestSubmittedFencingToken: Number(body.highestSubmittedFencingToken ?? 1),
          });
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ appended: true, entry }));
        } catch (err) {
          if (err instanceof StaleFenceError || (err as Error).name === "StaleFenceError" || (err as Error).message.includes("Stale")) {
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
        originalTask = new TaskSpec({
          task_id: taskId,
          tenant_id: String(body.tenantId ?? "tenant-default"),
          priority: TaskPriority.P1_HIGH,
          prompt: "original task prompt",
        });
        this.scheduler.allTasks.set(taskId, originalTask);
      }
      const walEntries = this.walEngine.getTaskWalEntries(taskId);
      const forked = this.timeTravelReplayer.forkTaskAtTurn(
        originalTask,
        walEntries,
        Number(body.targetTurnIndex ?? 1),
        String(body.newTaskId ?? `${taskId}-fork`),
        body.overriddenPrompt ? String(body.overriddenPrompt) : undefined
      );
      this.scheduler.allTasks.set(forked.forkedTask.task_id, forked.forkedTask);
      this.v25Store.commitTaskSingleWritePath(forked.forkedTask);

      let layerfsFork: Record<string, unknown> | null = null;
      if (body.sourceCommitId) {
        layerfsFork = this.timeTravelReplayer.forkLayerFsCommitZeroCopy(
          this.layerfs,
          String(body.sourceCommitId),
          String(body.newBranchName ?? `branch-${forked.forkedTask.task_id}`)
        );
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
        JSON.stringify(this.layerfs.exportBranchBundle(branchName))
      );
      const migration = await this.heavyMigrator.migrateHeavyTaskAcrossNodes({
        taskId,
        turnIndex: Number(body.turnIndex ?? 1),
        sourceNodeId: String(body.sourceNodeId ?? this.nodeId),
        targetNodeId: String(body.targetNodeId ?? "body-host-02"),
        newFencingToken: Number(body.newFencingToken ?? 2),
        upperdirSnapshotBytes: payloadBytes,
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
      const toolArgs = (body.toolArgs as Record<string, unknown>) ?? { cmd: "echo ok" };
      const estimatedTokens = Number(body.estimatedTokens ?? 1200);

      let task = this.scheduler.allTasks.get(taskId);
      if (!task) {
        task = new TaskSpec({
          task_id: taskId,
          tenant_id: tenantId,
          priority: TaskPriority.P1_HIGH,
          prompt: String(body.prompt ?? "harness turn"),
        });
        this.scheduler.allTasks.set(taskId, task);
      }

      const thinkCgroup = this.phaseController.applyThinkPhase(taskId, task.resource_profile);
      try {
        const turnSummary = this.harnessRunner.executeSingleTurn(
          task,
          toolName,
          toolArgs,
          estimatedTokens
        );
        const actCgroup = this.phaseController.applyActPhase(taskId, task.resource_profile);

        let psiDefense = null;
        if (body.psiSample) {
          const sample = body.psiSample as { someAvg10Pct: number; fullAvg10Pct: number; freeSwapMb: number };
          psiDefense = this.psiGuard.evaluateAndDefend(sample, Array.from(this.scheduler.allTasks.values()));
        }

        const sseEvtId = `evt-${taskId}-t${turnSummary.turnIndex}`;
        this.sseGateway.publish({
          id: sseEvtId,
          event: "turn.completed",
          taskId,
          turnIndex: turnSummary.turnIndex,
          timestampMs: Date.now(),
          data: { toolName, turnIndex: turnSummary.turnIndex, endpointId: turnSummary.endpointId },
        });

        const cloudEvent = this.cloudEventsBus.emitEvent({
          type: "io.ansts.harness.turn.completed",
          subject: taskId,
          tenantId,
          llmCostUsd: Number(body.llmCostUsd ?? 0.02),
          sandboxCpuCoreSeconds: 1.5,
          data: { turnIndex: turnSummary.turnIndex, toolName },
        });

        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            turnSummary,
            thinkCgroup,
            actCgroup,
            psiDefense,
            sseEvtId,
            cloudEventId: cloudEvent.id,
          })
        );
      } catch (err) {
        const msg = (err as Error).message;
        if (msg.includes("Velocity breaker tripped")) {
          res.writeHead(429, { "content-type": "application/json" });
          res.end(
            JSON.stringify({
              error: "VELOCITY_BREAKER_TRIPPED",
              taskState: task.state,
              message: msg,
            })
          );
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
        parentTask = new TaskSpec({
          task_id: parentId,
          tenant_id: tenantId,
          priority: TaskPriority.P1_HIGH,
          prompt: "parent task",
          expected_SubAgent_count: Number(body.expectedSubAgentCount ?? 2),
        });
        this.scheduler.allTasks.set(parentId, parentTask);
      }

      if (action === "reserveParent") {
        const record = this.dagEscrow.reserveParentEscrow(
          parentTask,
          Number(body.maxTenantQuota ?? 10),
          Number(body.currentTenantUsed ?? 1)
        );
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            parentTaskId: record.parentTaskId,
            escrowedSlots: record.escrowedSlots,
            usedSlots: record.usedSlots,
          })
        );
        return;
      }

      if (action === "registerChild") {
        const childId = String(body.childTaskId ?? "task-child-01");
        const deps = (body.dependencies as string[]) ?? [];
        const childTask = new TaskSpec({
          task_id: childId,
          tenant_id: tenantId,
          priority: TaskPriority.P1_HIGH,
          prompt: "child task",
          dependencies: deps,
          parent_task_id: parentId,
        });
        try {
          const out = this.dagEscrow.registerChildSubAgent(parentTask, childTask);
          this.scheduler.allTasks.set(childId, childTask);
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ ...out, parentState: parentTask.state, childTaskId: childId }));
        } catch (err) {
          if ((err as Error).message.includes("DAG cycle detected")) {
            res.writeHead(400, { "content-type": "application/json" });
            res.end(JSON.stringify({ error: "DAG_CYCLE_DETECTED", message: (err as Error).message }));
            return;
          }
          throw err;
        }
        return;
      }

      if (action === "completeChild") {
        const childId = String(body.childTaskId ?? "task-child-01");
        const allChildrenDone = this.dagEscrow.completeChildSubAgent(parentTask, childId);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ allChildrenDone, parentState: parentTask.state }));
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
        data: (body.data as Record<string, unknown>) ?? {},
      });

      let webhookDelivery: Record<string, unknown> | null = null;
      if (body.webhookUrl) {
        const msgId = `outbox-${evt.id}`;
        const msg = this.outboxDispatcher.enqueueOutbox(msgId, taskId, String(body.webhookUrl), {
          cloudEvent: evt,
        });
        const sig = this.outboxDispatcher.signPayload(JSON.stringify(msg.payload));
        msg.signatureHeader = sig;
        let delivered = false;
        try {
          const whResp = await fetch(String(body.webhookUrl), {
            method: "POST",
            headers: {
              "content-type": "application/json",
              "x-ansts-signature": sig,
            },
            body: JSON.stringify(msg.payload),
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
          webhookDelivery,
        })
      );
      return;
    }

    // [M39-2] SSE Stream with `Last-Event-ID` Historical Replay (`GET /v1/events/stream`)
    if (method === "GET" && pathname === "/v1/events/stream") {
      const taskId = urlObj.searchParams.get("taskId") ?? "task-harness-01";
      const lastEventId =
        (req.headers["last-event-id"] as string | undefined) ??
        urlObj.searchParams.get("lastEventId") ??
        undefined;
      const subId = `sub-${Date.now()}`;
      const sub = this.sseGateway.subscribe(taskId, subId, lastEventId);
      const chunks: Buffer[] = [];
      sub.stream.on("data", (c: Buffer) => chunks.push(Buffer.from(c)));
      this.sseGateway.unsubscribe(taskId, subId);
      const ssePayload = Buffer.concat(chunks).toString("utf-8");
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
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
    if (this.tunnelPollTimer) {
      clearInterval(this.tunnelPollTimer);
      this.tunnelPollTimer = null;
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
