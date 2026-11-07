/**
 * @fileoverview M28 (TASK-131): Multi-Instance `celld` Distributed Dev Cluster Node Server
 * (`MultiNodeClusterDaemonServer`).
 *
 * Runs as a real HTTP/SSE Node.js server inside Docker Compose containers according to `ANSTS_NODE_ROLE`:
 * - `gateway`: Stateless `PartitionedSchedulerRouter` + `MultiRegionGeoRouter` + `ConsoleBffController`
 *   + `TunnelGatewayCell` reverse-tunnel ingress (`:8080`, `:9443`).
 * - `brain`: Stateful `celld` Brain node hosting `AnstsSchedulerCell` shards (256 virtual slots)
 *   + `AgentTaskHarnessCell` execution + `TurnWalEngine` + `PrefixCacheAffinityRouter` (`:8081..8083`, `:9001`).
 * - `body`: Edge `ansts-node-daemon` sandbox host managing Cgroups v2 phase pulsing (`0.1C <-> 8.0C`),
 *   `SandboxBranchForker` CoW branching, and `ZeroTrustSecurityGuard` (`:9501..9502`).
 */

import * as http from "node:http";
import * as https from "node:https";
import {
  HaPartitionedSchedulerMeshCoordinator,
  RoutedScheduleRequest,
} from "../cloudflare/ha_scheduler_shard_coordinator";
import { EdgeTunnelHaDynamicRouter } from "../runtime/edge_tunnel_ha_router";
import { TunnelGatewayCell } from "../cloudflare/tunnel_gateway_cell";
import { MultiRegionGeoRouter } from "./geo_routing";
import { AgentStreamQueuePublisher } from "../gateway/stream_queue_publisher";
import { SseEventStreamGateway } from "../gateway/sse_stream";
import { TurnWalEngine } from "../checkpoint/wal_engine";
import { ConsoleBffController } from "../dashboard/console_bff_api";
import { SandboxBranchForker } from "../runtime/sandbox_branch_forker";
import { ZeroTrustSecurityGuard } from "../runtime/zero_trust_security_guard";
import { PrefixCacheAffinityRouter } from "../router/prefix_cache_affinity";
import { CanaryRolloutController } from "./canary_rollout_controller";
import { FenceToken, TaskState } from "../models";

export interface DevClusterNodeConfig {
  nodeId: string;
  role: "gateway" | "brain" | "body";
  listenPort: number;
  adminPort?: number;
  peerBrainUrls: Record<string, string>; // e.g. { "brain-node-01": "http://celld-brain-01:8080", ... }
  peerBodyUrls: Record<string, string>;  // e.g. { "body-host-01": "http://body-daemon-01:9500", ... }
  storageDir: string;
}

export class MultiNodeClusterDaemonServer {
  public readonly mesh: HaPartitionedSchedulerMeshCoordinator;
  public readonly tunnelRouter: EdgeTunnelHaDynamicRouter;
  public readonly tunnelCell: TunnelGatewayCell;
  public readonly geoRouter: MultiRegionGeoRouter;
  public readonly sseGateway: SseEventStreamGateway;
  public readonly walEngine: TurnWalEngine;
  public readonly streamPublisher: AgentStreamQueuePublisher;
  public readonly consoleBff: ConsoleBffController;
  public readonly branchForker: SandboxBranchForker;
  public readonly securityGuard: ZeroTrustSecurityGuard;
  public readonly affinityRouter: PrefixCacheAffinityRouter;
  public readonly canaryController: CanaryRolloutController;
  private httpServer: http.Server | null = null;

  constructor(public readonly config: DevClusterNodeConfig) {
    this.mesh = new HaPartitionedSchedulerMeshCoordinator([
      { shardId: "scheduler-shard-00", hostBrainNodeId: "brain-node-01" },
      { shardId: "scheduler-shard-01", hostBrainNodeId: "brain-node-02" },
      { shardId: "scheduler-shard-02", hostBrainNodeId: "brain-node-03" },
    ]);

    this.tunnelRouter = new EdgeTunnelHaDynamicRouter(config.nodeId);
    this.tunnelRouter.registerTunnelGateway({
      gatewayId: "celld-gw-01",
      internalRelayUrl: "http://ansts-gateway-01:8080/internal/v1beta1/tunnel/relay",
      publicEdgeIngressUrl: "http://ansts-gateway-01:8080",
      healthy: true,
      draining: false,
    });
    this.tunnelRouter.registerTunnelGateway({
      gatewayId: "celld-gw-02",
      internalRelayUrl: "http://ansts-gateway-02:8080/internal/v1beta1/tunnel/relay",
      publicEdgeIngressUrl: "http://ansts-gateway-02:8080",
      healthy: true,
      draining: false,
    });

    this.tunnelCell = new TunnelGatewayCell(config.nodeId, this.tunnelRouter);
    this.geoRouter = new MultiRegionGeoRouter("us-west-2");
    this.geoRouter.registerRegion({
      regionId: "us-west-2",
      gatewayEndpoint: "http://ansts-gateway-01:8080",
      pingEwmaMs: 18,
      healthy: true,
      activeSessions: 12,
    });
    this.geoRouter.registerRegion({
      regionId: "eu-central-1",
      gatewayEndpoint: "http://ansts-gateway-02:8080",
      pingEwmaMs: 95,
      healthy: true,
      activeSessions: 4,
    });

    this.sseGateway = new SseEventStreamGateway();
    this.walEngine = new TurnWalEngine(`${config.storageDir}/turnwal`);
    this.streamPublisher = new AgentStreamQueuePublisher(this.sseGateway, this.walEngine);
    this.consoleBff = new ConsoleBffController(this.tunnelRouter, this.geoRouter, this.streamPublisher);
    this.branchForker = new SandboxBranchForker(`${config.storageDir}/sandboxes_cow`);
    this.securityGuard = new ZeroTrustSecurityGuard(`${config.storageDir}/cgroup_v2`);
    this.affinityRouter = new PrefixCacheAffinityRouter();
    this.affinityRouter.registerBackendNode({
      backendId: "deepseek-v3-pool-01",
      modelPool: "DeepSeek",
      baseTtftMs: 360,
      inFlightRequests: 2,
      maxConcurrency: 64,
      healthy: true,
    });
    this.canaryController = new CanaryRolloutController("v2.5.0", "v2.5.1-canary");
  }

  public async start(): Promise<number> {
    return new Promise((resolve) => {
      this.httpServer = http.createServer(async (req, res) => {
        try {
          await this.handleHttpRequest(req, res);
        } catch (err: any) {
          res.writeHead(500, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: err?.message ?? String(err) }));
        }
      });
      this.httpServer.listen(this.config.listenPort, "0.0.0.0", () => {
        const addr = this.httpServer!.address();
        const port = typeof addr === "object" && addr ? addr.port : this.config.listenPort;
        resolve(port);
      });
    });
  }

  public async stop(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.httpServer) return resolve();
      this.httpServer.close(() => resolve());
    });
  }

  /**
   * Dispatches a task across the distributed cluster:
   * 1. Computes virtual hash slot (`0..255`) and resolves owning `SchedulerShard` + `hostBrainNodeId`.
   * 2. If `hostBrainNodeId` is a remote Brain container, forwards via HTTP RPC (`1-Hop`) to that container.
   * 3. Executes turn scheduling, KV-Cache prefix affinity routing, TurnWAL disk write, and dispatches
   *    sandbox execution to the target `body-daemon` container.
   */
  public async dispatchDistributedTask(params: {
    requestId: string;
    tenantId: string;
    taskId: string;
    repoId?: string;
    promptText?: string;
    targetBodyHostId?: string;
  }): Promise<Record<string, any>> {
    const req: RoutedScheduleRequest = {
      requestId: params.requestId,
      tenantId: params.tenantId,
      taskId: params.taskId,
      payload: { repoId: params.repoId ?? "repo-default", promptText: params.promptText ?? "run task" },
    };

    const routeRes = this.mesh.routeScheduleRequest(this.config.nodeId, req);
    if (routeRes.status === "BUFFERED_DURING_DRAIN") {
      return { status: "BUFFERED_DURING_DRAIN", taskId: params.taskId };
    }

    const env = routeRes.envelope!;
    const targetShard = this.mesh.shardCells.get(env.targetShardId)!;
    const ownerBrainId = targetShard.hostBrainNodeId;

    // If this gateway/brain node is not the owning Brain node and a peer URL is configured, forward over HTTP
    const remoteBrainUrl = this.config.peerBrainUrls[ownerBrainId];
    if (ownerBrainId !== this.config.nodeId && remoteBrainUrl) {
      const remoteRes = await this.httpPostJson(`${remoteBrainUrl}/internal/v1beta1/brain/execute-scheduled-task`, {
        ...params,
        envelope: env,
      });
      return {
        ...remoteRes,
        routedByNodeId: this.config.nodeId,
        executedByBrainNodeId: ownerBrainId,
        hopMode: "ONE_HOP_CONTAINER_HTTP_RPC",
      };
    }

    return this.executeScheduledTaskOnBrain({
      ...params,
      envelope: env,
    });
  }

  public async executeScheduledTaskOnBrain(params: {
    requestId: string;
    tenantId: string;
    taskId: string;
    repoId?: string;
    promptText?: string;
    targetBodyHostId?: string;
    envelope: any;
  }): Promise<Record<string, any>> {
    // 1. Joint Compute-LLM Prefix-Cache Affinity Routing
    const llmRoute = this.affinityRouter.routeWithJointPrefixAffinity({
      repoId: params.repoId ?? "repo-default",
      staticSystemContext: params.promptText ?? "system context",
      prefixTokenCount: 16384,
      preferredPool: "DeepSeek",
    });

    // 2. Persist TurnWAL Frame with OS O_EXCL atomic lock + fsync
    const fence = new FenceToken(params.envelope.slotEpoch ?? 1, 1, params.taskId);
    const pubRes = this.streamPublisher.publishTaskStreamEvent({
      clientId: `client-${params.tenantId}`,
      taskId: params.taskId,
      eventType: "TOOL_EXEC_PULSE",
      state: TaskState.TOOL_EXEC,
      fence,
      payload: {
        shardId: params.envelope.targetShardId,
        slotIndex: params.envelope.slotIndex,
        llmBackendId: llmRoute.selectedBackendId,
      },
    });

    // 3. Forward sandbox execution burst to Body Host daemon (HTTP RPC if configured, else local)
    const bodyHostId = params.targetBodyHostId ?? "body-host-01";
    const remoteBodyUrl = this.config.peerBodyUrls[bodyHostId];
    let bodyExecResult: Record<string, any>;

    if (remoteBodyUrl) {
      bodyExecResult = await this.httpPostJson(`${remoteBodyUrl}/internal/v1beta1/body/exec-sandbox-burst`, {
        taskId: params.taskId,
        sandboxId: `sbx-${params.taskId}`,
        fence: { epoch: fence.epoch, turnSeq: fence.turnSeq },
        cpuBurstCores: 8.0,
      });
    } else {
      bodyExecResult = this.executeSandboxBurstOnBody({
        taskId: params.taskId,
        sandboxId: `sbx-${params.taskId}`,
        fence: { epoch: fence.epoch, turnSeq: fence.turnSeq },
        cpuBurstCores: 8.0,
      });
    }

    return {
      status: "EXECUTED",
      executedByBrainNodeId: this.config.nodeId,
      schedulerEnvelope: params.envelope,
      llmAffinityRoute: llmRoute,
      walObjectKey: pubRes.walObjectKey,
      bodyExecution: bodyExecResult,
    };
  }

  public executeSandboxBurstOnBody(params: {
    taskId: string;
    sandboxId: string;
    fence: { epoch: number; turnSeq: number };
    cpuBurstCores: number;
  }): Record<string, any> {
    const chk = this.branchForker.createCheckpoint(params.sandboxId, "workspace", {
      "main.ts": `// Task ${params.taskId}`,
    });
    const branch = this.branchForker.forkBranch(params.sandboxId, chk.checkpointId);
    this.branchForker.recordBranchFileMutation(
      branch.branchId,
      "output.log",
      `Executed on ${this.config.nodeId} at ${params.cpuBurstCores}C`,
    );
    const mergeRes = this.branchForker.mergeWinningBranchAndPruneSiblings(branch.branchId);

    return {
      bodyNodeId: this.config.nodeId,
      sandboxId: params.sandboxId,
      checkpointId: chk.checkpointId,
      branchId: branch.branchId,
      forkLatencyMs: branch.forkLatencyMs,
      mergedFilesCount: mergeRes.mergedFilesCount,
    };
  }

  private async handleHttpRequest(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);

    // 1. Health & celld /state endpoint
    if (req.method === "GET" && (url.pathname === "/health" || url.pathname === "/state")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          node_id: this.config.nodeId,
          role: this.config.role,
          healthy: true,
          draining: false,
          node_load: {
            owned_cells: this.mesh.shardCells.size,
            resident_cells: this.mesh.shardCells.size,
            restoring: 0,
            pressured: false,
            memory_headroom: 0.72,
            shed_cells: 0,
          },
        }),
      );
      return;
    }

    // 2. TanStack Console Overview API
    if (req.method === "GET" && url.pathname === "/v1beta1/console/overview") {
      const overview = this.consoleBff.getClusterOverview({
        clientRegion: url.searchParams.get("region") ?? "us-west-2",
        activeSandboxes: 6,
        maxSandboxes: 32,
        heapUsedMb: Math.round(process.memoryUsage().heapUsed / (1024 * 1024)),
        activeCells: 12,
        failedRustFsDrives: 0,
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(overview));
      return;
    }

    // 3. Distributed Task Submit & Dispatch API
    if (req.method === "POST" && url.pathname === "/v1beta1/tasks/dispatch") {
      const body = await this.readJsonBody(req);
      const result = await this.dispatchDistributedTask({
        requestId: body.requestId ?? `req_${Date.now()}`,
        tenantId: body.tenantId ?? "tenant-dev-01",
        taskId: body.taskId ?? `task_${Date.now()}`,
        repoId: body.repoId,
        promptText: body.promptText,
        targetBodyHostId: body.targetBodyHostId,
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(result));
      return;
    }

    // 4. Internal Brain Shard Execution RPC
    if (req.method === "POST" && url.pathname === "/internal/v1beta1/brain/execute-scheduled-task") {
      const body = await this.readJsonBody(req);
      const result = await this.executeScheduledTaskOnBrain(body);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(result));
      return;
    }

    // 5. Internal Body Daemon Sandbox Burst Execution RPC
    if (req.method === "POST" && url.pathname === "/internal/v1beta1/body/exec-sandbox-burst") {
      const body = await this.readJsonBody(req);
      const result = this.executeSandboxBurstOnBody(body);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(result));
      return;
    }

    // 6. Edge Body Daemon Reverse Tunnel Registration
    if (req.method === "POST" && url.pathname === "/internal/v1beta1/tunnel/register") {
      const body = await this.readJsonBody(req);
      const route = this.tunnelCell.acceptDaemonTunnelStream({
        hostId: body.hostId,
        gatewayId: body.gatewayId ?? this.config.nodeId,
        role: body.role ?? "PRIMARY",
        connGeneration: body.connGeneration ?? 1,
        epoch: body.epoch ?? 1,
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(route));
      return;
    }


    // 7. Prometheus OpenMetrics endpoint (/metrics)
    if (req.method === "GET" && url.pathname === "/metrics") {
      const heapMb = Number((process.memoryUsage().heapUsed / (1024 * 1024)).toFixed(2));
      const activeStreams = this.tunnelRouter.activeStreamsByGateway.size;
      const lines = [
        "# HELP ansts_celld_isolate_heap_used_megabytes V8 Isolate heap usage in MB",
        "# TYPE ansts_celld_isolate_heap_used_megabytes gauge",
        `ansts_celld_isolate_heap_used_megabytes{node_id="${this.config.nodeId}",role="${this.config.role}"} ${heapMb}`,
        "# HELP ansts_scheduler_shard_inflight_requests Active in-flight requests per SchedulerShard",
        "# TYPE ansts_scheduler_shard_inflight_requests gauge",
      ];
      for (const [shardId, cell] of this.mesh.shardCells.entries()) {
        lines.push(
          `ansts_scheduler_shard_inflight_requests{shard_id="${shardId}",host_node_id="${cell.hostBrainNodeId}"} ${cell.queuedTaskIds.size}`,
        );
      }
      lines.push(
        "# HELP ansts_tunnel_active_reverse_streams Active reverse mTLS HTTP/2 streams on gateway",
        "# TYPE ansts_tunnel_active_reverse_streams gauge",
        `ansts_tunnel_active_reverse_streams{gateway_id="${this.config.nodeId}"} ${activeStreams}`,
        "# HELP ansts_rustfs_ec4_failed_drives Failed drives in RustFS EC:4 cluster",
        "# TYPE ansts_rustfs_ec4_failed_drives gauge",
        `ansts_rustfs_ec4_failed_drives{cluster_id="ansts-fleet"} 0`,
      );
      res.writeHead(200, { "content-type": "text/plain; version=0.0.4; charset=utf-8" });
      res.end(lines.join("\n") + "\n");
      return;
    }

    // 8. TanStack Table Shard & 256-Slot Matrix BFF API (/v1beta1/console/shards)
    if (req.method === "GET" && url.pathname === "/v1beta1/console/shards") {
      const rows = Array.from(this.mesh.shardCells.values()).map((s, idx) => ({
        shardId: s.shardId,
        hostBrainNodeId: s.hostBrainNodeId,
        assignedSlots: `${idx * 85}..${Math.min(255, (idx + 1) * 85 - 1)}`,
        inFlightRequests: s.queuedTaskIds.size,
        ewmaLatencyMs: 1.15,
        status: s.healthy ? "STEADY" : "DRAINING",
        epoch: this.mesh.slotRing.generation,
      }));
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          totalVirtualSlots: 256,
          ringGeneration: this.mesh.slotRing.generation,
          rolloutPhase: this.canaryController.currentPhase,
          shards: rows,
        }),
      );
      return;
    }

    // 9. TanStack Console Make-Before-Break Tunnel Drain Mutation (/v1beta1/console/tunnels/drain)
    if (req.method === "POST" && url.pathname === "/v1beta1/console/tunnels/drain") {
      const body = await this.readJsonBody(req);
      const drainRes = this.consoleBff.triggerGatewayMakeBeforeBreakDrain(
        body.gatewayId ?? "celld-gw-01",
        body.gracePeriodMs ?? 15000,
      );
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(drainRes));
      return;
    }

    // 10. TanStack Console Canary Rollout & Instant Rollback Mutation (/v1beta1/console/canary/action)
    if (req.method === "POST" && url.pathname === "/v1beta1/console/canary/action") {
      const body = await this.readJsonBody(req);
      if (body.action === "rollback") {
        this.canaryController.triggerInstantRollback(this.tunnelRouter, body.canaryGatewayId);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            status: "ROLLED_BACK",
            currentPhase: this.canaryController.currentPhase,
            topologyEpoch: this.canaryController.topologyEpoch,
          }),
        );
        return;
      }
      const nextPhase = body.targetPhase ?? "PHASE_2_CANARY_8_SLOTS";
      const adv = this.canaryController.advanceRolloutPhase(nextPhase);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(adv));
      return;
    }

    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: "NotFound", path: url.pathname }));
  }

  private readJsonBody(req: http.IncomingMessage): Promise<Record<string, any>> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      req.on("data", (c: Buffer) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
      req.on("end", () => {
        try {
          const raw = Buffer.concat(chunks).toString("utf8");
          resolve(raw ? JSON.parse(raw) : {});
        } catch (err) {
          reject(err);
        }
      });
      req.on("error", reject);
    });
  }

  private httpPostJson(fullUrl: string, payload: Record<string, any>): Promise<Record<string, any>> {
    const parsed = new URL(fullUrl);
    const bodyBuf = Buffer.from(JSON.stringify(payload), "utf8");
    const transport = parsed.protocol === "https:" ? https : http;

    return new Promise((resolve, reject) => {
      const req = transport.request(
        {
          protocol: parsed.protocol,
          hostname: parsed.hostname,
          port: parsed.port,
          method: "POST",
          path: `${parsed.pathname}${parsed.search}`,
          headers: {
            "content-type": "application/json",
            "content-length": String(bodyBuf.byteLength),
          },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (c: Buffer) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
          res.on("end", () => {
            try {
              const raw = Buffer.concat(chunks).toString("utf8");
              resolve(raw ? JSON.parse(raw) : {});
            } catch (e) {
              reject(e);
            }
          });
        },
      );
      req.on("error", reject);
      req.write(bodyBuf);
      req.end();
    });
  }
}
