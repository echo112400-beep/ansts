/**
 * TASK-30, TASK-51 & M12-M13 Unified Cluster Facade (`AnstsClusterRuntime`) and `anstsctl` CLI Tool.
 * Supports Non-K8s Bare-Metal / Systemd / Docker Compose manual scale-out (`anstsctl fleet scale-out | scale-in`).
 */

import { WalCompactorAndPagedRestorer } from "../checkpoint/compactor";
import { DeterministicTimeTravelReplayer } from "../checkpoint/time_travel";
import { TurnWalEngine } from "../checkpoint/wal_engine";
import { FleetHaAndAutoscalerController } from "../cloudflare/celld_ha_engine";
import { MultiRegionFederationRouter } from "../cloudflare/federation_router";
import { PartitionedSchedulerRouter } from "../cloudflare/horizontal_scaler";
import { BidirectionalBackpressureController } from "../cluster/backpressure_loop";
import { ManualFleetScaleController } from "../cluster/manual_scale_controller";
import { CognitiveRouter } from "../cognitive_router";
import { FinOpsBudgetController } from "../finops/budget_controller";
import { ApiGatewayServer } from "../gateway/server";
import { LeaseCoordinator } from "../lease_coordinator";
import { NodeDaemon } from "../node_daemon";
import { BayesianTaskProfiler } from "../profiler";
import { MultiTenantQueueManager } from "../queue_manager";
import { McpDynamicToolRegistry } from "../saga/mcp_registry";
import { SagaToolGateway } from "../saga_gateway";
import { CoreScheduler5D } from "../scheduler";
import { CloudEventsBus } from "../telemetry/cloudevents";
import { OtelAndPrometheusExporter } from "../telemetry/otel_prometheus";

export class AnstsClusterRuntime {
  public readonly profiler = new BayesianTaskProfiler();
  public readonly queueMgr = new MultiTenantQueueManager();
  public readonly leaseCoord = new LeaseCoordinator();
  public readonly cogRouter = new CognitiveRouter(this.leaseCoord);
  public readonly sagaGw = new SagaToolGateway(this.leaseCoord);
  public readonly mcpRegistry = new McpDynamicToolRegistry(this.sagaGw);
  public readonly finopsController = new FinOpsBudgetController();
  public readonly otelExporter = new OtelAndPrometheusExporter();
  public readonly federationRouter = new MultiRegionFederationRouter();
  public readonly schedulerRouter = new PartitionedSchedulerRouter({ initialShardCount: 8 });
  public readonly fleetController = new FleetHaAndAutoscalerController();
  public readonly manualScaleController = new ManualFleetScaleController(
    this.fleetController,
    this.schedulerRouter,
  );
  public readonly scheduler = new CoreScheduler5D(
    this.queueMgr,
    this.profiler,
    this.leaseCoord,
    this.cogRouter,
  );
  public readonly nodeDaemon = new NodeDaemon(this.leaseCoord, this.cogRouter, this.sagaGw);
  public readonly apiGateway = new ApiGatewayServer(this.scheduler);
  public readonly walEngine = new TurnWalEngine();
  public readonly compactor = new WalCompactorAndPagedRestorer();
  public readonly replayer = new DeterministicTimeTravelReplayer();
  public readonly eventsBus = new CloudEventsBus();
  public readonly bpController = new BidirectionalBackpressureController();

  constructor() {
    this.scheduler.registerTenant("fin-enterprise", 4.0);
    this.scheduler.registerTenant("codegen-pro", 2.0);
    this.scheduler.registerTenant("research-lab", 1.0);
    this.finopsController.registerTenantBudget("fin-enterprise", 500.0);
    this.finopsController.registerTenantBudget("codegen-pro", 250.0);
    this.finopsController.registerTenantBudget("research-lab", 100.0);

    for (const idx of [1, 2, 3]) {
      this.scheduler.registerNode({ nodeId: `worker-us-west-0${idx}`, driver: "firecracker" });
    }
    this.fleetController.registerOrUpdateNodeLoad({
      node_id: "celld-node-01",
      sampled_ms: Date.now(),
      owned_cells: 420,
      placement_weight: 100,
      resident_cells: 85,
      host_websockets: 12,
      rss_bytes: 256 * 1024 * 1024,
      in_use_bytes: 128 * 1024 * 1024,
      cpu_percent_x100: 2500,
      open_fds: 64,
      pressured: false,
      memory_headroom: true,
      shed_cells: 0,
      restoring: 0,
      draining: false,
    });
    this.fleetController.registerOrUpdateNodeLoad({
      node_id: "celld-node-02",
      sampled_ms: Date.now(),
      owned_cells: 180,
      placement_weight: 100,
      resident_cells: 40,
      host_websockets: 8,
      rss_bytes: 192 * 1024 * 1024,
      in_use_bytes: 96 * 1024 * 1024,
      cpu_percent_x100: 1500,
      open_fds: 48,
      pressured: false,
      memory_headroom: true,
      shed_cells: 0,
      restoring: 0,
      draining: false,
    });
  }

  public clusterTopSnapshot(): Record<string, any> {
    const shares = this.scheduler.compute5dDominantShares();
    return {
      tenants_dominant_share: shares,
      active_tasks: this.scheduler.tasks.size,
      backpressure_stage: this.bpController.currentStage,
      scheduler_shards_count: this.schedulerRouter.getShardStates().length,
      model_pools: {
        DeepSeek: {
          state: this.cogRouter.pools.DeepSeek.breakerState,
          ttft_sec: this.cogRouter.pools.DeepSeek.ttftSec,
        },
        "GLM-5.3": {
          state: this.cogRouter.pools["GLM-5.3"].breakerState,
          ttft_sec: this.cogRouter.pools["GLM-5.3"].ttftSec,
        },
        "Qwen-3.8": {
          state: this.cogRouter.pools["Qwen-3.8"].breakerState,
          ttft_sec: this.cogRouter.pools["Qwen-3.8"].ttftSec,
        },
      },
    };
  }

  public exportPrometheusMetricsText(): string {
    const snap = this.clusterTopSnapshot();
    return this.otelExporter.renderPrometheusMetrics({
      tenantDominantShares: snap.tenants_dominant_share,
      schedulerShards: this.schedulerRouter.getShardStates(),
      activeQueueShards: 8,
      modelPools: {
        DeepSeek: {
          state: snap.model_pools.DeepSeek.state,
          ttftSec: snap.model_pools.DeepSeek.ttft_sec,
        },
        "GLM-5.3": {
          state: snap.model_pools["GLM-5.3"].state,
          ttftSec: snap.model_pools["GLM-5.3"].ttft_sec,
        },
        "Qwen-3.8": {
          state: snap.model_pools["Qwen-3.8"].state,
          ttftSec: snap.model_pools["Qwen-3.8"].ttft_sec,
        },
      },
    });
  }
}

export function runCli(argv: string[]): Record<string, any> {
  const cluster = new AnstsClusterRuntime();
  if (argv[0] === "top") {
    return cluster.clusterTopSnapshot();
  }
  if (argv[0] === "metrics") {
    return { prometheus_text: cluster.exportPrometheusMetricsText() };
  }
  if (argv[0] === "chaos") {
    cluster.cogRouter.tripPoolBreaker("DeepSeek", 0.50);
    const stage = cluster.bpController.evaluate(
      { model429Rate: 0.50 },
      Array.from(cluster.scheduler.tasks.values()),
    );
    return { chaos: "deepseek-429", breaker: "OPEN", backpressure_stage: stage };
  }
  if (argv[0] === "fleet") {
    const sub = argv[1] ?? "status";
    if (sub === "status") {
      const totalOwnedCells = Array.from(cluster.fleetController.nodes.values()).reduce(
        (acc, n) => acc + n.owned_cells,
        0,
      );
      return {
        totalOwnedCells,
        ...cluster.fleetController.evaluateFleetAutoscalingAndRollout(),
      };
    }
    if (sub === "rebalance") {
      return cluster.fleetController.computeRebalanceBatch() ?? { movedCells: 0 };
    }
    if (sub === "rolling-restart") {
      const gate = cluster.fleetController.evaluateFleetAutoscalingAndRollout();
      const handoff = cluster.fleetController.prepareShutdownHandoff("celld-node-01", true);
      return { rolling_gate: gate, preserve_handoff: handoff };
    }
    if (sub === "scale-out") {
      const nodeId = argv[2] ?? "celld-node-03";
      const bodyId = argv[3] ?? "body-host-03";
      return cluster.manualScaleController.executeManualScaleOut({
        addBrainNodes: [{ nodeId, advertiseAddress: "10.240.16.13:9001", placementWeight: 100 }],
        addBodyHosts: [{ hostId: bodyId, ipAddress: "10.240.32.13", maxBurstSandboxes: 20 }],
        addSchedulerShards: 4,
      });
    }
    if (sub === "scale-in") {
      const nodeId = argv[2] ?? "celld-node-02";
      const mode = (argv[3] as "preserve" | "drain") ?? "drain";
      return cluster.manualScaleController.executeManualNodeDrain(nodeId, mode);
    }
  }
  return {};
}
