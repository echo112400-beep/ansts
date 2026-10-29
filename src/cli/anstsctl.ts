/**
 * TASK-30: Unified Cluster Facade (AnstsClusterRuntime) and anstsctl CLI Tool.
 */

import { WalCompactorAndPagedRestorer } from "../checkpoint/compactor";
import { DeterministicTimeTravelReplayer } from "../checkpoint/time_travel";
import { TurnWalEngine } from "../checkpoint/wal_engine";
import { BidirectionalBackpressureController } from "../cluster/backpressure_loop";
import { CognitiveRouter } from "../cognitive_router";
import { ApiGatewayServer } from "../gateway/server";
import { LeaseCoordinator } from "../lease_coordinator";
import { NodeDaemon } from "../node_daemon";
import { BayesianTaskProfiler } from "../profiler";
import { MultiTenantQueueManager } from "../queue_manager";
import { SagaToolGateway } from "../saga_gateway";
import { CoreScheduler5D } from "../scheduler";
import { CloudEventsBus } from "../telemetry/cloudevents";

export class AnstsClusterRuntime {
  public readonly profiler = new BayesianTaskProfiler();
  public readonly queueMgr = new MultiTenantQueueManager();
  public readonly leaseCoord = new LeaseCoordinator();
  public readonly cogRouter = new CognitiveRouter(this.leaseCoord);
  public readonly sagaGw = new SagaToolGateway(this.leaseCoord);
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
    for (const idx of [1, 2, 3]) {
      this.scheduler.registerNode({ nodeId: `worker-us-west-0${idx}`, driver: "firecracker" });
    }
  }

  public clusterTopSnapshot(): Record<string, any> {
    const shares = this.scheduler.compute5dDominantShares();
    return {
      tenants_dominant_share: shares,
      active_tasks: this.scheduler.tasks.size,
      backpressure_stage: this.bpController.currentStage,
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
}

export function runCli(argv: string[]): Record<string, any> {
  const cluster = new AnstsClusterRuntime();
  if (argv[0] === "top") {
    return cluster.clusterTopSnapshot();
  }
  if (argv[0] === "chaos") {
    cluster.cogRouter.tripPoolBreaker("DeepSeek", 0.50);
    const stage = cluster.bpController.evaluate(
      { model429Rate: 0.50 },
      Array.from(cluster.scheduler.tasks.values()),
    );
    return { chaos: "deepseek-429", breaker: "OPEN", backpressure_stage: stage };
  }
  return {};
}
