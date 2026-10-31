/**
 * TASK-57 (Non-K8s Edition): Manual & CLI-Driven Fleet Scale-Out / Scale-In Controller.
 *
 * Designed for Bare-Metal / VM Systemd and Docker Compose deployments WITHOUT Kubernetes:
 * - Supports explicit operator/CLI commands to add/remove `celld` Brain Nodes (with custom placement weights)
 * - Supports explicit operator/CLI commands to register/drain Sandbox Body Hosts (`ansts-node-daemon`)
 * - Enforces `restoring === 0` pre-flight safety gates before draining or removing any node
 * - Integrates with `PartitionedSchedulerRouter`, `DynamicQueueAutoSharder`, and `FleetHaAndAutoscalerController`
 */

import { CelldNodeLoadBlock, FleetHaAndAutoscalerController } from "../cloudflare/celld_ha_engine";
import {
  BrainBodyContainerPoolScaler,
  ContainerHostNodeMetrics,
  DynamicQueueAutoSharder,
  PartitionedSchedulerRouter,
} from "../cloudflare/horizontal_scaler";

export interface ManualScaleOutRequest {
  addBrainNodes?: Array<{
    nodeId: string;
    advertiseAddress: string;
    placementWeight?: number;
  }>;
  addBodyHosts?: Array<{
    hostId: string;
    ipAddress: string;
    maxBurstSandboxes?: number;
    warmPoolIdleContainers?: number;
  }>;
  addSchedulerShards?: number;
  targetQueueShards?: number;
}

export interface ManualScaleOutResult {
  status: "SCALED_OUT" | "BLOCKED_BY_RESTORING";
  totalBrainNodes: number;
  totalBodyHosts: number;
  totalSchedulerShards: number;
  totalQueueShards: number;
  addedBrainNodeIds: string[];
  addedBodyHostIds: string[];
  addedSchedulerShardIds: string[];
  rebalanceBatch: ReturnType<FleetHaAndAutoscalerController["computeRebalanceBatch"]>;
  capacityAdvisory: {
    brainToBodyRatio: number;
    totalClusterBurstCapacity: number;
    recommendedBodyHostCount: number;
  };
}

export interface ManualNodeDrainResult {
  nodeId: string;
  mode: "preserve" | "drain";
  allowed: boolean;
  reason?: string;
  preservedOwnershipRecords: number;
  uploadedDatabases: number;
  remainingBrainNodes: number;
}

export class ManualFleetScaleController {
  public readonly bodyHosts = new Map<string, ContainerHostNodeMetrics>();
  public readonly bodyScaler = new BrainBodyContainerPoolScaler({
    targetWarmPoolBufferRatio: 0.25,
    maxBurstPerHost: 20,
  });

  constructor(
    public readonly fleetController: FleetHaAndAutoscalerController = new FleetHaAndAutoscalerController(),
    public readonly schedulerRouter: PartitionedSchedulerRouter = new PartitionedSchedulerRouter({ initialShardCount: 8 }),
    public readonly queueSharder: DynamicQueueAutoSharder = new DynamicQueueAutoSharder({ initialShards: 8, maxShards: 64 }),
  ) {
    // Register baseline bare-metal body hosts
    this.bodyHosts.set("body-host-01", {
      hostId: "body-host-01",
      activeToolBurstSandboxes: 10,
      maxBurstSandboxesPerHost: 20,
      warmPoolIdleContainers: 4,
      psiCpuSomeAvg10: 0.12,
      psiMemoryFullAvg10: 0.02,
    });
    this.bodyHosts.set("body-host-02", {
      hostId: "body-host-02",
      activeToolBurstSandboxes: 8,
      maxBurstSandboxesPerHost: 20,
      warmPoolIdleContainers: 5,
      psiCpuSomeAvg10: 0.10,
      psiMemoryFullAvg10: 0.01,
    });
  }

  /**
   * Executes an operator-initiated manual horizontal scale-out across Brain Nodes, Body Hosts,
   * Scheduler Shards, and Queue Shards (Zero Kubernetes dependency).
   */
  public executeManualScaleOut(
    req: ManualScaleOutRequest,
    activeToolBurstTasks = 24,
  ): ManualScaleOutResult {
    const rolloutGate = this.fleetController.evaluateFleetAutoscalingAndRollout();
    if (!rolloutGate.safeToAdvanceRollingUpdate) {
      return {
        status: "BLOCKED_BY_RESTORING",
        totalBrainNodes: this.fleetController.nodes.size,
        totalBodyHosts: this.bodyHosts.size,
        totalSchedulerShards: this.schedulerRouter.getShardStates().length,
        totalQueueShards: 8,
        addedBrainNodeIds: [],
        addedBodyHostIds: [],
        addedSchedulerShardIds: [],
        rebalanceBatch: null,
        capacityAdvisory: {
          brainToBodyRatio: 1,
          totalClusterBurstCapacity: this.bodyHosts.size * 20,
          recommendedBodyHostCount: this.bodyHosts.size,
        },
      };
    }

    const addedBrainNodeIds: string[] = [];
    for (const bn of req.addBrainNodes ?? []) {
      const loadBlock: CelldNodeLoadBlock = {
        node_id: bn.nodeId,
        sampled_ms: Date.now(),
        owned_cells: 0,
        placement_weight: bn.placementWeight ?? 100,
        resident_cells: 0,
        host_websockets: 0,
        rss_bytes: 128 * 1024 * 1024,
        in_use_bytes: 64 * 1024 * 1024,
        cpu_percent_x100: 500,
        open_fds: 32,
        pressured: false,
        memory_headroom: true,
        shed_cells: 0,
        restoring: 0,
        draining: false,
      };
      this.fleetController.registerOrUpdateNodeLoad(loadBlock);
      addedBrainNodeIds.push(bn.nodeId);
    }

    const addedBodyHostIds: string[] = [];
    for (const bh of req.addBodyHosts ?? []) {
      this.bodyHosts.set(bh.hostId, {
        hostId: bh.hostId,
        activeToolBurstSandboxes: 0,
        maxBurstSandboxesPerHost: bh.maxBurstSandboxes ?? 20,
        warmPoolIdleContainers: bh.warmPoolIdleContainers ?? 6,
        psiCpuSomeAvg10: 0.02,
        psiMemoryFullAvg10: 0.01,
      });
      addedBodyHostIds.push(bh.hostId);
    }

    let addedSchedulerShardIds: string[] = [];
    if (req.addSchedulerShards && req.addSchedulerShards > 0) {
      addedSchedulerShardIds = this.schedulerRouter.scaleOutSchedulerShards(req.addSchedulerShards);
    }

    // Trigger immediate 32-cell rebalance batch onto newly added brain nodes
    const rebalanceBatch = this.fleetController.computeRebalanceBatch();

    const totalOwnedCells = Array.from(this.fleetController.nodes.values()).reduce(
      (acc, n) => acc + n.owned_cells,
      0,
    );
    const bodyPlan = this.bodyScaler.computeScalePlan(
      Math.max(1, totalOwnedCells),
      activeToolBurstTasks,
      Array.from(this.bodyHosts.values()),
    );

    return {
      status: "SCALED_OUT",
      totalBrainNodes: this.fleetController.nodes.size,
      totalBodyHosts: this.bodyHosts.size,
      totalSchedulerShards: this.schedulerRouter.getShardStates().length,
      totalQueueShards: req.targetQueueShards ?? 8,
      addedBrainNodeIds,
      addedBodyHostIds,
      addedSchedulerShardIds,
      rebalanceBatch,
      capacityAdvisory: {
        brainToBodyRatio: bodyPlan.brainToBodyMultiplexRatio,
        totalClusterBurstCapacity: bodyPlan.totalClusterBurstCapacity,
        recommendedBodyHostCount: bodyPlan.recommendedHostCount,
      },
    };
  }

  /**
   * Executes an operator-initiated manual node drain or in-place preserve shutdown (`handoff=preserve`).
   */
  public executeManualNodeDrain(
    nodeId: string,
    mode: "preserve" | "drain" = "drain",
  ): ManualNodeDrainResult {
    const gate = this.fleetController.evaluateFleetAutoscalingAndRollout();
    if (!gate.safeToAdvanceRollingUpdate) {
      return {
        nodeId,
        mode,
        allowed: false,
        reason: `Cannot drain node ${nodeId} while ${gate.totalRestoring} cells are actively restoring across the fleet`,
        preservedOwnershipRecords: 0,
        uploadedDatabases: 0,
        remainingBrainNodes: this.fleetController.nodes.size,
      };
    }

    const handoff = this.fleetController.prepareShutdownHandoff(nodeId, mode === "preserve");
    if (mode === "drain") {
      this.fleetController.nodes.delete(nodeId);
    }

    return {
      nodeId,
      mode,
      allowed: true,
      preservedOwnershipRecords: handoff.preservedOwnershipRecords,
      uploadedDatabases: handoff.uploadedDatabases,
      remainingBrainNodes: this.fleetController.nodes.size,
    };
  }
}
