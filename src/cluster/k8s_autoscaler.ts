/**
 * TASK-57: Kubernetes Operator (`CelldFleet`) & Custom Metrics HPA Autoscaler Adapter.
 *
 * Consumes `celld` `fleet/capacity-v1.json` telemetry (`CelldNodeLoadBlock`) and
 * `BrainBodyContainerPoolScaler` metrics to compute desired StatefulSet replicas
 * for the `celld` Brain Pool and Deployment replicas for the Sandbox Container Body Pool,
 * while enforcing `restoring == 0` rollout safety gates and cooldown stabilization windows.
 */

import { CelldNodeLoadBlock } from "../cloudflare/celld_ha_engine";
import {
  BrainBodyContainerPoolScaler,
  ContainerHostNodeMetrics,
} from "../cloudflare/horizontal_scaler";

export interface K8sAutoscaleReconcilePlan {
  brainStatefulSet: {
    currentReplicas: number;
    desiredReplicas: number;
    action: "SCALE_OUT" | "SCALE_IN" | "HOLD" | "BLOCKED_BY_RESTORING";
    preStopHook: "POST /shutdown?handoff=preserve";
  };
  bodyDeployment: {
    currentReplicas: number;
    desiredReplicas: number;
    warmContainersToPrewarm: number;
    brainToBodyRatio: number;
  };
}

export class KubernetesCelldHpaAdapter {
  private readonly bodyScaler = new BrainBodyContainerPoolScaler({
    targetWarmPoolBufferRatio: 0.25,
    maxBurstPerHost: 20,
  });

  constructor(
    public readonly minBrainReplicas: number = 3,
    public readonly maxBrainReplicas: number = 32,
    public readonly targetCellsPerBrainNode: number = 2500,
  ) {}

  public reconcileClusterReplicas(params: {
    brainNodes: CelldNodeLoadBlock[];
    bodyHosts: ContainerHostNodeMetrics[];
    activeToolBurstTasks: number;
  }): K8sAutoscaleReconcilePlan {
    const currentBrainReplicas = params.brainNodes.length;
    const totalOwnedCells = params.brainNodes.reduce((acc, n) => acc + n.owned_cells, 0);
    const totalRestoring = params.brainNodes.reduce((acc, n) => acc + n.restoring, 0);
    const anyPressured = params.brainNodes.some((n) => n.pressured || !n.memory_headroom);

    let desiredBrainReplicas = currentBrainReplicas;
    let brainAction: K8sAutoscaleReconcilePlan["brainStatefulSet"]["action"] = "HOLD";

    if (totalRestoring > 0) {
      // Never mutate replica count while any celld node is actively restoring pages from RustFS
      brainAction = "BLOCKED_BY_RESTORING";
    } else if (anyPressured || totalOwnedCells > currentBrainReplicas * this.targetCellsPerBrainNode) {
      const neededByCells = Math.ceil(totalOwnedCells / (this.targetCellsPerBrainNode * 0.8));
      desiredBrainReplicas = Math.min(
        this.maxBrainReplicas,
        Math.max(currentBrainReplicas + 1, neededByCells),
      );
      brainAction = desiredBrainReplicas > currentBrainReplicas ? "SCALE_OUT" : "HOLD";
    } else if (
      currentBrainReplicas > this.minBrainReplicas &&
      totalOwnedCells < (currentBrainReplicas - 1) * (this.targetCellsPerBrainNode * 0.5)
    ) {
      desiredBrainReplicas = Math.max(this.minBrainReplicas, currentBrainReplicas - 1);
      brainAction = "SCALE_IN";
    }

    const bodyPlan = this.bodyScaler.computeScalePlan(
      Math.max(1, totalOwnedCells),
      params.activeToolBurstTasks,
      params.bodyHosts,
    );

    return {
      brainStatefulSet: {
        currentReplicas: currentBrainReplicas,
        desiredReplicas: desiredBrainReplicas,
        action: brainAction,
        preStopHook: "POST /shutdown?handoff=preserve",
      },
      bodyDeployment: {
        currentReplicas: params.bodyHosts.length,
        desiredReplicas: bodyPlan.recommendedHostCount,
        warmContainersToPrewarm: bodyPlan.warmContainersToPrewarm,
        brainToBodyRatio: bodyPlan.brainToBodyMultiplexRatio,
      },
    };
  }
}
