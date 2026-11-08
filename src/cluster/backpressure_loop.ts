/**
 * @fileoverview M6 TASK-24: End-to-End Bidirectional Backpressure Closed-Loop Coordinator.
 */
import { IngressBackpressureController } from "../gateway/backpressure.js";
import { TaskPriority } from "../models.js";

export interface ClusterCongestionSnapshot {
  maxNodePsiSomePct: number;
  globalRateLimitedEndpoints: number;
  totalEndpoints: number;
  globalQueueDepth: number;
}

export class BidirectionalBackpressureController {
  private readonly ingressController: IngressBackpressureController;

  constructor(ingressController = new IngressBackpressureController()) {
    this.ingressController = ingressController;
  }

  public computeClusterHealthState(snapshot: ClusterCongestionSnapshot): {
    mode: "GREEN" | "YELLOW_DEGRADED" | "RED_SHEDDING";
    recommendedDispatchRateScale: number;
    allowBatchAdmission: boolean;
  } {
    const llmUnhealthyRatio =
      snapshot.totalEndpoints > 0
        ? snapshot.globalRateLimitedEndpoints / snapshot.totalEndpoints
        : 0;

    if (snapshot.maxNodePsiSomePct >= 75.0 || llmUnhealthyRatio >= 0.75) {
      const p3Check = this.ingressController.evaluateAdmission({
        priority: TaskPriority.P3_BATCH,
        globalQueueDepth: snapshot.globalQueueDepth,
        tenantActiveTasks: 0,
        clusterPsiPressurePct: 90.0,
      });
      return {
        mode: "RED_SHEDDING",
        recommendedDispatchRateScale: 0.2,
        allowBatchAdmission: p3Check.allowed,
      };
    }

    if (snapshot.maxNodePsiSomePct >= 45.0 || llmUnhealthyRatio >= 0.35) {
      return {
        mode: "YELLOW_DEGRADED",
        recommendedDispatchRateScale: 0.55,
        allowBatchAdmission: false,
      };
    }

    return {
      mode: "GREEN",
      recommendedDispatchRateScale: 1.0,
      allowBatchAdmission: true,
    };
  }
}
