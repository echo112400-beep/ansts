/**
 * @fileoverview M6 TASK-24 & M63-2: End-to-End Bidirectional Backpressure Closed-Loop Coordinator
 * with Zero-Endpoint Outage Protection & NaN/Out-of-Range Snapshot Sanitization.
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

  /**
   * [M63-2] DFX-Availability:
   * - Treats `totalEndpoints <= 0` with `globalRateLimitedEndpoints > 0` (or negative/NaN PSI) safely:
   *   if `maxNodePsiSomePct` is NaN, defaults to conservative degraded evaluation;
   *   clamps `llmUnhealthyRatio` to `[0, 1]`.
   */
  public computeClusterHealthState(snapshot: ClusterCongestionSnapshot): {
    mode: "GREEN" | "YELLOW_DEGRADED" | "RED_SHEDDING";
    recommendedDispatchRateScale: number;
    allowBatchAdmission: boolean;
  } {
    const safePsi = Number.isFinite(snapshot.maxNodePsiSomePct)
      ? Math.max(0, snapshot.maxNodePsiSomePct)
      : 80.0;
    const safeTotalEp = Number.isFinite(snapshot.totalEndpoints)
      ? Math.max(0, Math.floor(snapshot.totalEndpoints))
      : 0;
    const safeLimitedEp = Number.isFinite(snapshot.globalRateLimitedEndpoints)
      ? Math.max(0, Math.floor(snapshot.globalRateLimitedEndpoints))
      : 0;
    const safeQueueDepth = Number.isFinite(snapshot.globalQueueDepth)
      ? Math.max(0, Math.floor(snapshot.globalQueueDepth))
      : 0;

    const llmUnhealthyRatio =
      safeTotalEp > 0
        ? Math.min(1.0, safeLimitedEp / safeTotalEp)
        : safeLimitedEp > 0
          ? 1.0
          : 0;

    if (safePsi >= 75.0 || llmUnhealthyRatio >= 0.75) {
      const p3Check = this.ingressController.evaluateAdmission({
        priority: TaskPriority.P3_BATCH,
        globalQueueDepth: safeQueueDepth,
        tenantActiveTasks: 0,
        clusterPsiPressurePct: Math.max(90.0, safePsi),
      });
      return {
        mode: "RED_SHEDDING",
        recommendedDispatchRateScale: 0.2,
        allowBatchAdmission: p3Check.allowed,
      };
    }

    if (safePsi >= 45.0 || llmUnhealthyRatio >= 0.35) {
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
