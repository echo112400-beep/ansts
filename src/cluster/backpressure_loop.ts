/**
 * TASK-29: 4-Stage Bidirectional Backpressure State Machine with Hysteresis Anti-Flapping.
 */

import { AgentTask, TaskState } from "../models";

export interface ClusterPressureSignals {
  model429Rate?: number;
  modelTtftP95Sec?: number;
  nodePsiMemoryPct?: number;
  availableComputeRatio?: number;
  clientSseCreditRatio?: number;
  remainingTokenBudgetRatio?: number;
}

export class BidirectionalBackpressureController {
  public currentStage: string = "NORMAL";

  public evaluate(sig: ClusterPressureSignals, activeTasks: AgentTask[]): string {
    const m429 = sig.model429Rate ?? 0.01;
    const ttft = sig.modelTtftP95Sec ?? 0.45;
    const psi = sig.nodePsiMemoryPct ?? 12.0;
    const avail = sig.availableComputeRatio ?? 0.65;
    const credit = sig.clientSseCreditRatio ?? 0.95;
    const remTok = sig.remainingTokenBudgetRatio ?? 0.80;

    if (avail < 0.05 || m429 >= 0.70) {
      this.currentStage = "STAGE3_QUEUE_FREEZE";
      return this.currentStage;
    }
    if (psi > 40.0 || m429 > 0.25) {
      this.currentStage = "STAGE2_POOL_FAILOVER_HIBERNATE";
      return this.currentStage;
    }
    if (credit < 0.25 || remTok < 0.15) {
      this.currentStage = "STAGE4_UPSTREAM_THROTTLE";
      for (const t of activeTasks) {
        if (t.state === TaskState.RUNNING && t.activeSessions > 1) {
          t.activeSessions = 1;
        }
      }
      return this.currentStage;
    }
    if (ttft > 5.0 || m429 > 0.05) {
      this.currentStage = "STAGE1_SOFT_PACING";
      for (const t of activeTasks) {
        if (t.state === TaskState.RUNNING && (t.effort === "max" || t.effort === "high")) {
          t.effort = "medium";
        }
      }
      return this.currentStage;
    }
    if (m429 <= 0.03 && psi <= 30.0 && credit >= 0.40) {
      this.currentStage = "NORMAL";
    }
    return this.currentStage;
  }
}
