/**
 * TASK-13: Turn-Level Dynamic Cognitive Routing across DeepSeek, GLM-5.3, and Qwen-3.8.
 */

import { LeaseCoordinator } from "./lease_coordinator";
import { AgentTask, EFFORT_MULTIPLIERS, FenceToken, ModelPoolName, ReasoningEffort } from "./models";

export interface ModelPoolHealth {
  poolName: ModelPoolName;
  breakerState: "CLOSED" | "OPEN" | "HALF_OPEN";
  errorRate429: number;
  ttftSec: number;
  quotaHeadroomRatio: number;
}

export class CognitiveRouter {
  public pools: Record<ModelPoolName, ModelPoolHealth>;

  constructor(public readonly leaseCoordinator: LeaseCoordinator) {
    this.pools = {
      DeepSeek: {
        poolName: "DeepSeek",
        breakerState: "CLOSED",
        errorRate429: 0.01,
        ttftSec: 0.40,
        quotaHeadroomRatio: 0.92,
      },
      "GLM-5.3": {
        poolName: "GLM-5.3",
        breakerState: "CLOSED",
        errorRate429: 0.01,
        ttftSec: 0.18,
        quotaHeadroomRatio: 0.95,
      },
      "Qwen-3.8": {
        poolName: "Qwen-3.8",
        breakerState: "CLOSED",
        errorRate429: 0.01,
        ttftSec: 0.35,
        quotaHeadroomRatio: 0.94,
      },
    };
  }

  public tripPoolBreaker(poolName: ModelPoolName, errorRate: number = 0.45): void {
    const pool = this.pools[poolName];
    pool.errorRate429 = errorRate;
    if (errorRate >= 0.25) {
      pool.breakerState = "OPEN";
    }
  }

  public resetPoolBreaker(poolName: ModelPoolName): void {
    const pool = this.pools[poolName];
    pool.errorRate429 = 0.01;
    pool.breakerState = "CLOSED";
  }

  public routeTurn(
    task: AgentTask,
    fence: FenceToken,
    isReadOnlyTurn: boolean = false,
    consecutiveToolErrors: number = 0,
  ): { routedModel: ModelPoolName; effectiveEffort: ReasoningEffort; tokensConsumed: number } {
    this.leaseCoordinator.verifyAndAdvanceFence(fence);

    let routedModel: ModelPoolName;
    let effectiveEffort: ReasoningEffort;

    if (isReadOnlyTurn && this.pools[task.fastUtilityModel].breakerState === "CLOSED") {
      routedModel = task.fastUtilityModel;
      effectiveEffort = "minimal";
    } else {
      const primaryPool = this.pools[task.primaryModel];
      if (primaryPool.breakerState === "OPEN") {
        routedModel = task.fallbackModels[0];
        effectiveEffort = task.effort === "high" || task.effort === "max" ? "medium" : task.effort;
      } else {
        routedModel = task.primaryModel;
        effectiveEffort = consecutiveToolErrors >= 2 ? "max" : task.effort;
      }
    }

    const mult = EFFORT_MULTIPLIERS[effectiveEffort] ?? 1.0;
    const tokensConsumed = Math.round(task.reservedTps * mult * 10);
    task.usedTokens = Math.min(task.maxTotalTokens, task.usedTokens + tokensConsumed);
    task.assignedModelPool = routedModel;
    return { routedModel, effectiveEffort, tokensConsumed };
  }
}
