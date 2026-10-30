/**
 * TASK-12, TASK-13, TASK-14 Unified: Turn-Level Dynamic Cognitive Router
 * Composing ThreeStateCircuitBreaker, SlidingWindowTokenBucket, and ModelPoolClientAdapter.
 */

import { LeaseCoordinator } from "./lease_coordinator";
import { AgentTask, EFFORT_MULTIPLIERS, FenceToken, ModelPoolName, ReasoningEffort } from "./models";
import { SlidingWindowTokenBucket, ThreeStateCircuitBreaker } from "./router/breaker_limiter";
import { ModelPoolClientAdapter, SseTokenChunk, TurnMeteringSummary } from "./router/pool_client";

export interface ModelPoolHealth {
  poolName: ModelPoolName;
  breakerState: "CLOSED" | "OPEN" | "HALF_OPEN";
  errorRate429: number;
  ttftSec: number;
  quotaHeadroomRatio: number;
}

export class CognitiveRouter {
  public pools: Record<ModelPoolName, ModelPoolHealth>;
  public breakers: Record<ModelPoolName, ThreeStateCircuitBreaker>;
  public tokenBuckets: Record<ModelPoolName, SlidingWindowTokenBucket>;
  public readonly poolAdapter = new ModelPoolClientAdapter();

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
    this.breakers = {
      DeepSeek: new ThreeStateCircuitBreaker("DeepSeek", 0.25, 15.0, 10.0),
      "GLM-5.3": new ThreeStateCircuitBreaker("GLM-5.3", 0.25, 15.0, 10.0),
      "Qwen-3.8": new ThreeStateCircuitBreaker("Qwen-3.8", 0.25, 15.0, 10.0),
    };
    this.tokenBuckets = {
      DeepSeek: new SlidingWindowTokenBucket(1800, 3600),
      "GLM-5.3": new SlidingWindowTokenBucket(2400, 4800),
      "Qwen-3.8": new SlidingWindowTokenBucket(2000, 4000),
    };
  }

  public tripPoolBreaker(poolName: ModelPoolName, errorRate: number = 0.45, now: number = 10.0): void {
    const pool = this.pools[poolName];
    const brk = this.breakers[poolName];
    pool.errorRate429 = errorRate;
    if (errorRate >= 0.25) {
      for (let i = 0; i < 4; i++) {
        brk.recordOutcome(false, 2.0, now);
      }
      pool.breakerState = brk.state;
    }
  }

  public resetPoolBreaker(poolName: ModelPoolName): void {
    const pool = this.pools[poolName];
    const brk = this.breakers[poolName];
    pool.errorRate429 = 0.01;
    brk.state = "CLOSED";
    brk.recentOutcomes = [];
    pool.breakerState = "CLOSED";
  }

  public routeTurn(
    task: AgentTask,
    fence: FenceToken,
    isReadOnlyTurn: boolean = false,
    consecutiveToolErrors: number = 0,
    now: number = 10.0,
  ): {
    routedModel: ModelPoolName;
    effectiveEffort: ReasoningEffort;
    tokensConsumed: number;
    pacingDelaySec: number;
    chunks: SseTokenChunk[];
    metering: TurnMeteringSummary;
  } {
    this.leaseCoordinator.verifyAndAdvanceFence(fence);

    // Sync breaker states (including HALF_OPEN cooldown transitions)
    for (const name of ["DeepSeek", "GLM-5.3", "Qwen-3.8"] as ModelPoolName[]) {
      this.breakers[name].allowRequest(now);
      this.pools[name].breakerState = this.breakers[name].state;
    }

    let routedModel: ModelPoolName;
    let effectiveEffort: ReasoningEffort;

    if (isReadOnlyTurn && this.pools[task.fastUtilityModel].breakerState !== "OPEN") {
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
    const pacingDelaySec = this.tokenBuckets[routedModel].consumeWithPacing(tokensConsumed, now);
    const { chunks, summary } = this.poolAdapter.streamCompletion(routedModel, effectiveEffort, 400);

    task.usedTokens = Math.min(task.maxTotalTokens, task.usedTokens + tokensConsumed);
    task.assignedModelPool = routedModel;
    return {
      routedModel,
      effectiveEffort,
      tokensConsumed,
      pacingDelaySec,
      chunks,
      metering: summary,
    };
  }
}
