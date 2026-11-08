/**
 * @fileoverview M12 TASK-44: Real-Time Tenant Token Budget Quota Enforcer & Model Auto-Downgrade Controller.
 */
export interface TenantBudgetPolicy {
  tenantId: string;
  dailyBudgetUsd: number;
  softDowngradeThresholdRatio: number;
  preferredModel: string;
  fallbackEconomyModel: string;
}

/**
 * Per-tenant budget ledger exposed through `FinOpsBudgetController.budgets` and rendered by the
 * console FinOps / W-DRF radar.
 */
export interface TenantBudgetState {
  tenantId: string;
  dailyHardCapUsd: number;
  softWarnRatio: number;
  /** LLM token spend attributed to the tenant since the ledger was opened. */
  spentModelUsd: number;
  /** Sandbox CPU-core-second spend attributed to the tenant since the ledger was opened. */
  spentSandboxUsd: number;
  turnsRecorded: number;
}

/** Per-1K-token USD price and per-core-second sandbox price used for turn spend attribution. */
const MODEL_PRICE_PER_1K_TOKENS_USD: Record<string, number> = {
  DeepSeek: 0.002,
  "GLM-5.3": 0.0012,
  "Qwen-3.8": 0.0006,
};
const SANDBOX_USD_PER_CPU_CORE_SECOND = 0.00004;

export class FinOpsBudgetController {
  private readonly policies: Map<string, TenantBudgetPolicy> = new Map();
  private readonly spendByTenantUsd: Map<string, number> = new Map();
  public readonly budgets: Map<string, TenantBudgetState> = new Map();

  public setPolicy(policy: TenantBudgetPolicy): void {
    this.policies.set(policy.tenantId, policy);
  }

  /**
   * Opens (or replaces) the tenant budget ledger and the matching auto-downgrade policy, so the
   * console-seeded hard cap and soft warning ratio are the same ones `evaluateBudgetAndModel` enforces.
   */
  public registerTenantBudget(
    tenantId: string,
    dailyHardCapUsd: number,
    softWarnRatio: number,
  ): TenantBudgetState {
    const existing = this.budgets.get(tenantId);
    const state: TenantBudgetState = {
      tenantId,
      dailyHardCapUsd,
      softWarnRatio,
      spentModelUsd: existing?.spentModelUsd ?? 0,
      spentSandboxUsd: existing?.spentSandboxUsd ?? 0,
      turnsRecorded: existing?.turnsRecorded ?? 0,
    };
    this.budgets.set(tenantId, state);
    this.setPolicy({
      tenantId,
      dailyBudgetUsd: dailyHardCapUsd,
      softDowngradeThresholdRatio: softWarnRatio,
      preferredModel: "claude-3-7-sonnet",
      fallbackEconomyModel: "gpt-4o-mini",
    });
    return state;
  }

  /**
   * Attributes one executed turn (LLM tokens by model pool + sandbox CPU-core-seconds) to the task's
   * tenant ledger and to the shared spend tracker consumed by `evaluateBudgetAndModel`.
   */
  public recordTurnSpend(params: {
    task: { tenantId: string; taskId: string };
    pool: string;
    inputTokens: number;
    reasoningTokens: number;
    outputTokens: number;
    activeCpuCoreSeconds: number;
  }): {
    tenantId: string;
    taskId: string;
    modelCostUsd: number;
    sandboxCostUsd: number;
    totalCostUsd: number;
    dailySpendUsd: number;
  } {
    const pricePer1k = MODEL_PRICE_PER_1K_TOKENS_USD[params.pool] ?? MODEL_PRICE_PER_1K_TOKENS_USD.DeepSeek;
    const totalTokens = params.inputTokens + params.reasoningTokens + params.outputTokens;
    const modelCostUsd = Number(((totalTokens / 1000) * pricePer1k).toFixed(6));
    const sandboxCostUsd = Number(
      (params.activeCpuCoreSeconds * SANDBOX_USD_PER_CPU_CORE_SECOND).toFixed(6),
    );
    const totalCostUsd = Number((modelCostUsd + sandboxCostUsd).toFixed(6));

    const ledger =
      this.budgets.get(params.task.tenantId) ??
      this.registerTenantBudget(params.task.tenantId, Number.POSITIVE_INFINITY, 0.8);
    ledger.spentModelUsd = Number((ledger.spentModelUsd + modelCostUsd).toFixed(6));
    ledger.spentSandboxUsd = Number((ledger.spentSandboxUsd + sandboxCostUsd).toFixed(6));
    ledger.turnsRecorded += 1;

    return {
      tenantId: params.task.tenantId,
      taskId: params.task.taskId,
      modelCostUsd,
      sandboxCostUsd,
      totalCostUsd,
      dailySpendUsd: this.recordSpend(params.task.tenantId, totalCostUsd),
    };
  }

  public recordSpend(tenantId: string, costUsd: number): number {
    const next = Number(((this.spendByTenantUsd.get(tenantId) ?? 0) + costUsd).toFixed(6));
    this.spendByTenantUsd.set(tenantId, next);
    return next;
  }

  public evaluateBudgetAndModel(tenantId: string): {
    allowed: boolean;
    selectedModel: string;
    downgraded: boolean;
    utilizationRatio: number;
  } {
    const policy = this.policies.get(tenantId);
    if (!policy) {
      return { allowed: true, selectedModel: "claude-3-7-sonnet", downgraded: false, utilizationRatio: 0 };
    }
    const spent = this.spendByTenantUsd.get(tenantId) ?? 0;
    const ratio = Number((spent / Math.max(0.0001, policy.dailyBudgetUsd)).toFixed(4));
    if (spent >= policy.dailyBudgetUsd) {
      return {
        allowed: false,
        selectedModel: policy.fallbackEconomyModel,
        downgraded: true,
        utilizationRatio: ratio,
      };
    }
    if (ratio >= policy.softDowngradeThresholdRatio) {
      return {
        allowed: true,
        selectedModel: policy.fallbackEconomyModel,
        downgraded: true,
        utilizationRatio: ratio,
      };
    }
    return {
      allowed: true,
      selectedModel: policy.preferredModel,
      downgraded: false,
      utilizationRatio: ratio,
    };
  }
}
