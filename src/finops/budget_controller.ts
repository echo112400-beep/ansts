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

export class FinOpsBudgetController {
  private readonly policies: Map<string, TenantBudgetPolicy> = new Map();
  private readonly spendByTenantUsd: Map<string, number> = new Map();

  public setPolicy(policy: TenantBudgetPolicy): void {
    this.policies.set(policy.tenantId, policy);
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
