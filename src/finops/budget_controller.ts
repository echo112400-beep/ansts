/**
 * @fileoverview M12 TASK-44 & M53-2: Real-Time Tenant Token Budget Quota Enforcer,
 * UTC Daily Window Auto-Reset & Spend Refund Controller.
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
  private readonly activeUtcDayByTenant: Map<string, string> = new Map();

  private static resolveUtcDayKey(dayOrTime?: string | number): string {
    if (typeof dayOrTime === "string" && dayOrTime.length > 0) {
      return dayOrTime.slice(0, 10);
    }
    const ts = typeof dayOrTime === "number" ? dayOrTime : Date.now();
    return new Date(ts).toISOString().slice(0, 10);
  }

  private ensureUtcDayWindow(tenantId: string, dayOrTime?: string | number): boolean {
    const utcDayKey = FinOpsBudgetController.resolveUtcDayKey(dayOrTime);
    const prevDay = this.activeUtcDayByTenant.get(tenantId);
    if (prevDay && prevDay !== utcDayKey) {
      this.spendByTenantUsd.set(tenantId, 0);
      this.activeUtcDayByTenant.set(tenantId, utcDayKey);
      return true;
    }
    if (!prevDay) {
      this.activeUtcDayByTenant.set(tenantId, utcDayKey);
    }
    return false;
  }

  public setPolicy(policy: TenantBudgetPolicy): void {
    this.policies.set(policy.tenantId, policy);
  }

  public configureTenant(params: {
    tenantId: string;
    dailyBudgetUsd: number;
    softDowngradeThresholdRatio: number;
    preferredModel?: string;
    primaryModel?: string;
    fallbackEconomyModel: string;
  }): void {
    this.setPolicy({
      tenantId: params.tenantId,
      dailyBudgetUsd: params.dailyBudgetUsd,
      softDowngradeThresholdRatio: params.softDowngradeThresholdRatio,
      preferredModel: params.preferredModel ?? params.primaryModel ?? "DeepSeek-R1",
      fallbackEconomyModel: params.fallbackEconomyModel,
    });
  }

  public recordSpend(tenantId: string, costUsd: number, dayOrTime?: string | number): number {
    this.ensureUtcDayWindow(tenantId, dayOrTime);
    const next = Number(((this.spendByTenantUsd.get(tenantId) ?? 0) + costUsd).toFixed(6));
    this.spendByTenantUsd.set(tenantId, next);
    return next;
  }

  /**
   * [M53-2] Refunds reserved or rolled-back spend so aborted turns never permanently exhaust tenant budget.
   */
  public refundSpend(tenantId: string, refundUsd: number, dayOrTime?: string | number): number {
    this.ensureUtcDayWindow(tenantId, dayOrTime);
    const curr = this.spendByTenantUsd.get(tenantId) ?? 0;
    const next = Number(Math.max(0, curr - Math.max(0, refundUsd)).toFixed(6));
    this.spendByTenantUsd.set(tenantId, next);
    return next;
  }

  public evaluateBudgetAndModel(
    tenantId: string,
    dayOrTime?: string | number
  ): {
    allowed: boolean;
    selectedModel: string;
    downgraded: boolean;
    utilizationRatio: number;
    windowResetTriggered: boolean;
    windowReset: boolean;
  } {
    const windowResetTriggered = this.ensureUtcDayWindow(tenantId, dayOrTime);
    const policy = this.policies.get(tenantId);
    if (!policy) {
      return {
        allowed: true,
        selectedModel: "claude-3-7-sonnet",
        downgraded: false,
        utilizationRatio: 0,
        windowResetTriggered,
        windowReset: windowResetTriggered,
      };
    }
    const spent = this.spendByTenantUsd.get(tenantId) ?? 0;
    const ratio = Number((spent / Math.max(0.0001, policy.dailyBudgetUsd)).toFixed(4));
    if (spent >= policy.dailyBudgetUsd) {
      return {
        allowed: false,
        selectedModel: policy.fallbackEconomyModel,
        downgraded: true,
        utilizationRatio: ratio,
        windowResetTriggered,
        windowReset: windowResetTriggered,
      };
    }
    if (ratio >= policy.softDowngradeThresholdRatio) {
      return {
        allowed: true,
        selectedModel: policy.fallbackEconomyModel,
        downgraded: true,
        utilizationRatio: ratio,
        windowResetTriggered,
        windowReset: windowResetTriggered,
      };
    }
    return {
      allowed: true,
      selectedModel: policy.preferredModel,
      downgraded: false,
      utilizationRatio: ratio,
      windowResetTriggered,
      windowReset: windowResetTriggered,
    };
  }
}
