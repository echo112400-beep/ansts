"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.FinOpsBudgetController = void 0; /**
 * @fileoverview M12 TASK-44 & M53-2: Real-Time Tenant Token Budget Quota Enforcer,
 * UTC Daily Window Auto-Reset & Spend Refund Controller.
 */








class FinOpsBudgetController {
  policies = new Map();
  spendByTenantUsd = new Map();
  activeUtcDayByTenant = new Map();

  static resolveUtcDayKey(dayOrTime) {
    if (typeof dayOrTime === "string" && dayOrTime.length > 0) {
      return dayOrTime.slice(0, 10);
    }
    const ts = typeof dayOrTime === "number" ? dayOrTime : Date.now();
    return new Date(ts).toISOString().slice(0, 10);
  }

  ensureUtcDayWindow(tenantId, dayOrTime) {
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

  setPolicy(policy) {
    this.policies.set(policy.tenantId, policy);
  }

  configureTenant(params)






  {
    this.setPolicy({
      tenantId: params.tenantId,
      dailyBudgetUsd: params.dailyBudgetUsd,
      softDowngradeThresholdRatio: params.softDowngradeThresholdRatio,
      preferredModel: params.preferredModel ?? params.primaryModel ?? "DeepSeek-R1",
      fallbackEconomyModel: params.fallbackEconomyModel
    });
  }

  recordSpend(tenantId, costUsd, dayOrTime) {
    this.ensureUtcDayWindow(tenantId, dayOrTime);
    const next = Number(((this.spendByTenantUsd.get(tenantId) ?? 0) + costUsd).toFixed(6));
    this.spendByTenantUsd.set(tenantId, next);
    return next;
  }

  /**
   * [M53-2] Refunds reserved or rolled-back spend so aborted turns never permanently exhaust tenant budget.
   */
  refundSpend(tenantId, refundUsd, dayOrTime) {
    this.ensureUtcDayWindow(tenantId, dayOrTime);
    const curr = this.spendByTenantUsd.get(tenantId) ?? 0;
    const next = Number(Math.max(0, curr - Math.max(0, refundUsd)).toFixed(6));
    this.spendByTenantUsd.set(tenantId, next);
    return next;
  }

  evaluateBudgetAndModel(
  tenantId,
  dayOrTime)







  {
    const windowResetTriggered = this.ensureUtcDayWindow(tenantId, dayOrTime);
    const policy = this.policies.get(tenantId);
    if (!policy) {
      return {
        allowed: true,
        selectedModel: "claude-3-7-sonnet",
        downgraded: false,
        utilizationRatio: 0,
        windowResetTriggered,
        windowReset: windowResetTriggered
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
        windowReset: windowResetTriggered
      };
    }
    if (ratio >= policy.softDowngradeThresholdRatio) {
      return {
        allowed: true,
        selectedModel: policy.fallbackEconomyModel,
        downgraded: true,
        utilizationRatio: ratio,
        windowResetTriggered,
        windowReset: windowResetTriggered
      };
    }
    return {
      allowed: true,
      selectedModel: policy.preferredModel,
      downgraded: false,
      utilizationRatio: ratio,
      windowResetTriggered,
      windowReset: windowResetTriggered
    };
  }
}exports.FinOpsBudgetController = FinOpsBudgetController;