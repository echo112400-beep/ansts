"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.FinOpsBudgetController = void 0; /**
 * @fileoverview M12 TASK-44: Real-Time Tenant Token Budget Quota Enforcer & Model Auto-Downgrade Controller.
 */








class FinOpsBudgetController {
  policies = new Map();
  spendByTenantUsd = new Map();

  setPolicy(policy) {
    this.policies.set(policy.tenantId, policy);
  }

  recordSpend(tenantId, costUsd) {
    const next = Number(((this.spendByTenantUsd.get(tenantId) ?? 0) + costUsd).toFixed(6));
    this.spendByTenantUsd.set(tenantId, next);
    return next;
  }

  evaluateBudgetAndModel(tenantId)




  {
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
        utilizationRatio: ratio
      };
    }
    if (ratio >= policy.softDowngradeThresholdRatio) {
      return {
        allowed: true,
        selectedModel: policy.fallbackEconomyModel,
        downgraded: true,
        utilizationRatio: ratio
      };
    }
    return {
      allowed: true,
      selectedModel: policy.preferredModel,
      downgraded: false,
      utilizationRatio: ratio
    };
  }
}exports.FinOpsBudgetController = FinOpsBudgetController;