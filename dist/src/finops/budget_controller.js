"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.FinOpsBudgetController = void 0; /**
 * @fileoverview M12 TASK-44, M53-2 & M61-2: Real-Time Tenant Token Budget Quota Enforcer,
 * UTC Daily Window Auto-Reset, Spend Refund Controller, Idempotent Spend/Refund Guard & SQLite Persistence.
 */










class FinOpsBudgetController {
  policies = new Map();
  spendByTenantUsd = new Map();
  activeUtcDayByTenant = new Map();
  seenLedgerIdempotencyKeys = new Set();
  sql;

  constructor(sql) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M61-2] Binds SQLite storage (`persisted_finops_ledgers`) so tenant budget policies, daily spend,
   * active UTC day windows, and idempotency tokens survive restarts.
   */
  bindSqlStorage(sql) {
    this.sql = sql;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS persisted_finops_ledgers (tenant_id TEXT PRIMARY KEY, payload_json TEXT)");

    const rows = this.sql.
    exec("SELECT * FROM persisted_finops_ledgers").
    toArray();
    let restored = 0;
    for (const r of rows) {
      const tenantId = String(r.tenant_id ?? "");
      const parsed = JSON.parse(String(r.payload_json ?? "{}"));
      if (!tenantId) continue;
      if (parsed.policy && typeof parsed.policy === "object") {
        this.policies.set(tenantId, parsed.policy);
      }
      if (parsed.spendUsd !== undefined) {
        this.spendByTenantUsd.set(tenantId, Number(parsed.spendUsd ?? 0));
      }
      if (parsed.activeUtcDay) {
        this.activeUtcDayByTenant.set(tenantId, String(parsed.activeUtcDay));
      }
      if (Array.isArray(parsed.seenIdemKeys)) {
        for (const k of parsed.seenIdemKeys) {
          this.seenLedgerIdempotencyKeys.add(String(k));
        }
      }
      restored += 1;
    }
    return restored;
  }

  persistTenantLedger(tenantId) {
    if (!this.sql) return;
    const prefix = `${tenantId}::`;
    const tenantIdemKeys = Array.from(this.seenLedgerIdempotencyKeys).filter((k) =>
    k.startsWith(prefix));

    this.sql.exec(
    "INSERT OR REPLACE INTO persisted_finops_ledgers (tenant_id, payload_json) VALUES (?, ?)",
    tenantId,
    JSON.stringify({
      tenantId,
      policy: this.policies.get(tenantId),
      spendUsd: this.spendByTenantUsd.get(tenantId) ?? 0,
      activeUtcDay: this.activeUtcDayByTenant.get(tenantId),
      seenIdemKeys: tenantIdemKeys
    }));

  }

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
      this.persistTenantLedger(tenantId);
      return true;
    }
    if (!prevDay) {
      this.activeUtcDayByTenant.set(tenantId, utcDayKey);
    }
    return false;
  }

  setPolicy(policy) {
    this.policies.set(policy.tenantId, policy);
    this.persistTenantLedger(policy.tenantId);
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

  /**
   * [M61-2] Idempotent spend recording: when `idempotencyKey` is provided, duplicate retries
   * return the current spend without double-charging the tenant's daily budget.
   */
  recordSpend(
  tenantId,
  costUsd,
  dayOrTime,
  idempotencyKey)
  {
    this.ensureUtcDayWindow(tenantId, dayOrTime);
    if (idempotencyKey) {
      const scoped = `${tenantId}::spend::${idempotencyKey}`;
      if (this.seenLedgerIdempotencyKeys.has(scoped)) {
        return this.spendByTenantUsd.get(tenantId) ?? 0;
      }
      this.seenLedgerIdempotencyKeys.add(scoped);
    }
    const safeCost = Number.isFinite(costUsd) && costUsd > 0 ? costUsd : 0;
    const next = Number(((this.spendByTenantUsd.get(tenantId) ?? 0) + safeCost).toFixed(6));
    this.spendByTenantUsd.set(tenantId, next);
    this.persistTenantLedger(tenantId);
    return next;
  }

  /**
   * [M53-2 & M61-2] Idempotent spend refund: when `idempotencyKey` is provided, duplicate refund
   * retries do not deduct the refund amount a second time.
   */
  refundSpend(
  tenantId,
  refundUsd,
  dayOrTime,
  idempotencyKey)
  {
    this.ensureUtcDayWindow(tenantId, dayOrTime);
    if (idempotencyKey) {
      const scoped = `${tenantId}::refund::${idempotencyKey}`;
      if (this.seenLedgerIdempotencyKeys.has(scoped)) {
        return this.spendByTenantUsd.get(tenantId) ?? 0;
      }
      this.seenLedgerIdempotencyKeys.add(scoped);
    }
    const curr = this.spendByTenantUsd.get(tenantId) ?? 0;
    const safeRefund = Number.isFinite(refundUsd) && refundUsd > 0 ? refundUsd : 0;
    const next = Number(Math.max(0, curr - safeRefund).toFixed(6));
    this.spendByTenantUsd.set(tenantId, next);
    this.persistTenantLedger(tenantId);
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