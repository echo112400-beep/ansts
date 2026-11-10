/**
 * @fileoverview M12 TASK-44, M53-2 & M61-2: Real-Time Tenant Token Budget Quota Enforcer,
 * UTC Daily Window Auto-Reset, Spend Refund Controller, Idempotent Spend/Refund Guard & SQLite Persistence.
 */
import { SqlStorageLike } from "../cloudflare/env.js";

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
  private readonly seenLedgerIdempotencyKeys: Set<string> = new Set();
  private sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M61-2] Binds SQLite storage (`persisted_finops_ledgers`) so tenant budget policies, daily spend,
   * active UTC day windows, and idempotency tokens survive restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): number {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_finops_ledgers (tenant_id TEXT PRIMARY KEY, payload_json TEXT)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM persisted_finops_ledgers")
      .toArray();
    let restored = 0;
    for (const r of rows) {
      const tenantId = String(r.tenant_id ?? "");
      const parsed = JSON.parse(String(r.payload_json ?? "{}")) as Record<string, unknown>;
      if (!tenantId) continue;
      if (parsed.policy && typeof parsed.policy === "object") {
        this.policies.set(tenantId, parsed.policy as TenantBudgetPolicy);
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

  private persistTenantLedger(tenantId: string): void {
    if (!this.sql) return;
    const prefix = `${tenantId}::`;
    const tenantIdemKeys = Array.from(this.seenLedgerIdempotencyKeys).filter((k) =>
      k.startsWith(prefix)
    );
    this.sql.exec(
      "INSERT OR REPLACE INTO persisted_finops_ledgers (tenant_id, payload_json) VALUES (?, ?)",
      tenantId,
      JSON.stringify({
        tenantId,
        policy: this.policies.get(tenantId),
        spendUsd: this.spendByTenantUsd.get(tenantId) ?? 0,
        activeUtcDay: this.activeUtcDayByTenant.get(tenantId),
        seenIdemKeys: tenantIdemKeys,
      })
    );
  }

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
      this.persistTenantLedger(tenantId);
      return true;
    }
    if (!prevDay) {
      this.activeUtcDayByTenant.set(tenantId, utcDayKey);
    }
    return false;
  }

  public setPolicy(policy: TenantBudgetPolicy): void {
    this.policies.set(policy.tenantId, policy);
    this.persistTenantLedger(policy.tenantId);
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

  /**
   * [M61-2] Idempotent spend recording: when `idempotencyKey` is provided, duplicate retries
   * return the current spend without double-charging the tenant's daily budget.
   */
  public recordSpend(
    tenantId: string,
    costUsd: number,
    dayOrTime?: string | number,
    idempotencyKey?: string
  ): number {
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
  public refundSpend(
    tenantId: string,
    refundUsd: number,
    dayOrTime?: string | number,
    idempotencyKey?: string
  ): number {
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
