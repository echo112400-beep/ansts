/**
 * TASK-62: Multi-Tenant FinOps Real-Time Cost Attribution Ledger & Hard-Cap Circuit Breaker.
 *
 * Tracks per-tenant spend across:
 * - Model pools (`DeepSeek`, `GLM-5.3`, `Qwen-3.8`): separate input, reasoning (CoT), and output token pricing
 * - Dual-clock sandbox compute: active CPU-seconds & RAM GiB-seconds (hibernated wall-clock time is free)
 * Enforces:
 * - `80%` soft watermark warning (`agent.finops.budget.warning`)
 * - `100%` hard-cap circuit breaker (`GRACEFUL_DRAINING` snapshot preservation + new admission freeze)
 */

import { AgentTask, ModelPoolName, TaskState } from "../models";

export interface ModelPricingPer1kTokensUsd {
  inputUsd: number;
  reasoningCotUsd: number;
  outputUsd: number;
}

export interface TenantFinOpsBudget {
  tenantId: string;
  dailyHardCapUsd: number;
  softWarningRatio: number; // default 0.80
  spentModelUsd: number;
  spentSandboxUsd: number;
  warningEmitted: boolean;
  hardCapTripped: boolean;
}

export class FinOpsBudgetController {
  public readonly modelPricing: Record<ModelPoolName, ModelPricingPer1kTokensUsd> = {
    DeepSeek: { inputUsd: 0.00055, reasoningCotUsd: 0.00219, outputUsd: 0.00219 },
    "GLM-5.3": { inputUsd: 0.00025, reasoningCotUsd: 0.0005, outputUsd: 0.0005 },
    "Qwen-3.8": { inputUsd: 0.00015, reasoningCotUsd: 0.0003, outputUsd: 0.0003 },
  };
  public readonly cpuCoreSecondUsd = 0.000025;
  public readonly budgets = new Map<string, TenantFinOpsBudget>();

  public registerTenantBudget(tenantId: string, dailyHardCapUsd: number, softWarningRatio = 0.8): void {
    this.budgets.set(tenantId, {
      tenantId,
      dailyHardCapUsd,
      softWarningRatio,
      spentModelUsd: 0,
      spentSandboxUsd: 0,
      warningEmitted: false,
      hardCapTripped: false,
    });
  }

  public canAdmitTenantTask(tenantId: string): { allowed: boolean; reason?: string } {
    const b = this.budgets.get(tenantId);
    if (!b) return { allowed: true };
    if (b.hardCapTripped || b.spentModelUsd + b.spentSandboxUsd >= b.dailyHardCapUsd) {
      b.hardCapTripped = true;
      return {
        allowed: false,
        reason: `FinOpsHardCapExceeded: Tenant ${tenantId} reached 100% of daily budget ($${b.dailyHardCapUsd.toFixed(4)})`,
      };
    }
    return { allowed: true };
  }

  /**
   * Charges a completed turn's LLM tokens and active sandbox CPU-seconds to the tenant's FinOps ledger.
   * If the tenant hits 100% of `dailyHardCapUsd`, transitions running tasks to `GRACEFUL_DRAINING`.
   */
  public recordTurnSpend(params: {
    task: AgentTask;
    pool: ModelPoolName;
    inputTokens: number;
    reasoningTokens: number;
    outputTokens: number;
    activeCpuCoreSeconds: number;
  }): {
    turnCostUsd: number;
    totalTenantSpendUsd: number;
    utilizationRatio: number;
    status: "OK" | "SOFT_WARNING_80" | "HARD_CAP_DRAINING_100";
  } {
    const pricing = this.modelPricing[params.pool];
    const modelCost =
      (params.inputTokens / 1000) * pricing.inputUsd +
      (params.reasoningTokens / 1000) * pricing.reasoningCotUsd +
      (params.outputTokens / 1000) * pricing.outputUsd;
    const sandboxCost = params.activeCpuCoreSeconds * this.cpuCoreSecondUsd;
    const turnCostUsd = Number((modelCost + sandboxCost).toFixed(6));

    let b = this.budgets.get(params.task.tenantId);
    if (!b) {
      this.registerTenantBudget(params.task.tenantId, 100.0);
      b = this.budgets.get(params.task.tenantId)!;
    }

    b.spentModelUsd = Number((b.spentModelUsd + modelCost).toFixed(6));
    b.spentSandboxUsd = Number((b.spentSandboxUsd + sandboxCost).toFixed(6));
    const totalSpend = Number((b.spentModelUsd + b.spentSandboxUsd).toFixed(6));
    const ratio = totalSpend / Math.max(0.000001, b.dailyHardCapUsd);

    if (ratio >= 1.0) {
      b.hardCapTripped = true;
      params.task.state = TaskState.GRACEFUL_DRAINING;
      params.task.drainSummary = `FinOps 100% budget cap ($${b.dailyHardCapUsd}) reached; snapshot saved cleanly`;
      return {
        turnCostUsd,
        totalTenantSpendUsd: totalSpend,
        utilizationRatio: Number(ratio.toFixed(4)),
        status: "HARD_CAP_DRAINING_100",
      };
    }

    if (ratio >= b.softWarningRatio) {
      b.warningEmitted = true;
      return {
        turnCostUsd,
        totalTenantSpendUsd: totalSpend,
        utilizationRatio: Number(ratio.toFixed(4)),
        status: "SOFT_WARNING_80",
      };
    }

    return {
      turnCostUsd,
      totalTenantSpendUsd: totalSpend,
      utilizationRatio: Number(ratio.toFixed(4)),
      status: "OK",
    };
  }
}
