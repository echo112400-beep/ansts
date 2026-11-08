/**
 * @fileoverview M4 TASK-16: Semantic Loop Detector, Token Velocity Breaker & JSON AST Validator.
 */
export interface TurnDiagnostic {
  turnIndex: number;
  toolName: string;
  argsHash: string;
  tokensConsumed: number;
  timestampMs: number;
}

export class SyntacticAndVelocityBreaker {
  private readonly historyByTask: Map<string, TurnDiagnostic[]> = new Map();
  private readonly maxIdenticalConsecutiveCalls: number;
  private readonly maxTokensPerMinutePerTask: number;

  constructor(maxIdenticalConsecutiveCalls = 3, maxTokensPerMinutePerTask = 120_000) {
    this.maxIdenticalConsecutiveCalls = maxIdenticalConsecutiveCalls;
    this.maxTokensPerMinutePerTask = maxTokensPerMinutePerTask;
  }

  public static validateJsonToolCall(rawJson: string): { valid: boolean; parsed?: Record<string, unknown>; error?: string } {
    try {
      const parsed = JSON.parse(rawJson) as Record<string, unknown>;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        return { valid: false, error: "Tool call AST root must be a JSON object" };
      }
      return { valid: true, parsed };
    } catch (err) {
      return { valid: false, error: (err as Error).message };
    }
  }

  /**
   * Computes the Progress Velocity `V_k` of a turn from its pass rate, the share of errors it eliminated
   * relative to the previous turn, and the entropy of its diff. Deterministic blend in `[0, 1]`:
   * a value below `0.15` means the turn made no measurable progress (the shadow replay lab counts it as a
   * velocity-breaker backtrack).
   */
  public computeProgressVelocity(
    passRate: number,
    errorReductionRatio: number,
    diffEntropy: number
  ): number {
    const clamp01 = (v: number): number => (Number.isFinite(v) ? Math.min(1, Math.max(0, v)) : 0);
    const passComponent = clamp01(passRate) * 0.5;
    const errorComponent = clamp01(errorReductionRatio) * 0.3;
    const focusComponent = (1 - clamp01(diffEntropy)) * 0.2;
    return Number(clamp01(passComponent + errorComponent + focusComponent).toFixed(4));
  }

  public recordAndCheckTurn(
    taskId: string,
    diag: TurnDiagnostic
  ): { tripped: boolean; reason?: "SEMANTIC_DEAD_LOOP" | "TOKEN_VELOCITY_EXCEEDED" } {
    let list = this.historyByTask.get(taskId);
    if (!list) {
      list = [];
      this.historyByTask.set(taskId, list);
    }
    list.push(diag);

    if (list.length >= this.maxIdenticalConsecutiveCalls) {
      const recent = list.slice(-this.maxIdenticalConsecutiveCalls);
      const allSame = recent.every(
        (d) => d.toolName === diag.toolName && d.argsHash === diag.argsHash
      );
      if (allSame) {
        return { tripped: true, reason: "SEMANTIC_DEAD_LOOP" };
      }
    }

    const windowStart = diag.timestampMs - 60_000;
    const tokensInLastMin = list
      .filter((d) => d.timestampMs >= windowStart)
      .reduce((acc, d) => acc + d.tokensConsumed, 0);
    if (tokensInLastMin > this.maxTokensPerMinutePerTask) {
      return { tripped: true, reason: "TOKEN_VELOCITY_EXCEEDED" };
    }

    return { tripped: false };
  }
}
