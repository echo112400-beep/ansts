/**
 * @fileoverview M4 TASK-16, M55-3 & M61-3: Semantic Loop Detector, Token Velocity Breaker & JSON AST Validator
 * with Bounded Sliding-Window Pruning, Same-Turn Retry Coalescing, SQLite Persistence & Streak-Safe Breakpoint Reset.
 */
import { SqlStorageLike } from "../cloudflare/env.js";

export interface TurnDiagnostic {
  turnIndex: number;
  toolName: string;
  argsHash: string;
  tokensConsumed: number;
  timestampMs: number;
  streakCleared?: boolean;
}

export class SyntacticAndVelocityBreaker {
  private readonly historyByTask: Map<string, TurnDiagnostic[]> = new Map();
  private readonly maxIdenticalConsecutiveCalls: number;
  private readonly maxTokensPerMinutePerTask: number;
  private sql?: SqlStorageLike;

  constructor(
    maxIdenticalConsecutiveCalls = 3,
    maxTokensPerMinutePerTask = 120_000,
    sql?: SqlStorageLike
  ) {
    this.maxIdenticalConsecutiveCalls = maxIdenticalConsecutiveCalls;
    this.maxTokensPerMinutePerTask = maxTokensPerMinutePerTask;
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M61-3] Binds SQLite storage so per-task velocity/loop breaker diagnostics survive restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): number {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_velocity_breaker (task_id TEXT PRIMARY KEY, diagnostics_json TEXT)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM persisted_velocity_breaker")
      .toArray();
    let restored = 0;
    for (const r of rows) {
      const taskId = String(r.task_id);
      const list = JSON.parse(String(r.diagnostics_json ?? "[]")) as TurnDiagnostic[];
      this.historyByTask.set(taskId, list);
      restored += 1;
    }
    return restored;
  }

  private persistTaskHistory(taskId: string): void {
    if (!this.sql) return;
    const list = this.historyByTask.get(taskId);
    if (!list || list.length === 0) {
      this.sql.exec("DELETE FROM persisted_velocity_breaker WHERE task_id = ?", taskId);
    } else {
      this.sql.exec(
        "INSERT OR REPLACE INTO persisted_velocity_breaker (task_id, diagnostics_json) VALUES (?, ?)",
        taskId,
        JSON.stringify(list)
      );
    }
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
   * [M61-3] Coalesces retries at the exact same `turnIndex` (when `!d.streakCleared`) in-place
   * so retrying a single turn due to network timeout or pre-checkpoint crash recovery never
   * double-counts tokens or falsely trips `SEMANTIC_DEAD_LOOP`.
   */
  public recordAndCheckTurn(
    taskId: string,
    diag: TurnDiagnostic
  ): {
    tripped: boolean;
    reason?: "SEMANTIC_DEAD_LOOP" | "TOKEN_VELOCITY_EXCEEDED";
    coalescedSameTurnRetry?: boolean;
  } {
    let list = this.historyByTask.get(taskId);
    if (!list) {
      list = [];
      this.historyByTask.set(taskId, list);
    }

    let coalescedSameTurnRetry = false;
    const existingSameTurnIdx = list.findIndex(
      (d) => !d.streakCleared && d.turnIndex === diag.turnIndex
    );
    if (existingSameTurnIdx >= 0) {
      list[existingSameTurnIdx] = diag;
      coalescedSameTurnRetry = true;
    } else {
      list.push(diag);
    }

    const windowStart = diag.timestampMs - 60_000;
    if (list.length > this.maxIdenticalConsecutiveCalls * 2) {
      const tailToKeep = list.slice(-this.maxIdenticalConsecutiveCalls);
      const withinWindow = list.filter((d) => d.timestampMs >= windowStart);
      list = withinWindow.length >= tailToKeep.length ? withinWindow : tailToKeep;
      this.historyByTask.set(taskId, list);
    }
    this.persistTaskHistory(taskId);

    const activeStreakPool = list.filter((d) => !d.streakCleared);
    if (activeStreakPool.length >= this.maxIdenticalConsecutiveCalls) {
      const recent = activeStreakPool.slice(-this.maxIdenticalConsecutiveCalls);
      const allSame = recent.every(
        (d) => d.toolName === diag.toolName && d.argsHash === diag.argsHash
      );
      if (allSame) {
        return { tripped: true, reason: "SEMANTIC_DEAD_LOOP", coalescedSameTurnRetry };
      }
    }

    const tokensInLastMin = list
      .filter((d) => d.timestampMs >= windowStart)
      .reduce((acc, d) => acc + d.tokensConsumed, 0);
    if (tokensInLastMin > this.maxTokensPerMinutePerTask) {
      return { tripped: true, reason: "TOKEN_VELOCITY_EXCEEDED", coalescedSameTurnRetry };
    }

    return { tripped: false, coalescedSameTurnRetry };
  }

  /**
   * [M55-3 & M61-3] Resets a task's velocity/loop breaker history after human approval or Saga/Time-Travel/Breakpoint rollback.
   */
  public resetTaskBreaker(taskId: string, fromTurnInclusive?: number): void {
    if (fromTurnInclusive === undefined) {
      this.historyByTask.delete(taskId);
      this.persistTaskHistory(taskId);
      return;
    }
    const list = this.historyByTask.get(taskId);
    if (!list) return;
    const filtered = list
      .filter((d) => d.turnIndex < fromTurnInclusive)
      .map((d) => ({ ...d, streakCleared: true }));
    if (filtered.length === 0) {
      this.historyByTask.delete(taskId);
    } else {
      this.historyByTask.set(taskId, filtered);
    }
    this.persistTaskHistory(taskId);
  }
}
