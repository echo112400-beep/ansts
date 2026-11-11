"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SyntacticAndVelocityBreaker = void 0; /**
 * @fileoverview M4 TASK-16, M55-3, M61-3 & M69-4: Semantic Loop Detector, Token Velocity Breaker & JSON AST Validator
 * with Bounded Sliding-Window Pruning, Same-Turn Retry Coalescing, Non-Negative Finite Token & Threshold Sanitization,
 * SQLite Persistence & Streak-Safe Breakpoint Reset.
 */











class SyntacticAndVelocityBreaker {
  historyByTask = new Map();
  maxIdenticalConsecutiveCalls;
  maxTokensPerMinutePerTask;
  sql;

  constructor(
  maxIdenticalConsecutiveCalls = 3,
  maxTokensPerMinutePerTask = 120_000,
  sql)
  {
    this.maxIdenticalConsecutiveCalls =
    Number.isFinite(maxIdenticalConsecutiveCalls) && maxIdenticalConsecutiveCalls >= 1 ?
    Math.floor(maxIdenticalConsecutiveCalls) :
    3;
    this.maxTokensPerMinutePerTask =
    Number.isFinite(maxTokensPerMinutePerTask) && maxTokensPerMinutePerTask > 0 ?
    maxTokensPerMinutePerTask :
    120_000;
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M61-3] Binds SQLite storage so per-task velocity/loop breaker diagnostics survive restarts.
   */
  bindSqlStorage(sql) {
    this.sql = sql;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS persisted_velocity_breaker (task_id TEXT PRIMARY KEY, diagnostics_json TEXT)");

    const rows = this.sql.
    exec("SELECT * FROM persisted_velocity_breaker").
    toArray();
    let restored = 0;
    for (const r of rows) {
      const taskId = String(r.task_id);
      const list = JSON.parse(String(r.diagnostics_json ?? "[]"));
      this.historyByTask.set(taskId, list);
      restored += 1;
    }
    return restored;
  }

  persistTaskHistory(taskId) {
    if (!this.sql) return;
    const list = this.historyByTask.get(taskId);
    if (!list || list.length === 0) {
      this.sql.exec("DELETE FROM persisted_velocity_breaker WHERE task_id = ?", taskId);
    } else {
      this.sql.exec(
      "INSERT OR REPLACE INTO persisted_velocity_breaker (task_id, diagnostics_json) VALUES (?, ?)",
      taskId,
      JSON.stringify(list));

    }
  }

  static validateJsonToolCall(rawJson) {
    try {
      const parsed = JSON.parse(rawJson);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
        return { valid: false, error: "Tool call AST root must be a JSON object" };
      }
      return { valid: true, parsed };
    } catch (err) {
      return { valid: false, error: err.message };
    }
  }

  /**
   * [M61-3 & M69-4] Coalesces retries at the exact same `turnIndex` (when `!d.streakCleared`) in-place
   * and sanitizes `tokensConsumed` + `timestampMs` against `NaN`/negative values so malformed telemetry
   * can never poison the 60-second sliding token sum or bypass `TOKEN_VELOCITY_EXCEEDED`.
   */
  recordAndCheckTurn(
  taskId,
  diag)




  {
    const safeTokens =
    Number.isFinite(diag.tokensConsumed) && diag.tokensConsumed > 0 ?
    Math.floor(diag.tokensConsumed) :
    0;
    const safeTs = Number.isFinite(diag.timestampMs) && diag.timestampMs > 0 ? diag.timestampMs : Date.now();
    const sanitizedDiag = {
      ...diag,
      tokensConsumed: safeTokens,
      timestampMs: safeTs
    };

    let list = this.historyByTask.get(taskId);
    if (!list) {
      list = [];
      this.historyByTask.set(taskId, list);
    }

    let coalescedSameTurnRetry = false;
    const existingSameTurnIdx = list.findIndex(
    (d) => !d.streakCleared && d.turnIndex === sanitizedDiag.turnIndex);

    if (existingSameTurnIdx >= 0) {
      list[existingSameTurnIdx] = sanitizedDiag;
      coalescedSameTurnRetry = true;
    } else {
      list.push(sanitizedDiag);
    }

    const windowStart = safeTs - 60_000;
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
      (d) => d.toolName === sanitizedDiag.toolName && d.argsHash === sanitizedDiag.argsHash);

      if (allSame) {
        return { tripped: true, reason: "SEMANTIC_DEAD_LOOP", coalescedSameTurnRetry };
      }
    }

    const tokensInLastMin = list.
    filter((d) => d.timestampMs >= windowStart).
    reduce((acc, d) => acc + (Number.isFinite(d.tokensConsumed) && d.tokensConsumed > 0 ? d.tokensConsumed : 0), 0);
    if (tokensInLastMin > this.maxTokensPerMinutePerTask) {
      return { tripped: true, reason: "TOKEN_VELOCITY_EXCEEDED", coalescedSameTurnRetry };
    }

    return { tripped: false, coalescedSameTurnRetry };
  }

  /**
   * [M55-3 & M61-3] Resets a task's velocity/loop breaker history after human approval or Saga/Time-Travel/Breakpoint rollback.
   */
  resetTaskBreaker(taskId, fromTurnInclusive) {
    if (fromTurnInclusive === undefined) {
      this.historyByTask.delete(taskId);
      this.persistTaskHistory(taskId);
      return;
    }
    const list = this.historyByTask.get(taskId);
    if (!list) return;
    const filtered = list.
    filter((d) => d.turnIndex < fromTurnInclusive).
    map((d) => ({ ...d, streakCleared: true }));
    if (filtered.length === 0) {
      this.historyByTask.delete(taskId);
    } else {
      this.historyByTask.set(taskId, filtered);
    }
    this.persistTaskHistory(taskId);
  }
}exports.SyntacticAndVelocityBreaker = SyntacticAndVelocityBreaker;