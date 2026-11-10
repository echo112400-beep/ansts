"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SyntacticAndVelocityBreaker = void 0; /**
 * @fileoverview M4 TASK-16 & M55-3: Semantic Loop Detector, Token Velocity Breaker & JSON AST Validator
 * with Bounded Sliding-Window Pruning & Streak-Safe Post-Approval/Rollback Breaker Reset.
 */









class SyntacticAndVelocityBreaker {
  historyByTask = new Map();
  maxIdenticalConsecutiveCalls;
  maxTokensPerMinutePerTask;

  constructor(maxIdenticalConsecutiveCalls = 3, maxTokensPerMinutePerTask = 120_000) {
    this.maxIdenticalConsecutiveCalls = maxIdenticalConsecutiveCalls;
    this.maxTokensPerMinutePerTask = maxTokensPerMinutePerTask;
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

  recordAndCheckTurn(
  taskId,
  diag)
  {
    let list = this.historyByTask.get(taskId);
    if (!list) {
      list = [];
      this.historyByTask.set(taskId, list);
    }
    list.push(diag);

    const windowStart = diag.timestampMs - 60_000;
    if (list.length > this.maxIdenticalConsecutiveCalls * 2) {
      const tailToKeep = list.slice(-this.maxIdenticalConsecutiveCalls);
      const withinWindow = list.filter((d) => d.timestampMs >= windowStart);
      list = withinWindow.length >= tailToKeep.length ? withinWindow : tailToKeep;
      this.historyByTask.set(taskId, list);
    }

    const activeStreakPool = list.filter((d) => !d.streakCleared);
    if (activeStreakPool.length >= this.maxIdenticalConsecutiveCalls) {
      const recent = activeStreakPool.slice(-this.maxIdenticalConsecutiveCalls);
      const allSame = recent.every(
      (d) => d.toolName === diag.toolName && d.argsHash === diag.argsHash);

      if (allSame) {
        return { tripped: true, reason: "SEMANTIC_DEAD_LOOP" };
      }
    }

    const tokensInLastMin = list.
    filter((d) => d.timestampMs >= windowStart).
    reduce((acc, d) => acc + d.tokensConsumed, 0);
    if (tokensInLastMin > this.maxTokensPerMinutePerTask) {
      return { tripped: true, reason: "TOKEN_VELOCITY_EXCEEDED" };
    }

    return { tripped: false };
  }

  /**
   * [M55-3] Resets a task's velocity/loop breaker history after human approval or Saga/Time-Travel rollback.
   * Marks retained pre-reset turns with `streakCleared = true` so a resumed task is not immediately
   * re-locked out by pre-reset turns.
   */
  resetTaskBreaker(taskId, fromTurnInclusive) {
    if (fromTurnInclusive === undefined) {
      this.historyByTask.delete(taskId);
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
  }
}exports.SyntacticAndVelocityBreaker = SyntacticAndVelocityBreaker;