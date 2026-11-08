"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SyntacticAndVelocityBreaker = void 0; /**
 * @fileoverview M4 TASK-16: Semantic Loop Detector, Token Velocity Breaker & JSON AST Validator.
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

    if (list.length >= this.maxIdenticalConsecutiveCalls) {
      const recent = list.slice(-this.maxIdenticalConsecutiveCalls);
      const allSame = recent.every(
      (d) => d.toolName === diag.toolName && d.argsHash === diag.argsHash);

      if (allSame) {
        return { tripped: true, reason: "SEMANTIC_DEAD_LOOP" };
      }
    }

    const windowStart = diag.timestampMs - 60_000;
    const tokensInLastMin = list.
    filter((d) => d.timestampMs >= windowStart).
    reduce((acc, d) => acc + d.tokensConsumed, 0);
    if (tokensInLastMin > this.maxTokensPerMinutePerTask) {
      return { tripped: true, reason: "TOKEN_VELOCITY_EXCEEDED" };
    }

    return { tripped: false };
  }
}exports.SyntacticAndVelocityBreaker = SyntacticAndVelocityBreaker;