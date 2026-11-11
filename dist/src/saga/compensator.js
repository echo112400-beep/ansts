"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SagaCompensatorEngine = void 0; /**
 * @fileoverview M3 TASK-09, M53-1 & M64-3: Fault-Tolerant Reverse-Order (LIFO) Saga Compensation Rollback Engine
 * with SQLite Persistence (`persisted_saga_steps`), Idempotent Forward-Step Recording & Dynamic Compensation Resolver Across Restarts.
 */















class SagaCompensatorEngine {
  stepsByTask = new Map();
  onStepCompensatedCallback;
  toolCompensatorResolver;


  sql;

  constructor(sql) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  setOnStepCompensated(cb) {
    this.onStepCompensatedCallback = cb;
  }

  /**
   * [M64-3] Registers a dynamic lookup resolver so steps rehydrated from SQLite after a process
   * restart can still resolve their tool's `compensateHandler` by `toolName`.
   */
  setToolCompensatorResolver(
  resolver)
  {
    this.toolCompensatorResolver = resolver;
  }

  /**
   * [M64-3] Binds SQLite storage (`persisted_saga_steps`) so recorded Saga steps and their
   * compensation statuses survive restarts.
   */
  bindSqlStorage(sql) {
    this.sql = sql;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS persisted_saga_steps (step_id TEXT PRIMARY KEY, task_id TEXT, payload_json TEXT)");

    const rows = this.sql.
    exec("SELECT * FROM persisted_saga_steps").
    toArray();
    let restored = 0;
    for (const r of rows) {
      const parsed = JSON.parse(String(r.payload_json ?? "{}"));
      const stepId = String(r.step_id ?? parsed.stepId ?? "");
      const taskId = String(r.task_id ?? parsed.taskId ?? "");
      if (!stepId || !taskId) continue;
      const step = {
        stepId,
        taskId,
        turnIndex: Number(parsed.turnIndex ?? 1),
        toolName: String(parsed.toolName ?? ""),
        idempotencyKey: parsed.idempotencyKey ? String(parsed.idempotencyKey) : undefined,
        forwardPayload:
        parsed.forwardPayload && typeof parsed.forwardPayload === "object" ?
        parsed.forwardPayload :
        {},
        compensated: Boolean(parsed.compensated ?? false),
        compensationResult:
        parsed.compensationResult && typeof parsed.compensationResult === "object" ?
        parsed.compensationResult :
        undefined,
        compensationError: parsed.compensationError ? String(parsed.compensationError) : undefined
      };
      const list = this.stepsByTask.get(taskId) ?? [];
      const existingIdx = list.findIndex((s) => s.stepId === stepId);
      if (existingIdx >= 0) {
        const prevHandler = list[existingIdx].compensateHandler;
        list[existingIdx] = { ...step, compensateHandler: prevHandler };
      } else {
        list.push(step);
      }
      this.stepsByTask.set(taskId, list);
      restored += 1;
    }
    return restored;
  }

  persistStep(step) {
    if (!this.sql) return;
    this.sql.exec(
    "INSERT OR REPLACE INTO persisted_saga_steps (step_id, task_id, payload_json) VALUES (?, ?, ?)",
    step.stepId,
    step.taskId,
    JSON.stringify({
      stepId: step.stepId,
      taskId: step.taskId,
      turnIndex: step.turnIndex,
      toolName: step.toolName,
      idempotencyKey: step.idempotencyKey,
      forwardPayload: step.forwardPayload,
      compensated: step.compensated,
      compensationResult: step.compensationResult,
      compensationError: step.compensationError
    }));

  }

  /**
   * [M64-3] Idempotent forward step recording: prevents duplicate entries for the same `stepId`
   * or `(taskId, turnIndex, idempotencyKey)` and persists to `persisted_saga_steps`.
   */
  recordForwardStep(step) {
    let list = this.stepsByTask.get(step.taskId);
    if (!list) {
      list = [];
      this.stepsByTask.set(step.taskId, list);
    }
    const existing = list.find(
    (s) =>
    !s.compensated && (
    s.stepId === step.stepId ||
    step.idempotencyKey !== undefined &&
    s.turnIndex === step.turnIndex &&
    s.idempotencyKey === step.idempotencyKey));

    if (existing) {
      if (step.compensateHandler && !existing.compensateHandler) {
        existing.compensateHandler = step.compensateHandler;
      }
      return existing;
    }
    const full = { ...step, compensated: false };
    list.push(full);
    this.persistStep(full);
    return full;
  }

  /**
   * [M53-1 & M64-3] Fault-tolerant LIFO rollback: isolates per-step exceptions so a failing
   * compensation hook on step K never blocks steps K-1 .. 0 from rolling back, resolves
   * `compensateHandler` dynamically after restarts, persists updated status to SQLite,
   * and notifies `SagaToolGateway` to invalidate stale idempotency keys for rolled-back turns.
   */
  rollbackTask(taskId, upToTurnInclusive = 0) {
    const list = this.stepsByTask.get(taskId) ?? [];
    const rolledBack = [];
    for (let i = list.length - 1; i >= 0; i--) {
      const step = list[i];
      const handler =
      step.compensateHandler ?? (
      this.toolCompensatorResolver ? this.toolCompensatorResolver(step.toolName) : undefined);
      if (!step.compensated && step.turnIndex >= upToTurnInclusive && handler) {
        try {
          step.compensationResult = handler(step.forwardPayload);
          step.compensated = true;
        } catch (err) {
          step.compensationError = err.message;
          step.compensated = true;
        }
        this.persistStep(step);
        if (this.onStepCompensatedCallback) {
          this.onStepCompensatedCallback(step);
        }
        rolledBack.push(step);
      }
    }
    return rolledBack;
  }

  getTaskSteps(taskId) {
    return [...(this.stepsByTask.get(taskId) ?? [])];
  }
}exports.SagaCompensatorEngine = SagaCompensatorEngine;