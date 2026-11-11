/**
 * @fileoverview M3 TASK-09, M53-1 & M64-3: Fault-Tolerant Reverse-Order (LIFO) Saga Compensation Rollback Engine
 * with SQLite Persistence (`persisted_saga_steps`), Idempotent Forward-Step Recording & Dynamic Compensation Resolver Across Restarts.
 */
import { SqlStorageLike } from "../cloudflare/env.js";

export interface SagaStepRecord {
  stepId: string;
  taskId: string;
  turnIndex: number;
  toolName: string;
  idempotencyKey?: string;
  forwardPayload: Record<string, unknown>;
  compensateHandler?: (payload: Record<string, unknown>) => Record<string, unknown>;
  compensated: boolean;
  compensationResult?: Record<string, unknown>;
  compensationError?: string;
}

export class SagaCompensatorEngine {
  private readonly stepsByTask: Map<string, SagaStepRecord[]> = new Map();
  private onStepCompensatedCallback?: (step: SagaStepRecord) => void;
  private toolCompensatorResolver?: (
    toolName: string
  ) => ((payload: Record<string, unknown>) => Record<string, unknown>) | undefined;
  private sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  public setOnStepCompensated(cb: (step: SagaStepRecord) => void): void {
    this.onStepCompensatedCallback = cb;
  }

  /**
   * [M64-3] Registers a dynamic lookup resolver so steps rehydrated from SQLite after a process
   * restart can still resolve their tool's `compensateHandler` by `toolName`.
   */
  public setToolCompensatorResolver(
    resolver: (toolName: string) => ((payload: Record<string, unknown>) => Record<string, unknown>) | undefined
  ): void {
    this.toolCompensatorResolver = resolver;
  }

  /**
   * [M64-3] Binds SQLite storage (`persisted_saga_steps`) so recorded Saga steps and their
   * compensation statuses survive restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): number {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_saga_steps (step_id TEXT PRIMARY KEY, task_id TEXT, payload_json TEXT)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM persisted_saga_steps")
      .toArray();
    let restored = 0;
    for (const r of rows) {
      const parsed = JSON.parse(String(r.payload_json ?? "{}")) as Record<string, unknown>;
      const stepId = String(r.step_id ?? parsed.stepId ?? "");
      const taskId = String(r.task_id ?? parsed.taskId ?? "");
      if (!stepId || !taskId) continue;
      const step: SagaStepRecord = {
        stepId,
        taskId,
        turnIndex: Number(parsed.turnIndex ?? 1),
        toolName: String(parsed.toolName ?? ""),
        idempotencyKey: parsed.idempotencyKey ? String(parsed.idempotencyKey) : undefined,
        forwardPayload:
          parsed.forwardPayload && typeof parsed.forwardPayload === "object"
            ? (parsed.forwardPayload as Record<string, unknown>)
            : {},
        compensated: Boolean(parsed.compensated ?? false),
        compensationResult:
          parsed.compensationResult && typeof parsed.compensationResult === "object"
            ? (parsed.compensationResult as Record<string, unknown>)
            : undefined,
        compensationError: parsed.compensationError ? String(parsed.compensationError) : undefined,
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

  private persistStep(step: SagaStepRecord): void {
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
        compensationError: step.compensationError,
      })
    );
  }

  /**
   * [M64-3] Idempotent forward step recording: prevents duplicate entries for the same `stepId`
   * or `(taskId, turnIndex, idempotencyKey)` and persists to `persisted_saga_steps`.
   */
  public recordForwardStep(step: Omit<SagaStepRecord, "compensated">): SagaStepRecord {
    let list = this.stepsByTask.get(step.taskId);
    if (!list) {
      list = [];
      this.stepsByTask.set(step.taskId, list);
    }
    const existing = list.find(
      (s) =>
        !s.compensated &&
        (s.stepId === step.stepId ||
          (step.idempotencyKey !== undefined &&
            s.turnIndex === step.turnIndex &&
            s.idempotencyKey === step.idempotencyKey))
    );
    if (existing) {
      if (step.compensateHandler && !existing.compensateHandler) {
        existing.compensateHandler = step.compensateHandler;
      }
      return existing;
    }
    const full: SagaStepRecord = { ...step, compensated: false };
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
  public rollbackTask(taskId: string, upToTurnInclusive = 0): SagaStepRecord[] {
    const list = this.stepsByTask.get(taskId) ?? [];
    const rolledBack: SagaStepRecord[] = [];
    for (let i = list.length - 1; i >= 0; i--) {
      const step = list[i];
      const handler =
        step.compensateHandler ??
        (this.toolCompensatorResolver ? this.toolCompensatorResolver(step.toolName) : undefined);
      if (!step.compensated && step.turnIndex >= upToTurnInclusive && handler) {
        try {
          step.compensationResult = handler(step.forwardPayload);
          step.compensated = true;
        } catch (err) {
          step.compensationError = (err as Error).message;
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

  public getTaskSteps(taskId: string): SagaStepRecord[] {
    return [...(this.stepsByTask.get(taskId) ?? [])];
  }
}
