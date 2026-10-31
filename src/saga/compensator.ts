/**
 * TASK-16: Persistent Saga Transaction Journal and LIFO Reverse Compensation Executor.
 */

import { FenceToken } from "../models";
import { TaskOperations } from "../persistence/task-operations";

export interface CompensatableActionRecord {
  taskId: string;
  fence: FenceToken;
  forwardTool: string;
  compensationTool: string;
  payload: string;
  compensated: boolean;
  attempts: number;
}

export interface DurableCompensationResult {
  operationId: string;
  status: "COMPENSATED" | "COMPENSATION_FAILED" | "OUTCOME_UNKNOWN" | "SKIPPED";
  error?: string;
}

export type DurableCompensationExecutor = (input: {
  operationId: string;
  attemptId: string;
  idempotencyKey: string;
  toolName: string;
  args: unknown;
}) => Promise<boolean>;

export class SagaCompensatorEngine {
  private records: Map<string, CompensatableActionRecord[]> = new Map();

  constructor(public readonly maxRetries: number = 3, public readonly taskOperations?: TaskOperations) {}

  /** Runs pending persisted compensations in LIFO order and leaves every outcome queryable. */
  public async compensatePersisted(
    taskId: string,
    afterTurnExclusive: number,
    fence: FenceToken,
    executor: DurableCompensationExecutor,
  ): Promise<DurableCompensationResult[]> {
    if (!this.taskOperations) throw new Error("TASK_OPERATIONS_NOT_CONFIGURED");
    const pending = this.taskOperations.listCompensations(taskId, afterTurnExclusive);
    const results: DurableCompensationResult[] = [];
    for (const item of pending) {
      if (item.status !== "PENDING" && item.status !== "COMPENSATION_FAILED") {
        results.push({ operationId: item.operationId, status: "SKIPPED" });
        continue;
      }
      const attemptId = `${item.operationId}:compensation:${item.attempts + 1}`;
      const attempt = await this.taskOperations.beginCompensation({ operationId: item.operationId, attemptId, fence });
      if (!attempt.started) {
        results.push({ operationId: item.operationId, status: "SKIPPED" });
        continue;
      }
      try {
        const succeeded = await executor({
          operationId: item.operationId,
          attemptId,
          idempotencyKey: `${item.operationId}:compensate`,
          toolName: item.toolName,
          args: item.args,
        });
        const status = succeeded ? "COMPENSATED" : "COMPENSATION_FAILED";
        const error = succeeded ? undefined : "Compensation executor reported a known failure.";
        await this.taskOperations.recordCompensationOutcome({ operationId: item.operationId, attemptId, fence, outcome: status, error });
        results.push({ operationId: item.operationId, status, ...(error ? { error } : {}) });
      } catch (cause) {
        const error = cause instanceof Error ? cause.message : String(cause);
        await this.taskOperations.recordCompensationOutcome({ operationId: item.operationId, attemptId, fence, outcome: "OUTCOME_UNKNOWN", error });
        results.push({ operationId: item.operationId, status: "OUTCOME_UNKNOWN", error });
      }
    }
    return results;
  }

  public registerForwardAction(
    taskId: string,
    fence: FenceToken,
    forwardTool: string,
    compensationTool: string,
    payload: string,
  ): CompensatableActionRecord {
    const rec: CompensatableActionRecord = {
      taskId,
      fence,
      forwardTool,
      compensationTool,
      payload,
      compensated: false,
      attempts: 0,
    };
    const list = this.records.get(taskId) ?? [];
    list.push(rec);
    this.records.set(taskId, list);
    return rec;
  }

  public compensateLifo(
    taskId: string,
    afterTurnExclusive: number,
    customExecutor?: (compTool: string, payload: string) => boolean,
  ): string[] {
    const list = this.records.get(taskId) ?? [];
    const candidates = list.filter((r) => r.fence.turnSeq > afterTurnExclusive && !r.compensated);
    const executed: string[] = [];
    for (let i = candidates.length - 1; i >= 0; i--) {
      const rec = candidates[i];
      for (let attempt = 0; attempt < this.maxRetries; attempt++) {
        rec.attempts += 1;
        const ok = customExecutor ? customExecutor(rec.compensationTool, rec.payload) : true;
        if (ok) {
          rec.compensated = true;
          executed.push(`${rec.compensationTool}(${rec.payload})`);
          break;
        }
      }
    }
    return executed;
  }
}
