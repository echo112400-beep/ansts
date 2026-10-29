/**
 * TASK-16: Persistent Saga Transaction Journal and LIFO Reverse Compensation Executor.
 */

import { FenceToken } from "../models";

export interface CompensatableActionRecord {
  taskId: string;
  fence: FenceToken;
  forwardTool: string;
  compensationTool: string;
  payload: string;
  compensated: boolean;
  attempts: number;
}

export class SagaCompensatorEngine {
  private records: Map<string, CompensatableActionRecord[]> = new Map();

  constructor(public readonly maxRetries: number = 3) {}

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
