/**
 * @fileoverview M3 TASK-09 & M53-1: Fault-Tolerant Reverse-Order (LIFO) Saga Compensation Rollback Engine.
 */
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

  public setOnStepCompensated(cb: (step: SagaStepRecord) => void): void {
    this.onStepCompensatedCallback = cb;
  }

  public recordForwardStep(step: Omit<SagaStepRecord, "compensated">): void {
    let list = this.stepsByTask.get(step.taskId);
    if (!list) {
      list = [];
      this.stepsByTask.set(step.taskId, list);
    }
    list.push({ ...step, compensated: false });
  }

  /**
   * [M53-1] Fault-tolerant LIFO rollback: isolates per-step exceptions so a failing
   * compensation hook on step K never blocks steps K-1 .. 0 from rolling back,
   * and notifies `SagaToolGateway` to invalidate stale idempotency keys for rolled-back turns.
   */
  public rollbackTask(taskId: string, upToTurnInclusive = 0): SagaStepRecord[] {
    const list = this.stepsByTask.get(taskId) ?? [];
    const rolledBack: SagaStepRecord[] = [];
    for (let i = list.length - 1; i >= 0; i--) {
      const step = list[i];
      if (!step.compensated && step.turnIndex >= upToTurnInclusive && step.compensateHandler) {
        try {
          step.compensationResult = step.compensateHandler(step.forwardPayload);
          step.compensated = true;
        } catch (err) {
          step.compensationError = (err as Error).message;
          step.compensated = true;
        }
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
