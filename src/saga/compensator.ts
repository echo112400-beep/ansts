/**
 * @fileoverview M3 TASK-09: Reverse-Order Saga Compensation Rollback Engine.
 */
export interface SagaStepRecord {
  stepId: string;
  taskId: string;
  turnIndex: number;
  toolName: string;
  forwardPayload: Record<string, unknown>;
  compensateHandler?: (payload: Record<string, unknown>) => Record<string, unknown>;
  compensated: boolean;
  compensationResult?: Record<string, unknown>;
}

export class SagaCompensatorEngine {
  private readonly stepsByTask: Map<string, SagaStepRecord[]> = new Map();

  public recordForwardStep(step: Omit<SagaStepRecord, "compensated">): void {
    let list = this.stepsByTask.get(step.taskId);
    if (!list) {
      list = [];
      this.stepsByTask.set(step.taskId, list);
    }
    list.push({ ...step, compensated: false });
  }

  public rollbackTask(taskId: string, upToTurnInclusive = 0): SagaStepRecord[] {
    const list = this.stepsByTask.get(taskId) ?? [];
    const rolledBack: SagaStepRecord[] = [];
    for (let i = list.length - 1; i >= 0; i--) {
      const step = list[i];
      if (!step.compensated && step.turnIndex >= upToTurnInclusive && step.compensateHandler) {
        step.compensationResult = step.compensateHandler(step.forwardPayload);
        step.compensated = true;
        rolledBack.push(step);
      }
    }
    return rolledBack;
  }

  public getTaskSteps(taskId: string): SagaStepRecord[] {
    return [...(this.stepsByTask.get(taskId) ?? [])];
  }
}
