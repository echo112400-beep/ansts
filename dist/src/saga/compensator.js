"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SagaCompensatorEngine = void 0; /**
 * @fileoverview M3 TASK-09: Reverse-Order Saga Compensation Rollback Engine.
 */











class SagaCompensatorEngine {
  stepsByTask = new Map();

  recordForwardStep(step) {
    let list = this.stepsByTask.get(step.taskId);
    if (!list) {
      list = [];
      this.stepsByTask.set(step.taskId, list);
    }
    list.push({ ...step, compensated: false });
  }

  rollbackTask(taskId, upToTurnInclusive = 0) {
    const list = this.stepsByTask.get(taskId) ?? [];
    const rolledBack = [];
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

  getTaskSteps(taskId) {
    return [...(this.stepsByTask.get(taskId) ?? [])];
  }
}exports.SagaCompensatorEngine = SagaCompensatorEngine;