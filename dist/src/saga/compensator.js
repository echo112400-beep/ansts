"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SagaCompensatorEngine = void 0; /**
 * @fileoverview M3 TASK-09 & M53-1: Fault-Tolerant Reverse-Order (LIFO) Saga Compensation Rollback Engine.
 */













class SagaCompensatorEngine {
  stepsByTask = new Map();
  onStepCompensatedCallback;

  setOnStepCompensated(cb) {
    this.onStepCompensatedCallback = cb;
  }

  recordForwardStep(step) {
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
  rollbackTask(taskId, upToTurnInclusive = 0) {
    const list = this.stepsByTask.get(taskId) ?? [];
    const rolledBack = [];
    for (let i = list.length - 1; i >= 0; i--) {
      const step = list[i];
      if (!step.compensated && step.turnIndex >= upToTurnInclusive && step.compensateHandler) {
        try {
          step.compensationResult = step.compensateHandler(step.forwardPayload);
          step.compensated = true;
        } catch (err) {
          step.compensationError = err.message;
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

  getTaskSteps(taskId) {
    return [...(this.stepsByTask.get(taskId) ?? [])];
  }
}exports.SagaCompensatorEngine = SagaCompensatorEngine;