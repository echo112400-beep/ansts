"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.HierarchicalQueueManager = void 0;


var _models = require("./models.js"); /**
 * @fileoverview M2 TASK-06: Hierarchical Priority + DRF Multi-Tenant Fairness + Aging Queue Manager.
 */class HierarchicalQueueManager {
  queuedTasks = new Map();
  tenantActiveCount = new Map();
  agingBoostPerSec;

  constructor(agingBoostPerSec = 0.05) {
    this.agingBoostPerSec = agingBoostPerSec;
  }

  enqueue(task) {
    this.queuedTasks.set(task.task_id, task);
  }

  getTenantActiveCount(tenantId) {
    return this.tenantActiveCount.get(tenantId) ?? 0;
  }

  totalQueued() {
    return this.queuedTasks.size;
  }

  computeEffectiveScore(task, nowSec) {
    const basePriorityScore = {
      [_models.TaskPriority.P0_EMERGENCY]: 1000,
      [_models.TaskPriority.P1_HIGH]: 300,
      [_models.TaskPriority.P2_STANDARD]: 150,
      [_models.TaskPriority.P3_BATCH]: 50
    };
    const base = basePriorityScore[task.priority] ?? 100;
    const waitSeconds = Math.max(0, nowSec - task.created_at);
    const agingBonus = waitSeconds * this.agingBoostPerSec;
    const activeForTenant = this.getTenantActiveCount(task.tenant_id);
    const drfPenalty = activeForTenant * 25.0;
    return base + agingBonus - drfPenalty;
  }

  popNext() {
    if (this.queuedTasks.size === 0) return null;
    const nowSec = Date.now() / 1000;
    let bestTask = null;
    let bestScore = -Infinity;

    for (const candidate of this.queuedTasks.values()) {
      const score = this.computeEffectiveScore(candidate, nowSec);
      if (score > bestScore) {
        bestScore = score;
        bestTask = candidate;
      }
    }

    if (bestTask) {
      this.queuedTasks.delete(bestTask.task_id);
      this.tenantActiveCount.set(
      bestTask.tenant_id,
      this.getTenantActiveCount(bestTask.tenant_id) + 1);

    }
    return bestTask;
  }

  completeTask(task) {
    this.queuedTasks.delete(task.task_id);
    const curr = this.getTenantActiveCount(task.tenant_id);
    if (curr > 0) {
      this.tenantActiveCount.set(task.tenant_id, curr - 1);
    }
  }
}exports.HierarchicalQueueManager = HierarchicalQueueManager;