"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.HierarchicalQueueManager = void 0;


var _models = require("./models.js"); /**
 * @fileoverview M2 TASK-06 & M16 TASK-80: O(log N) Indexed Binary Max-Heap Priority + DRF Fairness + Aging Queue Manager.
 */




class HierarchicalQueueManager {
  queuedTasks = new Map();
  maxHeap = [];
  tenantActiveCount = new Map();
  agingBoostPerSec;

  constructor(agingBoostPerSec = 0.05) {
    this.agingBoostPerSec = agingBoostPerSec;
  }

  enqueue(task) {
    this.queuedTasks.set(task.task_id, task);
    const nowSec = Date.now() / 1000;
    this.heapPush({
      task,
      enqueuedScore: this.computeEffectiveScore(task, nowSec)
    });
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

    if (this.queuedTasks.size <= 64) {
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

    while (this.maxHeap.length > 0) {
      const top = this.heapPop();
      if (!this.queuedTasks.has(top.task.task_id)) continue;
      this.queuedTasks.delete(top.task.task_id);
      this.tenantActiveCount.set(
      top.task.tenant_id,
      this.getTenantActiveCount(top.task.tenant_id) + 1);

      return top.task;
    }
    return null;
  }

  completeTask(task) {
    this.queuedTasks.delete(task.task_id);
    const curr = this.getTenantActiveCount(task.tenant_id);
    if (curr > 0) {
      this.tenantActiveCount.set(task.tenant_id, curr - 1);
    }
  }

  heapPush(entry) {
    this.maxHeap.push(entry);
    let idx = this.maxHeap.length - 1;
    while (idx > 0) {
      const parentIdx = idx - 1 >> 1;
      if (this.maxHeap[parentIdx].enqueuedScore >= this.maxHeap[idx].enqueuedScore) break;
      const tmp = this.maxHeap[parentIdx];
      this.maxHeap[parentIdx] = this.maxHeap[idx];
      this.maxHeap[idx] = tmp;
      idx = parentIdx;
    }
  }

  heapPop() {
    if (this.maxHeap.length === 0) return undefined;
    const top = this.maxHeap[0];
    const last = this.maxHeap.pop();
    if (this.maxHeap.length > 0) {
      this.maxHeap[0] = last;
      let idx = 0;
      const len = this.maxHeap.length;
      while (true) {
        const left = (idx << 1) + 1;
        const right = left + 1;
        let largest = idx;
        if (left < len && this.maxHeap[left].enqueuedScore > this.maxHeap[largest].enqueuedScore) {
          largest = left;
        }
        if (right < len && this.maxHeap[right].enqueuedScore > this.maxHeap[largest].enqueuedScore) {
          largest = right;
        }
        if (largest === idx) break;
        const tmp = this.maxHeap[idx];
        this.maxHeap[idx] = this.maxHeap[largest];
        this.maxHeap[largest] = tmp;
        idx = largest;
      }
    }
    return top;
  }
}exports.HierarchicalQueueManager = HierarchicalQueueManager;