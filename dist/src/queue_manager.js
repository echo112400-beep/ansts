"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.HierarchicalQueueManager = void 0;



var _models = require("./models.js"); /**
 * @fileoverview M2 TASK-06, M16 TASK-80, M51-3, M60-5 & M68-3: Unified Per-Tenant Binary Max-Heap + Dynamic DRF/Aging/Preemption-Compensation
 * Tournament Queue Manager with Idempotent Enqueue, In-Place Priority Promotion & Eager Tombstone/Empty-Heap Cleanup.
 */



class HierarchicalQueueManager {
  queuedTasks = new Map();
  tenantHeaps = new Map();
  tenantActiveCount = new Map();
  agingBoostPerSec;
  preemptionCompensationBoost;

  constructor(agingBoostPerSec = 0.05, preemptionCompensationBoost = 120.0) {
    this.agingBoostPerSec =
    Number.isFinite(agingBoostPerSec) && agingBoostPerSec >= 0 ? agingBoostPerSec : 0.05;
    this.preemptionCompensationBoost =
    Number.isFinite(preemptionCompensationBoost) && preemptionCompensationBoost >= 0 ?
    preemptionCompensationBoost :
    120.0;
  }

  getBasePriorityScore(priority) {
    const basePriorityScore = {
      [_models.TaskPriority.P0_EMERGENCY]: 1000,
      [_models.TaskPriority.P1_HIGH]: 300,
      [_models.TaskPriority.P2_STANDARD]: 150,
      [_models.TaskPriority.P3_BATCH]: 50
    };
    return basePriorityScore[priority] ?? 100;
  }

  computeStaticKey(task) {
    const preemptionBonus = (task.preempted_count ?? 0) * this.preemptionCompensationBoost;
    return (
      this.getBasePriorityScore(task.priority) +
      preemptionBonus -
      task.created_at * this.agingBoostPerSec);

  }

  /**
   * [M60-5] Idempotent enqueue: if `task.task_id` is already present in the tenant heap,
   * replaces and re-heapifies it in place rather than inserting duplicate heap nodes.
   */
  enqueue(task) {
    const alreadyQueued = this.queuedTasks.has(task.task_id);
    this.queuedTasks.set(task.task_id, task);
    let heap = this.tenantHeaps.get(task.tenant_id);
    if (!heap) {
      heap = [];
      this.tenantHeaps.set(task.tenant_id, heap);
    }
    if (alreadyQueued) {
      const filtered = heap.filter((n) => n.task.task_id !== task.task_id);
      heap.length = 0;
      for (const item of filtered) {
        this.heapPush(heap, item);
      }
    }
    const intraTenantStaticKey = this.computeStaticKey(task);
    this.heapPush(heap, { task, intraTenantStaticKey });
  }

  /**
   * [M60-5] Promotes/updates the priority of an already-queued task when a higher-urgency
   * duplicate request is merged into it, re-heapifying the tenant queue immediately.
   */
  reprioritizeQueuedTask(task, newPriority) {
    task.priority = newPriority;
    if (!this.queuedTasks.has(task.task_id)) {
      return false;
    }
    this.enqueue(task);
    return true;
  }

  /**
   * [M68-3] Eagerly removes `taskId` from both `queuedTasks` and the owning tenant's binary heap,
   * and deletes empty tenant heap arrays so cancelled/dead-lettered tasks never linger as zombie heap nodes.
   */
  removeQueuedTask(taskId) {
    const existing = this.queuedTasks.get(taskId);
    this.queuedTasks.delete(taskId);
    if (existing) {
      const heap = this.tenantHeaps.get(existing.tenant_id);
      if (heap) {
        const filtered = heap.filter((n) => n.task.task_id !== taskId);
        if (filtered.length === 0) {
          this.tenantHeaps.delete(existing.tenant_id);
        } else if (filtered.length !== heap.length) {
          heap.length = 0;
          for (const item of filtered) {
            this.heapPush(heap, item);
          }
        }
      }
      return;
    }
    for (const [tenantId, heap] of this.tenantHeaps.entries()) {
      const filtered = heap.filter((n) => n.task.task_id !== taskId);
      if (filtered.length === 0) {
        this.tenantHeaps.delete(tenantId);
      } else if (filtered.length !== heap.length) {
        heap.length = 0;
        for (const item of filtered) {
          this.heapPush(heap, item);
        }
      }
    }
  }

  activeTenantHeapCount() {
    return this.tenantHeaps.size;
  }

  getTenantActiveCount(tenantId) {
    return this.tenantActiveCount.get(tenantId) ?? 0;
  }

  totalQueued() {
    return this.queuedTasks.size;
  }

  computeEffectiveScore(task, nowSec) {
    const base = this.getBasePriorityScore(task.priority);
    const waitSeconds = Math.max(0, nowSec - task.created_at);
    const agingBonus = waitSeconds * this.agingBoostPerSec;
    const preemptionBonus = (task.preempted_count ?? 0) * this.preemptionCompensationBoost;
    const activeForTenant = this.getTenantActiveCount(task.tenant_id);
    const drfPenalty = activeForTenant * 25.0;
    return base + agingBonus + preemptionBonus - drfPenalty;
  }

  popNext() {
    if (this.queuedTasks.size === 0) {
      this.tenantHeaps.clear();
      return null;
    }
    const nowSec = Date.now() / 1000;

    let winningTenantId = null;
    let winningScore = -Infinity;

    for (const [tenantId, heap] of Array.from(this.tenantHeaps.entries())) {
      while (heap.length > 0 && !this.queuedTasks.has(heap[0].task.task_id)) {
        this.heapPop(heap);
      }
      if (heap.length === 0) {
        this.tenantHeaps.delete(tenantId);
        continue;
      }
      const topTask = heap[0].task;
      const dynamicScore = this.computeEffectiveScore(topTask, nowSec);
      if (dynamicScore > winningScore) {
        winningScore = dynamicScore;
        winningTenantId = tenantId;
      }
    }

    if (!winningTenantId) return null;
    const winningHeap = this.tenantHeaps.get(winningTenantId);
    const poppedNode = this.heapPop(winningHeap);
    if (winningHeap.length === 0) {
      this.tenantHeaps.delete(winningTenantId);
    }
    const bestTask = poppedNode.task;

    this.queuedTasks.delete(bestTask.task_id);
    this.tenantActiveCount.set(
    bestTask.tenant_id,
    this.getTenantActiveCount(bestTask.tenant_id) + 1);

    return bestTask;
  }

  completeTask(task) {
    this.removeQueuedTask(task.task_id);
    const curr = this.getTenantActiveCount(task.tenant_id);
    if (curr > 1) {
      this.tenantActiveCount.set(task.tenant_id, curr - 1);
    } else {
      this.tenantActiveCount.delete(task.tenant_id);
    }
  }

  heapPush(heap, node) {
    heap.push(node);
    let idx = heap.length - 1;
    while (idx > 0) {
      const parentIdx = idx - 1 >> 1;
      if (heap[parentIdx].intraTenantStaticKey >= heap[idx].intraTenantStaticKey) break;
      const tmp = heap[parentIdx];
      heap[parentIdx] = heap[idx];
      heap[idx] = tmp;
      idx = parentIdx;
    }
  }

  heapPop(heap) {
    if (heap.length === 0) return undefined;
    const top = heap[0];
    const last = heap.pop();
    if (heap.length > 0) {
      heap[0] = last;
      let idx = 0;
      const len = heap.length;
      while (true) {
        const left = (idx << 1) + 1;
        const right = left + 1;
        let largest = idx;
        if (left < len && heap[left].intraTenantStaticKey > heap[largest].intraTenantStaticKey) {
          largest = left;
        }
        if (right < len && heap[right].intraTenantStaticKey > heap[largest].intraTenantStaticKey) {
          largest = right;
        }
        if (largest === idx) break;
        const tmp = heap[idx];
        heap[idx] = heap[largest];
        heap[largest] = tmp;
        idx = largest;
      }
    }
    return top;
  }
}exports.HierarchicalQueueManager = HierarchicalQueueManager;