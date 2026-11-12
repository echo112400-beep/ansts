/**
 * @fileoverview M2 TASK-06, M16 TASK-80, M51-3, M60-5 & M68-3: Unified Per-Tenant Binary Max-Heap + Dynamic DRF/Aging/Preemption-Compensation
 * Tournament Queue Manager with Idempotent Enqueue, In-Place Priority Promotion & Eager Tombstone/Empty-Heap Cleanup.
 */
import { TaskPriority, TaskSpec } from "./models.js";

interface TenantHeapNode {
  task: TaskSpec;
  intraTenantStaticKey: number;
}

export class HierarchicalQueueManager {
  private readonly queuedTasks: Map<string, TaskSpec> = new Map();
  private readonly tenantHeaps: Map<string, TenantHeapNode[]> = new Map();
  public readonly tenantActiveCount: Map<string, number> = new Map();
  private readonly agingBoostPerSec: number;
  private readonly preemptionCompensationBoost: number;

  constructor(agingBoostPerSec = 0.05, preemptionCompensationBoost = 120.0) {
    this.agingBoostPerSec =
      Number.isFinite(agingBoostPerSec) && agingBoostPerSec >= 0 ? agingBoostPerSec : 0.05;
    this.preemptionCompensationBoost =
      Number.isFinite(preemptionCompensationBoost) && preemptionCompensationBoost >= 0
        ? preemptionCompensationBoost
        : 120.0;
  }

  private getBasePriorityScore(priority: TaskPriority): number {
    const basePriorityScore: Record<TaskPriority, number> = {
      [TaskPriority.P0_EMERGENCY]: 1000,
      [TaskPriority.P1_HIGH]: 300,
      [TaskPriority.P2_STANDARD]: 150,
      [TaskPriority.P3_BATCH]: 50,
    };
    return basePriorityScore[priority] ?? 100;
  }

  private computeStaticKey(task: TaskSpec): number {
    const preemptionBonus = (task.preempted_count ?? 0) * this.preemptionCompensationBoost;
    return (
      this.getBasePriorityScore(task.priority) +
      preemptionBonus -
      task.created_at * this.agingBoostPerSec
    );
  }

  /**
   * [M60-5] Idempotent enqueue: if `task.task_id` is already present in the tenant heap,
   * replaces and re-heapifies it in place rather than inserting duplicate heap nodes.
   */
  public enqueue(task: TaskSpec): void {
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
  public reprioritizeQueuedTask(task: TaskSpec, newPriority: TaskPriority): boolean {
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
  public removeQueuedTask(taskId: string): void {
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

  public activeTenantHeapCount(): number {
    return this.tenantHeaps.size;
  }

  public getTenantActiveCount(tenantId: string): number {
    return this.tenantActiveCount.get(tenantId) ?? 0;
  }

  public totalQueued(): number {
    return this.queuedTasks.size;
  }

  public computeEffectiveScore(task: TaskSpec, nowSec: number): number {
    const base = this.getBasePriorityScore(task.priority);
    const waitSeconds = Math.max(0, nowSec - task.created_at);
    const agingBonus = waitSeconds * this.agingBoostPerSec;
    const preemptionBonus = (task.preempted_count ?? 0) * this.preemptionCompensationBoost;
    const activeForTenant = this.getTenantActiveCount(task.tenant_id);
    const drfPenalty = activeForTenant * 25.0;
    return base + agingBonus + preemptionBonus - drfPenalty;
  }

  public popNext(): TaskSpec | null {
    if (this.queuedTasks.size === 0) {
      this.tenantHeaps.clear();
      return null;
    }
    const nowSec = Date.now() / 1000;

    let winningTenantId: string | null = null;
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
    const winningHeap = this.tenantHeaps.get(winningTenantId)!;
    const poppedNode = this.heapPop(winningHeap)!;
    if (winningHeap.length === 0) {
      this.tenantHeaps.delete(winningTenantId);
    }
    const bestTask = poppedNode.task;

    this.queuedTasks.delete(bestTask.task_id);
    this.tenantActiveCount.set(
      bestTask.tenant_id,
      this.getTenantActiveCount(bestTask.tenant_id) + 1
    );
    return bestTask;
  }

  public completeTask(task: TaskSpec): void {
    this.removeQueuedTask(task.task_id);
    const curr = this.getTenantActiveCount(task.tenant_id);
    if (curr > 1) {
      this.tenantActiveCount.set(task.tenant_id, curr - 1);
    } else {
      this.tenantActiveCount.delete(task.tenant_id);
    }
  }

  private heapPush(heap: TenantHeapNode[], node: TenantHeapNode): void {
    heap.push(node);
    let idx = heap.length - 1;
    while (idx > 0) {
      const parentIdx = (idx - 1) >> 1;
      if (heap[parentIdx].intraTenantStaticKey >= heap[idx].intraTenantStaticKey) break;
      const tmp = heap[parentIdx];
      heap[parentIdx] = heap[idx];
      heap[idx] = tmp;
      idx = parentIdx;
    }
  }

  private heapPop(heap: TenantHeapNode[]): TenantHeapNode | undefined {
    if (heap.length === 0) return undefined;
    const top = heap[0];
    const last = heap.pop()!;
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

  /**
   * [M79] Returns a real-time, non-destructive inspection snapshot of all per-tenant Binary Max-Heaps,
   * priority bands (P0..P3), DRF active penalties, aging boosts, and the next tournament winner.
   */
  public inspectQueueSnapshot(nowSec = Date.now() / 1000): {
    totalQueuedCount: number;
    activeTenantHeapsCount: number;
    priorityBreakdown: { P0: number; P1: number; P2: number; P3: number };
    nextTournamentWinner: {
      tenantId: string;
      taskId: string;
      priority: number;
      effectiveScore: number;
    } | null;
    tenantHeaps: Array<{
      tenantId: string;
      queuedCount: number;
      activeRunningCount: number;
      drfPenalty: number;
      topTaskId: string | null;
      topEffectiveScore: number | null;
      queuedItems: Array<{
        taskId: string;
        tenantId: string;
        priority: number;
        priorityLabel: string;
        baseScore: number;
        agingBonus: number;
        preemptionBonus: number;
        drfPenalty: number;
        effectiveScore: number;
        waitSeconds: number;
        prompt: string;
      }>;
    }>;
  } {
    const priorityBreakdown = { P0: 0, P1: 0, P2: 0, P3: 0 };
    for (const t of this.queuedTasks.values()) {
      const p = Number(t.priority ?? 1);
      if (p === 0) priorityBreakdown.P0 += 1;
      else if (p === 1) priorityBreakdown.P1 += 1;
      else if (p === 2) priorityBreakdown.P2 += 1;
      else priorityBreakdown.P3 += 1;
    }

    let nextTournamentWinner: {
      tenantId: string;
      taskId: string;
      priority: number;
      effectiveScore: number;
    } | null = null;

    const tenantHeaps: Array<{
      tenantId: string;
      queuedCount: number;
      activeRunningCount: number;
      drfPenalty: number;
      topTaskId: string | null;
      topEffectiveScore: number | null;
      queuedItems: Array<{
        taskId: string;
        tenantId: string;
        priority: number;
        priorityLabel: string;
        baseScore: number;
        agingBonus: number;
        preemptionBonus: number;
        drfPenalty: number;
        effectiveScore: number;
        waitSeconds: number;
        prompt: string;
      }>;
    }> = [];

    const allTenantKeys = new Set<string>([
      ...this.tenantHeaps.keys(),
      ...this.tenantActiveCount.keys(),
    ]);

    for (const tenantId of Array.from(allTenantKeys).sort()) {
      const rawHeap = this.tenantHeaps.get(tenantId) ?? [];
      const validNodes = rawHeap.filter((n) => this.queuedTasks.has(n.task.task_id));
      const activeRunningCount = this.getTenantActiveCount(tenantId);
      const drfPenalty = Number((activeRunningCount * 25.0).toFixed(2));

      const queuedItems = validNodes
        .map((n) => {
          const task = n.task;
          const prio = Number(task.priority ?? 1);
          const prioLabel = prio === 0 ? "P0" : prio === 1 ? "P1" : prio === 2 ? "P2" : "P3";
          const baseScore = this.getBasePriorityScore(task.priority);
          const waitSeconds = Math.max(0, Number((nowSec - task.created_at).toFixed(1)));
          const agingBonus = Number((waitSeconds * this.agingBoostPerSec).toFixed(2));
          const preemptionBonus = Number(
            ((task.preempted_count ?? 0) * this.preemptionCompensationBoost).toFixed(2)
          );
          const effectiveScore = Number(this.computeEffectiveScore(task, nowSec).toFixed(2));
          return {
            taskId: task.task_id,
            tenantId: task.tenant_id,
            priority: prio,
            priorityLabel: prioLabel,
            baseScore,
            agingBonus,
            preemptionBonus,
            drfPenalty,
            effectiveScore,
            waitSeconds,
            prompt: task.prompt ?? "",
          };
        })
        .sort((a, b) => b.effectiveScore - a.effectiveScore);

      const topItem = queuedItems[0] ?? null;
      if (topItem) {
        if (!nextTournamentWinner || topItem.effectiveScore > nextTournamentWinner.effectiveScore) {
          nextTournamentWinner = {
            tenantId,
            taskId: topItem.taskId,
            priority: topItem.priority,
            effectiveScore: topItem.effectiveScore,
          };
        }
      }

      if (queuedItems.length > 0 || activeRunningCount > 0) {
        tenantHeaps.push({
          tenantId,
          queuedCount: queuedItems.length,
          activeRunningCount,
          drfPenalty,
          topTaskId: topItem ? topItem.taskId : null,
          topEffectiveScore: topItem ? topItem.effectiveScore : null,
          queuedItems,
        });
      }
    }

    return {
      totalQueuedCount: this.queuedTasks.size,
      activeTenantHeapsCount: this.tenantHeaps.size,
      priorityBreakdown,
      nextTournamentWinner,
      tenantHeaps,
    };
  }
}
