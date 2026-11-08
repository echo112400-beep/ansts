/**
 * @fileoverview M2 TASK-06 & M16 TASK-80: Unified Per-Tenant Binary Max-Heap + Dynamic DRF/Aging Tournament Queue Manager.
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

  constructor(agingBoostPerSec = 0.05) {
    this.agingBoostPerSec = agingBoostPerSec;
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

  public enqueue(task: TaskSpec): void {
    this.queuedTasks.set(task.task_id, task);
    let heap = this.tenantHeaps.get(task.tenant_id);
    if (!heap) {
      heap = [];
      this.tenantHeaps.set(task.tenant_id, heap);
    }
    const intraTenantStaticKey =
      this.getBasePriorityScore(task.priority) - task.created_at * this.agingBoostPerSec;
    this.heapPush(heap, { task, intraTenantStaticKey });
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
    const activeForTenant = this.getTenantActiveCount(task.tenant_id);
    const drfPenalty = activeForTenant * 25.0;
    return base + agingBonus - drfPenalty;
  }

  public popNext(): TaskSpec | null {
    if (this.queuedTasks.size === 0) return null;
    const nowSec = Date.now() / 1000;

    let winningTenantId: string | null = null;
    let winningScore = -Infinity;

    for (const [tenantId, heap] of this.tenantHeaps.entries()) {
      while (heap.length > 0 && !this.queuedTasks.has(heap[0].task.task_id)) {
        this.heapPop(heap);
      }
      if (heap.length === 0) continue;
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
    const bestTask = poppedNode.task;

    this.queuedTasks.delete(bestTask.task_id);
    this.tenantActiveCount.set(
      bestTask.tenant_id,
      this.getTenantActiveCount(bestTask.tenant_id) + 1
    );
    return bestTask;
  }

  public completeTask(task: TaskSpec): void {
    this.queuedTasks.delete(task.task_id);
    const curr = this.getTenantActiveCount(task.tenant_id);
    if (curr > 0) {
      this.tenantActiveCount.set(task.tenant_id, curr - 1);
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
}
