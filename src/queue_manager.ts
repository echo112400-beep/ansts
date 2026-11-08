/**
 * @fileoverview M2 TASK-06: Hierarchical Priority + DRF Multi-Tenant Fairness + Aging Queue Manager.
 */
import { TaskPriority, TaskSpec } from "./models.js";

export class HierarchicalQueueManager {
  private readonly queuedTasks: Map<string, TaskSpec> = new Map();
  public readonly tenantActiveCount: Map<string, number> = new Map();
  private readonly agingBoostPerSec: number;

  constructor(agingBoostPerSec = 0.05) {
    this.agingBoostPerSec = agingBoostPerSec;
  }

  public enqueue(task: TaskSpec): void {
    this.queuedTasks.set(task.task_id, task);
  }

  public getTenantActiveCount(tenantId: string): number {
    return this.tenantActiveCount.get(tenantId) ?? 0;
  }

  public totalQueued(): number {
    return this.queuedTasks.size;
  }

  private computeEffectiveScore(task: TaskSpec, nowSec: number): number {
    const basePriorityScore: Record<TaskPriority, number> = {
      [TaskPriority.P0_EMERGENCY]: 1000,
      [TaskPriority.P1_HIGH]: 300,
      [TaskPriority.P2_STANDARD]: 150,
      [TaskPriority.P3_BATCH]: 50,
    };
    const base = basePriorityScore[task.priority] ?? 100;
    const waitSeconds = Math.max(0, nowSec - task.created_at);
    const agingBonus = waitSeconds * this.agingBoostPerSec;
    const activeForTenant = this.getTenantActiveCount(task.tenant_id);
    const drfPenalty = activeForTenant * 25.0;
    return base + agingBonus - drfPenalty;
  }

  public popNext(): TaskSpec | null {
    if (this.queuedTasks.size === 0) return null;
    const nowSec = Date.now() / 1000;
    let bestTask: TaskSpec | null = null;
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
        this.getTenantActiveCount(bestTask.tenant_id) + 1
      );
    }
    return bestTask;
  }

  public completeTask(task: TaskSpec): void {
    this.queuedTasks.delete(task.task_id);
    const curr = this.getTenantActiveCount(task.tenant_id);
    if (curr > 0) {
      this.tenantActiveCount.set(task.tenant_id, curr - 1);
    }
  }
}
