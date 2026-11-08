/**
 * @fileoverview M2 TASK-08: 5D Weighted Scoring Engine, Preemption & DAG Orchestration Core Scheduler.
 */
import { LeaseCoordinator } from "./lease_coordinator.js";
import { NodeHeartbeat, TaskPriority, TaskSpec, TaskState } from "./models.js";
import { TaskResourceProfiler } from "./profiler.js";
import { HierarchicalQueueManager } from "./queue_manager.js";

export class CoreScheduler5D {
  public readonly queueManager: HierarchicalQueueManager;
  public readonly leaseCoordinator: LeaseCoordinator;
  public readonly profiler: TaskResourceProfiler;
  public readonly nodes: Map<string, NodeHeartbeat> = new Map();
  public readonly allTasks: Map<string, TaskSpec> = new Map();
  public readonly idempotencyIndex: Map<string, string> = new Map();
  public readonly completedIds: Set<string> = new Set();

  constructor(leaseTtlSeconds = 15.0) {
    this.queueManager = new HierarchicalQueueManager();
    this.leaseCoordinator = new LeaseCoordinator(leaseTtlSeconds);
    this.profiler = new TaskResourceProfiler();
  }

  public registerOrUpdateNode(heartbeat: NodeHeartbeat): void {
    this.nodes.set(heartbeat.node_id, heartbeat);
  }

  public submitTask(task: TaskSpec): TaskSpec {
    if (task.idempotency_key) {
      const existingId = this.idempotencyIndex.get(task.idempotency_key);
      if (existingId !== undefined) {
        return this.allTasks.get(existingId)!;
      }
    }

    task.resource_profile = this.profiler.recommendProfile(task);
    this.allTasks.set(task.task_id, task);
    if (task.idempotency_key) {
      this.idempotencyIndex.set(task.idempotency_key, task.task_id);
    }

    if (this.dependenciesSatisfied(task)) {
      task.state = TaskState.QUEUED;
      this.queueManager.enqueue(task);
    } else {
      task.state = TaskState.SUBMITTED;
    }
    return task;
  }

  private dependenciesSatisfied(task: TaskSpec): boolean {
    return task.dependencies.every((depId) => this.completedIds.has(depId));
  }

  public static scoreNodeForTask(task: TaskSpec, node: NodeHeartbeat): number {
    if (!node.healthy || node.psi_memory_some_avg10 > 60.0) {
      return -1.0;
    }
    const reqCpu = task.resource_profile.think_cpu_cores;
    const reqMem = task.resource_profile.act_memory_mb;
    if (node.free_cpu_cores < reqCpu || node.free_memory_mb < reqMem) {
      return -1.0;
    }

    const cpuBalance = Math.min(1.0, node.free_cpu_cores / Math.max(1.0, node.total_cpu_cores));
    const memBalance = Math.min(1.0, node.free_memory_mb / Math.max(1.0, node.total_memory_mb));
    const sResource = (cpuBalance + memBalance) * 15.0;
    const sWarmPool = node.warm_pool_ready > 0 ? 25.0 : 5.0;
    const sCacheLocality =
      task.prefix_Cache_hash && node.cached_images.has(task.prefix_Cache_hash) ? 20.0 : 0.0;
    const sPsiHealth = Math.max(0.0, 20.0 * (1.0 - node.psi_memory_some_avg10 / 60.0));
    const sQueuePenalty = Math.max(0.0, 10.0 - node.running_tasks * 0.5);

    return sResource + sWarmPool + sCacheLocality + sPsiHealth + sQueuePenalty;
  }

  public scheduleTick(): Array<[string, string, number]> {
    const placements: Array<[string, string, number]> = [];
    const maxAttempts = this.queueManager.totalQueued();
    let attempts = 0;

    while (attempts < maxAttempts) {
      attempts += 1;
      const task = this.queueManager.popNext();
      if (!task) break;

      let bestNode: NodeHeartbeat | null = null;
      let bestScore = -1.0;
      for (const node of this.nodes.values()) {
        const s = CoreScheduler5D.scoreNodeForTask(task, node);
        if (s > bestScore) {
          bestScore = s;
          bestNode = node;
        }
      }

      if (bestNode !== null) {
        const lease = this.leaseCoordinator.acquireLease(task.task_id, bestNode.node_id);
        task.state = TaskState.LEASED;
        task.assigned_node_id = bestNode.node_id;
        task.lease_fencing_token = lease.fencing_token;

        bestNode.free_cpu_cores -= task.resource_profile.think_cpu_cores;
        bestNode.free_memory_mb -= Math.floor(task.resource_profile.act_memory_mb * 0.25);
        bestNode.running_tasks += 1;
        if (bestNode.warm_pool_ready > 0) {
          bestNode.warm_pool_ready -= 1;
        }
        placements.push([task.task_id, bestNode.node_id, lease.fencing_token]);
      } else {
        if (task.priority <= TaskPriority.P1_HIGH && this.tryPreemptBatchTask(task)) {
          this.queueManager.enqueue(task);
          this.queueManager.completeTask(task);
        } else {
          this.queueManager.completeTask(task);
          this.queueManager.enqueue(task);
        }
      }
    }
    return placements;
  }

  private tryPreemptBatchTask(urgentTask: TaskSpec): boolean {
    const batchVictims = Array.from(this.allTasks.values()).filter(
      (t) =>
        t.priority === TaskPriority.P3_BATCH &&
        (t.state === TaskState.LEASED ||
          t.state === TaskState.RUNNING_THINK ||
          t.state === TaskState.RUNNING_ACT)
    );
    if (batchVictims.length === 0) return false;

    const victim = batchVictims[0];
    const nodeId = victim.assigned_node_id;
    victim.state = TaskState.PREEMPTING;

    if (nodeId && this.nodes.has(nodeId)) {
      const node = this.nodes.get(nodeId)!;
      node.free_cpu_cores = Math.min(
        node.total_cpu_cores,
        node.free_cpu_cores + Math.max(urgentTask.resource_profile.think_cpu_cores, 1.0)
      );
      node.free_memory_mb = Math.min(
        node.total_memory_mb,
        node.free_memory_mb + urgentTask.resource_profile.act_memory_mb
      );
      node.running_tasks = Math.max(0, node.running_tasks - 1);
    }

    this.leaseCoordinator.revokeLease(victim.task_id);
    this.queueManager.completeTask(victim);
    victim.state = TaskState.QUEUED;
    victim.assigned_node_id = undefined;
    this.queueManager.enqueue(victim);
    return true;
  }

  public completeTaskExecution(
    taskId: string,
    nodeId: string,
    fencingToken: number,
    observedPeakCpu = 1.0,
    observedPeakMemoryMb = 1024
  ): boolean {
    if (!this.leaseCoordinator.validateFencingToken(taskId, fencingToken)) {
      return false;
    }
    const task = this.allTasks.get(taskId);
    if (!task) return false;

    task.state = TaskState.COMPLETED;
    this.completedIds.add(taskId);
    this.leaseCoordinator.revokeLease(taskId);
    this.queueManager.completeTask(task);
    this.profiler.recordExecutionMetrics(task, observedPeakCpu, observedPeakMemoryMb);

    const node = this.nodes.get(nodeId);
    if (node) {
      node.free_cpu_cores = Math.min(
        node.total_cpu_cores,
        node.free_cpu_cores + task.resource_profile.think_cpu_cores
      );
      node.free_memory_mb = Math.min(
        node.total_memory_mb,
        node.free_memory_mb + Math.floor(task.resource_profile.act_memory_mb * 0.25)
      );
      node.running_tasks = Math.max(0, node.running_tasks - 1);
    }

    for (const candidate of this.allTasks.values()) {
      if (candidate.state === TaskState.SUBMITTED && this.dependenciesSatisfied(candidate)) {
        candidate.state = TaskState.QUEUED;
        this.queueManager.enqueue(candidate);
      }
    }
    return true;
  }

  public reconcileOrphanedLeases(): string[] {
    const expired = this.leaseCoordinator.scanExpiredLeases();
    const recoveredIds: string[] = [];
    for (const lease of expired) {
      const task = this.allTasks.get(lease.task_id);
      if (!task || task.state === TaskState.COMPLETED || task.state === TaskState.DEAD_LETTER) {
        continue;
      }
      this.queueManager.completeTask(task);
      task.retry_count += 1;
      if (task.retry_count > 3) {
        task.state = TaskState.DEAD_LETTER;
      } else {
        task.state = TaskState.QUEUED;
        task.assigned_node_id = undefined;
        this.queueManager.enqueue(task);
        recoveredIds.push(task.task_id);
      }
    }
    return recoveredIds;
  }
}
