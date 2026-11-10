"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CoreScheduler5D = void 0;



var _lease_coordinator = require("./lease_coordinator.js");
var _models = require("./models.js");
var _profiler = require("./profiler.js");
var _queue_manager = require("./queue_manager.js"); /**
 * @fileoverview M2 TASK-08 & M51: 5D Weighted Scoring Engine, Anti-Stale Heartbeat TTL,
 * Head-of-Line Non-Blocking Dispatch, Cascading DAG Dead-Letter Propagation & Fair Preemption Core Scheduler.
 */class CoreScheduler5D {queueManager;
  leaseCoordinator;
  profiler;
  nodes = new Map();
  allTasks = new Map();
  idempotencyIndex = new Map();
  completedIds = new Set();
  deadLetterIds = new Set();
  nodeHeartbeatTtlSeconds;
  maxPreemptionsPerTask;
  preemptionCooldownSeconds;

  constructor(
  leaseTtlSeconds = 15.0,
  nodeHeartbeatTtlSeconds = 30.0,
  maxPreemptionsPerTask = 3,
  preemptionCooldownSeconds = 5.0)
  {
    this.queueManager = new _queue_manager.HierarchicalQueueManager();
    this.leaseCoordinator = new _lease_coordinator.LeaseCoordinator(leaseTtlSeconds);
    this.profiler = new _profiler.TaskResourceProfiler();
    this.nodeHeartbeatTtlSeconds = nodeHeartbeatTtlSeconds;
    this.maxPreemptionsPerTask = maxPreemptionsPerTask;
    this.preemptionCooldownSeconds = preemptionCooldownSeconds;
  }

  registerOrUpdateNode(heartbeat) {
    this.nodes.set(heartbeat.node_id, heartbeat);
  }

  /**
   * [M51-1 / Stale-1 Fix] Marks any node whose `timestamp` is older than `nodeHeartbeatTtlSeconds` as unhealthy.
   */
  evictStaleNodes(nowSec = Date.now() / 1000) {
    const staleNodeIds = [];
    for (const [nodeId, node] of this.nodes.entries()) {
      if (node.healthy && nowSec - node.timestamp > this.nodeHeartbeatTtlSeconds) {
        node.healthy = false;
        staleNodeIds.push(nodeId);
      }
    }
    return staleNodeIds;
  }

  submitTask(task) {
    if (task.idempotency_key) {
      const existingId = this.idempotencyIndex.get(task.idempotency_key);
      if (existingId !== undefined) {
        return this.allTasks.get(existingId);
      }
    }

    task.resource_profile = this.profiler.recommendProfile(task);
    this.allTasks.set(task.task_id, task);
    if (task.idempotency_key) {
      this.idempotencyIndex.set(task.idempotency_key, task.task_id);
    }

    // [M51-2 / Deadlock-2 Fix] If any upstream dependency is already in DEAD_LETTER, immediately fail fast
    const failedDep = task.dependencies.find(
    (depId) => this.deadLetterIds.has(depId) || this.allTasks.get(depId)?.state === _models.TaskState.DEAD_LETTER);

    if (failedDep) {
      task.state = _models.TaskState.DEAD_LETTER;
      task.dead_letter_reason = `UPSTREAM_DEPENDENCY_DEAD_LETTER:${failedDep}`;
      this.deadLetterIds.add(task.task_id);
      return task;
    }

    if (this.dependenciesSatisfied(task)) {
      task.state = _models.TaskState.QUEUED;
      this.queueManager.enqueue(task);
    } else {
      task.state = _models.TaskState.SUBMITTED;
    }
    return task;
  }

  dependenciesSatisfied(task) {
    return task.dependencies.every((depId) => this.completedIds.has(depId));
  }

  static scoreNodeForTask(
  task,
  node,
  nowSec = Date.now() / 1000,
  heartbeatTtlSeconds = 30.0)
  {
    if (!node.healthy || node.psi_memory_some_avg10 > 60.0) {
      return -1.0;
    }
    // [M51-1 / Stale-1 Fix] Reject nodes with stale heartbeat timestamps
    if (nowSec - node.timestamp > heartbeatTtlSeconds) {
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

  /**
   * [M51-2 / Deadlock-1 Fix] Eliminates intra-tenant Head-of-Line (HoL) blocking:
   * Tasks that cannot fit any node (even after preemption attempts) are held in `deferredTasks`
   * until the tick finishes so smaller tasks behind them in the same tenant's heap can be scheduled.
   */
  scheduleTick(nowSec = Date.now() / 1000) {
    this.evictStaleNodes(nowSec);
    const placements = [];
    const deferredTasks = [];
    const maxAttempts = Math.max(1, this.queueManager.totalQueued() * 2);
    let attempts = 0;

    while (attempts < maxAttempts && this.queueManager.totalQueued() > 0) {
      attempts += 1;
      const task = this.queueManager.popNext();
      if (!task) break;

      let bestNode = this.findBestNodeForTask(task, nowSec);

      if (bestNode === null && task.priority <= _models.TaskPriority.P1_HIGH) {
        if (this.tryPreemptBatchTask(task, nowSec)) {
          bestNode = this.findBestNodeForTask(task, nowSec);
        }
      }

      if (bestNode !== null) {
        const lease = this.leaseCoordinator.acquireLease(task.task_id, bestNode.node_id, nowSec);
        task.state = _models.TaskState.LEASED;
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
        this.queueManager.completeTask(task);
        deferredTasks.push(task);
      }
    }

    for (const deferred of deferredTasks) {
      this.queueManager.enqueue(deferred);
    }

    return placements;
  }

  findBestNodeForTask(task, nowSec) {
    let bestNode = null;
    let bestScore = -1.0;
    for (const node of this.nodes.values()) {
      const s = CoreScheduler5D.scoreNodeForTask(task, node, nowSec, this.nodeHeartbeatTtlSeconds);
      if (s > bestScore) {
        bestScore = s;
        bestNode = node;
      }
    }
    return bestNode;
  }

  /**
   * [M51-3 / Starvation-1 Fix] Selects the `P3_BATCH` victim with the lowest `preempted_count`
   * that is not within `preemptionCooldownSeconds` and has not exceeded `maxPreemptionsPerTask`.
   */
  tryPreemptBatchTask(urgentTask, nowSec = Date.now() / 1000) {
    const batchVictims = Array.from(this.allTasks.values()).filter(
    (t) =>
    t.priority === _models.TaskPriority.P3_BATCH && (
    t.state === _models.TaskState.LEASED ||
    t.state === _models.TaskState.RUNNING_THINK ||
    t.state === _models.TaskState.RUNNING_ACT) &&
    (t.preempted_count ?? 0) < this.maxPreemptionsPerTask && (
    t.last_preempted_at === 0 || nowSec - t.last_preempted_at >= this.preemptionCooldownSeconds));

    if (batchVictims.length === 0) return false;

    batchVictims.sort((a, b) => {
      const pDiff = (a.preempted_count ?? 0) - (b.preempted_count ?? 0);
      if (pDiff !== 0) return pDiff;
      return b.created_at - a.created_at;
    });

    const victim = batchVictims[0];
    const nodeId = victim.assigned_node_id;
    victim.state = _models.TaskState.PREEMPTING;
    victim.preempted_count = (victim.preempted_count ?? 0) + 1;
    victim.last_preempted_at = nowSec;

    if (nodeId && this.nodes.has(nodeId)) {
      const node = this.nodes.get(nodeId);
      node.free_cpu_cores = Math.min(
      node.total_cpu_cores,
      node.free_cpu_cores + Math.max(urgentTask.resource_profile.think_cpu_cores, victim.resource_profile.think_cpu_cores, 1.0));

      node.free_memory_mb = Math.min(
      node.total_memory_mb,
      node.free_memory_mb + Math.max(urgentTask.resource_profile.act_memory_mb, Math.floor(victim.resource_profile.act_memory_mb * 0.25)));

      node.running_tasks = Math.max(0, node.running_tasks - 1);
    }

    this.leaseCoordinator.revokeLease(victim.task_id);
    this.queueManager.completeTask(victim);
    victim.state = _models.TaskState.QUEUED;
    victim.assigned_node_id = undefined;
    this.queueManager.enqueue(victim);
    return true;
  }

  completeTaskExecution(
  taskId,
  nodeId,
  fencingToken,
  observedPeakCpu = 1.0,
  observedPeakMemoryMb = 1024)
  {
    if (!this.leaseCoordinator.validateFencingToken(taskId, fencingToken)) {
      return false;
    }
    const task = this.allTasks.get(taskId);
    if (!task) return false;

    task.state = _models.TaskState.COMPLETED;
    this.completedIds.add(taskId);
    this.leaseCoordinator.revokeLease(taskId);
    this.queueManager.completeTask(task);
    this.profiler.recordExecutionMetrics(task, observedPeakCpu, observedPeakMemoryMb);

    const node = this.nodes.get(nodeId);
    if (node) {
      node.free_cpu_cores = Math.min(
      node.total_cpu_cores,
      node.free_cpu_cores + task.resource_profile.think_cpu_cores);

      node.free_memory_mb = Math.min(
      node.total_memory_mb,
      node.free_memory_mb + Math.floor(task.resource_profile.act_memory_mb * 0.25));

      node.running_tasks = Math.max(0, node.running_tasks - 1);
    }

    for (const candidate of this.allTasks.values()) {
      if (candidate.state === _models.TaskState.SUBMITTED && this.dependenciesSatisfied(candidate)) {
        candidate.state = _models.TaskState.QUEUED;
        this.queueManager.enqueue(candidate);
      }
    }
    return true;
  }

  /**
   * [M51-2 / Deadlock-2 Fix] Cascades `DEAD_LETTER` state to all transitive downstream dependents
   * so child tasks never hang forever in `SUBMITTED` when a parent/dependency dies.
   */
  cascadeDeadLetterToDependents(failedTaskId) {
    this.deadLetterIds.add(failedTaskId);
    const cascadedIds = [];
    let progress = true;
    while (progress) {
      progress = false;
      for (const candidate of this.allTasks.values()) {
        if (
        candidate.state !== _models.TaskState.DEAD_LETTER &&
        candidate.state !== _models.TaskState.COMPLETED &&
        candidate.dependencies.some((depId) => this.deadLetterIds.has(depId)))
        {
          const triggerDep = candidate.dependencies.find((depId) => this.deadLetterIds.has(depId));
          if (candidate.state === _models.TaskState.QUEUED) {
            this.queueManager.removeQueuedTask(candidate.task_id);
          }
          candidate.state = _models.TaskState.DEAD_LETTER;
          candidate.dead_letter_reason = `UPSTREAM_DEPENDENCY_DEAD_LETTER:${triggerDep}`;
          this.deadLetterIds.add(candidate.task_id);
          cascadedIds.push(candidate.task_id);
          progress = true;
        }
      }
    }
    return cascadedIds;
  }

  /**
   * [M51-1 / Stale-2 Fix & M51-2 / Deadlock-2 Fix] Reclaims CPU/memory/`running_tasks` on the
   * original node when an orphaned lease expires, and cascades `DEAD_LETTER` to downstream DAG tasks.
   */
  reconcileOrphanedLeases(nowSec = Date.now() / 1000) {
    const expired = this.leaseCoordinator.scanExpiredLeases(nowSec);
    const recoveredIds = [];
    for (const lease of expired) {
      const task = this.allTasks.get(lease.task_id);
      if (!task || task.state === _models.TaskState.COMPLETED || task.state === _models.TaskState.DEAD_LETTER) {
        continue;
      }

      const oldNode = this.nodes.get(lease.node_id);
      if (oldNode) {
        oldNode.free_cpu_cores = Math.min(
        oldNode.total_cpu_cores,
        oldNode.free_cpu_cores + task.resource_profile.think_cpu_cores);

        oldNode.free_memory_mb = Math.min(
        oldNode.total_memory_mb,
        oldNode.free_memory_mb + Math.floor(task.resource_profile.act_memory_mb * 0.25));

        oldNode.running_tasks = Math.max(0, oldNode.running_tasks - 1);
      }

      this.queueManager.completeTask(task);
      task.retry_count += 1;
      if (task.retry_count > 3) {
        task.state = _models.TaskState.DEAD_LETTER;
        task.dead_letter_reason = "MAX_LEASE_RETRIES_EXCEEDED";
        task.assigned_node_id = undefined;
        this.cascadeDeadLetterToDependents(task.task_id);
      } else {
        task.state = _models.TaskState.QUEUED;
        task.assigned_node_id = undefined;
        this.queueManager.enqueue(task);
        recoveredIds.push(task.task_id);
      }
    }
    return recoveredIds;
  }
}exports.CoreScheduler5D = CoreScheduler5D;