/**
 * @fileoverview M2 TASK-08, M51, M60 & M68-4: 5D Weighted Scoring Engine, Anti-Stale Heartbeat TTL,
 * Head-of-Line Non-Blocking Dispatch, Cascading DAG Dead-Letter Propagation with Active Lease/Resource Reclamation,
 * Fair Preemption, Tenant-Scoped Idempotency, Terminal Content-Fingerprint Release & Mergeable Duplicate In-Flight Coalescing Core Scheduler.
 */
import * as crypto from "node:crypto";
import { LeaseCoordinator } from "./lease_coordinator.js";
import { NodeHeartbeat, TaskPriority, TaskSpec, TaskState } from "./models.js";
import { TaskResourceProfiler } from "./profiler.js";
import { HierarchicalQueueManager } from "./queue_manager.js";

export interface DuplicateInspectionResult {
  deduplicated: boolean;
  mergedBy: "IDEMPOTENCY_KEY" | "CONTENT_FINGERPRINT" | "NONE";
  priorityPromoted: boolean;
  canonicalTask: TaskSpec;
}

const MERGEABLE_ACTIVE_STATES = new Set<TaskState>([
  TaskState.SUBMITTED,
  TaskState.QUEUED,
  TaskState.LEASED,
  TaskState.RUNNING_THINK,
  TaskState.RUNNING_ACT,
  TaskState.WAITING_SUB_AGENT,
  TaskState.SUSPENDED_APPROVAL,
]);

const LEASED_OR_RUNNING_STATES = new Set<TaskState>([
  TaskState.LEASED,
  TaskState.RUNNING_THINK,
  TaskState.RUNNING_ACT,
  TaskState.WAITING_SUB_AGENT,
  TaskState.SUSPENDED_APPROVAL,
]);

export class CoreScheduler5D {
  public readonly queueManager: HierarchicalQueueManager;
  public readonly leaseCoordinator: LeaseCoordinator;
  public readonly profiler: TaskResourceProfiler;
  public readonly nodes: Map<string, NodeHeartbeat> = new Map();
  public readonly allTasks: Map<string, TaskSpec> = new Map();
  public readonly idempotencyIndex: Map<string, string> = new Map();
  public readonly contentFingerprintIndex: Map<string, string> = new Map();
  public readonly completedIds: Set<string> = new Set();
  public readonly deadLetterIds: Set<string> = new Set();
  public readonly nodeHeartbeatTtlSeconds: number;
  public readonly maxPreemptionsPerTask: number;
  public readonly preemptionCooldownSeconds: number;

  constructor(
    leaseTtlSeconds = 15.0,
    nodeHeartbeatTtlSeconds = 30.0,
    maxPreemptionsPerTask = 3,
    preemptionCooldownSeconds = 5.0
  ) {
    this.queueManager = new HierarchicalQueueManager();
    this.leaseCoordinator = new LeaseCoordinator(leaseTtlSeconds);
    this.profiler = new TaskResourceProfiler();
    this.nodeHeartbeatTtlSeconds = nodeHeartbeatTtlSeconds;
    this.maxPreemptionsPerTask = maxPreemptionsPerTask;
    this.preemptionCooldownSeconds = preemptionCooldownSeconds;
  }

  /**
   * [M60-2] Tenant-scoped idempotency key builder preventing cross-tenant collisions.
   */
  public static scopeIdempotencyKey(tenantId: string, idempotencyKey: string): string {
    return `${tenantId}::${idempotencyKey}`;
  }

  /**
   * [M60-5] Computes a deterministic SHA-256 content fingerprint across `(tenant_id, parent_task_id, sorted dependencies, normalized prompt)`
   * so identical concurrent submissions can be inspected and merged into an existing in-flight task.
   */
  public static computeTaskContentFingerprint(
    task: Pick<TaskSpec, "tenant_id" | "prompt" | "dependencies" | "parent_task_id">
  ): string {
    const sortedDeps = [...(task.dependencies ?? [])].sort().join(",");
    const canonical = `${task.tenant_id}::${task.parent_task_id ?? ""}::${sortedDeps}::${(task.prompt ?? "").trim()}`;
    return crypto.createHash("sha256").update(canonical, "utf-8").digest("hex");
  }

  private releaseContentFingerprintIfOwned(task: TaskSpec): void {
    if (
      task.content_fingerprint &&
      this.contentFingerprintIndex.get(task.content_fingerprint) === task.task_id
    ) {
      this.contentFingerprintIndex.delete(task.content_fingerprint);
    }
  }

  public resolveTaskByIdempotencyKey(tenantId: string, idempotencyKey: string): TaskSpec | undefined {
    const scoped = CoreScheduler5D.scopeIdempotencyKey(tenantId, idempotencyKey);
    const id = this.idempotencyIndex.get(scoped) ?? this.idempotencyIndex.get(idempotencyKey);
    if (!id) return undefined;
    const found = this.allTasks.get(id);
    if (found && found.tenant_id === tenantId) {
      return found;
    }
    return undefined;
  }

  public registerOrUpdateNode(heartbeat: NodeHeartbeat): void {
    this.nodes.set(heartbeat.node_id, heartbeat);
  }

  /**
   * [M51-1 / Stale-1 Fix] Marks any node whose `timestamp` is older than `nodeHeartbeatTtlSeconds` as unhealthy.
   */
  public evictStaleNodes(nowSec = Date.now() / 1000): string[] {
    const staleNodeIds: string[] = [];
    for (const [nodeId, node] of this.nodes.entries()) {
      if (node.healthy && nowSec - node.timestamp > this.nodeHeartbeatTtlSeconds) {
        node.healthy = false;
        staleNodeIds.push(nodeId);
      }
    }
    return staleNodeIds;
  }

  private mergeIntoCanonicalTask(
    canonical: TaskSpec,
    incoming: TaskSpec,
    mergedBy: "IDEMPOTENCY_KEY" | "CONTENT_FINGERPRINT"
  ): DuplicateInspectionResult {
    if (incoming.task_id !== canonical.task_id) {
      if (!canonical.merged_request_ids.includes(incoming.task_id)) {
        canonical.merged_request_ids.push(incoming.task_id);
        canonical.merged_duplicate_count += 1;
      } else {
        canonical.merged_duplicate_count += 1;
      }
    } else {
      canonical.merged_duplicate_count += 1;
    }

    if (incoming.idempotency_key) {
      const scopedIncoming = CoreScheduler5D.scopeIdempotencyKey(
        incoming.tenant_id,
        incoming.idempotency_key
      );
      this.idempotencyIndex.set(scopedIncoming, canonical.task_id);
      if (!canonical.merged_idempotency_keys.includes(incoming.idempotency_key)) {
        canonical.merged_idempotency_keys.push(incoming.idempotency_key);
      }
    }

    let priorityPromoted = false;
    if (incoming.priority < canonical.priority && MERGEABLE_ACTIVE_STATES.has(canonical.state)) {
      if (canonical.state === TaskState.QUEUED || canonical.state === TaskState.SUBMITTED) {
        this.queueManager.reprioritizeQueuedTask(canonical, incoming.priority);
      } else {
        canonical.priority = incoming.priority;
      }
      priorityPromoted = true;
    }

    return {
      deduplicated: true,
      mergedBy,
      priorityPromoted,
      canonicalTask: canonical,
    };
  }

  /**
   * [M60-5] Inspects an incoming task for tenant-scoped `idempotency_key` matches OR (when `mergeDuplicateInFlight` is enabled)
   * identical content-fingerprint in-flight/queued matches, merging duplicate metadata and promoting priority if needed.
   */
  public inspectAndMergeDuplicateTask(
    task: TaskSpec,
    options?: { mergeDuplicateInFlight?: boolean }
  ): DuplicateInspectionResult {
    if (!task.content_fingerprint) {
      task.content_fingerprint = CoreScheduler5D.computeTaskContentFingerprint(task);
    }

    if (task.idempotency_key) {
      const scopedKey = CoreScheduler5D.scopeIdempotencyKey(task.tenant_id, task.idempotency_key);
      const existingId = this.idempotencyIndex.get(scopedKey);
      if (existingId !== undefined) {
        const existingTask = this.allTasks.get(existingId);
        if (existingTask && existingTask.tenant_id === task.tenant_id) {
          return this.mergeIntoCanonicalTask(existingTask, task, "IDEMPOTENCY_KEY");
        }
      }
    }

    if (options?.mergeDuplicateInFlight) {
      const fpTaskId = this.contentFingerprintIndex.get(task.content_fingerprint);
      if (fpTaskId !== undefined) {
        const existingTask = this.allTasks.get(fpTaskId);
        if (
          existingTask &&
          existingTask.tenant_id === task.tenant_id &&
          MERGEABLE_ACTIVE_STATES.has(existingTask.state)
        ) {
          return this.mergeIntoCanonicalTask(existingTask, task, "CONTENT_FINGERPRINT");
        }
      }
    }

    return {
      deduplicated: false,
      mergedBy: "NONE",
      priorityPromoted: false,
      canonicalTask: task,
    };
  }

  public submitTask(task: TaskSpec, options?: { mergeDuplicateInFlight?: boolean }): TaskSpec {
    const inspection = this.inspectAndMergeDuplicateTask(task, options);
    if (inspection.deduplicated) {
      return inspection.canonicalTask;
    }

    task.resource_profile = this.profiler.recommendProfile(task);
    this.allTasks.set(task.task_id, task);
    if (task.idempotency_key) {
      const scopedKey = CoreScheduler5D.scopeIdempotencyKey(task.tenant_id, task.idempotency_key);
      this.idempotencyIndex.set(scopedKey, task.task_id);
      if (!task.merged_idempotency_keys.includes(task.idempotency_key)) {
        task.merged_idempotency_keys.push(task.idempotency_key);
      }
    }

    // [M51-2 / Deadlock-2 Fix] If any upstream dependency is already in DEAD_LETTER, immediately fail fast
    const failedDep = task.dependencies.find(
      (depId) => this.deadLetterIds.has(depId) || this.allTasks.get(depId)?.state === TaskState.DEAD_LETTER
    );
    if (failedDep) {
      task.state = TaskState.DEAD_LETTER;
      task.dead_letter_reason = `UPSTREAM_DEPENDENCY_DEAD_LETTER:${failedDep}`;
      this.deadLetterIds.add(task.task_id);
      return task;
    }

    this.contentFingerprintIndex.set(task.content_fingerprint, task.task_id);

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

  public static scoreNodeForTask(
    task: TaskSpec,
    node: NodeHeartbeat,
    nowSec = Date.now() / 1000,
    heartbeatTtlSeconds = 30.0
  ): number {
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
  public scheduleTick(nowSec = Date.now() / 1000): Array<[string, string, number]> {
    this.evictStaleNodes(nowSec);
    const placements: Array<[string, string, number]> = [];
    const deferredTasks: TaskSpec[] = [];
    const maxAttempts = Math.max(1, this.queueManager.totalQueued() * 2);
    let attempts = 0;

    while (attempts < maxAttempts && this.queueManager.totalQueued() > 0) {
      attempts += 1;
      const task = this.queueManager.popNext();
      if (!task) break;

      let bestNode = this.findBestNodeForTask(task, nowSec);

      if (bestNode === null && task.priority <= TaskPriority.P1_HIGH) {
        if (this.tryPreemptBatchTask(task, nowSec)) {
          bestNode = this.findBestNodeForTask(task, nowSec);
        }
      }

      if (bestNode !== null) {
        const lease = this.leaseCoordinator.acquireLease(task.task_id, bestNode.node_id, nowSec);
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
        this.queueManager.completeTask(task);
        deferredTasks.push(task);
      }
    }

    for (const deferred of deferredTasks) {
      this.queueManager.enqueue(deferred);
    }

    return placements;
  }

  private findBestNodeForTask(task: TaskSpec, nowSec: number): NodeHeartbeat | null {
    let bestNode: NodeHeartbeat | null = null;
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
  private tryPreemptBatchTask(urgentTask: TaskSpec, nowSec = Date.now() / 1000): boolean {
    const batchVictims = Array.from(this.allTasks.values()).filter(
      (t) =>
        t.priority === TaskPriority.P3_BATCH &&
        (t.state === TaskState.LEASED ||
          t.state === TaskState.RUNNING_THINK ||
          t.state === TaskState.RUNNING_ACT) &&
        (t.preempted_count ?? 0) < this.maxPreemptionsPerTask &&
        (t.last_preempted_at === 0 || nowSec - t.last_preempted_at >= this.preemptionCooldownSeconds)
    );
    if (batchVictims.length === 0) return false;

    batchVictims.sort((a, b) => {
      const pDiff = (a.preempted_count ?? 0) - (b.preempted_count ?? 0);
      if (pDiff !== 0) return pDiff;
      return b.created_at - a.created_at;
    });

    const victim = batchVictims[0];
    const nodeId = victim.assigned_node_id;
    victim.state = TaskState.PREEMPTING;
    victim.preempted_count = (victim.preempted_count ?? 0) + 1;
    victim.last_preempted_at = nowSec;

    if (nodeId && this.nodes.has(nodeId)) {
      const node = this.nodes.get(nodeId)!;
      node.free_cpu_cores = Math.min(
        node.total_cpu_cores,
        node.free_cpu_cores +
          Math.max(urgentTask.resource_profile.think_cpu_cores, victim.resource_profile.think_cpu_cores, 1.0)
      );
      node.free_memory_mb = Math.min(
        node.total_memory_mb,
        node.free_memory_mb +
          Math.max(
            urgentTask.resource_profile.act_memory_mb,
            Math.floor(victim.resource_profile.act_memory_mb * 0.25)
          )
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
    if (!task || task.state === TaskState.COMPLETED || task.state === TaskState.DEAD_LETTER) {
      return false;
    }

    task.state = TaskState.COMPLETED;
    this.completedIds.add(taskId);
    this.leaseCoordinator.revokeLease(taskId);
    this.queueManager.completeTask(task);
    this.releaseContentFingerprintIfOwned(task);
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

  /**
   * [M51-2 / Deadlock-2 Fix & M68-4] Cascades `DEAD_LETTER` state to all transitive downstream dependents
   * so child tasks never hang forever in `SUBMITTED` when a parent/dependency dies, AND reclaims any active
   * lease, node CPU/memory/`running_tasks`, and `tenantActiveCount` if a dependent task was already leased or running.
   */
  public cascadeDeadLetterToDependents(failedTaskId: string): string[] {
    this.deadLetterIds.add(failedTaskId);
    const failedRoot = this.allTasks.get(failedTaskId);
    if (failedRoot) {
      this.releaseContentFingerprintIfOwned(failedRoot);
    }
    const cascadedIds: string[] = [];
    let progress = true;
    while (progress) {
      progress = false;
      for (const candidate of this.allTasks.values()) {
        if (
          candidate.state !== TaskState.DEAD_LETTER &&
          candidate.state !== TaskState.COMPLETED &&
          candidate.dependencies.some((depId) => this.deadLetterIds.has(depId))
        ) {
          const triggerDep = candidate.dependencies.find((depId) => this.deadLetterIds.has(depId))!;
          const activeLease = this.leaseCoordinator.getLease(candidate.task_id);
          if (activeLease || LEASED_OR_RUNNING_STATES.has(candidate.state)) {
            const nodeIdToRelease = activeLease?.node_id ?? candidate.assigned_node_id;
            if (nodeIdToRelease) {
              const node = this.nodes.get(nodeIdToRelease);
              if (node) {
                node.free_cpu_cores = Math.min(
                  node.total_cpu_cores,
                  node.free_cpu_cores + candidate.resource_profile.think_cpu_cores
                );
                node.free_memory_mb = Math.min(
                  node.total_memory_mb,
                  node.free_memory_mb + Math.floor(candidate.resource_profile.act_memory_mb * 0.25)
                );
                node.running_tasks = Math.max(0, node.running_tasks - 1);
              }
            }
            this.leaseCoordinator.revokeLease(candidate.task_id);
            this.queueManager.completeTask(candidate);
            candidate.assigned_node_id = undefined;
          } else if (candidate.state === TaskState.QUEUED) {
            this.queueManager.removeQueuedTask(candidate.task_id);
          }
          candidate.state = TaskState.DEAD_LETTER;
          candidate.dead_letter_reason = `UPSTREAM_DEPENDENCY_DEAD_LETTER:${triggerDep}`;
          this.releaseContentFingerprintIfOwned(candidate);
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
  public reconcileOrphanedLeases(nowSec = Date.now() / 1000): string[] {
    const expired = this.leaseCoordinator.scanExpiredLeases(nowSec);
    const recoveredIds: string[] = [];
    for (const lease of expired) {
      const task = this.allTasks.get(lease.task_id);
      if (!task || task.state === TaskState.COMPLETED || task.state === TaskState.DEAD_LETTER) {
        continue;
      }

      const oldNode = this.nodes.get(lease.node_id);
      if (oldNode) {
        oldNode.free_cpu_cores = Math.min(
          oldNode.total_cpu_cores,
          oldNode.free_cpu_cores + task.resource_profile.think_cpu_cores
        );
        oldNode.free_memory_mb = Math.min(
          oldNode.total_memory_mb,
          oldNode.free_memory_mb + Math.floor(task.resource_profile.act_memory_mb * 0.25)
        );
        oldNode.running_tasks = Math.max(0, oldNode.running_tasks - 1);
      }

      this.queueManager.completeTask(task);
      task.retry_count += 1;
      if (task.retry_count > 3) {
        task.state = TaskState.DEAD_LETTER;
        task.dead_letter_reason = "MAX_LEASE_RETRIES_EXCEEDED";
        task.assigned_node_id = undefined;
        this.releaseContentFingerprintIfOwned(task);
        this.cascadeDeadLetterToDependents(task.task_id);
      } else {
        task.state = TaskState.QUEUED;
        task.assigned_node_id = undefined;
        this.queueManager.enqueue(task);
        recoveredIds.push(task.task_id);
      }
    }
    return recoveredIds;
  }

  /**
   * [M71-1 / F1] Adaptive DAG Subgraph Re-planning in CoreScheduler5D:
   * - Prunes obsolete/cancelled/failed child tasks (`pruneChildIds` & `fallbackForFailedChildId`),
   *   reclaiming active leases, node CPU/memory/`running_tasks`, and queue entries.
   * - Injects new dynamic fan-out or fallback tasks (`injectChildren`) and coordinates with `DagEscrowCoordinator.replanSubgraph`.
   * - Automatically rewires downstream tasks whose `dependencies` referenced `fallbackForFailedChildId` or `pruneChildIds`
   *   to depend on the newly injected replacement tasks, and revives downstream tasks that were previously dead-lettered
   *   due to `UPSTREAM_DEPENDENCY_DEAD_LETTER:<fallbackForFailedChildId>`.
   */
  public replanDagSubgraph(params: {
    parentTaskId: string;
    pruneChildIds?: string[];
    injectChildren?: TaskSpec[];
    fallbackForFailedChildId?: string;
    escrowCoordinator?: {
      replanSubgraph: (args: {
        parent: TaskSpec;
        pruneChildIds?: string[];
        injectChildren?: TaskSpec[];
        fallbackForFailedChildId?: string;
        maxTenantQuota?: number;
        currentTenantUsed?: number;
      }) => {
        replanRevision: number;
        prunedChildIds: string[];
        injectedChildIds: string[];
        reclaimedSlots: number;
        expandedEscrowSlots: number;
        activeChildCount: number;
        parentState: TaskState;
      };
    };
    maxTenantQuota?: number;
    currentTenantUsed?: number;
  }): {
    parentTaskId: string;
    prunedChildIds: string[];
    injectedChildIds: string[];
    rewiredDownstreamTaskIds: string[];
    revivedDownstreamTaskIds: string[];
    replanRevision: number;
    parentState: TaskState;
  } {
    const parent = this.allTasks.get(params.parentTaskId);
    if (!parent) {
      throw new Error(`Parent task '${params.parentTaskId}' not found in CoreScheduler5D`);
    }

    const pruneSet = new Set<string>([
      ...(params.pruneChildIds ?? []),
      ...(params.fallbackForFailedChildId ? [params.fallbackForFailedChildId] : []),
    ]);

    const prunedChildIds: string[] = [];
    for (const pruneId of pruneSet) {
      this.deadLetterIds.delete(pruneId);
      const t = this.allTasks.get(pruneId);
      if (!t || t.state === TaskState.COMPLETED) continue;

      const activeLease = this.leaseCoordinator.getLease(t.task_id);
      if (activeLease || LEASED_OR_RUNNING_STATES.has(t.state)) {
        const nodeIdToRelease = activeLease?.node_id ?? t.assigned_node_id;
        if (nodeIdToRelease) {
          const node = this.nodes.get(nodeIdToRelease);
          if (node) {
            node.free_cpu_cores = Math.min(
              node.total_cpu_cores,
              node.free_cpu_cores + t.resource_profile.think_cpu_cores
            );
            node.free_memory_mb = Math.min(
              node.total_memory_mb,
              node.free_memory_mb + Math.floor(t.resource_profile.act_memory_mb * 0.25)
            );
            node.running_tasks = Math.max(0, node.running_tasks - 1);
          }
        }
        this.leaseCoordinator.revokeLease(t.task_id);
        this.queueManager.completeTask(t);
        t.assigned_node_id = undefined;
      } else if (t.state === TaskState.QUEUED) {
        this.queueManager.removeQueuedTask(t.task_id);
      }

      t.state = TaskState.CANCELLED;
      t.dead_letter_reason = "PRUNED_BY_ADAPTIVE_DAG_REPLAN";
      this.releaseContentFingerprintIfOwned(t);
      prunedChildIds.push(t.task_id);
    }

    let replanRevision = 1;
    if (params.escrowCoordinator) {
      const escrowRes = params.escrowCoordinator.replanSubgraph({
        parent,
        pruneChildIds: params.pruneChildIds,
        injectChildren: params.injectChildren,
        fallbackForFailedChildId: params.fallbackForFailedChildId,
        maxTenantQuota: params.maxTenantQuota,
        currentTenantUsed: params.currentTenantUsed,
      });
      replanRevision = escrowRes.replanRevision;
    }

    this.deadLetterIds.delete(parent.task_id);

    const injectedChildIds: string[] = [];
    for (const child of params.injectChildren ?? []) {
      child.dependencies = Array.from(
        new Set([
          ...child.dependencies.filter((d) => !pruneSet.has(d)),
          ...(child.dependencies.length === 0 ? [] : []),
        ])
      );
      this.submitTask(child);
      injectedChildIds.push(child.task_id);
    }

    const rewiredDownstreamTaskIds: string[] = [];
    const revivedDownstreamTaskIds: string[] = [];
    for (const candidate of this.allTasks.values()) {
      if (
        candidate.task_id === parent.task_id ||
        pruneSet.has(candidate.task_id) ||
        injectedChildIds.includes(candidate.task_id)
      ) {
        continue;
      }
      const hadPrunedDep = candidate.dependencies.some((d) => pruneSet.has(d));
      if (!hadPrunedDep) continue;

      const updatedDeps = new Set<string>();
      for (const dep of candidate.dependencies) {
        if (pruneSet.has(dep)) {
          for (const replId of injectedChildIds) {
            updatedDeps.add(replId);
          }
        } else {
          updatedDeps.add(dep);
        }
      }
      candidate.dependencies = Array.from(updatedDeps);
      rewiredDownstreamTaskIds.push(candidate.task_id);

      if (
        candidate.state === TaskState.DEAD_LETTER &&
        Array.from(pruneSet).some(
          (pId) => candidate.dead_letter_reason === `UPSTREAM_DEPENDENCY_DEAD_LETTER:${pId}`
        )
      ) {
        this.deadLetterIds.delete(candidate.task_id);
        candidate.dead_letter_reason = "";
        if (this.dependenciesSatisfied(candidate)) {
          candidate.state = TaskState.QUEUED;
          this.queueManager.enqueue(candidate);
        } else {
          candidate.state = TaskState.SUBMITTED;
        }
        revivedDownstreamTaskIds.push(candidate.task_id);
      } else if (candidate.state === TaskState.SUBMITTED && this.dependenciesSatisfied(candidate)) {
        candidate.state = TaskState.QUEUED;
        this.queueManager.enqueue(candidate);
      }
    }

    return {
      parentTaskId: parent.task_id,
      prunedChildIds,
      injectedChildIds,
      rewiredDownstreamTaskIds,
      revivedDownstreamTaskIds,
      replanRevision,
      parentState: parent.state,
    };
  }
}
