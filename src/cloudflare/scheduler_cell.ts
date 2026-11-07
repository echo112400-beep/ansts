/**
 * @fileoverview AnstsSchedulerCell — Durable Object / `celld` Cell implementing 5D Phase & Effort-Aware W-DRF,
 * Multi-Tenant Queueing, DAG Quota Escrow Vouchers, Bayesian Profiler Persistence, V8 Heap/SQL Budget Guards,
 * and both HTTP `fetch()` and Native JavaScript RPC (`js_rpc`) entrypoints.
 *
 * Durability contract (RPO = 0):
 * - Every state mutation (submit / tick / recover / complete) is one SQLite transaction. A throw rolls
 *   back the SQL projection *and* the in-memory runtime caches, so a caller that saw a failure can retry
 *   and observe exactly the pre-failure state.
 * - Queue publication is a transactional outbox: the event is written in the same transaction that admits
 *   the task, and only deleted after the queue acknowledges it. A crash between send and ack re-sends the
 *   *same* `event_id`, so consumers deduplicate instead of double-executing.
 * - Rehydration fails closed: a checkpoint whose payload no longer matches its checksum, or a lease table
 *   written by an incompatible (legacy) schema, refuses to boot into an empty-but-happy scheduler.
 *
 * Follows Google TypeScript Style Guide & Design-by-Contract invariants.
 */

import { AnstsClusterRuntime } from "../cli/anstsctl";
import { ClusterPressureSignals } from "../cluster/backpressure_loop";
import { TaskLeaseRecord } from "../lease_coordinator";
import { AgentTask, FenceToken, TaskState } from "../models";
import { V8HeapAndSqlBudgetGuard } from "./celld_native_optimizations";
import { CloudflareCelldEnv, DurableObjectState } from "./env";

interface RuntimeSnapshot {
  taskStates: Array<{
    taskId: string;
    state: TaskState;
    fence: FenceToken;
    assignedNodeId: string | null;
    assignedModelPool: string | null;
    activeSessions: number;
  }>;
  queueDepths: Map<string, number>;
  leaseRecords: TaskLeaseRecord[];
  leaseRenewals: Map<string, number>;
  escrowFastQueueLength: number;
  hibernatedResumeQueueLength: number;
}

export class AnstsSchedulerCell {
  public readonly cluster: AnstsClusterRuntime;
  public readonly heapSqlGuard = new V8HeapAndSqlBudgetGuard({
    heapLimitMb: 128,
    sqlStatementBudgetMs: 250,
  });

  /** `${tenantId}:${idempotencyKey}` -> taskId, mirrored in `cell_idempotency`. */
  private readonly idempotencyIndex: Map<string, string> = new Map();
  /** taskId -> last time the holding node proved liveness (lease renewal watermark). */
  private readonly leaseRenewals: Map<string, number> = new Map();
  private outboxSeq: number = 1;

  constructor(
    public readonly ctx: DurableObjectState,
    public readonly env: CloudflareCelldEnv,
  ) {
    this.cluster = new AnstsClusterRuntime();
    this.createSchema();
    this.rehydrateSchedulerFromSql();
  }

  // ---------------------------------------------------------------------------
  // Schema & durability primitives
  // ---------------------------------------------------------------------------

  private createSchema(): void {
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cell_tasks (task_id TEXT, tenant_id TEXT, priority_class TEXT, state TEXT, primary_model TEXT, epoch INTEGER, turn_seq INTEGER, enqueue_ts REAL, deadline_ts REAL, raw_spec_json TEXT)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cell_leases (task_id TEXT, node_id TEXT, model_pool TEXT, epoch INTEGER, turn_seq INTEGER, renewed_ts REAL)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cell_dag_vouchers (child_task_id TEXT, parent_task_id TEXT, delegated_tps INTEGER, delegated_tokens INTEGER)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cell_dags (dag_id TEXT, parent_task_id TEXT, children_json TEXT, race_mode INTEGER)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cell_idempotency (task_key TEXT, task_id TEXT)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cell_outbox (seq INTEGER, event_id TEXT, event_json TEXT)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS cell_profiler_snapshots (snapshot_id TEXT, profiler_json TEXT)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS scheduler_checkpoint (ckpt_id TEXT, payload TEXT, checksum TEXT)",
    );
  }

  /**
   * Runs `callback` inside the storage transaction primitive when the host provides one.
   * Hosts without `transactionSync` (in-memory simulators) execute inline.
   */
  private runAtomic<T>(callback: () => T): T {
    const storage = this.ctx.storage;
    if (typeof storage.transactionSync === "function") {
      return storage.transactionSync(callback);
    }
    return callback();
  }

  /** FNV-1a (sync, Worker-compatible). Detects torn/edited checkpoints at boot. */
  private static checksum(payload: string): string {
    let hash = 0x811c9dc5;
    for (let i = 0; i < payload.length; i++) {
      hash ^= payload.charCodeAt(i);
      hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    return hash.toString(16).padStart(8, "0");
  }

  private buildCheckpointPayload(): string {
    const tasks = Array.from(this.cluster.scheduler.tasks.values())
      .map((t) => ({
        task_id: t.taskId,
        tenant_id: t.tenantId,
        state: t.state,
        epoch: t.fence.epoch,
        turn_seq: t.fence.turnSeq,
        node_id: t.assignedNodeId,
      }))
      .sort((a, b) => a.task_id.localeCompare(b.task_id));
    const leases = this.ctx.storage.sql
      .exec("SELECT * FROM cell_leases")
      .toArray()
      .map((r) => ({ task_id: String(r.task_id), node_id: String(r.node_id), epoch: Number(r.epoch) }))
      .sort((a, b) => a.task_id.localeCompare(b.task_id));
    return JSON.stringify({ schema: 2, tasks, leases });
  }

  /** Writes the single-row checkpoint (delete+insert keeps it host-agnostic and idempotent). */
  private writeCheckpoint(): void {
    const payload = this.buildCheckpointPayload();
    this.ctx.storage.sql.exec("DELETE FROM scheduler_checkpoint");
    this.ctx.storage.sql.exec(
      "INSERT INTO scheduler_checkpoint (ckpt_id, payload, checksum) VALUES (?, ?, ?)",
      "latest",
      payload,
      AnstsSchedulerCell.checksum(payload),
    );
  }

  private persistTasks(): void {
    // DAG children and escrow fast-queue members are real tasks that may not be in `scheduler.tasks` yet.
    const byId = new Map<string, AgentTask>();
    for (const t of this.cluster.scheduler.tasks.values()) byId.set(t.taskId, t);
    for (const dag of this.cluster.queueMgr.activeDags.values()) {
      byId.set(dag.parentTask.taskId, dag.parentTask);
      for (const child of dag.children) byId.set(child.taskId, child);
    }
    for (const item of this.cluster.queueMgr.escrowFastQueue) byId.set(item.child.taskId, item.child);

    this.ctx.storage.sql.exec("DELETE FROM cell_tasks");
    for (const t of byId.values()) {
      this.ctx.storage.sql.exec(
        "INSERT INTO cell_tasks (task_id, tenant_id, priority_class, state, primary_model, epoch, turn_seq, enqueue_ts, deadline_ts, raw_spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        t.taskId,
        t.tenantId,
        t.priorityClass,
        t.state,
        t.primaryModel,
        t.fence.epoch,
        t.fence.turnSeq,
        t.enqueueTs,
        t.deadlineTs,
        JSON.stringify({ taskId: t.taskId, tenantId: t.tenantId }),
      );
    }
  }

  private persistLease(taskId: string): void {
    const lease = this.cluster.leaseCoord.getLease(taskId);
    this.ctx.storage.sql.exec("DELETE FROM cell_leases WHERE task_id = ?", taskId);
    if (!lease) return;
    this.ctx.storage.sql.exec(
      "INSERT INTO cell_leases (task_id, node_id, model_pool, epoch, turn_seq, renewed_ts) VALUES (?, ?, ?, ?, ?, ?)",
      lease.taskId,
      lease.nodeId,
      lease.modelPool,
      lease.fence.epoch,
      lease.fence.turnSeq,
      this.leaseRenewals.get(taskId) ?? 0,
    );
  }

  private enqueueOutboxEvent(eventId: string, task: AgentTask): void {
    const event = {
      event_id: eventId,
      event_type: "agent.task.queued",
      task_id: task.taskId,
      tenant_id: task.tenantId,
      epoch: task.fence.epoch,
      turn_seq: task.fence.turnSeq,
      payload: { task_id: task.taskId, tenant_id: task.tenantId, state: task.state },
    };
    this.ctx.storage.sql.exec(
      "INSERT INTO cell_outbox (seq, event_id, event_json) VALUES (?, ?, ?)",
      this.outboxSeq++,
      eventId,
      JSON.stringify(event),
    );
  }

  private snapshotRuntime(): RuntimeSnapshot {
    return {
      taskStates: Array.from(this.cluster.scheduler.tasks.values()).map((t) => ({
        taskId: t.taskId,
        state: t.state,
        fence: new FenceToken(t.fence.epoch, t.fence.turnSeq, t.taskId),
        assignedNodeId: t.assignedNodeId,
        assignedModelPool: t.assignedModelPool,
        activeSessions: t.activeSessions,
      })),
      queueDepths: new Map(
        Array.from(this.cluster.queueMgr.tenantQueues.entries()).map(([tenantId, q]) => [
          tenantId,
          q.length,
        ]),
      ),
      leaseRecords: this.cluster.leaseCoord.snapshotLeases(),
      leaseRenewals: new Map(this.leaseRenewals),
      escrowFastQueueLength: this.cluster.queueMgr.escrowFastQueue.length,
      hibernatedResumeQueueLength: this.cluster.scheduler.hibernatedResumeQueue.length,
    };
  }

  /** Compensating rollback for hosts without transactional SQL (in-memory caches). */
  private restoreRuntime(snapshot: RuntimeSnapshot): void {
    for (const saved of snapshot.taskStates) {
      const task = this.cluster.scheduler.tasks.get(saved.taskId);
      if (!task) continue;
      task.state = saved.state;
      task.fence = saved.fence;
      task.assignedNodeId = saved.assignedNodeId;
      task.assignedModelPool = saved.assignedModelPool;
      task.activeSessions = saved.activeSessions;
    }
    for (const [tenantId, depth] of snapshot.queueDepths.entries()) {
      const q = this.cluster.queueMgr.tenantQueues.get(tenantId);
      if (q && q.length > depth) q.length = depth;
    }
    this.cluster.leaseCoord.restoreLeases(snapshot.leaseRecords);
    this.leaseRenewals.clear();
    for (const [taskId, ts] of snapshot.leaseRenewals.entries()) this.leaseRenewals.set(taskId, ts);
    if (this.cluster.queueMgr.escrowFastQueue.length > snapshot.escrowFastQueueLength) {
      this.cluster.queueMgr.escrowFastQueue.length = snapshot.escrowFastQueueLength;
    }
    if (this.cluster.scheduler.hibernatedResumeQueue.length > snapshot.hibernatedResumeQueueLength) {
      this.cluster.scheduler.hibernatedResumeQueue.length = snapshot.hibernatedResumeQueueLength;
    }
  }

  // ---------------------------------------------------------------------------
  // Rehydration
  // ---------------------------------------------------------------------------

  /**
   * Rehydrates queued/running tasks, CAS leases, DAG escrow vouchers, pending outbox events, durable
   * idempotency keys, and Bayesian conjugate priors from embedded SQLite.
   *
   * @throws Error when the checkpoint checksum no longer matches its payload, or when the lease table was
   * written by an incompatible schema — the cell must not silently boot empty (split-brain risk).
   */
  public rehydrateSchedulerFromSql(): void {
    this.verifyCheckpointIntegrity();
    this.verifyLeaseSchemaVersion();

    const profilerRows = this.heapSqlGuard.executeBoundedSql({
      query: "SELECT * FROM cell_profiler_snapshots",
      simulatedDurationMs: 2,
      executor: () => this.ctx.storage.sql.exec("SELECT * FROM cell_profiler_snapshots").toArray(),
    }).rows;
    if (profilerRows.length > 0) {
      const latest = profilerRows[profilerRows.length - 1];
      this.cluster.profiler.loadFromJSONString(String(latest.profiler_json));
    }

    const taskRows = this.heapSqlGuard.executeBoundedSql({
      query: "SELECT * FROM cell_tasks",
      simulatedDurationMs: 3,
      executor: () => this.ctx.storage.sql.exec("SELECT * FROM cell_tasks").toArray(),
    }).rows;
    for (const r of taskRows) {
      const taskId = String(r.task_id);
      const t = new AgentTask({
        taskId,
        tenantId: String(r.tenant_id),
        priorityClass: String(r.priority_class ?? "P1"),
        primaryModel: (r.primary_model as any) ?? "DeepSeek",
        state: (r.state as TaskState) ?? TaskState.QUEUED,
        enqueueTs: Number(r.enqueue_ts ?? 0),
        deadlineTs: Number(r.deadline_ts ?? 0),
      });
      if (t.deadlineTs <= 0) {
        t.deadlineTs = t.enqueueTs + 300.0;
      }
      t.fence = new FenceToken(Number(r.epoch ?? 0), Number(r.turn_seq ?? 0), taskId);
      this.cluster.scheduler.tasks.set(taskId, t);
      if (t.state === TaskState.QUEUED) {
        this.cluster.queueMgr.enqueue(t);
      }
    }

    const leaseRows = this.heapSqlGuard.executeBoundedSql({
      query: "SELECT * FROM cell_leases",
      simulatedDurationMs: 2,
      executor: () => this.ctx.storage.sql.exec("SELECT * FROM cell_leases").toArray(),
    }).rows;
    for (const lr of leaseRows) {
      const taskId = String(lr.task_id);
      const nodeId = String(lr.node_id);
      const modelPool = String(lr.model_pool ?? "DeepSeek");
      const epoch = Number(lr.epoch ?? 1);
      const turnSeq = Number(lr.turn_seq ?? 1);
      this.leaseRenewals.set(taskId, Number(lr.renewed_ts ?? 0));
      let current = this.cluster.leaseCoord.acquireOrIncrementEpoch(taskId, nodeId, modelPool);
      while (current.epoch < epoch) {
        current = this.cluster.leaseCoord.acquireOrIncrementEpoch(taskId, nodeId, modelPool);
      }
      this.cluster.leaseCoord.verifyAndAdvanceFence(new FenceToken(epoch, turnSeq, taskId));

      const t = this.cluster.scheduler.tasks.get(taskId);
      if (t) {
        if (nodeId.length > 0) {
          t.state = TaskState.RUNNING;
          t.assignedNodeId = nodeId;
          t.assignedModelPool = modelPool;
          t.fence = new FenceToken(epoch, turnSeq, taskId);
        } else {
          t.state = TaskState.QUEUED;
          t.assignedNodeId = null;
          this.cluster.queueMgr.enqueue(t);
        }
      }
    }

    const idemRows = this.ctx.storage.sql.exec("SELECT * FROM cell_idempotency").toArray();
    for (const r of idemRows) {
      this.idempotencyIndex.set(String(r.task_key), String(r.task_id));
    }

    const dagRows = this.ctx.storage.sql.exec("SELECT * FROM cell_dags").toArray();
    for (const r of dagRows) {
      const parent = this.cluster.scheduler.tasks.get(String(r.parent_task_id));
      const childIds: string[] = JSON.parse(String(r.children_json));
      const children = childIds
        .map((cid) => this.cluster.scheduler.tasks.get(cid))
        .filter((c): c is AgentTask => c !== undefined);
      if (!parent || children.length === 0) continue;
      this.cluster.queueMgr.restoreDagGroup(
        String(r.dag_id),
        parent,
        children,
        Number(r.race_mode ?? 0) === 1,
      );
      // DAG children are admitted through the escrow fast queue, never the tenant queue.
      for (const child of children) {
        if (child.state === TaskState.QUEUED) this.cluster.queueMgr.removeFromTenantQueue(child.taskId);
      }
    }

    const voucherRows = this.ctx.storage.sql.exec("SELECT * FROM cell_dag_vouchers").toArray();
    const dagByChild = new Map<string, string>();
    for (const dag of this.cluster.queueMgr.activeDags.values()) {
      for (const child of dag.children) dagByChild.set(child.taskId, dag.dagId);
    }
    for (const v of voucherRows) {
      const childTaskId = String(v.child_task_id);
      this.cluster.queueMgr.restoreEscrowVoucher({
        parentTaskId: String(v.parent_task_id),
        childTaskId,
        delegatedTps: Number(v.delegated_tps ?? 0),
        delegatedTokens: Number(v.delegated_tokens ?? 0),
        dagId: dagByChild.get(childTaskId),
      });
    }
    for (const dag of this.cluster.queueMgr.activeDags.values()) {
      for (const child of dag.children) {
        if (child.state === TaskState.QUEUED) this.cluster.queueMgr.enqueueEscrowChild(child);
      }
    }

    const outboxRows = this.ctx.storage.sql.exec("SELECT * FROM cell_outbox").toArray();
    for (const r of outboxRows) {
      this.outboxSeq = Math.max(this.outboxSeq, Number(r.seq ?? 0) + 1);
    }
  }

  private verifyCheckpointIntegrity(): void {
    const rows = this.ctx.storage.sql.exec("SELECT * FROM scheduler_checkpoint").toArray();
    if (rows.length === 0) return;
    const row = rows[rows.length - 1];
    const payload = String(row.payload ?? "");
    const expected = AnstsSchedulerCell.checksum(payload);
    if (String(row.checksum ?? "") !== expected) {
      throw new Error(
        `SchedulerCheckpointCorrupt: checksum mismatch (stored=${String(row.checksum)}, computed=${expected}); ` +
          "refusing to boot an empty scheduler over damaged durable state",
      );
    }
    const parsed = JSON.parse(payload) as { schema?: number };
    if (Number(parsed.schema ?? 0) < 2) {
      throw new Error(
        "SchedulerCheckpointCorrupt: checkpoint schema predates v2; operator migration required",
      );
    }
  }

  private verifyLeaseSchemaVersion(): void {
    const rows = this.ctx.storage.sql.exec("SELECT * FROM cell_leases").toArray();
    for (const r of rows) {
      const missing =
        r.node_id === undefined ||
        r.node_id === null ||
        r.epoch === undefined ||
        r.epoch === null ||
        r.turn_seq === undefined ||
        r.turn_seq === null;
      if (missing) {
        throw new Error(
          `SchedulerStateRequiresMigration: legacy cell_leases row for task '${String(r.task_id)}' has no ` +
            "(node_id, epoch, turn_seq); operator migration required before this cell may boot",
        );
      }
    }
  }

  public persistProfilerSnapshot(): void {
    this.ctx.storage.sql.exec("DELETE FROM cell_profiler_snapshots WHERE snapshot_id = ?", "latest");
    this.ctx.storage.sql.exec(
      "INSERT INTO cell_profiler_snapshots (snapshot_id, profiler_json) VALUES (?, ?)",
      "latest",
      this.cluster.profiler.toJSONString(),
    );
  }

  // ---------------------------------------------------------------------------
  // Submission
  // ---------------------------------------------------------------------------

  /**
   * Native `js_rpc` entrypoint for zero-serialization intra-node task submission (`celld` RPC compatibility).
   *
   * Idempotency is durable: the `${tenantId}:${idempotencyKey}` -> taskId mapping is written in the same
   * transaction as the task, so a replay after restart returns the original task instead of a duplicate.
   */
  public submitTaskRpc(
    rawSpec: Record<string, any>,
    nowSec: number = Date.now() / 1000.0,
    signal?: AbortSignal,
  ): { statusCode: number; headers: Record<string, string>; body: Record<string, any> } {
    if (signal?.aborted) {
      throw new Error("SchedulerRpcAborted: caller AbortSignal triggered");
    }
    return this.runAtomic(() => this.submitTaskAtomic(rawSpec, nowSec));
  }

  private submitTaskAtomic(
    rawSpec: Record<string, any>,
    nowSec: number,
  ): { statusCode: number; headers: Record<string, string>; body: Record<string, any> } {
    const meta = (rawSpec?.metadata ?? {}) as Record<string, any>;
    const taskId = String(meta.task_id ?? "");
    const idemKey = String(meta.idempotency_key ?? taskId);
    const durableKey = `${String(meta.tenant_id ?? "default")}:${idemKey}`;

    const knownTaskId = this.idempotencyIndex.get(durableKey);
    if (knownTaskId !== undefined) {
      return {
        statusCode: 200,
        headers: { "X-Idempotent-Replay": "true" },
        body: { task_id: knownTaskId, deduplicated: true },
      };
    }
    if (taskId.length > 0 && this.cluster.scheduler.tasks.has(taskId)) {
      return {
        statusCode: 409,
        headers: {},
        body: {
          error: "TaskConflict",
          message: `task_id '${taskId}' already accepted under a different idempotency_key`,
          task_id: taskId,
        },
      };
    }

    const res = this.cluster.apiGateway.submitTaskSpec(rawSpec, nowSec);
    if (res.statusCode !== 202) return res;

    const task = this.cluster.scheduler.tasks.get(String(res.body.task_id))!;
    this.idempotencyIndex.set(durableKey, task.taskId);
    this.ctx.storage.sql.exec(
      "INSERT INTO cell_idempotency (task_key, task_id) VALUES (?, ?)",
      durableKey,
      task.taskId,
    );
    this.persistTasks();
    this.persistLease(task.taskId);
    this.persistProfilerSnapshot();
    this.enqueueOutboxEvent(`evt_${task.taskId}_queued`, task);
    this.writeCheckpoint();
    return res;
  }

  /**
   * Native `js_rpc` entrypoint for 5D W-DRF scheduling tick.
   *
   * @throws Error after rolling back BOTH the SQL projection and the in-memory caches, so the caller sees
   * 503 while the cluster state is exactly what it was before the tick.
   */
  public scheduleTickRpc(nowSec: number = Date.now() / 1000.0): {
    dispatched_count: number;
    dispatched_tasks: Array<Record<string, any>>;
  } {
    const snapshot = this.snapshotRuntime();
    try {
      return this.runAtomic(() => this.scheduleTickAtomic(nowSec));
    } catch (error) {
      this.restoreRuntime(snapshot);
      throw error;
    }
  }

  private scheduleTickAtomic(nowSec: number): {
    dispatched_count: number;
    dispatched_tasks: Array<Record<string, any>>;
  } {
    const dispatched = this.cluster.scheduler.scheduleTick(nowSec);
    for (const t of dispatched) {
      const nodeId = t.assignedNodeId ?? "worker-us-west-01";
      this.cluster.leaseCoord.recordNodeHeartbeat(nodeId, nowSec);
      this.leaseRenewals.set(t.taskId, nowSec);
      this.persistLease(t.taskId);
    }
    this.persistTasks();
    this.writeCheckpoint();
    return {
      dispatched_count: dispatched.length,
      dispatched_tasks: dispatched.map((t) => ({
        task_id: t.taskId,
        node_id: t.assignedNodeId,
        model_pool: t.assignedModelPool,
        fence: { epoch: t.fence.epoch, turn_seq: t.fence.turnSeq },
      })),
    };
  }

  // ---------------------------------------------------------------------------
  // Recovery, heartbeats & lease validation
  // ---------------------------------------------------------------------------

  /**
   * Reclaims tasks whose holder stopped heart-beating. Idempotent: a second recovery pass over the same
   * cluster returns no tasks, and the reclaimed lease epoch always advances so the old holder's fence is
   * permanently stale.
   */
  public recoverDeadLeases(nowSec: number = Date.now() / 1000.0): { recovered_tasks: string[] } {
    const snapshot = this.snapshotRuntime();
    try {
      return this.runAtomic(() => {
        const deadNodes = new Set(this.cluster.leaseCoord.detectDeadNodes(nowSec));
        const timeoutSec = this.cluster.leaseCoord.heartbeatTimeoutSec;
        const recovered: string[] = [];

        for (const task of this.cluster.scheduler.tasks.values()) {
          const lease = this.cluster.leaseCoord.getLease(task.taskId);
          if (!lease || lease.nodeId.length === 0) continue;
          if (task.state === TaskState.SUCCEEDED || task.state === TaskState.CIRCUIT_BROKEN) continue;
          // A lease is only live while its holder renews it inside the heartbeat window; this is what
          // makes recovery work across a full process restart, where in-memory heartbeats are gone.
          const lastRenewal = this.leaseRenewals.get(task.taskId) ?? 0;
          const expired = nowSec - lastRenewal > timeoutSec;
          if (!deadNodes.has(lease.nodeId) && !expired) continue;

          // Bump the epoch and drop the node assignment: the old fence can never validate again.
          this.cluster.leaseCoord.acquireOrIncrementEpoch(task.taskId, "", lease.modelPool);
          this.leaseRenewals.set(task.taskId, nowSec);
          // The holder missed the renewal window, so treat it as failed and keep it out of dispatch.
          deadNodes.add(lease.nodeId);
          task.state = TaskState.QUEUED;
          task.assignedNodeId = null;
          task.assignedModelPool = null;
          task.fence = this.cluster.leaseCoord.getLease(task.taskId)!.fence;
          this.cluster.queueMgr.enqueue(task);
          this.persistLease(task.taskId);
          recovered.push(task.taskId);
        }

        for (const nodeId of deadNodes) {
          const node = this.cluster.scheduler.nodes.get(nodeId);
          if (node) node.alive = false;
        }
        if (recovered.length > 0) {
          this.persistTasks();
          this.writeCheckpoint();
        }
        return { recovered_tasks: recovered };
      });
    } catch (error) {
      this.restoreRuntime(snapshot);
      throw error;
    }
  }

  /**
   * Liveness heartbeat for a worker node. Also renews every lease it holds, so a healthy node never loses
   * its tasks to the recovery sweep.
   */
  public recordNodeHeartbeat(nodeId: string, nowSec: number = Date.now() / 1000.0): void {
    if (!this.cluster.scheduler.nodes.has(nodeId)) {
      this.cluster.scheduler.registerNode({ nodeId });
    }
    const node = this.cluster.scheduler.nodes.get(nodeId)!;
    node.alive = true;
    this.cluster.leaseCoord.recordNodeHeartbeat(nodeId, nowSec);
    this.runAtomic(() => {
      for (const task of this.cluster.scheduler.tasks.values()) {
        const lease = this.cluster.leaseCoord.getLease(task.taskId);
        if (!lease || lease.nodeId !== nodeId) continue;
        this.leaseRenewals.set(task.taskId, nowSec);
        this.persistLease(task.taskId);
      }
    });
  }

  /**
   * Fence validation for the data plane. A lease is only valid while its holder, epoch and turn sequence
   * all match the durable lease record — completed tasks hold no lease, so late writers are rejected.
   */
  public validateLease(body: Record<string, any>): { valid: boolean; reason: string } {
    const taskId = String(body.task_id ?? "");
    const lease = this.cluster.leaseCoord.getLease(taskId);
    if (!lease) return { valid: false, reason: "NoActiveLease" };
    const task = this.cluster.scheduler.tasks.get(taskId);
    if (task && (task.state === TaskState.SUCCEEDED || task.state === TaskState.CIRCUIT_BROKEN)) {
      return { valid: false, reason: `TaskTerminal:${task.state}` };
    }
    if (lease.nodeId !== String(body.node_id ?? "")) {
      return { valid: false, reason: "NodeMismatch" };
    }
    if (lease.fence.epoch !== Number(body.epoch ?? -1)) {
      return { valid: false, reason: "StaleEpoch" };
    }
    if (lease.fence.turnSeq !== Number(body.turn_seq ?? -1)) {
      return { valid: false, reason: "StaleTurnSequence" };
    }
    return { valid: true, reason: "OK" };
  }

  /**
   * Acknowledges task completion. The task becomes terminal and its lease is released in the same
   * transaction, so an obsolete holder can never re-acquire it after a restart.
   */
  public completeTask(body: Record<string, any>): {
    statusCode: number;
    body: Record<string, any>;
  } {
    const taskId = String(body.task_id ?? "");
    const task = this.cluster.scheduler.tasks.get(taskId);
    if (!task) {
      return { statusCode: 404, body: { error: "TaskNotFound", task_id: taskId } };
    }
    if (task.state === TaskState.SUCCEEDED) {
      return { statusCode: 200, body: { task_id: taskId, state: task.state, deduplicated: true } };
    }

    const snapshot = this.snapshotRuntime();
    try {
      return this.runAtomic(() => {
        const lease = this.cluster.leaseCoord.getLease(taskId);
        if (!lease) {
          return { statusCode: 409, body: { error: "StaleFenceRejected", reason: "NoActiveLease" } };
        }
        if (lease.fence.epoch !== Number(body.epoch ?? -1)) {
          return {
            statusCode: 409,
            body: {
              error: "StaleFenceRejected",
              reason: `incoming epoch ${String(body.epoch)} != current ${lease.fence.epoch}`,
            },
          };
        }
        if (lease.nodeId !== String(body.node_id ?? "")) {
          return {
            statusCode: 409,
            body: { error: "StaleFenceRejected", reason: "incoming node is not the lease holder" },
          };
        }

        task.state = TaskState.SUCCEEDED;
        task.activeSessions = 0;
        this.cluster.leaseCoord.releaseLease(taskId);
        this.leaseRenewals.delete(taskId);
        const q = this.cluster.queueMgr.tenantQueues.get(task.tenantId);
        if (q) {
          const idx = q.findIndex((t) => t.taskId === taskId);
          if (idx >= 0) q.splice(idx, 1);
        }
        this.persistLease(taskId);
        this.persistTasks();
        this.writeCheckpoint();
        return { statusCode: 200, body: { task_id: taskId, state: task.state, deduplicated: false } };
      });
    } catch (error) {
      this.restoreRuntime(snapshot);
      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // DAG escrow
  // ---------------------------------------------------------------------------

  /**
   * Yields the parent and issues HMAC-signed quota vouchers for its children. Durable and replay-safe: the
   * same `dag_id` returns the already-issued DAG instead of minting a second set of vouchers.
   */
  public spawnDagEscrow(body: Record<string, any>): { statusCode: number; body: Record<string, any> } {
    const dagId = String(body.dag_id ?? "");
    const childIds: string[] = Array.isArray(body.children_ids) ? body.children_ids.map(String) : [];
    if (dagId.length === 0 || childIds.length === 0) {
      return { statusCode: 400, body: { error: "InvalidDagEscrowRequest" } };
    }
    if (this.cluster.queueMgr.activeDags.has(dagId)) {
      const existing = this.cluster.queueMgr.activeDags.get(dagId)!;
      return {
        statusCode: 200,
        body: {
          dag_id: dagId,
          parent_state: existing.parentTask.state,
          vouchers_issued: existing.children.length,
          fast_path_dispatched: [],
          deduplicated: true,
        },
      };
    }

    const parentTaskId = String(body.parent_task_id ?? "");
    const snapshot = this.snapshotRuntime();
    try {
      return this.runAtomic(() => {
        let parent = this.cluster.scheduler.tasks.get(parentTaskId);
        if (!parent) {
          parent = new AgentTask({ taskId: parentTaskId, tenantId: String(body.tenant_id ?? "fin-enterprise") });
          this.cluster.scheduler.submitTask(parent);
        }
        const children = childIds.map((cid) => {
          const existing = this.cluster.scheduler.tasks.get(cid);
          if (existing) return existing;
          return new AgentTask({ taskId: cid, tenantId: parent!.tenantId });
        });
        const vouchers = this.cluster.queueMgr.spawnDagWithEscrow(
          dagId,
          parent,
          children,
          Boolean(body.race_mode),
        );
        this.ctx.storage.sql.exec("DELETE FROM cell_dags WHERE dag_id = ?", dagId);
        this.ctx.storage.sql.exec(
          "INSERT INTO cell_dags (dag_id, parent_task_id, children_json, race_mode) VALUES (?, ?, ?, ?)",
          dagId,
          parent.taskId,
          JSON.stringify(children.map((c) => c.taskId)),
          body.race_mode ? 1 : 0,
        );
        for (const v of vouchers) {
          this.ctx.storage.sql.exec(
            "INSERT INTO cell_dag_vouchers (child_task_id, parent_task_id, delegated_tps, delegated_tokens) VALUES (?, ?, ?, ?)",
            v.childTaskId,
            v.parentTaskId,
            v.delegatedTps,
            v.delegatedTokens,
          );
        }
        this.persistTasks();
        this.writeCheckpoint();
        return {
          statusCode: 200,
          body: {
            dag_id: dagId,
            parent_state: parent.state,
            vouchers_issued: vouchers.length,
            fast_path_dispatched: [],
            deduplicated: false,
          },
        };
      });
    } catch (error) {
      this.restoreRuntime(snapshot);
      throw error;
    }
  }

  // ---------------------------------------------------------------------------
  // Transactional outbox drain
  // ---------------------------------------------------------------------------

  /**
   * Publishes pending outbox events. The row is deleted only after the queue accepts the event, in the
   * same transaction as the checkpoint, so a send/ack crash retries the *same* `event_id`.
   *
   * @throws Error when the queue or the durability flush fails; pending rows stay put for the next alarm.
   */
  public async alarm(): Promise<void> {
    for (;;) {
      const rows = this.ctx.storage.sql.exec("SELECT * FROM cell_outbox").toArray();
      if (rows.length === 0) return;
      const next = rows
        .map((r) => ({ seq: Number(r.seq), eventId: String(r.event_id), json: String(r.event_json) }))
        .sort((a, b) => a.seq - b.seq)[0];
      const event = JSON.parse(next.json) as Record<string, any>;

      await this.env.ANSTS_EVENTS_QUEUE.send(event);

      this.runAtomic(() => {
        this.ctx.storage.sql.exec("DELETE FROM cell_outbox WHERE seq = ?", next.seq);
        this.writeCheckpoint();
      });
    }
  }

  // ---------------------------------------------------------------------------
  // HTTP surface
  // ---------------------------------------------------------------------------

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const now = Date.now() / 1000.0;
    const json = (body: unknown, status: number): Response =>
      new Response(JSON.stringify(body), {
        status,
        headers: { "content-type": "application/json" },
      });

    if (request.method === "POST" && url.pathname === "/tasks/submit") {
      const rawSpec = (await request.json()) as Record<string, any>;
      let res: { statusCode: number; headers: Record<string, string>; body: Record<string, any> };
      try {
        res = this.submitTaskRpc(rawSpec, now);
      } catch {
        return json({ error: "DurabilityUnconfirmed", retryable: true }, 503);
      }
      try {
        await this.ctx.storage.sync?.();
      } catch {
        // The transaction is already committed, so the retry is a harmless idempotent replay.
        return json({ error: "DurabilityUnconfirmed", retryable: true, task_id: res.body.task_id }, 503);
      }
      if (res.statusCode === 202) {
        const pending = await this.ctx.storage.getAlarm();
        if (pending === null) {
          await this.ctx.storage.setAlarm(Date.now() + 1000);
        }
      }
      return new Response(JSON.stringify(res.body), {
        status: res.statusCode,
        headers: { "content-type": "application/json", ...res.headers },
      });
    }

    if (request.method === "POST" && url.pathname === "/scheduler/tick") {
      try {
        return json(this.scheduleTickRpc(now), 200);
      } catch {
        return json({ error: "TickTransactionFailed", retryable: true }, 503);
      }
    }

    if (request.method === "POST" && url.pathname === "/scheduler/recover") {
      try {
        return json(this.recoverDeadLeases(now), 200);
      } catch {
        return json({ error: "RecoveryTransactionFailed", retryable: true }, 503);
      }
    }

    if (request.method === "POST" && url.pathname === "/nodes/heartbeat") {
      const body = (await request.json().catch(() => ({}))) as Record<string, any>;
      const nodeId = String(body.node_id ?? "worker-us-west-01");
      this.recordNodeHeartbeat(nodeId, now);
      return json({ node_id: nodeId, alive: true, heartbeat_ts: now }, 200);
    }

    if (request.method === "POST" && url.pathname === "/leases/validate") {
      const body = (await request.json().catch(() => ({}))) as Record<string, any>;
      return json(this.validateLease(body), 200);
    }

    if (request.method === "POST" && url.pathname === "/tasks/complete") {
      const body = (await request.json().catch(() => ({}))) as Record<string, any>;
      try {
        const res = this.completeTask(body);
        return json(res.body, res.statusCode);
      } catch {
        return json({ error: "CompletionTransactionFailed", retryable: true }, 503);
      }
    }

    if (request.method === "POST" && url.pathname === "/dag/spawn-escrow") {
      const body = (await request.json().catch(() => ({}))) as Record<string, any>;
      try {
        const res = this.spawnDagEscrow(body);
        return json(res.body, res.statusCode);
      } catch {
        return json({ error: "EscrowTransactionFailed", retryable: true }, 503);
      }
    }

    if (request.method === "POST" && url.pathname === "/chaos/inject") {
      const body = (await request.json()) as { scenario: string; error_rate?: number };
      if (body.scenario === "deepseek-429") {
        this.cluster.cogRouter.tripPoolBreaker("DeepSeek", body.error_rate ?? 0.5);
      }
      const stage = this.cluster.bpController.evaluate(
        { model429Rate: body.error_rate ?? 0.5 } as ClusterPressureSignals,
        Array.from(this.cluster.scheduler.tasks.values()),
      );
      return json(
        {
          scenario: body.scenario,
          deepseek_breaker: this.cluster.cogRouter.pools.DeepSeek.breakerState,
          backpressure_stage: stage,
        },
        200,
      );
    }

    if (request.method === "GET" && url.pathname === "/cluster/top") {
      const snap = this.cluster.clusterTopSnapshot();
      const persistedLeases = this.ctx.storage.sql.exec("SELECT * FROM cell_leases").toArray();
      const persistedTasks = this.ctx.storage.sql.exec("SELECT * FROM cell_tasks").toArray();
      const pendingOutbox = this.ctx.storage.sql.exec("SELECT * FROM cell_outbox").toArray();
      return json(
        {
          ...snap,
          celld_sqlite_leases_count: persistedLeases.length,
          celld_sqlite_tasks_count: persistedTasks.length,
          pending_outbox_events: pendingOutbox.length,
        },
        200,
      );
    }

    return json({ error: "NotFound", path: url.pathname }, 404);
  }
}
