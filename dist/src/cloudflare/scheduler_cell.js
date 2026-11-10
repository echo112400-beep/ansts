"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.AnstsSchedulerCell = void 0;




var _scheduler = require("../scheduler.js");
var _models = require("../models.js"); /**
 * @fileoverview M7 TASK-26 & M52-1: AnstsSchedulerCell (Stateful Durable Object Control Plane with SQLite Backing,
 * Monotonic Fencing Watermark Rehydration, Interrupted-Lease Recovery & Active-Lease Alarm Renewal).
 */class AnstsSchedulerCell {ctx;
  scheduler;

  constructor(ctx) {
    this.ctx = ctx;
    this.scheduler = new _scheduler.CoreScheduler5D(15.0);
    this.initSchema();
    this.rehydrateFromSql();
  }

  initSchema() {
    this.ctx.storage.sql.exec(
    "CREATE TABLE IF NOT EXISTS ansts_tasks (task_id TEXT PRIMARY KEY, tenant_id TEXT, priority INTEGER, state TEXT, fencing_token INTEGER, assigned_node_id TEXT, payload_json TEXT)");

    this.ctx.storage.sql.exec(
    "CREATE TABLE IF NOT EXISTS ansts_nodes (node_id TEXT PRIMARY KEY, heartbeat_json TEXT, updated_at INTEGER)");

  }

  rehydrateSchedulerFromSql() {
    return this.rehydrateFromSql();
  }

  async submitTaskRpc(task, signal) {
    if (signal?.aborted) {
      throw new Error("submitTaskRpc aborted by caller AbortSignal");
    }
    return this.submitTask(task);
  }

  async scheduleTickRpc(signal) {
    if (signal?.aborted) {
      throw new Error("scheduleTickRpc aborted by caller AbortSignal");
    }
    return this.alarm();
  }

  /**
   * [M52-1] Rehydrates registered nodes, monotonic fencing token watermarks, and interrupted LEASED tasks
   * (converting orphaned LEASED tasks back to QUEUED on cell migration/restart so they never hang).
   */
  rehydrateFromSql() {
    const nodeRows = this.ctx.storage.sql.
    exec("SELECT * FROM ansts_nodes").
    toArray();
    for (const nr of nodeRows) {
      if (!nr.heartbeat_json) continue;
      try {
        const parsedHb = JSON.parse(String(nr.heartbeat_json));
        this.scheduler.registerOrUpdateNode(
        new _models.NodeHeartbeat({
          node_id: String(nr.node_id),
          total_cpu_cores: Number(parsedHb.total_cpu_cores ?? 32),
          free_cpu_cores: Number(parsedHb.free_cpu_cores ?? 32),
          total_memory_mb: Number(parsedHb.total_memory_mb ?? 65536),
          free_memory_mb: Number(parsedHb.free_memory_mb ?? 65536),
          warm_pool_ready: Number(parsedHb.warm_pool_ready ?? 8),
          psi_memory_some_avg10: Number(parsedHb.psi_memory_some_avg10 ?? 1.5),
          running_tasks: Number(parsedHb.running_tasks ?? 0),
          healthy: Boolean(parsedHb.healthy ?? true)
        }));

      } catch {

        // ignore malformed node row
      }}

    const rows = this.ctx.storage.sql.
    exec("SELECT * FROM ansts_tasks").
    toArray();
    let count = 0;
    for (const r of rows) {
      if (!r.payload_json) continue;
      const parsed = JSON.parse(String(r.payload_json));
      const spec = new _models.TaskSpec({
        task_id: String(r.task_id),
        tenant_id: String(r.tenant_id),
        priority: Number(r.priority ?? _models.TaskPriority.P2_STANDARD),
        prompt: String(parsed.prompt ?? ""),
        max_turns: Number(parsed.max_turns ?? 30),
        timeout_seconds: Number(parsed.timeout_seconds ?? 1800),
        dependencies: Array.isArray(parsed.dependencies) ? parsed.dependencies.map(String) : []
      });
      spec.state = r.state ?? _models.TaskState.QUEUED;
      spec.lease_fencing_token = Number(r.fencing_token ?? 0);
      spec.retry_count = Number(parsed.retry_count ?? 0);
      spec.preempted_count = Number(parsed.preempted_count ?? 0);
      spec.assigned_node_id = r.assigned_node_id ? String(r.assigned_node_id) : undefined;

      if (spec.lease_fencing_token > 0) {
        this.scheduler.leaseCoordinator.seedHighestFencingToken(spec.task_id, spec.lease_fencing_token);
      }

      if (
      spec.state === _models.TaskState.LEASED ||
      spec.state === _models.TaskState.RUNNING_THINK ||
      spec.state === _models.TaskState.RUNNING_ACT)
      {
        if (!this.scheduler.leaseCoordinator.getLease(spec.task_id)) {
          spec.state = _models.TaskState.QUEUED;
          spec.assigned_node_id = undefined;
        }
      }

      this.scheduler.allTasks.set(spec.task_id, spec);
      if (spec.state === _models.TaskState.QUEUED) {
        this.scheduler.queueManager.enqueue(spec);
      } else if (spec.state === _models.TaskState.COMPLETED) {
        this.scheduler.completedIds.add(spec.task_id);
      } else if (spec.state === _models.TaskState.DEAD_LETTER) {
        this.scheduler.deadLetterIds.add(spec.task_id);
      }
      count += 1;
    }
    return count;
  }

  persistTask(task) {
    this.ctx.storage.sql.exec(
    "INSERT OR REPLACE INTO ansts_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
    task.task_id,
    task.tenant_id,
    task.priority,
    task.state,
    task.lease_fencing_token,
    task.assigned_node_id ?? "",
    JSON.stringify({
      prompt: task.prompt,
      max_turns: task.max_turns,
      timeout_seconds: task.timeout_seconds,
      dependencies: task.dependencies,
      retry_count: task.retry_count,
      preempted_count: task.preempted_count,
      dead_letter_reason: task.dead_letter_reason
    }));

  }

  async submitTask(task) {
    const submitted = this.scheduler.submitTask(task);
    this.persistTask(submitted);
    const currentAlarm = await this.ctx.storage.getAlarm();
    if (!currentAlarm) {
      await this.ctx.storage.setAlarm(Date.now() + 1000);
    }
    return submitted;
  }

  registerNode(hb) {
    this.scheduler.registerOrUpdateNode(hb);
    this.ctx.storage.sql.exec(
    "INSERT OR REPLACE INTO ansts_nodes (node_id, heartbeat_json, updated_at) VALUES (?, ?, ?)",
    hb.node_id,
    JSON.stringify(hb),
    Date.now());

  }

  /**
   * [M52-1] Persists all reconciled/dead-lettered tasks as well as newly placed tasks,
   * and renews the DO alarm whenever queued tasks OR active leases exist.
   */
  async alarm() {
    const recoveredIds = this.scheduler.reconcileOrphanedLeases();
    const placements = this.scheduler.scheduleTick();

    for (const t of this.scheduler.allTasks.values()) {
      this.persistTask(t);
    }

    if (
    this.scheduler.queueManager.totalQueued() > 0 ||
    this.scheduler.leaseCoordinator.activeLeaseCount() > 0)
    {
      await this.ctx.storage.setAlarm(Date.now() + 1000);
    }
    return { placements, recoveredIds };
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "POST" && url.pathname === "/submit") {
      const body = await request.json();
      const spec = new _models.TaskSpec({
        task_id: String(body.task_id),
        tenant_id: String(body.tenant_id ?? "default"),
        priority: Number(body.priority ?? _models.TaskPriority.P2_STANDARD),
        prompt: String(body.prompt ?? "")
      });
      const submitted = await this.submitTask(spec);
      return new Response(JSON.stringify({ task_id: submitted.task_id, state: submitted.state }), {
        status: 202,
        headers: { "content-type": "application/json" }
      });
    }
    if (request.method === "POST" && url.pathname === "/schedule-tick") {
      const res = await this.alarm();
      return new Response(JSON.stringify(res), {
        status: 200,
        headers: { "content-type": "application/json" }
      });
    }
    return new Response(JSON.stringify({ cellId: this.ctx.id.toString() }), { status: 200 });
  }
}exports.AnstsSchedulerCell = AnstsSchedulerCell;