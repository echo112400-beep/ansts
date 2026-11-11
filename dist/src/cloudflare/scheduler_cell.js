"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.AnstsSchedulerCell = void 0;





var _scheduler = require("../scheduler.js");
var _models = require("../models.js"); /**
 * @fileoverview M7 TASK-26, M52-1, M60-1 & M64-1/M64-2: AnstsSchedulerCell (Stateful Durable Object Control Plane with SQLite Backing,
 * Monotonic Fencing Watermark Rehydration, Tenant-Scoped Idempotency Index Restoration, 100% TaskSpec Field Fidelity,
 * Interrupted-Lease Recovery, Alarm Renewal & Worker Prefix-Aware Route Matching).
 */class AnstsSchedulerCell {ctx;scheduler;

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
   * [M52-1, M60-1 & M64-2] Rehydrates registered nodes, monotonic fencing token watermarks, tenant-scoped `idempotencyIndex`,
   * `contentFingerprintIndex`, full `TaskSpec` fields (`resource_profile`, `parent_task_id`, `expected_SubAgent_count`, `prefix_Cache_hash`),
   * `current_turn` breakpoint progress, and interrupted LEASED tasks.
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
      const rawProfile =
      parsed.resource_profile && typeof parsed.resource_profile === "object" ?
      parsed.resource_profile :
      undefined;
      const spec = new _models.TaskSpec({
        task_id: String(r.task_id),
        tenant_id: String(r.tenant_id),
        priority: Number(r.priority ?? _models.TaskPriority.P2_STANDARD),
        prompt: String(parsed.prompt ?? ""),
        max_turns: Number(parsed.max_turns ?? 30),
        timeout_seconds: Number(parsed.timeout_seconds ?? 1800),
        resource_profile: rawProfile,
        dependencies: Array.isArray(parsed.dependencies) ? parsed.dependencies.map(String) : [],
        idempotency_key: parsed.idempotency_key ? String(parsed.idempotency_key) : undefined,
        parent_task_id: parsed.parent_task_id ? String(parsed.parent_task_id) : undefined,
        expected_SubAgent_count:
        parsed.expected_SubAgent_count !== undefined ? Number(parsed.expected_SubAgent_count) : 0,
        prefix_Cache_hash: parsed.prefix_Cache_hash ? String(parsed.prefix_Cache_hash) : "",
        content_fingerprint: parsed.content_fingerprint ? String(parsed.content_fingerprint) : undefined,
        merged_duplicate_count:
        parsed.merged_duplicate_count !== undefined ? Number(parsed.merged_duplicate_count) : 0,
        merged_request_ids: Array.isArray(parsed.merged_request_ids) ?
        parsed.merged_request_ids.map(String) :
        [],
        merged_idempotency_keys: Array.isArray(parsed.merged_idempotency_keys) ?
        parsed.merged_idempotency_keys.map(String) :
        undefined
      });
      if (parsed.current_turn !== undefined) {
        spec.current_turn = Number(parsed.current_turn);
      }
      if (parsed.last_preempted_at !== undefined) {
        spec.last_preempted_at = Number(parsed.last_preempted_at);
      }
      if (parsed.created_at !== undefined) {
        spec.created_at = Number(parsed.created_at);
      }
      spec.state = r.state ?? _models.TaskState.QUEUED;
      spec.lease_fencing_token = Number(r.fencing_token ?? 0);
      spec.retry_count = Number(parsed.retry_count ?? 0);
      spec.preempted_count = Number(parsed.preempted_count ?? 0);
      spec.dead_letter_reason = parsed.dead_letter_reason ? String(parsed.dead_letter_reason) : undefined;
      spec.assigned_node_id = r.assigned_node_id ? String(r.assigned_node_id) : undefined;
      if (!spec.content_fingerprint) {
        spec.content_fingerprint = _scheduler.CoreScheduler5D.computeTaskContentFingerprint(spec);
      }

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
      if (spec.idempotency_key) {
        this.scheduler.idempotencyIndex.set(
        _scheduler.CoreScheduler5D.scopeIdempotencyKey(spec.tenant_id, spec.idempotency_key),
        spec.task_id);

      }
      for (const mKey of spec.merged_idempotency_keys) {
        this.scheduler.idempotencyIndex.set(
        _scheduler.CoreScheduler5D.scopeIdempotencyKey(spec.tenant_id, mKey),
        spec.task_id);

      }
      this.scheduler.contentFingerprintIndex.set(spec.content_fingerprint, spec.task_id);

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
      current_turn: task.current_turn,
      timeout_seconds: task.timeout_seconds,
      resource_profile: task.resource_profile,
      dependencies: task.dependencies,
      idempotency_key: task.idempotency_key,
      parent_task_id: task.parent_task_id,
      expected_SubAgent_count: task.expected_SubAgent_count,
      prefix_Cache_hash: task.prefix_Cache_hash,
      content_fingerprint: task.content_fingerprint,
      merged_duplicate_count: task.merged_duplicate_count,
      merged_request_ids: task.merged_request_ids,
      merged_idempotency_keys: task.merged_idempotency_keys,
      retry_count: task.retry_count,
      preempted_count: task.preempted_count,
      last_preempted_at: task.last_preempted_at,
      dead_letter_reason: task.dead_letter_reason,
      created_at: task.created_at
    }));

  }

  async submitTask(task, options) {
    const submitted = this.scheduler.submitTask(task, options);
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

  /**
   * [M64-1] Supports both direct Cell paths (`/submit`, `/schedule-tick`, `/register-node`, `/tasks`)
   * and Worker-forwarded prefixed paths (`/v1/scheduler/submit`, `/v1/scheduler/schedule-tick`, etc.).
   */
  async fetch(request) {
    const url = new URL(request.url);
    const normalizedPath = url.pathname.replace(/^\/v1\/scheduler(?=\/|$)/, "") || "/";

    if (request.method === "POST" && normalizedPath === "/submit") {
      const body = await request.json();
      const spec = new _models.TaskSpec({
        task_id: String(body.task_id ?? `task-${Date.now()}`),
        tenant_id: String(body.tenant_id ?? "default"),
        priority: Number(body.priority ?? _models.TaskPriority.P2_STANDARD),
        prompt: String(body.prompt ?? ""),
        max_turns: body.max_turns !== undefined ? Number(body.max_turns) : undefined,
        timeout_seconds: body.timeout_seconds !== undefined ? Number(body.timeout_seconds) : undefined,
        resource_profile:
        body.resource_profile && typeof body.resource_profile === "object" ?
        body.resource_profile :
        undefined,
        dependencies: Array.isArray(body.dependencies) ? body.dependencies.map(String) : undefined,
        idempotency_key: body.idempotency_key ? String(body.idempotency_key) : undefined,
        parent_task_id: body.parent_task_id ? String(body.parent_task_id) : undefined,
        expected_SubAgent_count:
        body.expected_SubAgent_count !== undefined ? Number(body.expected_SubAgent_count) : undefined,
        prefix_Cache_hash: body.prefix_Cache_hash ? String(body.prefix_Cache_hash) : undefined
      });
      const submitted = await this.submitTask(spec, {
        mergeDuplicateInFlight: Boolean(body.merge_duplicate ?? body.deduplicate_by_fingerprint ?? false)
      });
      return new Response(
      JSON.stringify({
        task_id: submitted.task_id,
        tenant_id: submitted.tenant_id,
        state: submitted.state,
        priority: submitted.priority,
        merged_duplicate_count: submitted.merged_duplicate_count
      }),
      {
        status: 202,
        headers: { "content-type": "application/json" }
      });

    }

    if (request.method === "POST" && normalizedPath === "/register-node") {
      const body = await request.json();
      const hb = new _models.NodeHeartbeat({
        node_id: String(body.node_id ?? "body-node-01"),
        total_cpu_cores: Number(body.total_cpu_cores ?? 32),
        free_cpu_cores: Number(body.free_cpu_cores ?? 32),
        total_memory_mb: Number(body.total_memory_mb ?? 65536),
        free_memory_mb: Number(body.free_memory_mb ?? 65536),
        warm_pool_ready: Number(body.warm_pool_ready ?? 8),
        psi_memory_some_avg10: Number(body.psi_memory_some_avg10 ?? 1.0),
        running_tasks: Number(body.running_tasks ?? 0),
        healthy: body.healthy !== undefined ? Boolean(body.healthy) : true
      });
      this.registerNode(hb);
      return new Response(JSON.stringify({ registered: true, node_id: hb.node_id }), {
        status: 200,
        headers: { "content-type": "application/json" }
      });
    }

    if (request.method === "POST" && normalizedPath === "/schedule-tick") {
      const res = await this.alarm();
      return new Response(JSON.stringify(res), {
        status: 200,
        headers: { "content-type": "application/json" }
      });
    }

    if (request.method === "GET" && normalizedPath === "/tasks") {
      const tasks = Array.from(this.scheduler.allTasks.values());
      return new Response(JSON.stringify({ cellId: this.ctx.id.toString(), totalTasks: tasks.length, tasks }), {
        status: 200,
        headers: { "content-type": "application/json" }
      });
    }

    return new Response(JSON.stringify({ cellId: this.ctx.id.toString(), totalTasks: this.scheduler.allTasks.size }), {
      status: 200,
      headers: { "content-type": "application/json" }
    });
  }
}exports.AnstsSchedulerCell = AnstsSchedulerCell;