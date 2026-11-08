/**
 * @fileoverview celld v2.5 scheduler consistency domain.
 *
 * The Durable Object owns SQLite. This module is the rebuildable cache:
 * tasks, leases, DAG escrow, idempotency, and the transactional outbox.
 * `transactionSync` commits those rows together; `sync` failure is reported
 * as 503 without pretending the commit did not happen.
 */
import { FenceToken, TaskState } from "../models.js";
import { CloudflareCelldEnv, DurableObjectStateLike, QueueProducerLike } from "./env.js";

const JSON_HEADERS = { "content-type": "application/json" };
const LEASE_TIMEOUT_MS = 12_000;
const CHECKPOINT_ID = 1;

interface SqlRow {
  [column: string]: unknown;
}

interface CommandResult {
  mutated: boolean;
  status: number;
  json: Record<string, unknown>;
  syncFailed?: boolean;
}

interface SchedNode {
  nodeId: string;
  alive: boolean;
}

interface MemLease {
  taskId: string;
  nodeId: string;
  fence: FenceToken;
  renewedTs: number;
}

interface DagRecord {
  parentTask: DurableSchedulerTask;
  children: DurableSchedulerTask[];
}

interface StoredProfile {
  tenantId: string;
  samples: number;
  execCpu: number;
}

export class DurableSchedulerTask {
  public readonly taskId: string;
  public readonly tenantId: string;
  public state: TaskState;
  public fence: FenceToken;
  public enqueueTs: number;
  public deadlineTs: number;
  public execCpu = 1;
  public cogCpu = 1;
  public reservedTps = 0;
  public completedNodeId?: string;
  public completedEpoch?: number;
  public completedTurnSeq?: number;
  public legacy?: {
    priority: number;
    prompt: string;
    maxTurns: number;
    timeoutSeconds: number;
    dependencies: string[];
    idempotencyKey?: string;
    assignedNodeId?: string;
    fencingToken: number;
  };

  constructor(params: {
    taskId: string;
    tenantId: string;
    state?: TaskState;
    enqueueTs?: number;
    deadlineTs?: number;
  }) {
    if (!params.taskId) throw new Error("DurableSchedulerTask requires a taskId");
    this.taskId = params.taskId;
    this.tenantId = params.tenantId;
    this.state = params.state ?? TaskState.QUEUED;
    this.enqueueTs = params.enqueueTs ?? Date.now();
    this.deadlineTs = params.deadlineTs ?? this.enqueueTs + 1_800_000;
    this.fence = new FenceToken(0, 0, params.taskId);
  }

  /** 5D admission vector restored with the task, not rebuilt from constructor defaults. */
  public demandVector(): { steadyCpu: number; cogCpu: number; execCpu: number; reservedTps: number } {
    return {
      steadyCpu: this.execCpu,
      cogCpu: this.cogCpu,
      execCpu: this.execCpu,
      reservedTps: this.reservedTps,
    };
  }
}

function fnv1a(text: string): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < text.length; i += 1) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}

function json(status: number, body: Record<string, unknown>): Response {
  return new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });
}

export class SchedulerV25Persistence {
  public readonly tasks = new Map<string, DurableSchedulerTask>();
  public readonly nodes = new Map<string, SchedNode>();
  public readonly tenantQueues = new Map<string, DurableSchedulerTask[]>();
  public readonly activeDags = new Map<string, DagRecord>();
  public readonly escrowFastQueue: Array<{ child: DurableSchedulerTask }> = [];
  public readonly issuedVouchers = new Map<string, { parentFence: FenceToken }>();
  public readonly facade: {
    scheduler: { tasks: Map<string, DurableSchedulerTask>; nodes: Map<string, SchedNode> };
    queueMgr: {
      tenantQueues: Map<string, DurableSchedulerTask[]>;
      activeDags: Map<string, DagRecord>;
      escrowFastQueue: Array<{ child: DurableSchedulerTask }>;
      issuedVouchers: Map<string, { parentFence: FenceToken }>;
    };
    leaseCoord: { getLease(taskId: string): MemLease | undefined };
    profiler: { snapshot(): StoredProfile[] };
  };

  private readonly idem = new Map<string, string>();
  private readonly profiles = new Map<string, StoredProfile>();
  private readonly leases = new Map<string, MemLease>();
  private revision = 0;

  constructor(
    private readonly ctx: DurableObjectStateLike,
    private readonly env?: CloudflareCelldEnv,
  ) {
    this.facade = {
      scheduler: { tasks: this.tasks, nodes: this.nodes },
      queueMgr: {
        tenantQueues: this.tenantQueues,
        activeDags: this.activeDags,
        escrowFastQueue: this.escrowFastQueue,
        issuedVouchers: this.issuedVouchers,
      },
      leaseCoord: { getLease: (taskId: string) => this.leases.get(taskId) },
      profiler: { snapshot: () => [...this.profiles.values()] },
    };
    this.seedNodes();
    this.createSchema();
    this.loadFromDatabase();
  }

  public handles(pathname: string, method: string): boolean {
    if (method === "GET" && pathname === "/cluster/top") return true;
    if (method !== "POST") return false;
    return [
      "/tasks",
      "/tasks/submit",
      "/tasks/complete",
      "/scheduler/tick",
      "/scheduler/recover",
      "/leases/validate",
      "/dag/spawn-escrow",
      "/nodes/heartbeat",
      "/internal/events/consume",
    ].includes(pathname);
  }

  public async handle(request: Request, pathname: string): Promise<Response> {
    try {
      if (request.method === "GET" && pathname === "/cluster/top") {
        return json(200, this.top());
      }
      const body = await readJson(request);
      if (pathname === "/tasks" || pathname === "/tasks/submit") return this.submit(body);
      if (pathname === "/scheduler/tick") return this.finish(await this.persist(() => this.applyTick()));
      if (pathname === "/scheduler/recover") return this.finish(await this.persist(() => this.applyRecover()));
      if (pathname === "/leases/validate") return json(200, this.validate(body));
      if (pathname === "/tasks/complete") return this.finish(await this.persist(() => this.applyComplete(body)));
      if (pathname === "/dag/spawn-escrow") return this.finish(await this.persist(() => this.applySpawn(body)));
      if (pathname === "/nodes/heartbeat") return this.finish(await this.persist(() => this.applyHeartbeat(body)));
      if (pathname === "/internal/events/consume") {
        const receipt = this.consumeEvent(body);
        return json(200, receipt);
      }
      return json(404, { error: "NOT_FOUND" });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return json(503, { error: message });
    }
  }

  /** Alarm delivers the outbox only. Recovery and placement stay on explicit routes. */
  public async flushOutbox(): Promise<void> {
    const queue = this.queueProducer();
    for (;;) {
      const pending = this.pendingOutbox();
      if (pending.length === 0) return;
      const next = pending[0];
      if (!queue) throw new Error("queue offline");
      await queue.send(next.event);
      const acked = await this.persist(() => this.ackOutbox(next.seq));
      if (acked.syncFailed) throw new Error("sync failed");
    }
  }

  private async submit(body: Record<string, unknown>): Promise<Response> {
    const result = await this.persist(() => this.applySubmit(body));
    if (result.syncFailed) return json(503, { error: "sync failed" });
    if (result.mutated) await this.ensureAlarm();
    return json(result.status, result.json);
  }

  private async finish(result: CommandResult): Promise<Response> {
    if (result.syncFailed) return json(503, { error: "sync failed" });
    return json(result.status, result.json);
  }

  /** Projects a legacy in-memory mutation back onto the canonical row. Does not delete the source table. */
  public syncLegacyTask(task: {
    task_id: string;
    tenant_id: string;
    priority: number;
    state: string;
    prompt: string;
    max_turns: number;
    timeout_seconds: number;
    dependencies: string[];
    idempotency_key?: string;
    assigned_node_id?: string;
    lease_fencing_token: number;
  }): void {
    const write = () => {
      const existing = this.tasks.get(task.task_id);
      if (!existing) return;
      existing.state = task.state as TaskState;
      existing.legacy = {
        priority: task.priority,
        prompt: task.prompt,
        maxTurns: task.max_turns,
        timeoutSeconds: task.timeout_seconds,
        dependencies: task.dependencies,
        idempotencyKey: task.idempotency_key ?? existing.legacy?.idempotencyKey,
        assignedNodeId: task.assigned_node_id,
        fencingToken: task.lease_fencing_token,
      };
      this.writeTask(existing);
      this.writeCheckpoint();
    };
    if (this.ctx.storage.transactionSync) this.ctx.storage.transactionSync(write);
    else write();
  }

  public async acceptLegacy(task: {
    task_id: string;
    tenant_id: string;
    priority: number;
    state: string;
    prompt: string;
    max_turns: number;
    timeout_seconds: number;
    dependencies: string[];
    idempotency_key?: string;
    assigned_node_id?: string;
    lease_fencing_token: number;
  }): Promise<CommandResult> {
    const result = await this.persist(() => this.applySubmit({
      metadata: { task_id: task.task_id, tenant_id: task.tenant_id, idempotency_key: task.idempotency_key ?? "" },
      legacy: {
        priority: task.priority,
        prompt: task.prompt,
        maxTurns: task.max_turns,
        timeoutSeconds: task.timeout_seconds,
        dependencies: task.dependencies,
        idempotencyKey: task.idempotency_key,
        assignedNodeId: task.assigned_node_id,
        fencingToken: task.lease_fencing_token,
      },
    }));
    if (!result.syncFailed && result.mutated) await this.ensureAlarm();
    return result;
  }

  /** Records a queue delivery. Ack only after this commits. A repeated event_id is a duplicate, not a second task. */
  public consumeEvent(event: Record<string, unknown>): { duplicate: boolean; event_id: string } {
    const eventId = String(event.event_id ?? "");
    if (!eventId) throw new Error("event_id is required");
    let duplicate = false;
    const write = () => {
      const existing = this.sql("SELECT event_id FROM cell_event_receipts WHERE event_id = ?", eventId);
      if (existing.length > 0) {
        duplicate = true;
        return;
      }
      this.sql(
        "INSERT INTO cell_event_receipts (event_id, task_id, event_type, received_at) VALUES (?, ?, ?, ?)",
        eventId,
        String(event.task_id ?? ""),
        String(event.event_type ?? ""),
        Date.now(),
      );
    };
    if (this.ctx.storage.transactionSync) this.ctx.storage.transactionSync(write);
    else write();
    return { duplicate, event_id: eventId };
  }

  public receiptCount(eventId?: string): number {
    if (!eventId) return this.sql("SELECT event_id FROM cell_event_receipts").length;
    return this.sql("SELECT event_id FROM cell_event_receipts WHERE event_id = ?", eventId).length;
  }

  private queueProducer(): QueueProducerLike | undefined {
    return this.env?.ANSTS_EVENTS_QUEUE;
  }

  private seedNodes(): void {
    for (const nodeId of ["worker-us-west-01", "worker-us-west-02"]) {
      if (!this.nodes.has(nodeId)) this.nodes.set(nodeId, { nodeId, alive: true });
    }
  }

  private sql(query: string, ...bindings: unknown[]): SqlRow[] {
    return this.ctx.storage.sql.exec<SqlRow>(query, ...bindings).toArray();
  }

  private createSchema(): void {
    const statements = [
      `CREATE TABLE IF NOT EXISTS scheduler_checkpoint (
        id INTEGER PRIMARY KEY, schema_version INTEGER NOT NULL, revision INTEGER NOT NULL,
        payload TEXT NOT NULL, checksum TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS cell_tasks (
        task_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, state TEXT NOT NULL,
        epoch INTEGER NOT NULL, turn_seq INTEGER NOT NULL, enqueue_ts INTEGER NOT NULL,
        deadline_ts INTEGER NOT NULL, spec_json TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS cell_leases (
        task_id TEXT PRIMARY KEY, node_id TEXT, model_pool TEXT, epoch INTEGER,
        turn_seq INTEGER, renewed_ts INTEGER)`,
      `CREATE TABLE IF NOT EXISTS cell_dags (
        dag_id TEXT PRIMARY KEY, parent_task_id TEXT NOT NULL, children_json TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS cell_dag_vouchers (
        child_task_id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, parent_task_id TEXT NOT NULL,
        parent_epoch INTEGER NOT NULL, parent_turn_seq INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS cell_idempotency (
        idem_key TEXT PRIMARY KEY, task_id TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS cell_outbox (
        seq INTEGER PRIMARY KEY, event_id TEXT NOT NULL, task_id TEXT NOT NULL,
        event_type TEXT NOT NULL, payload TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS cell_profiler_snapshots (
        tenant_id TEXT PRIMARY KEY, prior_json TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS cell_event_receipts (
        event_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, event_type TEXT NOT NULL, received_at INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS ansts_tasks (
        task_id TEXT PRIMARY KEY, tenant_id TEXT, priority INTEGER, state TEXT, fencing_token INTEGER,
        assigned_node_id TEXT, payload_json TEXT)`,
      `CREATE TABLE IF NOT EXISTS scheduler_migration (
        id INTEGER PRIMARY KEY, source TEXT NOT NULL, copied_count INTEGER NOT NULL, completed_at INTEGER NOT NULL)`,
    ];
    for (const statement of statements) this.sql(statement);
  }

  private loadFromDatabase(): void {
    this.tasks.clear();
    this.leases.clear();
    this.idem.clear();
    this.profiles.clear();
    this.activeDags.clear();
    this.issuedVouchers.clear();
    this.tenantQueues.clear();
    this.escrowFastQueue.splice(0, this.escrowFastQueue.length);

    const checkpoints = this.sql(
      "SELECT payload, checksum, schema_version, revision FROM scheduler_checkpoint WHERE id = ?",
      CHECKPOINT_ID,
    );
    if (checkpoints.length === 0) {
      this.assertNoLegacyLeases();
      return;
    }
    const row = checkpoints[0];
    const schemaVersion = Number(row.schema_version ?? 0);
    if (schemaVersion < 2) {
      throw new Error("scheduler checkpoint schema is older than 2; operator migration required");
    }
    const payload = String(row.payload ?? "");
    if (fnv1a(payload) !== String(row.checksum ?? "")) {
      throw new Error("scheduler checkpoint checksum mismatch");
    }
    const parsed = JSON.parse(payload) as { schema?: number };
    if ((parsed.schema ?? 0) < 2) {
      throw new Error("scheduler checkpoint schema is older than 2; operator migration required");
    }
    this.revision = Number(row.revision ?? 0);
    this.loadTasks();
    this.loadLeases();
    this.loadIdempotency();
    this.loadProfiler();
    this.loadDags();
  }

  /**
   * Copy `ansts_tasks` rows that the canonical tables do not yet have.
   * Source rows are left in place. A second call copies nothing new.
   * The same task id with a different state fails closed instead of picking a winner.
   */
  public importLegacyTasks(): { copied: number; alreadyPresent: number } {
    let copied = 0;
    let alreadyPresent = 0;
    const rows = this.sql("SELECT * FROM ansts_tasks");
    const write = () => {
      for (const row of rows) {
        const taskId = String(row.task_id ?? "");
        if (!taskId) continue;
        const existing = this.tasks.get(taskId);
        const legacyState = String(row.state ?? TaskState.QUEUED);
        if (existing) {
          if (existing.state !== legacyState && !(existing.state === TaskState.SUCCEEDED && legacyState === TaskState.COMPLETED)) {
            throw new Error(`legacy ansts_tasks row ${taskId} disagrees with canonical cell_tasks (${existing.state} vs ${legacyState})`);
          }
          alreadyPresent += 1;
          continue;
        }
        const parsed = JSON.parse(String(row.payload_json ?? "{}")) as Record<string, unknown>;
        const task = new DurableSchedulerTask({
          taskId,
          tenantId: String(row.tenant_id ?? "default"),
          state: legacyState as TaskState,
        });
        task.state = legacyState as TaskState;
        task.legacy = {
          priority: Number(row.priority ?? 2),
          prompt: String(parsed.prompt ?? ""),
          maxTurns: Number(parsed.max_turns ?? 30),
          timeoutSeconds: Number(parsed.timeout_seconds ?? 1800),
          dependencies: Array.isArray(parsed.dependencies) ? parsed.dependencies.map(String) : [],
          idempotencyKey: parsed.idempotency_key ? String(parsed.idempotency_key) : undefined,
          assignedNodeId: row.assigned_node_id ? String(row.assigned_node_id) : undefined,
          fencingToken: Number(row.fencing_token ?? 0),
        };
        this.tasks.set(taskId, task);
        if (task.state === TaskState.QUEUED) this.enqueueTenant(task);
        this.writeTask(task);
        copied += 1;
      }
      if (copied > 0) this.writeCheckpoint();
      this.sql("DELETE FROM scheduler_migration WHERE id = ?", 1);
      this.sql(
        "INSERT INTO scheduler_migration (id, source, copied_count, completed_at) VALUES (?, ?, ?, ?)",
        1,
        "ansts_tasks",
        copied,
        Date.now(),
      );
    };
    if (this.ctx.storage.transactionSync) this.ctx.storage.transactionSync(write);
    else write();
    return { copied, alreadyPresent };
  }

  private assertNoLegacyLeases(): void {
    const rows = this.sql("SELECT task_id, node_id, epoch, turn_seq FROM cell_leases");
    for (const row of rows) {
      if (row.node_id == null || row.epoch == null || row.turn_seq == null) {
        throw new Error("operator migration required: legacy cell_leases row lacks node_id, epoch, or turn_seq");
      }
    }
    if (rows.length > 0) {
      throw new Error("operator migration required: cell_leases exist without a scheduler checkpoint");
    }
  }

  private loadTasks(): void {
    const rows = this.sql(
      "SELECT task_id, tenant_id, state, epoch, turn_seq, enqueue_ts, deadline_ts, spec_json FROM cell_tasks",
    );
    for (const row of rows) {
      const spec = JSON.parse(String(row.spec_json ?? "{}")) as Record<string, unknown>;
      const task = new DurableSchedulerTask({
        taskId: String(row.task_id),
        tenantId: String(row.tenant_id),
        state: String(row.state) as TaskState,
        enqueueTs: Number(row.enqueue_ts),
        deadlineTs: Number(row.deadline_ts),
      });
      task.state = String(row.state) as TaskState;
      task.execCpu = Number(spec.execCpu ?? 1);
      task.cogCpu = Number(spec.cogCpu ?? 1);
      task.reservedTps = Number(spec.reservedTps ?? 0);
      task.fence = new FenceToken(Number(row.epoch ?? 0), Number(row.turn_seq ?? 0), task.taskId);
      if (spec.completedNodeId != null) task.completedNodeId = String(spec.completedNodeId);
      if (spec.completedEpoch != null) task.completedEpoch = Number(spec.completedEpoch);
      if (spec.completedTurnSeq != null) task.completedTurnSeq = Number(spec.completedTurnSeq);
      const legacy = spec.legacy as DurableSchedulerTask["legacy"] | undefined;
      if (legacy) task.legacy = legacy;
      this.tasks.set(task.taskId, task);
    }
  }

  private loadLeases(): void {
    const rows = this.sql("SELECT task_id, node_id, epoch, turn_seq, renewed_ts FROM cell_leases");
    for (const row of rows) {
      if (row.node_id == null || row.epoch == null || row.turn_seq == null) {
        throw new Error("operator migration required: legacy cell_leases row lacks node_id, epoch, or turn_seq");
      }
      const taskId = String(row.task_id);
      const fence = new FenceToken(Number(row.epoch), Number(row.turn_seq), taskId);
      const task = this.tasks.get(taskId);
      if (task) task.fence = fence;
      this.leases.set(taskId, {
        taskId,
        nodeId: String(row.node_id),
        fence,
        renewedTs: Number(row.renewed_ts ?? 0),
      });
    }
  }

  private loadIdempotency(): void {
    for (const row of this.sql("SELECT idem_key, task_id FROM cell_idempotency")) {
      this.idem.set(String(row.idem_key), String(row.task_id));
    }
  }

  private loadProfiler(): void {
    for (const row of this.sql("SELECT tenant_id, prior_json FROM cell_profiler_snapshots")) {
      const prior = JSON.parse(String(row.prior_json ?? "{}")) as Partial<StoredProfile>;
      const tenantId = String(row.tenant_id);
      this.profiles.set(tenantId, {
        tenantId,
        samples: Number(prior.samples ?? 1),
        execCpu: Number(prior.execCpu ?? 1),
      });
    }
  }

  private loadDags(): void {
    for (const row of this.sql(
      "SELECT child_task_id, parent_task_id, parent_epoch, parent_turn_seq FROM cell_dag_vouchers",
    )) {
      const parentId = String(row.parent_task_id);
      this.issuedVouchers.set(String(row.child_task_id), {
        parentFence: new FenceToken(Number(row.parent_epoch), Number(row.parent_turn_seq), parentId),
      });
    }
    const escrowIds = new Set<string>();
    for (const row of this.sql("SELECT dag_id, parent_task_id, children_json FROM cell_dags")) {
      const parent = this.tasks.get(String(row.parent_task_id));
      const ids = JSON.parse(String(row.children_json ?? "[]")) as string[];
      const children = ids.map((id) => this.tasks.get(id)).filter((task): task is DurableSchedulerTask => !!task);
      if (parent) this.activeDags.set(String(row.dag_id), { parentTask: parent, children });
      for (const child of children) {
        if (child.state === TaskState.QUEUED) {
          this.escrowFastQueue.push({ child });
          escrowIds.add(child.taskId);
        }
      }
    }
    for (const task of this.tasks.values()) {
      if (task.state === TaskState.QUEUED && !escrowIds.has(task.taskId)) this.enqueueTenant(task);
    }
  }

  private async persist(fn: () => CommandResult): Promise<CommandResult> {
    let result: CommandResult = { mutated: false, status: 500, json: { error: "not executed" } };
    const storage = this.ctx.storage;
    try {
      if (storage.transactionSync) storage.transactionSync(() => {
        result = fn();
      });
      else result = fn();
    } catch (error) {
      try {
        this.loadFromDatabase();
      } catch {
        // Keep the original disk error; the host transaction already rolled back.
      }
      throw error;
    }
    if (!result.mutated || !storage.sync) return result;
    try {
      await storage.sync();
      return result;
    } catch {
      return { ...result, syncFailed: true };
    }
  }

  private async ensureAlarm(): Promise<void> {
    const existing = await this.ctx.storage.getAlarm();
    if (!existing) await this.ctx.storage.setAlarm(Date.now() + 1000);
  }

  private applySubmit(body: Record<string, unknown>): CommandResult {
    const metadata = (body.metadata ?? {}) as Record<string, unknown>;
    const taskId = String(metadata.task_id ?? "");
    const tenantId = String(metadata.tenant_id ?? "default");
    const idempotencyKey = String(metadata.idempotency_key ?? "");
    if (!taskId) return { mutated: false, status: 400, json: { error: "task_id is required" } };
    const idemKey = `${tenantId}:${idempotencyKey}`;
    const existingId = idempotencyKey ? this.idem.get(idemKey) : undefined;
    if (existingId) {
      return { mutated: false, status: 200, json: { task_id: existingId, deduplicated: true } };
    }
    const legacyInput = body.legacy as DurableSchedulerTask["legacy"] | undefined;
    if (this.tasks.has(taskId)) {
      const currentKey = this.tasks.get(taskId)?.legacy?.idempotencyKey;
      if (!idempotencyKey || currentKey === idempotencyKey) {
        return { mutated: false, status: 200, json: { task_id: taskId, deduplicated: true } };
      }
      return { mutated: false, status: 409, json: { error: "task_id already exists with a different idempotency key" } };
    }
    const task = new DurableSchedulerTask({ taskId, tenantId, state: TaskState.QUEUED });
    task.legacy = {
      priority: legacyInput?.priority ?? 2,
      prompt: legacyInput?.prompt ?? "",
      maxTurns: legacyInput?.maxTurns ?? 30,
      timeoutSeconds: legacyInput?.timeoutSeconds ?? 1800,
      dependencies: legacyInput?.dependencies ?? [],
      idempotencyKey: idempotencyKey || legacyInput?.idempotencyKey,
      assignedNodeId: legacyInput?.assignedNodeId,
      fencingToken: legacyInput?.fencingToken ?? 0,
    };
    this.tasks.set(taskId, task);
    this.enqueueTenant(task);
    if (idempotencyKey) {
      this.idem.set(idemKey, taskId);
      this.sql("INSERT INTO cell_idempotency (idem_key, task_id) VALUES (?, ?)", idemKey, taskId);
    }
    this.recordProfile(tenantId);
    this.writeTask(task);
    this.enqueueOutbox({
      event_id: `evt_${taskId}_queued`,
      event_type: "agent.task.queued",
      task_id: taskId,
      tenant_id: tenantId,
    });
    this.writeCheckpoint();
    return { mutated: true, status: 202, json: { task_id: taskId, state: task.state } };
  }

  private applyTick(): CommandResult {
    const dispatched: Array<{ task_id: string; node_id: string; fence: { epoch: number; turn_seq: number } }> = [];
    for (const task of this.queuedTasks()) {
      const node = this.pickNode();
      if (!node) break;
      const fence = new FenceToken((task.fence?.epoch ?? 0) + 1, 0, task.taskId);
      task.fence = fence;
      task.state = TaskState.RUNNING;
      const lease: MemLease = { taskId: task.taskId, nodeId: node.nodeId, fence, renewedTs: Date.now() };
      this.leases.set(task.taskId, lease);
      this.removeFromQueues(task);
      this.writeTask(task);
      this.writeLease(lease);
      dispatched.push({
        task_id: task.taskId,
        node_id: node.nodeId,
        fence: { epoch: fence.epoch, turn_seq: fence.turnSeq },
      });
    }
    if (dispatched.length > 0) this.writeCheckpoint();
    return {
      mutated: dispatched.length > 0,
      status: 200,
      json: { dispatched_count: dispatched.length, dispatched_tasks: dispatched },
    };
  }

  private applyRecover(): CommandResult {
    const now = Date.now();
    const recovered: string[] = [];
    for (const lease of [...this.leases.values()]) {
      const task = this.tasks.get(lease.taskId);
      if (!task || task.state === TaskState.SUCCEEDED || task.state === TaskState.FAILED) continue;
      if (now - lease.renewedTs < LEASE_TIMEOUT_MS) continue;
      const node = this.nodes.get(lease.nodeId);
      if (node) node.alive = false;
      task.fence = new FenceToken(lease.fence.epoch + 1, 0, task.taskId);
      task.state = TaskState.QUEUED;
      this.leases.delete(lease.taskId);
      this.sql("DELETE FROM cell_leases WHERE task_id = ?", lease.taskId);
      this.enqueueTenant(task);
      this.writeTask(task);
      recovered.push(task.taskId);
    }
    if (recovered.length > 0) this.writeCheckpoint();
    return { mutated: recovered.length > 0, status: 200, json: { recovered_tasks: recovered } };
  }

  private validate(body: Record<string, unknown>): { valid: boolean } {
    const lease = this.leases.get(String(body.task_id ?? ""));
    if (!lease) return { valid: false };
    const valid =
      lease.nodeId === String(body.node_id ?? "") &&
      lease.fence.epoch === Number(body.epoch) &&
      lease.fence.turnSeq === Number(body.turn_seq);
    return { valid };
  }

  private applyComplete(body: Record<string, unknown>): CommandResult {
    const task = this.tasks.get(String(body.task_id ?? ""));
    if (!task) return { mutated: false, status: 404, json: { error: "task not found" } };
    const nodeId = String(body.node_id ?? "");
    const epoch = Number(body.epoch);
    const turnSeq = Number(body.turn_seq);
    if (task.state === TaskState.SUCCEEDED) {
      const same =
        task.completedNodeId === nodeId &&
        task.completedEpoch === epoch &&
        turnSeq >= (task.completedTurnSeq ?? 0);
      return same
        ? { mutated: false, status: 200, json: { deduplicated: true, task_id: task.taskId } }
        : { mutated: false, status: 409, json: { error: "obsolete completion" } };
    }
    const lease = this.leases.get(task.taskId);
    if (!lease || lease.nodeId !== nodeId || lease.fence.epoch !== epoch || turnSeq < lease.fence.turnSeq) {
      return { mutated: false, status: 409, json: { error: "lease mismatch" } };
    }
    task.state = TaskState.SUCCEEDED;
    task.completedNodeId = nodeId;
    task.completedEpoch = epoch;
    task.completedTurnSeq = turnSeq;
    task.fence = new FenceToken(epoch, turnSeq, task.taskId);
    this.leases.delete(task.taskId);
    this.sql("DELETE FROM cell_leases WHERE task_id = ?", task.taskId);
    this.removeFromQueues(task);
    this.writeTask(task);
    this.writeCheckpoint();
    return { mutated: true, status: 200, json: { task_id: task.taskId, state: task.state } };
  }

  private applySpawn(body: Record<string, unknown>): CommandResult {
    const dagId = String(body.dag_id ?? "");
    if (this.activeDags.has(dagId)) {
      return { mutated: false, status: 200, json: { deduplicated: true, dag_id: dagId } };
    }
    const parent = this.tasks.get(String(body.parent_task_id ?? ""));
    if (!parent || !parent.fence) {
      return { mutated: false, status: 404, json: { error: "parent task not found" } };
    }
    const childIds = Array.isArray(body.children_ids) ? body.children_ids.map(String) : [];
    const children: DurableSchedulerTask[] = [];
    for (const childId of childIds) {
      let child = this.tasks.get(childId);
      if (!child) {
        child = new DurableSchedulerTask({ taskId: childId, tenantId: parent.tenantId, state: TaskState.QUEUED });
        this.tasks.set(childId, child);
        this.writeTask(child);
      }
      children.push(child);
      this.issuedVouchers.set(childId, { parentFence: parent.fence });
      this.sql(
        "INSERT INTO cell_dag_vouchers (child_task_id, dag_id, parent_task_id, parent_epoch, parent_turn_seq) VALUES (?, ?, ?, ?, ?)",
        childId,
        dagId,
        parent.taskId,
        parent.fence.epoch,
        parent.fence.turnSeq,
      );
      this.escrowFastQueue.push({ child });
    }
    this.activeDags.set(dagId, { parentTask: parent, children });
    this.sql(
      "INSERT INTO cell_dags (dag_id, parent_task_id, children_json) VALUES (?, ?, ?)",
      dagId,
      parent.taskId,
      JSON.stringify(childIds),
    );
    this.writeCheckpoint();
    return { mutated: true, status: 200, json: { deduplicated: false, dag_id: dagId } };
  }

  private applyHeartbeat(body: Record<string, unknown>): CommandResult {
    const nodeId = String(body.node_id ?? "");
    if (!nodeId) return { mutated: false, status: 400, json: { error: "node_id is required" } };
    let node = this.nodes.get(nodeId);
    if (!node) {
      node = { nodeId, alive: true };
      this.nodes.set(nodeId, node);
    }
    node.alive = true;
    const now = Date.now();
    let renewed = 0;
    for (const lease of this.leases.values()) {
      if (lease.nodeId !== nodeId) continue;
      lease.renewedTs = now;
      this.writeLease(lease);
      renewed += 1;
    }
    if (renewed > 0) this.writeCheckpoint();
    return { mutated: renewed > 0, status: 200, json: { node_id: nodeId, alive: true, renewed_leases: renewed } };
  }

  private top(): Record<string, unknown> {
    return {
      pending_outbox_events: this.pendingOutbox().length,
      active_tasks: this.tasks.size,
      celld_sqlite_leases_count: this.leases.size,
      scheduler_revision: this.revision,
    };
  }

  private queuedTasks(): DurableSchedulerTask[] {
    const seen = new Set<string>();
    const out: DurableSchedulerTask[] = [];
    const consider = (task: DurableSchedulerTask | undefined) => {
      if (!task || task.state !== TaskState.QUEUED || seen.has(task.taskId)) return;
      seen.add(task.taskId);
      out.push(task);
    };
    for (const entry of this.escrowFastQueue) consider(entry.child);
    for (const queue of this.tenantQueues.values()) {
      for (const task of queue) consider(task);
    }
    return out;
  }

  private pickNode(): SchedNode | undefined {
    for (const node of this.nodes.values()) {
      if (node.alive) return node;
    }
    return undefined;
  }

  private enqueueTenant(task: DurableSchedulerTask): void {
    let queue = this.tenantQueues.get(task.tenantId);
    if (!queue) {
      queue = [];
      this.tenantQueues.set(task.tenantId, queue);
    }
    if (!queue.includes(task)) queue.push(task);
  }

  private removeFromQueues(task: DurableSchedulerTask): void {
    const queue = this.tenantQueues.get(task.tenantId);
    if (queue) {
      const index = queue.findIndex((item) => item.taskId === task.taskId);
      if (index >= 0) queue.splice(index, 1);
    }
    for (let i = this.escrowFastQueue.length - 1; i >= 0; i -= 1) {
      if (this.escrowFastQueue[i].child.taskId === task.taskId) this.escrowFastQueue.splice(i, 1);
    }
  }

  private recordProfile(tenantId: string): void {
    const existing = this.profiles.get(tenantId);
    const profile: StoredProfile = existing ?? { tenantId, samples: 1, execCpu: 1 };
    if (existing) profile.samples += 1;
    this.profiles.set(tenantId, profile);
    this.sql("DELETE FROM cell_profiler_snapshots WHERE tenant_id = ?", tenantId);
    this.sql(
      "INSERT INTO cell_profiler_snapshots (tenant_id, prior_json) VALUES (?, ?)",
      tenantId,
      JSON.stringify(profile),
    );
  }

  private writeTask(task: DurableSchedulerTask): void {
    const spec = {
      execCpu: task.execCpu,
      cogCpu: task.cogCpu,
      reservedTps: task.reservedTps,
      completedNodeId: task.completedNodeId ?? null,
      completedEpoch: task.completedEpoch ?? null,
      completedTurnSeq: task.completedTurnSeq ?? null,
      legacy: task.legacy ?? null,
    };
    this.projectAnsts(task);
    this.sql("DELETE FROM cell_tasks WHERE task_id = ?", task.taskId);
    this.sql(
      "INSERT INTO cell_tasks (task_id, tenant_id, state, epoch, turn_seq, enqueue_ts, deadline_ts, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      task.taskId,
      task.tenantId,
      task.state,
      task.fence.epoch,
      task.fence.turnSeq,
      task.enqueueTs,
      task.deadlineTs,
      JSON.stringify(spec),
    );
  }

  private projectAnsts(task: DurableSchedulerTask): void {
    const legacy = task.legacy ?? {
      priority: 2,
      prompt: "",
      maxTurns: 30,
      timeoutSeconds: 1800,
      dependencies: [],
      fencingToken: task.fence.epoch,
    };
    this.sql("DELETE FROM ansts_tasks WHERE task_id = ?", task.taskId);
    this.sql(
      "INSERT INTO ansts_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
      task.taskId,
      task.tenantId,
      legacy.priority,
      task.state,
      legacy.fencingToken,
      legacy.assignedNodeId ?? "",
      JSON.stringify({
        prompt: legacy.prompt,
        max_turns: legacy.maxTurns,
        timeout_seconds: legacy.timeoutSeconds,
        dependencies: legacy.dependencies,
        idempotency_key: legacy.idempotencyKey ?? null,
      }),
    );
  }

  private writeLease(lease: MemLease): void {
    this.sql("DELETE FROM cell_leases WHERE task_id = ?", lease.taskId);
    this.sql(
      "INSERT INTO cell_leases (task_id, node_id, model_pool, epoch, turn_seq, renewed_ts) VALUES (?, ?, ?, ?, ?, ?)",
      lease.taskId,
      lease.nodeId,
      "",
      lease.fence.epoch,
      lease.fence.turnSeq,
      lease.renewedTs,
    );
  }

  private enqueueOutbox(event: { event_id: string; event_type: string; task_id: string; tenant_id: string }): void {
    const rows = this.sql("SELECT seq FROM cell_outbox ORDER BY seq DESC");
    const seq = rows.length > 0 ? Number(rows[0].seq) + 1 : 1;
    this.sql(
      "INSERT INTO cell_outbox (seq, event_id, task_id, event_type, payload) VALUES (?, ?, ?, ?, ?)",
      seq,
      event.event_id,
      event.task_id,
      event.event_type,
      JSON.stringify(event),
    );
  }

  private pendingOutbox(): Array<{ seq: number; event: Record<string, unknown> }> {
    return this.sql("SELECT seq, payload FROM cell_outbox ORDER BY seq ASC").map((row) => ({
      seq: Number(row.seq),
      event: JSON.parse(String(row.payload)) as Record<string, unknown>,
    }));
  }

  private ackOutbox(seq: number): CommandResult {
    this.sql("DELETE FROM cell_outbox WHERE seq = ?", seq);
    this.writeCheckpoint();
    return { mutated: true, status: 200, json: { acked_seq: seq } };
  }

  private writeCheckpoint(): void {
    this.revision += 1;
    const payload = JSON.stringify({
      schema: 2,
      revision: this.revision,
      taskIds: [...this.tasks.keys()].sort(),
      leaseIds: [...this.leases.keys()].sort(),
    });
    const checksum = fnv1a(payload);
    this.sql("DELETE FROM scheduler_checkpoint WHERE id = ?", CHECKPOINT_ID);
    this.sql(
      "INSERT INTO scheduler_checkpoint (id, schema_version, revision, payload, checksum) VALUES (?, ?, ?, ?, ?)",
      CHECKPOINT_ID,
      2,
      this.revision,
      payload,
      checksum,
    );
  }
}

async function readJson(request: Request): Promise<Record<string, unknown>> {
  const text = await request.text();
  if (!text.trim()) return {};
  const parsed = JSON.parse(text) as unknown;
  return parsed && typeof parsed === "object" ? (parsed as Record<string, unknown>) : {};
}
