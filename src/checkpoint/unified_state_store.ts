/**
 * @fileoverview M14 TASK-63 ~ TASK-66: Unified 100% Component Persistence Store & Cold-Start Rehydration Engine.
 */
import { SqlStorageLike } from "../cloudflare/env.js";
import { CoreScheduler5D } from "../scheduler.js";
import { TaskPriority, TaskSpec, TaskState } from "../models.js";
import {
  ContinuitySpanRecord,
  FaultTolerantObservabilityCoordinator,
} from "../telemetry/observability_recovery.js";

export class UnifiedComponentPersistenceStore {
  private readonly sql: SqlStorageLike;

  constructor(sql: SqlStorageLike) {
    this.sql = sql;
    this.initAllTables();
  }

  private initAllTables(): void {
    const tables = [
      "CREATE TABLE IF NOT EXISTS persisted_tasks (task_id TEXT PRIMARY KEY, payload_json TEXT)",
      "CREATE TABLE IF NOT EXISTS persisted_leases (task_id TEXT PRIMARY KEY, node_id TEXT, fencing_token INTEGER, expires_at REAL)",
      "CREATE TABLE IF NOT EXISTS persisted_escrows (parent_task_id TEXT PRIMARY KEY, payload_json TEXT)",
      "CREATE TABLE IF NOT EXISTS persisted_router_state (endpoint_id TEXT PRIMARY KEY, payload_json TEXT)",
      "CREATE TABLE IF NOT EXISTS persisted_saga_steps (step_id TEXT PRIMARY KEY, task_id TEXT, payload_json TEXT)",
      "CREATE TABLE IF NOT EXISTS persisted_approval_tickets (ticket_id TEXT PRIMARY KEY, payload_json TEXT)",
      "CREATE TABLE IF NOT EXISTS persisted_finops_ledgers (tenant_id TEXT PRIMARY KEY, payload_json TEXT)",
      "CREATE TABLE IF NOT EXISTS persisted_mcp_tools (tool_name TEXT PRIMARY KEY, payload_json TEXT)",
      "CREATE TABLE IF NOT EXISTS persisted_observability (cell_id TEXT PRIMARY KEY, payload_json TEXT)",
    ];
    for (const ddl of tables) {
      this.sql.exec(ddl);
    }
  }

  public snapshotSchedulerState(scheduler: CoreScheduler5D): { tasksSaved: number; leasesSaved: number } {
    let tasksSaved = 0;
    for (const task of scheduler.allTasks.values()) {
      this.sql.exec(
        "INSERT OR REPLACE INTO persisted_tasks (task_id, payload_json) VALUES (?, ?)",
        task.task_id,
        JSON.stringify(task)
      );
      tasksSaved += 1;
    }
    return { tasksSaved, leasesSaved: tasksSaved };
  }

  public rehydrateSchedulerFromSql(targetScheduler: CoreScheduler5D): number {
    const rows = this.sql.exec<Record<string, unknown>>("SELECT * FROM persisted_tasks").toArray();
    let restored = 0;
    for (const r of rows) {
      const raw = JSON.parse(String(r.payload_json)) as Record<string, unknown>;
      const spec = new TaskSpec({
        task_id: String(raw.task_id),
        tenant_id: String(raw.tenant_id),
        priority: Number(raw.priority ?? TaskPriority.P2_STANDARD) as TaskPriority,
        prompt: String(raw.prompt ?? ""),
        max_turns: Number(raw.max_turns ?? 30),
        timeout_seconds: Number(raw.timeout_seconds ?? 1800),
        dependencies: Array.isArray(raw.dependencies) ? raw.dependencies.map(String) : [],
      });
      spec.state = (raw.state as TaskState) ?? TaskState.QUEUED;
      spec.lease_fencing_token = Number(raw.lease_fencing_token ?? 0);
      spec.assigned_node_id = raw.assigned_node_id ? String(raw.assigned_node_id) : undefined;
      targetScheduler.allTasks.set(spec.task_id, spec);
      if (spec.state === TaskState.QUEUED) {
        targetScheduler.queueManager.enqueue(spec);
      } else if (spec.state === TaskState.COMPLETED) {
        targetScheduler.completedIds.add(spec.task_id);
      }
      restored += 1;
    }
    return restored;
  }

  /**
   * Persists the bounded ring spool (spooled OTLP spans + CloudEvents + drop counters + active trace
   * contexts) so in-flight telemetry survives `celld` Isolate hibernation or a node crash.
   */
  public persistObservabilityState(
    coordinator: FaultTolerantObservabilityCoordinator,
    cellId: string,
  ): { spansSaved: number; cloudEventsSaved: number } {
    const spooler = coordinator.spooler;
    const payload = {
      cellId,
      spooledSpans: spooler.spooledSpans,
      spooledCloudEvents: spooler.spooledCloudEvents,
      droppedSpansCount: spooler.droppedSpansCount,
      droppedCloudEventsCount: spooler.droppedCloudEventsCount,
      activeTaskTraceMap: Array.from(coordinator.tracePropagator.activeTaskTraceMap.entries()),
      savedAtMs: Date.now(),
    };
    this.sql.exec(
      "INSERT OR REPLACE INTO persisted_observability (cell_id, payload_json) VALUES (?, ?)",
      cellId,
      JSON.stringify(payload),
    );
    return {
      spansSaved: spooler.spooledSpans.length,
      cloudEventsSaved: spooler.spooledCloudEvents.length,
    };
  }

  /**
   * Rehydrates a fresh `FaultTolerantObservabilityCoordinator` from its persisted spool. Returns `false`
   * when the cell has no persisted observability state yet.
   */
  public rehydrateObservabilityState(
    coordinator: FaultTolerantObservabilityCoordinator,
    cellId: string,
  ): boolean {
    const row = this.sql
      .exec<Record<string, unknown>>(
        "SELECT * FROM persisted_observability WHERE cell_id = ?",
        cellId,
      )
      .one();
    if (!row || row.payload_json === undefined) {
      return false;
    }
    const payload = JSON.parse(String(row.payload_json)) as {
      spooledSpans?: ContinuitySpanRecord[];
      spooledCloudEvents?: Array<Record<string, any>>;
      droppedSpansCount?: number;
      droppedCloudEventsCount?: number;
      activeTaskTraceMap?: Array<[string, { traceId: string; lastSpanId: string }]>;
    };
    coordinator.spooler.spooledSpans = payload.spooledSpans ?? [];
    coordinator.spooler.spooledCloudEvents = payload.spooledCloudEvents ?? [];
    coordinator.spooler.droppedSpansCount = payload.droppedSpansCount ?? 0;
    coordinator.spooler.droppedCloudEventsCount = payload.droppedCloudEventsCount ?? 0;
    for (const [taskId, ctx] of payload.activeTaskTraceMap ?? []) {
      coordinator.tracePropagator.activeTaskTraceMap.set(taskId, ctx);
    }
    return true;
  }

  public saveComponentRecord(
    table:
      | "persisted_escrows"
      | "persisted_router_state"
      | "persisted_saga_steps"
      | "persisted_approval_tickets"
      | "persisted_finops_ledgers"
      | "persisted_mcp_tools",
    pkCol: string,
    pkVal: string,
    payload: Record<string, unknown>
  ): void {
    this.sql.exec(
      `INSERT OR REPLACE INTO ${table} (${pkCol}, payload_json) VALUES (?, ?)`,
      pkVal,
      JSON.stringify(payload)
    );
  }

  public loadComponentRecords(table: string): Array<Record<string, unknown>> {
    const rows = this.sql.exec<Record<string, unknown>>(`SELECT * FROM ${table}`).toArray();
    return rows.map((r) => JSON.parse(String(r.payload_json ?? "{}")) as Record<string, unknown>);
  }
}
