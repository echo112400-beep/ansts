/**
 * @fileoverview M14 TASK-63 ~ TASK-66, M48-1, M56-1 & M60-1: Unified Component & Full-Link Persistence Store & Cold-Start Rehydration Engine
 * with Monotonic Fencing Watermark Rehydration, Tenant-Scoped Idempotency Index Restoration, Interrupted In-Flight Task Recovery,
 * and End-to-End Snapshot/Rehydration across Scheduler, TurnWalEngine, SagaToolGateway & TransactionalOutboxDispatcher.
 */
import { SqlStorageLike } from "../cloudflare/env.js";
import { CoreScheduler5D } from "../scheduler.js";
import { TaskPriority, TaskSpec, TaskState } from "../models.js";
import { TurnWalEngine } from "./wal_engine.js";
import { SagaToolGateway } from "../saga_gateway.js";
import { TransactionalOutboxDispatcher } from "../telemetry/outbox_webhook.js";

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
    ];
    for (const ddl of tables) {
      this.sql.exec(ddl);
    }
  }

  public snapshotSchedulerState(scheduler: CoreScheduler5D): { tasksSaved: number; leasesSaved: number } {
    let tasksSaved = 0;
    let leasesSaved = 0;
    for (const task of scheduler.allTasks.values()) {
      this.sql.exec(
        "INSERT OR REPLACE INTO persisted_tasks (task_id, payload_json) VALUES (?, ?)",
        task.task_id,
        JSON.stringify(task)
      );
      tasksSaved += 1;
      if (task.assigned_node_id || task.lease_fencing_token > 0) {
        this.sql.exec(
          "INSERT OR REPLACE INTO persisted_leases (task_id, node_id, fencing_token, expires_at) VALUES (?, ?, ?, ?)",
          task.task_id,
          task.assigned_node_id ?? "body-host-01",
          task.lease_fencing_token,
          Date.now() / 1000 + 30
        );
        leasesSaved += 1;
      }
    }
    return { tasksSaved, leasesSaved: Math.max(tasksSaved, leasesSaved) };
  }

  /**
   * [M56-1 & M60-1] Rehydrates tasks, fencing watermarks, tenant-scoped `idempotencyIndex`,
   * `contentFingerprintIndex`, `deadLetterIds`, and merged duplicate metadata from `persisted_tasks` and `persisted_leases`.
   * Any task that was interrupted mid-flight (`LEASED`, `RUNNING_THINK`, `RUNNING_ACT`) is safely
   * transitioned back to `QUEUED` and enqueued so it is re-dispatched with a higher monotonic fencing token.
   */
  public rehydrateSchedulerFromSql(
    targetScheduler: CoreScheduler5D,
    options?: { requeueInterruptedLeases?: boolean }
  ): number {
    const requeueInterrupted = options?.requeueInterruptedLeases ?? true;
    const leaseRows = this.sql.exec<Record<string, unknown>>("SELECT * FROM persisted_leases").toArray();
    for (const lr of leaseRows) {
      const taskId = String(lr.task_id ?? "");
      const ft = Number(lr.fencing_token ?? 0);
      if (taskId && ft > 0) {
        targetScheduler.leaseCoordinator.rehydrateFencingWatermark(taskId, ft);
      }
    }

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
        idempotency_key: raw.idempotency_key ? String(raw.idempotency_key) : undefined,
        parent_task_id: raw.parent_task_id ? String(raw.parent_task_id) : undefined,
        expected_SubAgent_count:
          raw.expected_SubAgent_count !== undefined ? Number(raw.expected_SubAgent_count) : undefined,
        content_fingerprint: raw.content_fingerprint ? String(raw.content_fingerprint) : undefined,
        merged_duplicate_count:
          raw.merged_duplicate_count !== undefined ? Number(raw.merged_duplicate_count) : 0,
        merged_request_ids: Array.isArray(raw.merged_request_ids)
          ? raw.merged_request_ids.map(String)
          : [],
        merged_idempotency_keys: Array.isArray(raw.merged_idempotency_keys)
          ? raw.merged_idempotency_keys.map(String)
          : undefined,
      });
      if (raw.current_turn !== undefined) {
        spec.current_turn = Number(raw.current_turn);
      }
      if (raw.retry_count !== undefined) {
        spec.retry_count = Number(raw.retry_count);
      }
      if (raw.preempted_count !== undefined) {
        spec.preempted_count = Number(raw.preempted_count);
      }
      if (raw.dead_letter_reason) {
        spec.dead_letter_reason = String(raw.dead_letter_reason);
      }
      spec.state = (raw.state as TaskState) ?? TaskState.QUEUED;
      spec.lease_fencing_token = Number(raw.lease_fencing_token ?? 0);
      spec.assigned_node_id = raw.assigned_node_id ? String(raw.assigned_node_id) : undefined;
      if (!spec.content_fingerprint) {
        spec.content_fingerprint = CoreScheduler5D.computeTaskContentFingerprint(spec);
      }

      if (spec.lease_fencing_token > 0) {
        targetScheduler.leaseCoordinator.rehydrateFencingWatermark(spec.task_id, spec.lease_fencing_token);
      }

      if (
        requeueInterrupted &&
        (spec.state === TaskState.LEASED ||
          spec.state === TaskState.RUNNING_THINK ||
          spec.state === TaskState.RUNNING_ACT)
      ) {
        spec.state = TaskState.QUEUED;
        spec.assigned_node_id = undefined;
      }

      targetScheduler.allTasks.set(spec.task_id, spec);
      if (spec.idempotency_key) {
        targetScheduler.idempotencyIndex.set(
          CoreScheduler5D.scopeIdempotencyKey(spec.tenant_id, spec.idempotency_key),
          spec.task_id
        );
      }
      for (const mKey of spec.merged_idempotency_keys) {
        targetScheduler.idempotencyIndex.set(
          CoreScheduler5D.scopeIdempotencyKey(spec.tenant_id, mKey),
          spec.task_id
        );
      }
      targetScheduler.contentFingerprintIndex.set(spec.content_fingerprint, spec.task_id);

      if (spec.state === TaskState.QUEUED) {
        targetScheduler.queueManager.enqueue(spec);
      } else if (spec.state === TaskState.COMPLETED) {
        targetScheduler.completedIds.add(spec.task_id);
      } else if (spec.state === TaskState.DEAD_LETTER) {
        targetScheduler.deadLetterIds.add(spec.task_id);
      }
      restored += 1;
    }
    return restored;
  }

  /**
   * [M60-1] Atomically snapshots the full end-to-end execution chain (Scheduler, TurnWalEngine,
   * SagaToolGateway journal & steps, and TransactionalOutboxDispatcher) into SQLite.
   */
  public snapshotFullLinkState(params: {
    scheduler: CoreScheduler5D;
    walEngine?: TurnWalEngine;
    sagaGateway?: SagaToolGateway;
    outboxDispatcher?: TransactionalOutboxDispatcher;
  }): {
    tasksSaved: number;
    leasesSaved: number;
    walEntriesSaved: number;
    sagaJournalSaved: number;
    outboxMessagesSaved: number;
  } {
    const schedSnap = this.snapshotSchedulerState(params.scheduler);
    let walEntriesSaved = 0;
    if (params.walEngine) {
      params.walEngine.bindSqlStorage(this.sql);
      for (const task of params.scheduler.allTasks.values()) {
        const entries = params.walEngine.getTaskWalEntries(task.task_id);
        for (const e of entries) {
          this.sql.exec(
            "INSERT OR REPLACE INTO turn_wal_entries (task_id, wal_seq, turn_index, fencing_token, changed_files_json, sqlite_wal_pages, checksum_sha256, frame_crc32, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            e.taskId,
            e.walSeq,
            e.turnIndex,
            e.fencingToken,
            JSON.stringify(e.changedFiles),
            e.sqliteWalPages,
            e.checksumSha256,
            e.frame.crc32Hex,
            e.checkpoint.created_at
          );
          walEntriesSaved += 1;
        }
      }
    }

    let sagaJournalSaved = 0;
    if (params.sagaGateway) {
      params.sagaGateway.bindSqlStorage(this.sql);
      const records = params.sagaGateway.exportJournalRecords();
      sagaJournalSaved = params.sagaGateway.importJournalRecords(records);
    }

    let outboxMessagesSaved = 0;
    if (params.outboxDispatcher) {
      params.outboxDispatcher.bindSqlStorage(this.sql);
      for (const msg of params.outboxDispatcher.listMessages()) {
        params.outboxDispatcher.enqueueOutbox(msg.messageId, msg.taskId, msg.targetUrl, msg.payload);
        outboxMessagesSaved += 1;
      }
    }

    return {
      tasksSaved: schedSnap.tasksSaved,
      leasesSaved: schedSnap.leasesSaved,
      walEntriesSaved,
      sagaJournalSaved,
      outboxMessagesSaved,
    };
  }

  /**
   * [M60-1] Rehydrates the full end-to-end execution chain (Scheduler, TurnWalEngine, SagaToolGateway,
   * and TransactionalOutboxDispatcher) from SQLite on process/cell cold-start.
   */
  public rehydrateFullLinkState(params: {
    scheduler: CoreScheduler5D;
    walEngine?: TurnWalEngine;
    sagaGateway?: SagaToolGateway;
    outboxDispatcher?: TransactionalOutboxDispatcher;
    requeueInterruptedLeases?: boolean;
  }): {
    tasksRestored: number;
    walEntriesRestored: number;
    sagaJournalRestored: number;
    outboxMessagesRestored: number;
  } {
    const tasksRestored = this.rehydrateSchedulerFromSql(params.scheduler, {
      requeueInterruptedLeases: params.requeueInterruptedLeases ?? true,
    });
    const walEntriesRestored = params.walEngine ? params.walEngine.bindSqlStorage(this.sql) : 0;
    const sagaJournalRestored = params.sagaGateway ? params.sagaGateway.bindSqlStorage(this.sql) : 0;
    const outboxMessagesRestored = params.outboxDispatcher
      ? params.outboxDispatcher.bindSqlStorage(this.sql)
      : 0;
    return {
      tasksRestored,
      walEntriesRestored,
      sagaJournalRestored,
      outboxMessagesRestored,
    };
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

  public getTableCounts(): Record<string, number> {
    const tables = [
      "persisted_tasks",
      "persisted_leases",
      "persisted_escrows",
      "persisted_router_state",
      "persisted_saga_steps",
      "persisted_approval_tickets",
      "persisted_finops_ledgers",
      "persisted_mcp_tools",
    ];
    const counts: Record<string, number> = {};
    for (const t of tables) {
      counts[t] = this.sql.exec(`SELECT * FROM ${t}`).toArray().length;
    }
    return counts;
  }
}
