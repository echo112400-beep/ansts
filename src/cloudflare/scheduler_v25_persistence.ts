/**
 * @fileoverview v2.5 Single-Write-Path Scheduler Persistence & Queue Deduplication Store (`b566a53`).
 */
import { SqlStorageLike } from "./env.js";
import { TaskPriority, TaskSpec, TaskState } from "../models.js";

export class SchedulerV25SingleWritePathStore {
  private readonly sql: SqlStorageLike;
  private readonly seenIdempotencyKeys: Set<string> = new Set();

  constructor(sql: SqlStorageLike) {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS v25_scheduler_tasks (task_id TEXT PRIMARY KEY, tenant_id TEXT, priority INTEGER, state TEXT, fencing_token INTEGER, idem_key TEXT, spec_json TEXT)"
    );
  }

  public commitTaskSingleWritePath(task: TaskSpec): { deduplicated: boolean; task: TaskSpec } {
    const idemKey = task.idempotency_key ?? `idem:${task.tenant_id}:${task.task_id}`;
    if (this.seenIdempotencyKeys.has(idemKey)) {
      const existing = this.sql
        .exec<Record<string, unknown>>("SELECT * FROM v25_scheduler_tasks WHERE task_id = ?", task.task_id)
        .one();
      if (existing) {
        return { deduplicated: true, task };
      }
    }
    this.seenIdempotencyKeys.add(idemKey);
    this.sql.exec(
      "INSERT OR REPLACE INTO v25_scheduler_tasks (task_id, tenant_id, priority, state, fencing_token, idem_key, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
      task.task_id,
      task.tenant_id,
      task.priority,
      task.state,
      task.lease_fencing_token,
      idemKey,
      JSON.stringify(task)
    );
    return { deduplicated: false, task };
  }

  public listPersistedTasks(): TaskSpec[] {
    const rows = this.sql.exec<Record<string, unknown>>("SELECT * FROM v25_scheduler_tasks").toArray();
    return rows.map((r) => {
      const parsed = JSON.parse(String(r.spec_json ?? "{}")) as Record<string, unknown>;
      const t = new TaskSpec({
        task_id: String(r.task_id),
        tenant_id: String(r.tenant_id),
        priority: Number(r.priority ?? TaskPriority.P2_STANDARD) as TaskPriority,
        prompt: String(parsed.prompt ?? ""),
      });
      t.state = (r.state as TaskState) ?? TaskState.QUEUED;
      t.lease_fencing_token = Number(r.fencing_token ?? 0);
      return t;
    });
  }
}
