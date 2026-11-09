/**
 * @fileoverview v2.5 Single-Write-Path Scheduler Persistence & Queue Deduplication Store (`b566a53`).
 * Hardened with cold-start SQL rehydration of `seenIdempotencyKeys` & `maxFencingToken`,
 * strict single-write-path monotonic fencing token allocation, and RustFS S3 LTX replication.
 */
import { InMemoryCellSqlStorage, SqlStorageLike } from "./env.js";
import { TaskPriority, TaskSpec, TaskState } from "../models.js";

export class SchedulerV25SingleWritePathStore {
  public readonly sql: SqlStorageLike;
  private readonly seenIdempotencyKeys: Set<string> = new Set();
  private maxFencingToken = 0;

  constructor(sql: SqlStorageLike = new InMemoryCellSqlStorage()) {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS v25_scheduler_tasks (task_id TEXT PRIMARY KEY, tenant_id TEXT, priority INTEGER, state TEXT, fencing_token INTEGER, assigned_node_id TEXT, idem_key TEXT, spec_json TEXT)"
    );
    // Cold-start rehydration from durable SQL table
    const existingRows = this.sql.exec<Record<string, unknown>>("SELECT * FROM v25_scheduler_tasks").toArray();
    for (const r of existingRows) {
      if (r.idem_key) {
        this.seenIdempotencyKeys.add(String(r.idem_key));
      }
      const ft = Number(r.fencing_token ?? 0);
      if (ft > this.maxFencingToken) {
        this.maxFencingToken = ft;
      }
    }
  }

  public commitTaskSingleWritePath(task: TaskSpec, assignedNodeId?: string | null): {
    deduplicated: boolean;
    fencingToken: number;
    task: TaskSpec;
  } {
    const idemKey = task.idempotency_key ?? `idem:${task.tenant_id}:${task.task_id}`;
    if (this.seenIdempotencyKeys.has(idemKey)) {
      const existing = this.sql
        .exec<Record<string, unknown>>("SELECT * FROM v25_scheduler_tasks WHERE task_id = ?", task.task_id)
        .one();
      if (existing) {
        const existingToken = Number(existing.fencing_token ?? task.lease_fencing_token ?? 1);
        task.lease_fencing_token = existingToken;
        if (existing.state) task.state = existing.state as TaskState;
        if (existing.assigned_node_id) task.assigned_node_id = String(existing.assigned_node_id);
        return { deduplicated: true, fencingToken: existingToken, task };
      }
    }
    this.seenIdempotencyKeys.add(idemKey);
    if (task.lease_fencing_token > this.maxFencingToken) {
      this.maxFencingToken = task.lease_fencing_token;
    } else {
      this.maxFencingToken += 1;
      task.lease_fencing_token = this.maxFencingToken;
    }
    if (assignedNodeId !== undefined) {
      task.assigned_node_id = assignedNodeId ?? undefined;
    }

    this.sql.exec(
      "INSERT OR REPLACE INTO v25_scheduler_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, idem_key, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      task.task_id,
      task.tenant_id,
      task.priority,
      task.state,
      task.lease_fencing_token,
      task.assigned_node_id ?? "",
      idemKey,
      JSON.stringify(task)
    );
    return { deduplicated: false, fencingToken: task.lease_fencing_token, task };
  }

  public getPersistedTask(taskId: string): TaskSpec | null {
    const r = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM v25_scheduler_tasks WHERE task_id = ?", taskId)
      .one();
    if (!r) return null;
    return this.rowToTaskSpec(r);
  }

  public listPersistedTasks(): TaskSpec[] {
    const rows = this.sql.exec<Record<string, unknown>>("SELECT * FROM v25_scheduler_tasks").toArray();
    return rows.map((r) => this.rowToTaskSpec(r));
  }

  private rowToTaskSpec(r: Record<string, unknown>): TaskSpec {
    const parsed = JSON.parse(String(r.spec_json ?? "{}")) as Record<string, unknown>;
    const t = new TaskSpec({
      task_id: String(r.task_id),
      tenant_id: String(r.tenant_id),
      priority: Number(r.priority ?? TaskPriority.P2_STANDARD) as TaskPriority,
      prompt: String(parsed.prompt ?? ""),
      idempotency_key: r.idem_key ? String(r.idem_key) : undefined,
    });
    t.state = (r.state as TaskState) ?? TaskState.QUEUED;
    t.lease_fencing_token = Number(r.fencing_token ?? 0);
    if (r.assigned_node_id && String(r.assigned_node_id).length > 0) {
      t.assigned_node_id = String(r.assigned_node_id);
    }
    return t;
  }

  public async flushLtxSnapshotToS3(s3Endpoint: string, nodeId: string): Promise<boolean> {
    try {
      const rows = this.sql.exec<Record<string, unknown>>("SELECT * FROM v25_scheduler_tasks").toArray();
      const resp = await fetch(`${s3Endpoint.replace(/\/$/, "")}/ansts-fleet/ltx/${encodeURIComponent(nodeId)}.json`, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ nodeId, maxFencingToken: this.maxFencingToken, rows }),
      });
      return resp.ok;
    } catch {
      return false;
    }
  }

  public async restoreLtxSnapshotFromS3(s3Endpoint: string, nodeId: string): Promise<number> {
    try {
      const resp = await fetch(`${s3Endpoint.replace(/\/$/, "")}/ansts-fleet/ltx/${encodeURIComponent(nodeId)}.json`);
      if (!resp.ok) return 0;
      const data = (await resp.json()) as {
        maxFencingToken?: number;
        rows?: Array<Record<string, unknown>>;
      };
      let restored = 0;
      for (const r of data.rows ?? []) {
        this.sql.exec(
          "INSERT OR REPLACE INTO v25_scheduler_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, idem_key, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
          r.task_id,
          r.tenant_id,
          r.priority,
          r.state,
          r.fencing_token,
          r.assigned_node_id ?? "",
          r.idem_key,
          r.spec_json
        );
        if (r.idem_key) this.seenIdempotencyKeys.add(String(r.idem_key));
        const ft = Number(r.fencing_token ?? 0);
        if (ft > this.maxFencingToken) this.maxFencingToken = ft;
        restored += 1;
      }
      return restored;
    } catch {
      return 0;
    }
  }
}
