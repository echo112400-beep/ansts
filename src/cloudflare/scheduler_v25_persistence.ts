/**
 * @fileoverview v2.5 Single-Write-Path Scheduler Persistence & Queue Deduplication Store (`b566a53`) & M55-1.
 * Hardened with tenant-scoped cross-task idempotency lookup, full `spec_json` fidelity rehydration,
 * strict single-write-path monotonic fencing token allocation, and anti-regressive RustFS S3 LTX restoration.
 */
import { InMemoryCellSqlStorage, SqlStorageLike } from "./env.js";
import { TaskPriority, TaskSpec, TaskState } from "../models.js";

export class SchedulerV25SingleWritePathStore {
  public readonly sql: SqlStorageLike;
  private readonly seenIdempotencyKeys: Map<string, string> = new Map();
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
        const scopedKey = `${String(r.tenant_id ?? "default")}::${String(r.idem_key)}`;
        this.seenIdempotencyKeys.set(scopedKey, String(r.task_id));
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
    const scopedKey = `${task.tenant_id}::${idemKey}`;
    const mappedTaskId = this.seenIdempotencyKeys.get(scopedKey);
    if (mappedTaskId) {
      const existing =
        this.sql
          .exec<Record<string, unknown>>("SELECT * FROM v25_scheduler_tasks WHERE task_id = ?", mappedTaskId)
          .one() ??
        this.sql
          .exec<Record<string, unknown>>("SELECT * FROM v25_scheduler_tasks WHERE task_id = ?", task.task_id)
          .one();
      if (existing) {
        const restored = this.rowToTaskSpec(existing);
        return {
          deduplicated: true,
          fencingToken: restored.lease_fencing_token,
          task: restored,
        };
      }
    }
    this.seenIdempotencyKeys.set(scopedKey, task.task_id);
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
      max_turns: parsed.max_turns !== undefined ? Number(parsed.max_turns) : undefined,
      timeout_seconds: parsed.timeout_seconds !== undefined ? Number(parsed.timeout_seconds) : undefined,
      dependencies: Array.isArray(parsed.dependencies) ? (parsed.dependencies as string[]) : undefined,
      parent_task_id: parsed.parent_task_id ? String(parsed.parent_task_id) : undefined,
      expected_SubAgent_count:
        parsed.expected_SubAgent_count !== undefined ? Number(parsed.expected_SubAgent_count) : undefined,
    });
    if (parsed.current_turn !== undefined) {
      t.current_turn = Number(parsed.current_turn);
    }
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

  /**
   * [M55-1] Anti-regressive S3 LTX snapshot restoration:
   * Only overwrites a local task row if the incoming snapshot row has `fencing_token >= local.fencing_token`,
   * preventing lagging S3 snapshots from regressing a task's fencing epoch or resurrecting completed tasks.
   */
  public async restoreLtxSnapshotFromS3(s3Endpoint: string, nodeId: string): Promise<number> {
    try {
      const resp = await fetch(`${s3Endpoint.replace(/\/$/, "")}/ansts-fleet/ltx/${encodeURIComponent(nodeId)}.json`);
      if (!resp.ok) return 0;
      const data = (await resp.json()) as {
        maxFencingToken?: number;
        rows?: Array<Record<string, unknown>>;
      };
      if (data.maxFencingToken !== undefined && Number(data.maxFencingToken) > this.maxFencingToken) {
        this.maxFencingToken = Number(data.maxFencingToken);
      }
      let restored = 0;
      for (const r of data.rows ?? []) {
        const taskId = String(r.task_id);
        const incomingToken = Number(r.fencing_token ?? 0);
        const existingLocal = this.sql
          .exec<Record<string, unknown>>("SELECT * FROM v25_scheduler_tasks WHERE task_id = ?", taskId)
          .one();
        if (existingLocal && Number(existingLocal.fencing_token ?? 0) > incomingToken) {
          continue;
        }
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
        if (r.idem_key) {
          const scopedKey = `${String(r.tenant_id ?? "default")}::${String(r.idem_key)}`;
          this.seenIdempotencyKeys.set(scopedKey, taskId);
        }
        if (incomingToken > this.maxFencingToken) {
          this.maxFencingToken = incomingToken;
        }
        restored += 1;
      }
      return restored;
    } catch {
      return 0;
    }
  }
}
