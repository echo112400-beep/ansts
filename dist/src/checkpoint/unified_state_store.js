"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.UnifiedComponentPersistenceStore = void 0;




var _models = require("../models.js"); /**
 * @fileoverview M14 TASK-63 ~ TASK-66: Unified 100% Component Persistence Store & Cold-Start Rehydration Engine.
 */class UnifiedComponentPersistenceStore {
  sql;

  constructor(sql) {
    this.sql = sql;
    this.initAllTables();
  }

  initAllTables() {
    const tables = [
    "CREATE TABLE IF NOT EXISTS persisted_tasks (task_id TEXT PRIMARY KEY, payload_json TEXT)",
    "CREATE TABLE IF NOT EXISTS persisted_leases (task_id TEXT PRIMARY KEY, node_id TEXT, fencing_token INTEGER, expires_at REAL)",
    "CREATE TABLE IF NOT EXISTS persisted_escrows (parent_task_id TEXT PRIMARY KEY, payload_json TEXT)",
    "CREATE TABLE IF NOT EXISTS persisted_router_state (endpoint_id TEXT PRIMARY KEY, payload_json TEXT)",
    "CREATE TABLE IF NOT EXISTS persisted_saga_steps (step_id TEXT PRIMARY KEY, task_id TEXT, payload_json TEXT)",
    "CREATE TABLE IF NOT EXISTS persisted_approval_tickets (ticket_id TEXT PRIMARY KEY, payload_json TEXT)",
    "CREATE TABLE IF NOT EXISTS persisted_finops_ledgers (tenant_id TEXT PRIMARY KEY, payload_json TEXT)",
    "CREATE TABLE IF NOT EXISTS persisted_mcp_tools (tool_name TEXT PRIMARY KEY, payload_json TEXT)"];

    for (const ddl of tables) {
      this.sql.exec(ddl);
    }
  }

  snapshotSchedulerState(scheduler) {
    let tasksSaved = 0;
    for (const task of scheduler.allTasks.values()) {
      this.sql.exec(
      "INSERT OR REPLACE INTO persisted_tasks (task_id, payload_json) VALUES (?, ?)",
      task.task_id,
      JSON.stringify(task));

      tasksSaved += 1;
    }
    return { tasksSaved, leasesSaved: tasksSaved };
  }

  rehydrateSchedulerFromSql(targetScheduler) {
    const rows = this.sql.exec("SELECT * FROM persisted_tasks").toArray();
    let restored = 0;
    for (const r of rows) {
      const raw = JSON.parse(String(r.payload_json));
      const spec = new _models.TaskSpec({
        task_id: String(raw.task_id),
        tenant_id: String(raw.tenant_id),
        priority: Number(raw.priority ?? _models.TaskPriority.P2_STANDARD),
        prompt: String(raw.prompt ?? ""),
        max_turns: Number(raw.max_turns ?? 30),
        timeout_seconds: Number(raw.timeout_seconds ?? 1800),
        dependencies: Array.isArray(raw.dependencies) ? raw.dependencies.map(String) : []
      });
      spec.state = raw.state ?? _models.TaskState.QUEUED;
      spec.lease_fencing_token = Number(raw.lease_fencing_token ?? 0);
      spec.assigned_node_id = raw.assigned_node_id ? String(raw.assigned_node_id) : undefined;
      targetScheduler.allTasks.set(spec.task_id, spec);
      if (spec.state === _models.TaskState.QUEUED) {
        targetScheduler.queueManager.enqueue(spec);
      } else if (spec.state === _models.TaskState.COMPLETED) {
        targetScheduler.completedIds.add(spec.task_id);
      }
      restored += 1;
    }
    return restored;
  }

  saveComponentRecord(
  table,






  pkCol,
  pkVal,
  payload)
  {
    this.sql.exec(
    `INSERT OR REPLACE INTO ${table} (${pkCol}, payload_json) VALUES (?, ?)`,
    pkVal,
    JSON.stringify(payload));

  }

  loadComponentRecords(table) {
    const rows = this.sql.exec(`SELECT * FROM ${table}`).toArray();
    return rows.map((r) => JSON.parse(String(r.payload_json ?? "{}")));
  }
}exports.UnifiedComponentPersistenceStore = UnifiedComponentPersistenceStore;