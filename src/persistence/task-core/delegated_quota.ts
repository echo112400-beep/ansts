import { PersistenceObjectStorage } from "../contracts";
import { confirmCellWrite, withCellTransaction } from "../transactions";
import { readTaskExecution, TaskCoreError } from "./task_core";

/** Task-owned token reservations survive execution takeover; unknown usage never refills them. */
export class DelegatedTaskQuota {
  constructor(private readonly storage: PersistenceObjectStorage, private readonly taskId: string) {
    withCellTransaction(storage, () => {
      storage.sql.exec("CREATE TABLE IF NOT EXISTS task_delegated_quota_schema (version INTEGER PRIMARY KEY)");
      storage.sql.exec("INSERT OR IGNORE INTO task_delegated_quota_schema (version) VALUES (?)", 1);
      storage.sql.exec(`CREATE TABLE IF NOT EXISTS task_delegated_quota (
        task_id TEXT PRIMARY KEY, dag_id TEXT NOT NULL, voucher_id TEXT NOT NULL,
        token_limit INTEGER NOT NULL, tps_limit INTEGER NOT NULL, physical_tps INTEGER NOT NULL)`);
      storage.sql.exec(`CREATE TABLE IF NOT EXISTS task_delegated_token_calls (
        call_id TEXT PRIMARY KEY, reserved_tokens INTEGER NOT NULL, used_tokens INTEGER NOT NULL,
        status TEXT NOT NULL)`);
    });
  }

  public assignment(): any {
    return this.storage.sql.exec("SELECT * FROM task_delegated_quota WHERE task_id = ?", this.taskId).toArray()[0] ?? null;
  }

  public async assign(dagId: string, voucherId: string): Promise<void> {
    const current = this.assignment();
    if (current && (current.dag_id !== dagId || current.voucher_id !== voucherId)) {
      throw new TaskCoreError("DAG_QUOTA_ASSIGNMENT_CONFLICT", "Task quota belongs to another DAG voucher.");
    }
    if (!current) withCellTransaction(this.storage, () => this.storage.sql.exec(
      "INSERT INTO task_delegated_quota (task_id, dag_id, voucher_id, token_limit, tps_limit, physical_tps) VALUES (?, ?, ?, ?, ?, ?)",
      this.taskId, dagId, voucherId, 0, 0, 0));
    await confirmCellWrite(this.storage);
  }

  public state(): Record<string, unknown> {
    const assignment = this.assignment();
    if (!assignment) return {};
    const calls = this.storage.sql.exec<any>("SELECT * FROM task_delegated_token_calls").toArray();
    return { quota_dag_id: assignment.dag_id, quota_voucher_id: assignment.voucher_id,
      quota_tokens_used: calls.reduce((sum, call) => sum + Number(call.used_tokens), 0),
      quota_tokens_reserved: calls.reduce((sum, call) => sum + Number(call.reserved_tokens), 0),
      effective_delegated_tokens: Number(assignment.token_limit),
      effective_delegated_tps: Math.min(Number(assignment.tps_limit), Number(assignment.physical_tps)),
      pending_delegated_tps: Math.max(0, Number(assignment.tps_limit) - Number(assignment.physical_tps)) };
  }

  public async refresh(tokens: number, tps: number, physicalTps: number): Promise<void> {
    for (const value of [tokens, tps, physicalTps]) if (!Number.isSafeInteger(value) || value < 0) {
      throw new TaskCoreError("DAG_QUOTA_AUTHORITY_INVALID", "Delegated quota requires non-negative integer bounds.");
    }
    withCellTransaction(this.storage, () => this.storage.sql.exec(
      "UPDATE task_delegated_quota SET token_limit = ?, tps_limit = ?, physical_tps = ? WHERE task_id = ?",
      tokens, tps, physicalTps, this.taskId));
    await confirmCellWrite(this.storage);
  }

  public async reserve(callId: string, tokens: number, epoch: number): Promise<void> {
    withCellTransaction(this.storage, () => {
      const execution = readTaskExecution(this.storage.sql, this.taskId);
      if (!execution || execution.lifecycle_state !== "RUNNING" || Number(execution.business_epoch) !== epoch) {
        throw new TaskCoreError("STALE_EXECUTION_EPOCH", "Delegated token authorization requires the current running epoch.");
      }
      const existing = this.storage.sql.exec<any>("SELECT * FROM task_delegated_token_calls WHERE call_id = ?", callId).toArray()[0];
      if (existing) throw new TaskCoreError("DAG_TOKEN_CALL_ALREADY_AUTHORIZED", "A prior token authorization must recover through its provider journal.");
      const state = this.state();
      if (!Number.isSafeInteger(tokens) || tokens < 0 || Number(state.quota_tokens_used) + Number(state.quota_tokens_reserved) + tokens > Number(state.effective_delegated_tokens)) {
        throw new TaskCoreError("DAG_TOKEN_QUOTA_EXCEEDED", "The call's conservative token bound exceeds remaining delegated tokens.");
      }
      this.storage.sql.exec("INSERT INTO task_delegated_token_calls (call_id, reserved_tokens, used_tokens, status) VALUES (?, ?, ?, ?)",
        callId, tokens, 0, "HELD");
    });
    await confirmCellWrite(this.storage);
  }

  public async settle(callId: string, usedTokens: number): Promise<void> {
    withCellTransaction(this.storage, () => {
      const call = this.storage.sql.exec<any>("SELECT * FROM task_delegated_token_calls WHERE call_id = ?", callId).toArray()[0];
      if (!call || !Number.isSafeInteger(usedTokens) || usedTokens < 0 ||
          (call.status === "SETTLED" ? Number(call.used_tokens) !== usedTokens : usedTokens > Number(call.reserved_tokens))) {
        throw new TaskCoreError("DAG_TOKEN_USAGE_UNKNOWN", "Verified usage must match its original token authorization.");
      }
      if (call.status !== "SETTLED") this.storage.sql.exec(
        "UPDATE task_delegated_token_calls SET reserved_tokens = ?, used_tokens = ?, status = ? WHERE call_id = ?",
        0, usedTokens, "SETTLED", callId);
    });
    await confirmCellWrite(this.storage);
  }
}
