import { FenceToken, ToolEffectClass } from "../../models";
import { CellInboxEvent, CellMigration, CellOperationCommand, PersistenceObjectStorage, PersistenceSqlStorage } from "../contracts";
import { digestJson } from "../digest";
import { consumeCellInboxEvent } from "../inbox";
import { executeCellOperation } from "../operations";
import { assertBusinessEpoch, TaskExecutionRecord } from "../task-core/task_core";
import { confirmCellWrite } from "../transactions";

export type TaskOperationStatus = "APPROVAL_REQUIRED" | "READY" | "RUNNING" | "SUCCEEDED" | "FAILED" | "OUTCOME_UNKNOWN";
export type TaskOperationAttemptStatus = "RUNNING" | "SUCCEEDED" | "FAILED" | "OUTCOME_UNKNOWN";
export type TaskCompensationStatus = "PENDING" | "RUNNING" | "COMPENSATED" | "COMPENSATION_FAILED" | "OUTCOME_UNKNOWN";

export interface TaskOperationRequest {
  operationId: string;
  tenantId: string;
  fence: FenceToken;
  toolName: string;
  effectClass: ToolEffectClass;
  args: unknown;
  compensation?: { toolName: string; args: unknown };
  approval?: { scope: unknown };
  race?: { raceId: string; branchId: string };
}

export interface TaskOperationAttemptRecord {
  attemptId: string;
  operationId: string;
  attemptNumber: number;
  fence: FenceToken;
  status: TaskOperationAttemptStatus;
  outcomeSource?: "EXECUTOR" | "PROVIDER_LOOKUP";
  result?: unknown;
  error?: string;
}

export interface TaskOperationRecord {
  operationId: string;
  taskId: string;
  tenantId: string;
  turnSeq: number;
  toolName: string;
  effectClass: ToolEffectClass;
  args: unknown;
  argumentDigest: string;
  idempotencyKey: string;
  status: TaskOperationStatus;
  attempts: TaskOperationAttemptRecord[];
  compensation?: { toolName: string; args: unknown; status: TaskCompensationStatus; attempts: number; lastError?: string };
  approval?: { approvalId: string; approvedBy: string; scope: unknown; expiresAt: number };
  race?: { raceId: string; branchId: string; selectedWinner?: string };
  result?: unknown;
  lastError?: string;
}

export interface TaskOperationProvider<T = unknown> {
  supportsIdempotency: boolean;
  execute(input: { operationId: string; idempotencyKey: string; attemptId: string; toolName: string; args: unknown }): Promise<T>;
  /** NOT_FOUND must mean no attempt was accepted and no in-flight attempt can still commit; otherwise return UNKNOWN. */
  lookup?(input: { operationId: string; idempotencyKey: string }): Promise<
    { status: "FOUND"; result: T } | { status: "NOT_FOUND" } | { status: "UNKNOWN" }
  >;
}

export class TaskOperationError extends Error {
  constructor(public readonly code: string, message: string, public readonly status = 409) {
    super(message);
    this.name = "TaskOperationError";
  }
}

export const taskOperationsMigration: CellMigration = {
  version: 3,
  description: "task-owned operation intent, approval, provider attempts, race decisions and compensation",
  up(sql) {
    sql.exec(`CREATE TABLE IF NOT EXISTS task_operations (
      operation_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, tenant_id TEXT NOT NULL, turn_seq INTEGER NOT NULL,
      tool_name TEXT NOT NULL, effect_class TEXT NOT NULL, args_json TEXT NOT NULL, argument_digest TEXT NOT NULL,
      status TEXT NOT NULL, compensation_tool TEXT, compensation_args_json TEXT, approval_scope_json TEXT,
      race_id TEXT, race_branch_id TEXT, result_json TEXT, last_error TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    )`);
    sql.exec("CREATE INDEX IF NOT EXISTS task_operations_task_idx ON task_operations (task_id, turn_seq, created_at)");
    sql.exec(`CREATE TABLE IF NOT EXISTS task_operation_attempts (
      attempt_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL, attempt_number INTEGER NOT NULL, epoch INTEGER NOT NULL,
      turn_seq INTEGER NOT NULL, status TEXT NOT NULL, result_json TEXT, error_text TEXT, outcome_source TEXT, started_at INTEGER NOT NULL, completed_at INTEGER
    )`);
    sql.exec("CREATE INDEX IF NOT EXISTS task_operation_attempts_operation_idx ON task_operation_attempts (operation_id, attempt_number)");
    sql.exec(`CREATE TABLE IF NOT EXISTS task_operation_approvals (
      approval_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL, argument_digest TEXT NOT NULL, scope_digest TEXT NOT NULL,
      scope_json TEXT NOT NULL, approved_by TEXT NOT NULL, expires_at INTEGER NOT NULL, created_at INTEGER NOT NULL
    )`);
    sql.exec("CREATE UNIQUE INDEX IF NOT EXISTS task_operation_approvals_operation_idx ON task_operation_approvals (operation_id)");
    sql.exec("CREATE TABLE IF NOT EXISTS task_race_winner_receipts (race_id TEXT PRIMARY KEY, event_id TEXT NOT NULL, decision_id TEXT NOT NULL, winner_branch_id TEXT NOT NULL, created_at INTEGER NOT NULL)");
    sql.exec(`CREATE TABLE IF NOT EXISTS task_compensations (
      operation_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, turn_seq INTEGER NOT NULL, order_seq INTEGER NOT NULL,
      tool_name TEXT NOT NULL, args_json TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL, last_error TEXT, updated_at INTEGER NOT NULL
    )`);
    sql.exec("CREATE INDEX IF NOT EXISTS task_compensations_lifo_idx ON task_compensations (task_id, turn_seq, order_seq)");
    sql.exec(`CREATE TABLE IF NOT EXISTS task_compensation_attempts (
      attempt_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL, epoch INTEGER NOT NULL, turn_seq INTEGER NOT NULL,
      status TEXT NOT NULL, error_text TEXT, started_at INTEGER NOT NULL, completed_at INTEGER
    )`);
  },
};

interface OperationRow {
  operation_id: string; task_id: string; tenant_id: string; turn_seq: number; tool_name: string;
  effect_class: ToolEffectClass; args_json: string; argument_digest: string; status: TaskOperationStatus;
  compensation_tool: string | null; compensation_args_json: string | null; approval_scope_json: string | null;
  race_id: string | null; race_branch_id: string | null; result_json: string | null; last_error: string | null;
}
interface AttemptRow {
  attempt_id: string; operation_id: string; attempt_number: number; epoch: number; turn_seq: number;
  status: TaskOperationAttemptStatus; result_json: string | null; error_text: string | null; outcome_source: "EXECUTOR" | "PROVIDER_LOOKUP" | null;
}
interface ApprovalRow {
  approval_id: string; operation_id: string; argument_digest: string; scope_digest: string;
  scope_json: string; approved_by: string; expires_at: number;
}
interface CompensationRow {
  operation_id: string; task_id: string; turn_seq: number; order_seq: number; tool_name: string; args_json: string;
  status: TaskCompensationStatus; attempts: number; last_error: string | null;
}

export interface DagCellRaceWinnerEvent {
  eventId: string;
  decisionId: string;
  taskId: string;
  tenantId: string;
  raceId: string;
  winnerBranchId: string;
  aggregateVersion: number;
}

export function stableTaskOperationId(taskId: string, toolCallId: string): string {
  validateText(taskId, "taskId");
  validateText(toolCallId, "toolCallId");
  return `task-op:${taskId.length}:${taskId}:${toolCallId.length}:${toolCallId}`;
}

/** TaskCell's durable ledger for external effects, approval and compensation. */
export class TaskOperations {
  constructor(public readonly storage: PersistenceObjectStorage) {}

  public async prepareOperation(request: TaskOperationRequest): Promise<TaskOperationRecord> {
    validateRequest(request);
    if (request.effectClass === ToolEffectClass.STAGED_COMMIT && request.approval === undefined) {
      throw new TaskOperationError("TASK_OPERATION_APPROVAL_REQUIRED", "Staged commits require an explicit approval scope.", 403);
    }
    const taskId = request.fence.taskId;
    const identity = {
      toolName: request.toolName, effectClass: request.effectClass, args: request.args,
      compensation: request.compensation ?? null, approvalScope: request.approval?.scope ?? null, race: request.race ?? null,
    };
    const [identityDigest, argsDigest, compensationDigest, approvalDigest] = await Promise.all([
      digestJson(identity), digestJson(request.args),
      request.compensation ? digestJson(request.compensation.args) : Promise.resolve(null),
      request.approval ? digestJson(request.approval.scope) : Promise.resolve(null),
    ]);
    const argumentDigest = identityDigest.sha256;
    const existing = this.readOperation(request.operationId);
    if (existing) {
      assertSameRequest(existing, request, taskId, argumentDigest);
      await confirmCellWrite(this.storage);
      return this.getOperation(request.operationId)!;
    }
    const status = request.approval ? "APPROVAL_REQUIRED" : "READY";
    const command = this.command(taskId, request.tenantId, `task-operation:${request.operationId}:intent`, "task.external_operation.prepare", identity);
    const applied = await executeCellOperation(this.storage, command, (sql) => {
      const task = assertAuthorizedTask(sql, taskId, request.tenantId, request.fence, true);
      const concurrent = readOperation(sql, request.operationId);
      if (concurrent) {
        assertSameRequest(concurrent, request, taskId, argumentDigest);
        return concurrent.operation_id;
      }
      const now = Date.now();
      sql.exec(`INSERT INTO task_operations (
        operation_id, task_id, tenant_id, turn_seq, tool_name, effect_class, args_json, argument_digest, status,
        compensation_tool, compensation_args_json, approval_scope_json, race_id, race_branch_id, result_json, last_error, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      request.operationId, taskId, request.tenantId, request.fence.turnSeq, request.toolName, request.effectClass,
      argsDigest.canonicalJson, argumentDigest, status, request.compensation?.toolName ?? null,
      compensationDigest?.canonicalJson ?? null,
      approvalDigest?.canonicalJson ?? null, request.race?.raceId ?? null,
      request.race?.branchId ?? null, null, null, now, now);
      if (request.compensation) this.insertCompensation(sql, task.task_id, request, compensationDigest!.canonicalJson);
      return request.operationId;
    });
    return this.getOperation(applied.result)!;
  }

  public getOperation(operationId: string): TaskOperationRecord | null {
    const row = this.readOperation(operationId);
    if (!row) return null;
    const attempts = this.storage.sql.exec<AttemptRow>(
      "SELECT * FROM task_operation_attempts WHERE operation_id = ? ORDER BY attempt_number", operationId,
    ).toArray().map((attempt) => mapAttempt(attempt, row.task_id));
    const approval = this.storage.sql.exec<ApprovalRow>(
      "SELECT * FROM task_operation_approvals WHERE operation_id = ? ORDER BY created_at DESC LIMIT 1", operationId,
    ).toArray()[0];
    const raceDecision = row.race_id ? this.storage.sql.exec<{ winner_branch_id: string }>(
      "SELECT winner_branch_id FROM task_race_winner_receipts WHERE race_id = ?", row.race_id,
    ).toArray()[0] : undefined;
    const compensation = this.storage.sql.exec<CompensationRow>(
      "SELECT * FROM task_compensations WHERE operation_id = ?", operationId,
    ).toArray()[0];
    return {
      operationId: row.operation_id, taskId: row.task_id, tenantId: row.tenant_id, turnSeq: Number(row.turn_seq),
      toolName: row.tool_name, effectClass: row.effect_class, args: JSON.parse(row.args_json),
      argumentDigest: row.argument_digest, idempotencyKey: row.operation_id, status: row.status, attempts,
      ...(compensation ? { compensation: {
        toolName: compensation.tool_name, args: JSON.parse(compensation.args_json), status: compensation.status,
        attempts: Number(compensation.attempts), ...(compensation.last_error === null ? {} : { lastError: compensation.last_error }),
      } } : {}),
      ...(approval ? { approval: {
        approvalId: approval.approval_id, approvedBy: approval.approved_by, scope: JSON.parse(approval.scope_json), expiresAt: Number(approval.expires_at),
      } } : {}),
      ...(row.race_id ? { race: {
        raceId: row.race_id, branchId: row.race_branch_id!, ...(raceDecision ? { selectedWinner: raceDecision.winner_branch_id } : {}),
      } } : {}),
      ...(row.result_json === null ? {} : { result: JSON.parse(row.result_json) }),
      ...(row.last_error === null ? {} : { lastError: row.last_error }),
    };
  }

  public listOperations(taskId: string): TaskOperationRecord[] {
    return this.storage.sql.exec<OperationRow>(
      "SELECT * FROM task_operations WHERE task_id = ? ORDER BY turn_seq, created_at", taskId,
    ).toArray().map((row) => this.getOperation(row.operation_id)!).filter(Boolean);
  }

  public async approveOperation(input: {
    approvalId: string; operationId: string; argumentDigest: string; scope: unknown; approvedBy: string; expiresAt: number;
  }): Promise<TaskOperationRecord> {
    for (const [key, value] of Object.entries({ approvalId: input.approvalId, operationId: input.operationId, approvedBy: input.approvedBy })) validateText(value, key);
    if (!Number.isSafeInteger(input.expiresAt) || input.expiresAt <= Date.now()) {
      throw new TaskOperationError("TASK_OPERATION_APPROVAL_EXPIRED", "Approval expiry must be in the future.", 403);
    }
    const operation = this.readOperation(input.operationId);
    if (!operation) throw new TaskOperationError("TASK_OPERATION_NOT_FOUND", `Operation ${input.operationId} was not found.`, 404);
    const digestedScope = await digestJson(input.scope);
    if (operation.approval_scope_json !== digestedScope.canonicalJson) {
      throw new TaskOperationError("TASK_OPERATION_APPROVAL_SCOPE_MISMATCH", "Approval scope does not match the staged operation.", 403);
    }
    if (input.argumentDigest !== operation.argument_digest) {
      throw new TaskOperationError("TASK_OPERATION_APPROVAL_DIGEST_MISMATCH", "Approval digest does not match the staged operation.", 403);
    }
    const prior = this.storage.sql.exec<ApprovalRow>("SELECT * FROM task_operation_approvals WHERE approval_id = ?", input.approvalId).toArray()[0];
    if (prior) {
      if (prior.operation_id !== input.operationId || prior.argument_digest !== input.argumentDigest || prior.scope_digest !== digestedScope.sha256 ||
          prior.approved_by !== input.approvedBy || Number(prior.expires_at) !== input.expiresAt) {
        throw new TaskOperationError("TASK_OPERATION_APPROVAL_CONFLICT", `Approval ${input.approvalId} is already bound to different content.`, 403);
      }
      await confirmCellWrite(this.storage);
      return this.getOperation(input.operationId)!;
    }
    if (operation.status !== "APPROVAL_REQUIRED") throw new TaskOperationError("TASK_OPERATION_APPROVAL_STATE", "Operation is not waiting for approval.", 409);
    const command = this.command(operation.task_id, operation.tenant_id, `task-operation:${input.operationId}:approval:${input.approvalId}`, "task.external_operation.approve", input);
    await executeCellOperation(this.storage, command, (sql) => {
      const fresh = readOperation(sql, input.operationId);
      if (!fresh || fresh.argument_digest !== input.argumentDigest || fresh.approval_scope_json !== digestedScope.canonicalJson) {
        throw new TaskOperationError("TASK_OPERATION_APPROVAL_BINDING_CHANGED", "The operation changed while approval was recorded.", 409);
      }
      const alreadyApproved = sql.exec<ApprovalRow>("SELECT * FROM task_operation_approvals WHERE operation_id = ?", input.operationId).toArray()[0];
      if (alreadyApproved) {
        if (alreadyApproved.approval_id === input.approvalId && alreadyApproved.argument_digest === input.argumentDigest &&
            alreadyApproved.scope_digest === digestedScope.sha256 && alreadyApproved.approved_by === input.approvedBy &&
            Number(alreadyApproved.expires_at) === input.expiresAt) return input.operationId;
        throw new TaskOperationError("TASK_OPERATION_APPROVAL_CONFLICT", `Operation ${input.operationId} already has a different approval.`, 403);
      }
      if (fresh.status !== "APPROVAL_REQUIRED") throw new TaskOperationError("TASK_OPERATION_APPROVAL_STATE", `Operation is ${fresh.status}, not awaiting approval.`);
      const now = Date.now();
      sql.exec(`INSERT INTO task_operation_approvals (
        approval_id, operation_id, argument_digest, scope_digest, scope_json, approved_by, expires_at, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, input.approvalId, input.operationId, input.argumentDigest,
      digestedScope.sha256, digestedScope.canonicalJson, input.approvedBy, input.expiresAt, now);
      sql.exec("UPDATE task_operations SET status = ?, updated_at = ? WHERE operation_id = ?", "READY", now, input.operationId);
      return input.operationId;
    });
    return this.getOperation(input.operationId)!;
  }

  /** Consume a committed DagCell winner event. Call only from the trusted TaskCell inbox adapter. */
  public async recordRaceWinner(input: DagCellRaceWinnerEvent): Promise<string> {
    validateText(input.eventId, "eventId");
    validateText(input.raceId, "raceId");
    validateText(input.decisionId, "decisionId");
    validateText(input.winnerBranchId, "winnerBranchId");
    validateText(input.taskId, "taskId");
    validateText(input.tenantId, "tenantId");
    const payload = { taskId: input.taskId, raceId: input.raceId, decisionId: input.decisionId, winnerBranchId: input.winnerBranchId };
    const payloadDigest = (await digestJson(payload)).sha256;
    const event: CellInboxEvent = {
      eventId: input.eventId,
      eventType: "dag.race.winner_selected",
      tenantId: input.tenantId,
      aggregateType: "dag-race",
      aggregateId: input.raceId,
      aggregateVersion: input.aggregateVersion,
      payloadDigest,
      payload,
    };
    const existing = this.storage.sql.exec<{ event_id: string; decision_id: string; winner_branch_id: string }>(
      "SELECT * FROM task_race_winner_receipts WHERE race_id = ?", input.raceId,
    ).toArray()[0];
    if (existing) {
      if (existing.winner_branch_id !== input.winnerBranchId || existing.decision_id !== input.decisionId || existing.event_id !== input.eventId) {
        throw new TaskOperationError("TASK_RACE_WINNER_CONFLICT", `Race ${input.raceId} already selected another branch.`);
      }
      await confirmCellWrite(this.storage);
      return existing.winner_branch_id;
    }
    await consumeCellInboxEvent(this.storage, event, (sql) => {
      const task = sql.exec<TaskExecutionRecord>("SELECT * FROM task_execution WHERE task_id = ?", input.taskId).toArray()[0];
      if (!task || task.tenant_id !== input.tenantId) throw new TaskOperationError("TASK_RACE_EVENT_OWNER_MISMATCH", "DagCell winner event task or tenant does not match this Cell.");
      const current = sql.exec<{ event_id: string; decision_id: string; winner_branch_id: string }>(
        "SELECT * FROM task_race_winner_receipts WHERE race_id = ?", input.raceId,
      ).toArray()[0];
      if (current && (current.winner_branch_id !== input.winnerBranchId || current.decision_id !== input.decisionId || current.event_id !== input.eventId)) {
        throw new TaskOperationError("TASK_RACE_WINNER_CONFLICT", `Race ${input.raceId} already has a different DagCell winner event.`);
      }
      if (!current) sql.exec("INSERT INTO task_race_winner_receipts (race_id, event_id, decision_id, winner_branch_id, created_at) VALUES (?, ?, ?, ?, ?)", input.raceId, input.eventId, input.decisionId, input.winnerBranchId, Date.now());
    });
    return input.winnerBranchId;
  }

  /** Writes an attempt before invoking the provider and resolves uncertain results only through provider guarantees. */
  public async executeOperation<T>(request: TaskOperationRequest, provider: TaskOperationProvider<T>): Promise<{
    status: TaskOperationStatus | "RACE_BLOCKED"; operation: TaskOperationRecord; result?: T;
  }> {
    let operation = await this.prepareOperation(request);
    if (["SUCCEEDED", "FAILED", "APPROVAL_REQUIRED"].includes(operation.status)) {
      await confirmCellWrite(this.storage);
      return { status: operation.status, operation, ...(operation.result === undefined ? {} : { result: operation.result as T }) };
    }
    if (operation.race && operation.effectClass !== ToolEffectClass.PURE_READ && operation.race.selectedWinner !== operation.race.branchId) {
      return { status: "RACE_BLOCKED", operation };
    }
    if (operation.status === "RUNNING" || operation.status === "OUTCOME_UNKNOWN") {
      const previousAttempt = operation.attempts[operation.attempts.length - 1];
      if (!previousAttempt) throw new TaskOperationError("TASK_OPERATION_ATTEMPT_MISSING", `Operation ${operation.operationId} has no recorded attempt.`);
      let mayProceed = provider.supportsIdempotency || operation.effectClass === ToolEffectClass.PURE_READ;
      if (provider.lookup) {
        let lookup: Awaited<ReturnType<NonNullable<TaskOperationProvider<T>["lookup"]>>>;
        try {
          lookup = await provider.lookup({ operationId: operation.operationId, idempotencyKey: operation.idempotencyKey });
        } catch {
          lookup = { status: "UNKNOWN" };
        }
        if (lookup.status === "FOUND") {
          operation = previousAttempt.status === "OUTCOME_UNKNOWN"
            ? await this.resolveUnknownOutcome({ operationId: operation.operationId, attemptId: previousAttempt.attemptId, fence: request.fence, result: lookup.result })
            : await this.recordOperationOutcome({ operationId: operation.operationId, attemptId: previousAttempt.attemptId, fence: request.fence, outcome: "SUCCEEDED", result: lookup.result });
          return { status: operation.status, operation, result: lookup.result };
        }
        if (lookup.status === "NOT_FOUND") mayProceed = true;
        if (lookup.status === "UNKNOWN") mayProceed = provider.supportsIdempotency || operation.effectClass === ToolEffectClass.PURE_READ;
      }
      if (!mayProceed) {
        if (previousAttempt.status === "RUNNING") {
          operation = await this.markUnknown(operation.operationId, previousAttempt.attemptId, request.fence, "Provider cannot safely resolve or repeat the previous attempt.");
        }
        return { status: operation.status, operation };
      }
    }

    const attemptId = `${operation.operationId}:attempt:${operation.attempts.length + 1}`;
    const started = await this.openAttempt({
      operationId: operation.operationId, attemptId, fence: request.fence,
      allowRecovery: operation.status === "RUNNING" || operation.status === "OUTCOME_UNKNOWN",
    });
    if (!started.created) {
      const current = this.getOperation(operation.operationId)!;
      return { status: current.status, operation: current };
    }
    try {
      const result = await provider.execute({ operationId: operation.operationId, idempotencyKey: operation.idempotencyKey,
        attemptId: started.attempt.attemptId, toolName: operation.toolName, args: operation.args });
      operation = await this.recordOperationOutcome({ operationId: operation.operationId, attemptId: started.attempt.attemptId, fence: request.fence, outcome: "SUCCEEDED", result });
      return { status: operation.status, operation, result };
    } catch (error) {
      operation = await this.markUnknown(operation.operationId, started.attempt.attemptId, request.fence, errorMessage(error));
      return { status: operation.status, operation };
    }
  }

  public async beginAttempt(input: { operationId: string; attemptId: string; fence: FenceToken; allowRecovery?: boolean }): Promise<TaskOperationAttemptRecord> {
    return (await this.openAttempt(input)).attempt;
  }

  private async openAttempt(input: { operationId: string; attemptId: string; fence: FenceToken; allowRecovery?: boolean }): Promise<{ attempt: TaskOperationAttemptRecord; created: boolean }> {
    validateText(input.attemptId, "attemptId");
    const operation = this.readOperation(input.operationId);
    if (!operation) throw new TaskOperationError("TASK_OPERATION_NOT_FOUND", `Operation ${input.operationId} was not found.`, 404);
    const duplicate = this.storage.sql.exec<AttemptRow>("SELECT * FROM task_operation_attempts WHERE attempt_id = ?", input.attemptId).toArray()[0];
    if (duplicate) {
      if (duplicate.operation_id !== input.operationId) throw new TaskOperationError("TASK_OPERATION_ATTEMPT_CONFLICT", `Attempt ${input.attemptId} belongs to another operation.`);
      await confirmCellWrite(this.storage);
      return { attempt: mapAttempt(duplicate, operation.task_id), created: false };
    }
    const number = this.countAttempts(input.operationId) + 1;
    const command = this.command(operation.task_id, operation.tenant_id,
      `task-operation:${input.operationId}:attempt:${input.attemptId}`, "task.external_operation.begin_attempt",
      { operationId: input.operationId, attemptId: input.attemptId, attemptNumber: number });
    const committed = await executeCellOperation(this.storage, command, (sql) => {
      const task = assertAuthorizedTask(sql, operation.task_id, operation.tenant_id, input.fence, true);
      const current = readOperation(sql, input.operationId)!;
      const recoverable = input.allowRecovery && ["RUNNING", "OUTCOME_UNKNOWN"].includes(current.status);
      if (current.status !== "READY" && !recoverable) throw new TaskOperationError("TASK_OPERATION_NOT_READY", `Operation ${input.operationId} is ${current.status}.`);
      if (current.approval_scope_json !== null) this.assertLiveApproval(sql, current);
      this.assertRaceWinner(sql, current);
      const now = Date.now();
      sql.exec(`INSERT INTO task_operation_attempts (
        attempt_id, operation_id, attempt_number, epoch, turn_seq, status, result_json, error_text, outcome_source, started_at, completed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, input.attemptId, input.operationId, number,
      input.fence.epoch, input.fence.turnSeq, "RUNNING", null, null, null, now, null);
      sql.exec("UPDATE task_operations SET status = ?, last_error = ?, updated_at = ? WHERE operation_id = ?", "RUNNING", null, now, input.operationId);
      return task.task_id;
    });
    const inserted = this.storage.sql.exec<AttemptRow>("SELECT * FROM task_operation_attempts WHERE attempt_id = ?", input.attemptId).toArray()[0];
    return { attempt: mapAttempt(inserted, operation.task_id), created: committed.outcome === "APPLIED" };
  }

  public async recordOperationOutcome(input: {
    operationId: string; attemptId: string; fence: FenceToken; outcome: "SUCCEEDED" | "FAILED"; result?: unknown; error?: string;
  }): Promise<TaskOperationRecord> {
    return this.finishAttempt({ ...input, allowUnknownResolution: true, outcomeSource: "EXECUTOR" });
  }

  /** Resolve an unknown attempt only after the provider's authoritative lookup returns FOUND. */
  public async resolveUnknownOutcome(input: {
    operationId: string; attemptId: string; fence: FenceToken; result: unknown;
  }): Promise<TaskOperationRecord> {
    return this.finishAttempt({ ...input, outcome: "SUCCEEDED", outcomeSource: "PROVIDER_LOOKUP", allowUnknownResolution: true });
  }

  private async markUnknown(operationId: string, attemptId: string, fence: FenceToken, error: string): Promise<TaskOperationRecord> {
    return this.finishAttempt({ operationId, attemptId, fence, outcome: "OUTCOME_UNKNOWN", error });
  }

  private async finishAttempt(input: {
    operationId: string; attemptId: string; fence: FenceToken; outcome: TaskOperationAttemptStatus; result?: unknown; error?: string;
    outcomeSource?: "EXECUTOR" | "PROVIDER_LOOKUP"; allowUnknownResolution?: boolean;
  }): Promise<TaskOperationRecord> {
    const operation = this.readOperation(input.operationId);
    if (!operation) throw new TaskOperationError("TASK_OPERATION_NOT_FOUND", `Operation ${input.operationId} was not found.`, 404);
    const prior = this.storage.sql.exec<AttemptRow>("SELECT * FROM task_operation_attempts WHERE attempt_id = ?", input.attemptId).toArray()[0];
    if (!prior || prior.operation_id !== input.operationId) throw new TaskOperationError("TASK_OPERATION_ATTEMPT_NOT_FOUND", `Attempt ${input.attemptId} was not found.`, 404);
    const resultJson = input.result === undefined ? null : json(input.result);
    if (prior.status !== "RUNNING" && !(input.allowUnknownResolution && prior.status === "OUTCOME_UNKNOWN")) {
      if (prior.status !== input.outcome || prior.result_json !== resultJson || prior.error_text !== (input.error ?? null)) {
        throw new TaskOperationError("TASK_OPERATION_RESULT_CONFLICT", `Attempt ${input.attemptId} already has another outcome.`);
      }
      await confirmCellWrite(this.storage);
      return this.getOperation(input.operationId)!;
    }
    const command = this.command(operation.task_id, operation.tenant_id,
      `task-operation:${input.operationId}:attempt:${input.attemptId}:outcome:${input.outcomeSource ?? "EXECUTOR"}:${input.outcome}`, "task.external_operation.record_outcome",
      { operationId: input.operationId, attemptId: input.attemptId, outcome: input.outcome, result: input.result ?? null, error: input.error ?? null, outcomeSource: input.outcomeSource ?? "EXECUTOR" });
    await executeCellOperation(this.storage, command, (sql) => {
      assertAuthorizedTask(sql, operation.task_id, operation.tenant_id, input.fence, false);
      const fresh = sql.exec<AttemptRow>("SELECT * FROM task_operation_attempts WHERE attempt_id = ?", input.attemptId).toArray()[0];
      if (!fresh || (fresh.status !== "RUNNING" && !(input.allowUnknownResolution && fresh.status === "OUTCOME_UNKNOWN"))) {
        throw new TaskOperationError("TASK_OPERATION_ATTEMPT_STATE", `Attempt ${input.attemptId} is not in a resolvable state.`);
      }
      const now = Date.now();
      sql.exec("UPDATE task_operation_attempts SET status = ?, result_json = ?, error_text = ?, outcome_source = ?, completed_at = ? WHERE attempt_id = ?",
        input.outcome, resultJson, input.error ?? null, input.outcomeSource ?? "EXECUTOR", now, input.attemptId);
      sql.exec("UPDATE task_operations SET status = ?, result_json = ?, last_error = ?, updated_at = ? WHERE operation_id = ?",
        input.outcome, resultJson, input.error ?? null, now, input.operationId);
      return input.operationId;
    });
    return this.getOperation(input.operationId)!;
  }

  public listCompensations(taskId: string, afterTurnExclusive: number): Array<{
    operationId: string; turnSeq: number; orderSeq: number; toolName: string; args: unknown;
    status: TaskCompensationStatus; attempts: number; lastError?: string;
  }> {
    return this.storage.sql.exec<CompensationRow>(
      "SELECT * FROM task_compensations WHERE task_id = ? AND turn_seq > ? ORDER BY turn_seq DESC, order_seq DESC",
      taskId, afterTurnExclusive,
    ).toArray().map((row) => ({
      operationId: row.operation_id, turnSeq: Number(row.turn_seq), orderSeq: Number(row.order_seq),
      toolName: row.tool_name, args: JSON.parse(row.args_json), status: row.status, attempts: Number(row.attempts),
      ...(row.last_error === null ? {} : { lastError: row.last_error }),
    }));
  }

  public async beginCompensation(input: { operationId: string; attemptId: string; fence: FenceToken }): Promise<{ status: TaskCompensationStatus; started: boolean }> {
    validateText(input.attemptId, "attemptId");
    const row = this.storage.sql.exec<CompensationRow>("SELECT * FROM task_compensations WHERE operation_id = ?", input.operationId).toArray()[0];
    if (!row) throw new TaskOperationError("TASK_COMPENSATION_NOT_FOUND", `Compensation for ${input.operationId} was not found.`, 404);
    const previous = this.storage.sql.exec<{ operation_id: string }>("SELECT operation_id FROM task_compensation_attempts WHERE attempt_id = ?", input.attemptId).toArray()[0];
    if (previous) {
      if (previous.operation_id !== input.operationId) throw new TaskOperationError("TASK_COMPENSATION_ATTEMPT_CONFLICT", `Attempt ${input.attemptId} belongs to another compensation.`);
      await confirmCellWrite(this.storage);
      return { status: row.status, started: false };
    }
    const tenantId = this.readTaskTenant(row.task_id);
    const command = this.command(row.task_id, tenantId, `task-compensation:${input.operationId}:attempt:${input.attemptId}`,
      "task.compensation.begin", { operationId: input.operationId, attemptId: input.attemptId });
    const committed = await executeCellOperation(this.storage, command, (sql) => {
      const task = assertAuthorizedTask(sql, row.task_id, tenantId, input.fence, true);
      const current = sql.exec<CompensationRow>("SELECT * FROM task_compensations WHERE operation_id = ?", input.operationId).toArray()[0];
      if (!current || !["PENDING", "COMPENSATION_FAILED"].includes(current.status)) {
        throw new TaskOperationError("TASK_COMPENSATION_NOT_READY", `Compensation is ${current?.status ?? "missing"}.`);
      }
      const now = Date.now();
      sql.exec(`INSERT INTO task_compensation_attempts (
        attempt_id, operation_id, epoch, turn_seq, status, error_text, started_at, completed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, input.attemptId, input.operationId, input.fence.epoch,
      input.fence.turnSeq, "RUNNING", null, now, null);
      sql.exec("UPDATE task_compensations SET status = ?, attempts = ?, last_error = ?, updated_at = ? WHERE operation_id = ?",
        "RUNNING", Number(current.attempts) + 1, null, now, input.operationId);
      return task.task_id;
    });
    return { status: "RUNNING", started: committed.outcome === "APPLIED" };
  }

  public async recordCompensationOutcome(input: {
    operationId: string; attemptId: string; fence: FenceToken;
    outcome: "COMPENSATED" | "COMPENSATION_FAILED" | "OUTCOME_UNKNOWN"; error?: string;
  }): Promise<TaskCompensationStatus> {
    const row = this.storage.sql.exec<CompensationRow>("SELECT * FROM task_compensations WHERE operation_id = ?", input.operationId).toArray()[0];
    if (!row) throw new TaskOperationError("TASK_COMPENSATION_NOT_FOUND", `Compensation for ${input.operationId} was not found.`, 404);
    const attempt = this.storage.sql.exec<{ operation_id: string; status: string; error_text: string | null }>(
      "SELECT * FROM task_compensation_attempts WHERE attempt_id = ?", input.attemptId,
    ).toArray()[0];
    if (!attempt || attempt.operation_id !== input.operationId) throw new TaskOperationError("TASK_COMPENSATION_ATTEMPT_NOT_FOUND", `Attempt ${input.attemptId} was not found.`, 404);
    if (attempt.status !== "RUNNING") {
      if (attempt.status !== input.outcome || attempt.error_text !== (input.error ?? null)) {
        throw new TaskOperationError("TASK_COMPENSATION_RESULT_CONFLICT", `Attempt ${input.attemptId} already has another outcome.`);
      }
      await confirmCellWrite(this.storage);
      return row.status;
    }
    const tenantId = this.readTaskTenant(row.task_id);
    const command = this.command(row.task_id, tenantId, `task-compensation:${input.operationId}:attempt:${input.attemptId}:outcome`,
      "task.compensation.record_outcome", { operationId: input.operationId, attemptId: input.attemptId, outcome: input.outcome, error: input.error ?? null });
    await executeCellOperation(this.storage, command, (sql) => {
      assertAuthorizedTask(sql, row.task_id, tenantId, input.fence, false);
      const now = Date.now();
      sql.exec("UPDATE task_compensation_attempts SET status = ?, error_text = ?, completed_at = ? WHERE attempt_id = ?",
        input.outcome, input.error ?? null, now, input.attemptId);
      sql.exec("UPDATE task_compensations SET status = ?, last_error = ?, updated_at = ? WHERE operation_id = ?",
        input.outcome, input.error ?? null, now, input.operationId);
      return input.operationId;
    });
    return input.outcome;
  }

  private insertCompensation(sql: PersistenceSqlStorage, taskId: string, request: TaskOperationRequest, argsJson: string): void {
    const latest = sql.exec<{ order_seq: number }>(
      "SELECT order_seq FROM task_compensations WHERE task_id = ? ORDER BY order_seq DESC LIMIT 1", taskId,
    ).toArray()[0];
    const now = Date.now();
    sql.exec(`INSERT INTO task_compensations (
      operation_id, task_id, turn_seq, order_seq, tool_name, args_json, status, attempts, last_error, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, request.operationId, taskId, request.fence.turnSeq,
    latest ? Number(latest.order_seq) + 1 : 1, request.compensation!.toolName,
    argsJson, "PENDING", 0, null, now);
  }

  private readOperation(operationId: string): OperationRow | null {
    return this.storage.sql.exec<OperationRow>("SELECT * FROM task_operations WHERE operation_id = ?", operationId).toArray()[0] ?? null;
  }

  private countAttempts(operationId: string): number {
    return this.storage.sql.exec<AttemptRow>("SELECT * FROM task_operation_attempts WHERE operation_id = ?", operationId).toArray().length;
  }

  private readTaskTenant(taskId: string): string {
    const task = this.storage.sql.exec<TaskExecutionRecord>("SELECT * FROM task_execution WHERE task_id = ?", taskId).toArray()[0];
    if (!task) throw new TaskOperationError("TASK_NOT_INITIALIZED", `Task ${taskId} is not initialized.`, 404);
    return task.tenant_id;
  }

  private command(taskId: string, tenantId: string, operationId: string, operationType: string, payload: unknown): CellOperationCommand {
    const row = this.storage.sql.exec<{ version: number }>(
      "SELECT version FROM cell_aggregate_versions WHERE aggregate_type = ? AND aggregate_id = ?",
      "task-external-operations", taskId,
    ).toArray()[0];
    return { operationId, operationType, resourceId: taskId, tenantId, payload,
      aggregateType: "task-external-operations", aggregateId: taskId, expectedVersion: row ? Number(row.version) : 0 };
  }

  private assertLiveApproval(sql: PersistenceSqlStorage, operation: OperationRow): void {
    const approval = sql.exec<ApprovalRow>(
      "SELECT * FROM task_operation_approvals WHERE operation_id = ? ORDER BY created_at DESC LIMIT 1", operation.operation_id,
    ).toArray()[0];
    if (!approval || approval.argument_digest !== operation.argument_digest || approval.scope_json !== operation.approval_scope_json || Number(approval.expires_at) <= Date.now()) {
      throw new TaskOperationError("TASK_OPERATION_APPROVAL_REQUIRED", `Operation ${operation.operation_id} has no live matching approval.`, 403);
    }
  }

  private assertRaceWinner(sql: PersistenceSqlStorage, operation: OperationRow): void {
    if (!operation.race_id || operation.effect_class === ToolEffectClass.PURE_READ) return;
    const decision = sql.exec<{ winner_branch_id: string }>("SELECT winner_branch_id FROM task_race_winner_receipts WHERE race_id = ?", operation.race_id).toArray()[0];
    if (!decision) throw new TaskOperationError("TASK_OPERATION_RACE_NOT_SELECTED", `Race ${operation.race_id} has no selected winner.`);
    if (decision.winner_branch_id !== operation.race_branch_id) throw new TaskOperationError("TASK_OPERATION_RACE_LOST", `Branch ${operation.race_branch_id} lost race ${operation.race_id}.`);
  }
}

function assertAuthorizedTask(sql: PersistenceSqlStorage, taskId: string, tenantId: string, fence: FenceToken, requireNextTurn: boolean): TaskExecutionRecord {
  if (fence.taskId !== taskId) throw new TaskOperationError("TASK_OPERATION_TASK_MISMATCH", "Fence belongs to another task.");
  const task = sql.exec<TaskExecutionRecord>("SELECT * FROM task_execution WHERE task_id = ?", taskId).toArray()[0];
  if (!task) throw new TaskOperationError("TASK_NOT_INITIALIZED", `Task ${taskId} is not initialized.`, 404);
  if (task.tenant_id !== tenantId) throw new TaskOperationError("TASK_OPERATION_TENANT_MISMATCH", "Operation tenant does not own this task.", 403);
  try {
    assertBusinessEpoch(task, fence.epoch);
  } catch (error) {
    const code = error instanceof Error && "code" in error ? String(error.code) : "TASK_OPERATION_FENCE_REJECTED";
    throw new TaskOperationError(code, error instanceof Error ? error.message : String(error));
  }
  if (requireNextTurn && Number(task.last_turn_seq) + 1 !== fence.turnSeq) {
    throw new TaskOperationError("TASK_OPERATION_TURN_FENCE_REJECTED", `Task expects turn ${Number(task.last_turn_seq) + 1}; received ${fence.turnSeq}.`);
  }
  return task;
}

function validateRequest(request: TaskOperationRequest): void {
  validateText(request.operationId, "operationId");
  validateText(request.tenantId, "tenantId");
  validateText(request.fence.taskId, "taskId");
  validateText(request.toolName, "toolName");
  if (!Number.isSafeInteger(request.fence.epoch) || request.fence.epoch < 0 || !Number.isSafeInteger(request.fence.turnSeq) || request.fence.turnSeq < 1) {
    throw new TypeError("Operation fence requires a non-negative epoch and positive turn sequence.");
  }
  if (!Object.values(ToolEffectClass).includes(request.effectClass)) throw new TypeError("Unknown external operation effect class.");
  if (request.compensation) validateText(request.compensation.toolName, "compensation.toolName");
  if (request.race) { validateText(request.race.raceId, "race.raceId"); validateText(request.race.branchId, "race.branchId"); }
}

function assertSameRequest(row: OperationRow, request: TaskOperationRequest, taskId: string, argumentDigest: string): void {
  if (row.task_id !== taskId || row.tenant_id !== request.tenantId || Number(row.turn_seq) !== request.fence.turnSeq ||
      row.tool_name !== request.toolName || row.effect_class !== request.effectClass || row.argument_digest !== argumentDigest) {
    throw new TaskOperationError("TASK_OPERATION_CONFLICT", `Operation ${request.operationId} was reused with different content.`);
  }
}

function readOperation(sql: PersistenceSqlStorage, operationId: string): OperationRow | null {
  return sql.exec<OperationRow>("SELECT * FROM task_operations WHERE operation_id = ?", operationId).toArray()[0] ?? null;
}

function mapAttempt(row: AttemptRow, taskId: string): TaskOperationAttemptRecord {
  return { attemptId: row.attempt_id, operationId: row.operation_id, attemptNumber: Number(row.attempt_number),
    fence: new FenceToken(Number(row.epoch), Number(row.turn_seq), taskId), status: row.status,
    ...(row.outcome_source === null ? {} : { outcomeSource: row.outcome_source }),
    ...(row.result_json === null ? {} : { result: JSON.parse(row.result_json) }),
    ...(row.error_text === null ? {} : { error: row.error_text }) };
}

function validateText(value: string, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0) throw new TypeError(`${label} must be non-empty.`);
}

function json(value: unknown): string {
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new TypeError("Durable values must be JSON serializable.");
  return encoded;
}

function errorMessage(error: unknown): string { return error instanceof Error ? error.message : String(error); }
