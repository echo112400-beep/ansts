import {
  CellOperationCommand,
  CellOutboxDraft,
  PersistenceObjectStorage,
  PersistenceSqlStorage,
} from "../contracts";
import { CellOperationResult, executeCellOperation } from "../operations";
import { confirmCellWrite } from "../transactions";

export type TaskExecutionStatus =
  | "QUEUED"
  | "RESERVED"
  | "STARTING"
  | "RUNNING"
  | "SUSPECT"
  | "SUSPENDED"
  | "SUCCEEDED"
  | "FAILED";

export type TaskDispatchKind = "ordinary" | "recovery" | "dag";
export type TaskDispatchStage = "PREPARING" | "RESERVED" | "AUTHORIZED" | "STARTED";

export interface TaskExecutionRecord {
  task_id: string;
  tenant_id: string;
  region: string;
  resource_pool: string;
  lifecycle_state: TaskExecutionStatus;
  business_epoch: number;
  execution_version: number;
  current_dispatch_id: string | null;
  pending_dispatch_id: string | null;
  current_node_id: string | null;
  node_incarnation: string | null;
  last_turn_seq: number;
  task_spec_json: string;
  execution_policy_json: string;
  progress_json: string;
  heartbeat_at: number | null;
  suspected_at: number | null;
  suspect_reason: string | null;
  updated_at: number;
}

export interface TaskDispatchRecord {
  dispatch_id: string;
  task_id: string;
  kind: TaskDispatchKind;
  stage: TaskDispatchStage;
  node_id: string | null;
  node_incarnation: string | null;
  reservation_id: string | null;
  reservation_token: string | null;
  authorization_token: string | null;
  business_epoch: number | null;
  start_receipt: string | null;
  request_json: string;
  updated_at: number;
}

export interface TaskDispatchRequest {
  dag_id?: string;
  voucher_id?: string;
  delegated_tokens?: number;
  delegated_tps?: number;
  dispatch_id: string;
  task_id: string;
  tenant_id: string;
  kind: TaskDispatchKind;
  node_id: string;
  node_incarnation: string;
  region?: string;
  resource_pool?: string;
  reservation_id?: string;
  reservation_token?: string;
  task_spec?: unknown;
  execution_policy?: unknown;
}

export interface TaskDispatchAuthorization {
  dispatch_id: string;
  task_id: string;
  node_id: string;
  node_incarnation: string;
  reservation_id: string;
  reservation_token: string;
  authorization_token: string;
  business_epoch: number;
  turn_seq: number;
}

export class TaskCoreError extends Error {
  constructor(public readonly code: string, message: string, public readonly status = 409) {
    super(message);
    this.name = "TaskCoreError";
  }
}

export const taskCoreMigration = {
  version: 2,
  description: "task execution, business epochs and dispatch authorization",
  up(sql: PersistenceSqlStorage): void {
    sql.exec(`
      CREATE TABLE IF NOT EXISTS task_execution (
        task_id TEXT PRIMARY KEY,
        tenant_id TEXT NOT NULL,
        region TEXT NOT NULL DEFAULT 'default',
        resource_pool TEXT NOT NULL DEFAULT 'default',
        lifecycle_state TEXT NOT NULL,
        business_epoch INTEGER NOT NULL,
        execution_version INTEGER NOT NULL,
        current_dispatch_id TEXT,
        pending_dispatch_id TEXT,
        current_node_id TEXT,
        node_incarnation TEXT,
        last_turn_seq INTEGER NOT NULL,
        task_spec_json TEXT NOT NULL,
        execution_policy_json TEXT NOT NULL,
        progress_json TEXT NOT NULL,
        heartbeat_at INTEGER,
        suspected_at INTEGER,
        suspect_reason TEXT,
        updated_at INTEGER NOT NULL
      )
    `);
    sql.exec(`
      CREATE TABLE IF NOT EXISTS task_dispatches (
        dispatch_id TEXT PRIMARY KEY,
        task_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        stage TEXT NOT NULL,
        node_id TEXT,
        node_incarnation TEXT,
        reservation_id TEXT,
        reservation_token TEXT,
        authorization_token TEXT,
        business_epoch INTEGER,
        start_receipt TEXT,
        request_json TEXT NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);
    sql.exec("CREATE INDEX IF NOT EXISTS task_dispatches_task_idx ON task_dispatches (task_id, updated_at)");
  },
};

export function readTaskExecution(sql: PersistenceSqlStorage, taskId: string): TaskExecutionRecord | null {
  return sql.exec<TaskExecutionRecord>("SELECT * FROM task_execution WHERE task_id = ?", taskId).toArray()[0] ?? null;
}

export function readTaskDispatch(sql: PersistenceSqlStorage, dispatchId: string): TaskDispatchRecord | null {
  return sql.exec<TaskDispatchRecord>("SELECT * FROM task_dispatches WHERE dispatch_id = ?", dispatchId).toArray()[0] ?? null;
}

export function assertBusinessEpoch(record: TaskExecutionRecord, epoch: number, turnSeq?: number): void {
  if (epoch !== Number(record.business_epoch)) {
    throw new TaskCoreError(
      "STALE_EXECUTION_EPOCH",
      `Execution epoch ${epoch} is stale; task ${record.task_id} is at business epoch ${record.business_epoch}.`,
    );
  }
  if (record.lifecycle_state !== "RUNNING") {
    throw new TaskCoreError("TASK_NOT_EXECUTING", `Task ${record.task_id} is ${record.lifecycle_state}.`);
  }
  if (turnSeq !== undefined && turnSeq !== Number(record.last_turn_seq) + 1) {
    throw new TaskCoreError(
      "TASK_TURN_SEQUENCE_CONFLICT",
      `Task ${record.task_id} expects turn ${Number(record.last_turn_seq) + 1}, received ${turnSeq}.`,
    );
  }
}

export interface TaskTransactionContext {
  sql: PersistenceSqlStorage;
  task: TaskExecutionRecord;
  assertExecutionEpoch(epoch: number, turnSeq?: number): void;
  saveProgress(progress: unknown): void;
}

/**
 * Shared atomic extension point for task-owned operation and Turn modules. Callbacks are sync,
 * execute in the TaskCell SQL transaction, and may not perform network or other Cell I/O.
 */
export async function runTaskTransaction<T>(params: {
  storage: PersistenceObjectStorage;
  command: CellOperationCommand;
  outboxEvents?: CellOutboxDraft[];
  apply(context: TaskTransactionContext): T;
}): Promise<CellOperationResult<T>> {
  return executeCellOperation(
    params.storage,
    params.command,
    (sql) => {
      const task = readTaskExecution(sql, params.command.aggregateId);
      if (!task) throw new TaskCoreError("TASK_NOT_INITIALIZED", `Task ${params.command.aggregateId} is not initialized.`, 404);
      const context: TaskTransactionContext = {
        sql,
        task,
        assertExecutionEpoch: (epoch, turnSeq) => assertBusinessEpoch(task, epoch, turnSeq),
        saveProgress(progress) {
          const json = JSON.stringify(progress);
          if (json === undefined) throw new TypeError("Task progress must be JSON serializable.");
          sql.exec(
            "UPDATE task_execution SET progress_json = ?, updated_at = ? WHERE task_id = ?",
            json,
            Date.now(),
            task.task_id,
          );
        },
      };
      const result = params.apply(context);
      if (result && (typeof result === "object" || typeof result === "function") && "then" in result) {
        throw new TypeError("Task transaction callbacks must be synchronous; perform external I/O after commit.");
      }
      sql.exec(
        "UPDATE task_execution SET execution_version = ?, updated_at = ? WHERE task_id = ?",
        Number(task.execution_version) + 1,
        Date.now(),
        task.task_id,
      );
      return result;
    },
    params.outboxEvents ?? [],
  );
}

function aggregateVersion(sql: PersistenceSqlStorage, taskId: string): number {
  const row = sql
    .exec<{ version: number }>(
      "SELECT version FROM cell_aggregate_versions WHERE aggregate_type = ? AND aggregate_id = ?",
      "task-execution",
      taskId,
    )
    .toArray()[0];
  return row ? Number(row.version) : 0;
}

export class TaskCore {
  constructor(public readonly storage: PersistenceObjectStorage) {}

  public get(taskId: string): TaskExecutionRecord | null {
    return readTaskExecution(this.storage.sql, taskId);
  }

  public getDispatch(dispatchId: string): TaskDispatchRecord | null {
    return readTaskDispatch(this.storage.sql, dispatchId);
  }

  public async prepareDispatch(request: TaskDispatchRequest): Promise<TaskDispatchRecord> {
    validateDispatchRequest(request);
    await this.ensureTask(request);
    const current = this.get(request.task_id)!;
    const existingDispatch = this.getDispatch(request.dispatch_id);
    if (existingDispatch) {
      assertDispatchRequest(existingDispatch, request);
      if (["AUTHORIZED", "STARTED"].includes(existingDispatch.stage)) {
        const execution = this.get(request.task_id);
        if (execution?.current_dispatch_id !== request.dispatch_id || Number(execution.business_epoch) !== Number(existingDispatch.business_epoch)) {
          throw new TaskCoreError("STALE_DISPATCH_AUTHORIZATION", `Dispatch ${request.dispatch_id} is no longer the current TaskCell authorization.`);
        }
      }
      return existingDispatch;
    }
    const allowed = current.lifecycle_state === "QUEUED" ||
      (request.kind === "recovery" && ["RUNNING", "SUSPECT", "SUSPENDED"].includes(current.lifecycle_state));
    if (!allowed) throw new TaskCoreError("TASK_NOT_DISPATCHABLE", `Task ${request.task_id} is ${current.lifecycle_state}.`);
    if (current.pending_dispatch_id && current.pending_dispatch_id !== request.dispatch_id) {
      throw new TaskCoreError("TASK_DISPATCH_IN_PROGRESS", `Task ${request.task_id} already has dispatch ${current.pending_dispatch_id} in progress.`);
    }

    const command = taskCommand(request.task_id, request.tenant_id, `task-dispatch:${request.dispatch_id}:preparing`, "task.dispatch.prepare", {
      ...request,
    }, current.execution_version);
    const result = await runTaskTransaction({
      storage: this.storage,
      command,
      apply: ({ sql }) => {
        const task = readTaskExecution(sql, request.task_id)!;
        const canPrepare = task.lifecycle_state === "QUEUED" ||
          (request.kind === "recovery" && ["RUNNING", "SUSPECT", "SUSPENDED"].includes(task.lifecycle_state));
        if (!canPrepare) throw new TaskCoreError("TASK_NOT_DISPATCHABLE", `Task ${request.task_id} is ${task.lifecycle_state}.`);
        sql.exec(
          `INSERT INTO task_dispatches (
            dispatch_id, task_id, kind, stage, node_id, node_incarnation, reservation_id,
            reservation_token, authorization_token, business_epoch, start_receipt, request_json, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          request.dispatch_id,
          request.task_id,
          request.kind,
          "PREPARING",
          request.node_id,
          request.node_incarnation,
          null,
          null,
          null,
          null,
          null,
          JSON.stringify(request),
          Date.now(),
        );
        sql.exec("UPDATE task_execution SET pending_dispatch_id = ?, updated_at = ? WHERE task_id = ?", request.dispatch_id, Date.now(), request.task_id);
        return readTaskDispatch(sql, request.dispatch_id)!;
      },
    });
    return result.result;
  }

  public async recordReservation(params: TaskDispatchRequest): Promise<TaskDispatchRecord> {
    validateText(params.reservation_id, "reservation_id");
    validateText(params.reservation_token, "reservation_token");
    const dispatch = this.getDispatch(params.dispatch_id);
    if (!dispatch) throw new TaskCoreError("TASK_DISPATCH_NOT_FOUND", `Dispatch ${params.dispatch_id} is unknown.`, 404);
    assertDispatchRequest(dispatch, params);
    if (dispatch.stage !== "PREPARING") return dispatch;
    const task = this.get(params.task_id)!;
    const command = taskCommand(params.task_id, params.tenant_id, `task-dispatch:${params.dispatch_id}:reserved`, "task.dispatch.reserved", params, task.execution_version);
    const result = await runTaskTransaction({
      storage: this.storage,
      command,
      apply: ({ sql }) => {
        const current = readTaskDispatch(sql, params.dispatch_id)!;
        assertDispatchRequest(current, params);
        if (current.stage === "RESERVED" || current.stage === "AUTHORIZED" || current.stage === "STARTED") return current;
        if (current.stage !== "PREPARING") throw new TaskCoreError("TASK_DISPATCH_STAGE_CONFLICT", `Dispatch ${params.dispatch_id} is ${current.stage}.`);
        sql.exec(
          "UPDATE task_dispatches SET stage = ?, reservation_id = ?, reservation_token = ?, updated_at = ? WHERE dispatch_id = ?",
          "RESERVED", params.reservation_id, params.reservation_token, Date.now(), params.dispatch_id,
        );
        sql.exec("UPDATE task_execution SET lifecycle_state = ?, updated_at = ? WHERE task_id = ?", "RESERVED", Date.now(), params.task_id);
        return readTaskDispatch(sql, params.dispatch_id)!;
      },
    });
    return result.result;
  }

  public async authorizeDispatch(params: TaskDispatchRequest): Promise<TaskDispatchAuthorization> {
    validateText(params.reservation_id, "reservation_id");
    validateText(params.reservation_token, "reservation_token");
    const dispatch = this.getDispatch(params.dispatch_id);
    if (!dispatch) throw new TaskCoreError("TASK_DISPATCH_NOT_FOUND", `Dispatch ${params.dispatch_id} is unknown.`, 404);
    assertDispatchRequest(dispatch, params);
    if (dispatch.stage === "AUTHORIZED" || dispatch.stage === "STARTED") {
      const execution = this.get(params.task_id);
      assertCurrentDispatch(execution, dispatch);
      return authorizationFrom(dispatch, (execution?.last_turn_seq ?? 0) + 1);
    }
    if (dispatch.stage !== "RESERVED" || dispatch.reservation_id !== params.reservation_id || dispatch.reservation_token !== params.reservation_token) {
      throw new TaskCoreError("TASK_RESERVATION_INVALID", "TaskCell requires the current NodeCapacity reservation before authorization.");
    }
    const task = this.get(params.task_id)!;
    const operationId = `task-dispatch:${params.dispatch_id}:authorized`;
    const command = taskCommand(params.task_id, params.tenant_id, operationId, "task.dispatch.authorize", params, task.execution_version);
    const nextEpoch = Number(task.business_epoch) + 1;
    const authorizationToken = `auth:${params.dispatch_id}:${params.node_incarnation}:${nextEpoch}`;
    const result = await runTaskTransaction({
      storage: this.storage,
      command,
      outboxEvents: [{
        eventId: `task-start:${params.dispatch_id}:${nextEpoch}`,
        eventType: "task.execution.start_requested",
        tenantId: params.tenant_id,
        payload: {
          dispatch_id: params.dispatch_id,
          task_id: params.task_id,
          node_id: params.node_id,
          node_incarnation: params.node_incarnation,
          region: params.region ?? "default",
          resource_pool: params.resource_pool ?? "default",
          reservation_id: params.reservation_id,
          reservation_token: params.reservation_token,
          authorization_token: authorizationToken,
          business_epoch: nextEpoch,
        },
      }],
      apply: ({ sql }) => {
        const currentDispatch = readTaskDispatch(sql, params.dispatch_id)!;
        assertDispatchRequest(currentDispatch, params);
        if (currentDispatch.stage === "AUTHORIZED" || currentDispatch.stage === "STARTED") {
          const execution = readTaskExecution(sql, params.task_id)!;
          return authorizationFrom(currentDispatch, Number(execution.last_turn_seq) + 1);
        }
        if (currentDispatch.stage !== "RESERVED" || currentDispatch.reservation_token !== params.reservation_token) {
          throw new TaskCoreError("TASK_RESERVATION_INVALID", "Dispatch reservation is not current.");
        }
        const execution = readTaskExecution(sql, params.task_id)!;
        const epoch = Number(execution.business_epoch) + 1;
        sql.exec(
          "UPDATE task_dispatches SET stage = ?, node_id = ?, node_incarnation = ?, reservation_id = ?, reservation_token = ?, authorization_token = ?, business_epoch = ?, updated_at = ? WHERE dispatch_id = ?",
          "AUTHORIZED", params.node_id, params.node_incarnation, params.reservation_id,
          params.reservation_token, authorizationToken, epoch, Date.now(), params.dispatch_id,
        );
        sql.exec(
          "UPDATE task_execution SET lifecycle_state = ?, business_epoch = ?, current_dispatch_id = ?, pending_dispatch_id = ?, current_node_id = ?, node_incarnation = ?, region = ?, resource_pool = ?, heartbeat_at = ?, suspected_at = ?, suspect_reason = ?, updated_at = ? WHERE task_id = ?",
          "STARTING", epoch, params.dispatch_id, null, params.node_id, params.node_incarnation, params.region ?? "default", params.resource_pool ?? "default", Date.now(), null, null, Date.now(), params.task_id,
        );
        const latestExecution = readTaskExecution(sql, params.task_id)!;
        return authorizationFrom(readTaskDispatch(sql, params.dispatch_id)!, Number(latestExecution.last_turn_seq) + 1);
      },
    });
    return result.result;
  }

  public async markStarted(authorization: TaskDispatchAuthorization, startReceipt: string): Promise<TaskExecutionRecord> {
    if (!startReceipt.trim()) throw new TaskCoreError("START_EVIDENCE_REQUIRED", "A node start receipt is required.", 400);
    const dispatch = this.getDispatch(authorization.dispatch_id);
    if (!dispatch) throw new TaskCoreError("TASK_DISPATCH_NOT_FOUND", `Dispatch ${authorization.dispatch_id} is unknown.`, 404);
    assertAuthorization(dispatch, authorization);
    if (dispatch.stage === "STARTED") {
      if (dispatch.start_receipt !== startReceipt) {
        throw new TaskCoreError("START_RECEIPT_CONFLICT", "Dispatch already started with a different node receipt.");
      }
      const execution = this.get(authorization.task_id)!;
      assertCurrentDispatch(execution, dispatch);
      return execution;
    }
    const current = this.get(authorization.task_id)!;
    const command = taskCommand(
      authorization.task_id,
      current.tenant_id,
      `task-dispatch:${authorization.dispatch_id}:started`,
      "task.dispatch.started",
      { authorization, start_receipt: startReceipt },
      current.execution_version,
    );
    const result = await runTaskTransaction({
      storage: this.storage,
      command,
      apply: ({ sql }) => {
        const currentDispatch = readTaskDispatch(sql, authorization.dispatch_id)!;
        assertAuthorization(currentDispatch, authorization);
        const task = readTaskExecution(sql, authorization.task_id)!;
        if (currentDispatch.stage === "STARTED") {
          if (currentDispatch.start_receipt !== startReceipt) {
            throw new TaskCoreError("START_RECEIPT_CONFLICT", "Dispatch already started with a different node receipt.");
          }
          assertCurrentDispatch(task, currentDispatch);
          return task;
        }
        if (currentDispatch.stage !== "AUTHORIZED") throw new TaskCoreError("TASK_DISPATCH_STAGE_CONFLICT", "Task dispatch is not authorized.");
        sql.exec("UPDATE task_dispatches SET stage = ?, start_receipt = ?, updated_at = ? WHERE dispatch_id = ?", "STARTED", startReceipt, Date.now(), authorization.dispatch_id);
        sql.exec(
          "UPDATE cell_outbox SET status = ?, delivered_at = ?, last_error = ? WHERE event_id = ?",
          "DELIVERED", Date.now(), null, `task-start:${authorization.dispatch_id}:${authorization.business_epoch}`,
        );
        sql.exec(
          "UPDATE task_execution SET lifecycle_state = ?, heartbeat_at = ?, updated_at = ? WHERE task_id = ?",
          "RUNNING", Date.now(), Date.now(), authorization.task_id,
        );
        return readTaskExecution(sql, authorization.task_id)!;
      },
    });
    return result.result;
  }

  public async markSuspect(taskId: string, expectedEpoch: number, reason: string): Promise<TaskExecutionRecord> {
    const current = this.get(taskId);
    if (!current) throw new TaskCoreError("TASK_NOT_INITIALIZED", `Task ${taskId} is not initialized.`, 404);
    if (current.business_epoch !== expectedEpoch) throw new TaskCoreError("STALE_EXECUTION_EPOCH", "Cannot suspect a task using a stale business epoch.");
    if (current.lifecycle_state === "SUSPECT") return current;
    if (current.lifecycle_state !== "RUNNING" && current.lifecycle_state !== "STARTING") {
      throw new TaskCoreError("TASK_NOT_EXECUTING", `Task ${taskId} is ${current.lifecycle_state}.`);
    }
    const command = taskCommand(taskId, current.tenant_id, `task:${taskId}:suspect:${expectedEpoch}`, "task.execution.suspect", { expectedEpoch, reason }, current.execution_version);
    const result = await runTaskTransaction({
      storage: this.storage,
      command,
      apply: ({ sql }) => {
        sql.exec(
          "UPDATE task_execution SET lifecycle_state = ?, suspected_at = ?, suspect_reason = ?, updated_at = ? WHERE task_id = ?",
          "SUSPECT", Date.now(), reason, Date.now(), taskId,
        );
        return readTaskExecution(sql, taskId)!;
      },
    });
    return result.result;
  }

  public async markTurnCommitted(taskId: string, epoch: number, turnSeq: number, progress?: unknown): Promise<TaskExecutionRecord> {
    const current = this.get(taskId);
    if (!current) throw new TaskCoreError("TASK_NOT_INITIALIZED", `Task ${taskId} is not initialized.`, 404);
    const command = taskCommand(taskId, current.tenant_id, `task:${taskId}:turn:${epoch}:${turnSeq}`, "task.turn.committed", { epoch, turnSeq, progress }, current.execution_version);
    const result = await runTaskTransaction({
      storage: this.storage,
      command,
      apply: ({ sql, task, assertExecutionEpoch, saveProgress }) => {
        assertExecutionEpoch(epoch, turnSeq);
        sql.exec(
          "UPDATE task_execution SET last_turn_seq = ?, heartbeat_at = ?, updated_at = ? WHERE task_id = ?",
          turnSeq, Date.now(), Date.now(), taskId,
        );
        if (progress !== undefined) saveProgress(progress);
        return readTaskExecution(sql, taskId)!;
      },
    });
    return result.result;
  }

  /** Persist the executor's terminal result under its current business epoch. */
  public async finish(taskId: string, tenantId: string, epoch: number, status: "SUCCEEDED" | "FAILED"): Promise<TaskExecutionRecord> {
    const current = this.get(taskId);
    if (!current) throw new TaskCoreError("TASK_NOT_INITIALIZED", `Task ${taskId} is not initialized.`, 404);
    if (current.tenant_id !== tenantId) throw new TaskCoreError("TASK_TENANT_MISMATCH", "Task completion tenant does not own this task.", 403);
    if (Number(current.business_epoch) !== epoch) throw new TaskCoreError("STALE_EXECUTION_EPOCH", "Cannot complete a task using a stale business epoch.");
    if (current.lifecycle_state === status) {
      await confirmCellWrite(this.storage);
      return current;
    }
    if (["SUCCEEDED", "FAILED"].includes(current.lifecycle_state)) {
      throw new TaskCoreError("TASK_TERMINAL_CONFLICT", `Task ${taskId} is already ${current.lifecycle_state}.`);
    }
    if (status === "SUCCEEDED" && current.lifecycle_state !== "RUNNING") {
      throw new TaskCoreError("TASK_NOT_EXECUTING", `Task ${taskId} cannot succeed from ${current.lifecycle_state}.`);
    }
    const command = taskCommand(taskId, tenantId, `task:${taskId}:finish:${epoch}:${status}`, "task.execution.finished", { taskId, epoch, status }, current.execution_version);
    const result = await runTaskTransaction({
      storage: this.storage,
      command,
      outboxEvents: [{
        eventId: `task-finished:${taskId}:${epoch}:${status.toLowerCase()}`,
        eventType: "task.execution.finished",
        tenantId,
        payload: { task_id: taskId, business_epoch: epoch, status },
      }],
      apply: ({ sql }) => {
        const task = readTaskExecution(sql, taskId)!;
        if (Number(task.business_epoch) !== epoch) throw new TaskCoreError("STALE_EXECUTION_EPOCH", "Task epoch changed before terminal state was committed.");
        if (["SUCCEEDED", "FAILED"].includes(task.lifecycle_state)) {
          if (task.lifecycle_state !== status) throw new TaskCoreError("TASK_TERMINAL_CONFLICT", `Task ${taskId} is already ${task.lifecycle_state}.`);
          return task;
        }
        if (status === "SUCCEEDED" && task.lifecycle_state !== "RUNNING") {
          throw new TaskCoreError("TASK_NOT_EXECUTING", `Task ${taskId} cannot succeed from ${task.lifecycle_state}.`);
        }
        sql.exec("UPDATE task_execution SET lifecycle_state = ?, updated_at = ? WHERE task_id = ?", status, Date.now(), taskId);
        return readTaskExecution(sql, taskId)!;
      },
    });
    return result.result;
  }

  public async suspend(taskId: string, epoch: number, progress?: unknown): Promise<TaskExecutionRecord> {
    const current = this.get(taskId);
    if (!current) throw new TaskCoreError("TASK_NOT_INITIALIZED", `Task ${taskId} is not initialized.`, 404);
    if (current.business_epoch !== epoch) throw new TaskCoreError("STALE_EXECUTION_EPOCH", "Cannot suspend using a stale business epoch.");
    const command = taskCommand(taskId, current.tenant_id, `task:${taskId}:suspend:${epoch}`, "task.execution.suspend", { epoch, progress }, current.execution_version);
    const result = await runTaskTransaction({
      storage: this.storage,
      command,
      apply: ({ sql, saveProgress }) => {
        if (progress !== undefined) saveProgress(progress);
        sql.exec("UPDATE task_execution SET lifecycle_state = ?, updated_at = ? WHERE task_id = ?", "SUSPENDED", Date.now(), taskId);
        return readTaskExecution(sql, taskId)!;
      },
    });
    return result.result;
  }

  public async resume(taskId: string): Promise<TaskExecutionRecord> {
    const current = this.get(taskId);
    if (!current) throw new TaskCoreError("TASK_NOT_INITIALIZED", `Task ${taskId} is not initialized.`, 404);
    if (current.lifecycle_state === "QUEUED") return current;
    if (current.lifecycle_state !== "SUSPENDED") throw new TaskCoreError("TASK_NOT_SUSPENDED", `Task ${taskId} is ${current.lifecycle_state}.`);
    const command = taskCommand(taskId, current.tenant_id, `task:${taskId}:resume:${current.execution_version}`, "task.execution.resume", { taskId, epoch: current.business_epoch }, current.execution_version);
    const result = await runTaskTransaction({
      storage: this.storage,
      command,
      apply: ({ sql }) => {
        sql.exec("UPDATE task_execution SET lifecycle_state = ?, updated_at = ? WHERE task_id = ?", "QUEUED", Date.now(), taskId);
        return readTaskExecution(sql, taskId)!;
      },
    });
    return result.result;
  }

  private async ensureTask(request: TaskDispatchRequest): Promise<void> {
    const existing = this.get(request.task_id);
    const taskSpec = request.task_spec ?? (existing ? JSON.parse(existing.task_spec_json) : undefined);
    if (existing) {
      if (existing.tenant_id !== request.tenant_id) throw new TaskCoreError("TASK_TENANT_CONFLICT", "Task tenant cannot change.");
      if (request.task_spec !== undefined && JSON.stringify(request.task_spec) !== existing.task_spec_json) {
        throw new TaskCoreError("TASK_SPEC_CONFLICT", "The TaskSpec is immutable after TaskCell initialization.");
      }
      return;
    }
    if (taskSpec === undefined) throw new TaskCoreError("TASK_SPEC_REQUIRED", "Initial task dispatch requires the complete TaskSpec.", 400);
    const policy = request.execution_policy ?? (taskSpec as any)?.execution_policy ?? (taskSpec as any)?.spec?.execution_policy ?? {};
    const task = JSON.stringify(taskSpec);
    const operation = `task:${request.task_id}:initialize`;
    const command = taskCommand(request.task_id, request.tenant_id, operation, "task.execution.initialize", {
      task_id: request.task_id,
      tenant_id: request.tenant_id,
      region: request.region ?? "default",
      resource_pool: request.resource_pool ?? "default",
      task_spec: taskSpec,
      execution_policy: policy,
    }, 0);
    await executeCellOperation(this.storage, command, (sql) => {
      const row = readTaskExecution(sql, request.task_id);
      if (row) {
        if (row.tenant_id !== request.tenant_id || row.task_spec_json !== task) throw new TaskCoreError("TASK_SPEC_CONFLICT", "The TaskSpec is immutable after TaskCell initialization.");
        return row;
      }
      sql.exec(
        `INSERT INTO task_execution (
          task_id, tenant_id, lifecycle_state, business_epoch, execution_version, current_dispatch_id, pending_dispatch_id,
          current_node_id, node_incarnation, region, resource_pool, last_turn_seq, task_spec_json, execution_policy_json,
          progress_json, heartbeat_at, suspected_at, suspect_reason, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        request.task_id,
        request.tenant_id,
        "QUEUED",
        0,
        1,
        null,
        null,
        null,
        null,
        request.region ?? "default",
        request.resource_pool ?? "default",
        0,
        task,
        JSON.stringify(policy),
        "{}",
        null,
        null,
        null,
        Date.now(),
      );
      return readTaskExecution(sql, request.task_id)!;
    });
  }
}

function taskCommand(
  taskId: string,
  tenantId: string,
  operationId: string,
  operationType: string,
  payload: unknown,
  expectedVersion: number,
): CellOperationCommand {
  return {
    operationId,
    operationType,
    resourceId: taskId,
    tenantId,
    payload: jsonCompatible(payload),
    aggregateType: "task-execution",
    aggregateId: taskId,
    expectedVersion,
  };
}

function validateDispatchRequest(request: TaskDispatchRequest): void {
  for (const [name, value] of Object.entries({
    dispatch_id: request.dispatch_id,
    task_id: request.task_id,
    tenant_id: request.tenant_id,
    node_id: request.node_id,
    node_incarnation: request.node_incarnation,
  })) {
    if (typeof value !== "string" || !value.trim()) throw new TaskCoreError("INVALID_DISPATCH", `${name} is required.`, 400);
  }
  if (!["ordinary", "recovery", "dag"].includes(request.kind)) throw new TaskCoreError("INVALID_DISPATCH_KIND", "Dispatch kind must be ordinary, recovery or dag.", 400);
}

function assertDispatchRequest(dispatch: TaskDispatchRecord, request: TaskDispatchRequest): void {
  const original = JSON.parse(dispatch.request_json) as TaskDispatchRequest;
  if (
    dispatch.task_id !== request.task_id || dispatch.kind !== request.kind ||
    dispatch.node_id !== request.node_id || dispatch.node_incarnation !== request.node_incarnation ||
    original.tenant_id !== request.tenant_id ||
    (original.region ?? "default") !== (request.region ?? "default") ||
    (original.resource_pool ?? "default") !== (request.resource_pool ?? "default")
  ) {
    throw new TaskCoreError("TASK_DISPATCH_CONFLICT", `Dispatch ${request.dispatch_id} was reused with different identity or placement.`);
  }
  if (request.reservation_id !== undefined && dispatch.stage !== "PREPARING" &&
      (dispatch.reservation_id !== request.reservation_id || dispatch.reservation_token !== request.reservation_token)) {
    throw new TaskCoreError("TASK_RESERVATION_CONFLICT", `Dispatch ${request.dispatch_id} has a different reservation.`);
  }
}

function authorizationFrom(dispatch: TaskDispatchRecord, turnSeq = 1): TaskDispatchAuthorization {
  if (
    !dispatch.node_id || !dispatch.node_incarnation || !dispatch.reservation_id ||
    !dispatch.reservation_token || !dispatch.authorization_token || dispatch.business_epoch === null
  ) {
    throw new TaskCoreError("TASK_AUTHORIZATION_INCOMPLETE", `Dispatch ${dispatch.dispatch_id} has incomplete authorization.`);
  }
  const task = dispatch.task_id;
  return {
    dispatch_id: dispatch.dispatch_id,
    task_id: task,
    node_id: dispatch.node_id,
    node_incarnation: dispatch.node_incarnation,
    reservation_id: dispatch.reservation_id,
    reservation_token: dispatch.reservation_token,
    authorization_token: dispatch.authorization_token,
    business_epoch: Number(dispatch.business_epoch),
    turn_seq: turnSeq,
  };
}

function assertAuthorization(dispatch: TaskDispatchRecord, auth: TaskDispatchAuthorization): void {
  if (
    dispatch.task_id !== auth.task_id || dispatch.dispatch_id !== auth.dispatch_id ||
    dispatch.node_id !== auth.node_id || dispatch.node_incarnation !== auth.node_incarnation ||
    dispatch.reservation_id !== auth.reservation_id || dispatch.reservation_token !== auth.reservation_token ||
    dispatch.authorization_token !== auth.authorization_token || Number(dispatch.business_epoch) !== auth.business_epoch
  ) {
    throw new TaskCoreError("TASK_AUTHORIZATION_CONFLICT", "Task authorization does not match the current dispatch.");
  }
}

function assertCurrentDispatch(task: TaskExecutionRecord | null, dispatch: TaskDispatchRecord): void {
  if (
    !task || task.current_dispatch_id !== dispatch.dispatch_id ||
    Number(task.business_epoch) !== Number(dispatch.business_epoch)
  ) throw new TaskCoreError("STALE_DISPATCH_AUTHORIZATION", `Dispatch ${dispatch.dispatch_id} is no longer the current TaskCell authorization.`);
}

function validateText(value: string | undefined, label: string): void {
  if (typeof value !== "string" || value.trim().length === 0) throw new TaskCoreError("INVALID_DISPATCH", `${label} is required.`, 400);
}

function jsonCompatible<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}
