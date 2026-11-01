/**
 * AgentTaskHarnessCell: Brain-Body Disaggregated Durable Object / celld Cell (~0.47 MB RAM).
 * - Implements `rehydrateFromSql()` to reconstruct selected legacy metadata from local SQLite.
 *   The in-process simulator and legacy WAL path do not establish fleet or bucket durability.
 * - The legacy `RpoZeroDurabilityEngine` is an in-memory simulation; it is not Celld's proof.
 *   Real Cell write confirmation is provided by Celld's `storage.sync()` / response output gate.
 * - `DurableWorkflowContext` is also a local orchestration model, not the task-state authority.
 */

import { DeterministicTimeTravelReplayer } from "../checkpoint/time_travel";
import { CognitiveRouter } from "../cognitive_router";
import { LeaseCoordinator, StaleFenceError } from "../lease_coordinator";
import {
  AgentTask,
  FenceToken,
  ModelPoolName,
  ReasoningEffort,
  SandboxPhase,
  TaskState,
  ToolEffectClass,
  TurnWalFrame,
} from "../models";
import { NodeDaemon } from "../node_daemon";
import { SagaToolGateway } from "../saga_gateway";
import {
  DurableWorkflowContext,
  RpoZeroDurabilityEngine,
} from "./celld_ha_engine";
import { CloudflareCelldEnv, DurableObjectState, WebSocketLike } from "./env";
import { initializeCellProtocol } from "../persistence/schema";
import { CellOutboxConflictError, decodeCellOutboxCursor, listCellOutboxReplay, listDueCellOutbox, markCellOutboxBatchDelivered, pruneDeliveredCellOutboxPayloads } from "../persistence/outbox";
import { persistenceCellStub } from "../persistence/registry";
import {
  assertBusinessEpoch,
  TaskCore,
  TaskCoreError,
  TaskDispatchRequest,
  taskCoreMigration,
} from "../persistence/task-core";
import { stableTaskOperationId, TaskOperationError, TaskOperations, taskOperationsMigration } from "../persistence/task-operations";
import { NodeResourceVector } from "../persistence/node-capacity";
import { schedulerPoolCellKey } from "../persistence/scheduler-pool";
import { CloudflareTaskOperationProvider } from "./task_operation_provider";
import { R2SnapshotBlockStore } from "./r2_snapshot_block_store";
import { createTaskMeteredProviderCallExecutor } from "./metered_call";
import { normalizeChatMessages } from "./metered_provider";
import { CellAuthorityError } from "../router/cell_authorities";
import { MeteredProviderCallError, ProviderCallResultJournal } from "../router/provider_call";
import { digestJson } from "../persistence/digest";
import { TaskTurnError, TaskTurns, taskTurnUploadMigration, taskTurnsMigration } from "../persistence/task-turns";

export class AgentTaskHarnessCell {
  public readonly leaseCoord = new LeaseCoordinator();
  public readonly cogRouter = new CognitiveRouter(this.leaseCoord);
  public readonly sagaGw: SagaToolGateway;
  public readonly nodeDaemon: NodeDaemon;
  public readonly replayer = new DeterministicTimeTravelReplayer();
  public readonly durabilityEngine = new RpoZeroDurabilityEngine("bucket", 10000);
  /** Local Celld ownership lease epoch; distinct from TaskCore's business epoch. */
  public cellOwnershipEpoch: number = 0;
  public readonly workflowCtx: DurableWorkflowContext;
  public currentOwnerNodeId: string = "celld-node-01";
  public task: AgentTask;
  public clientCreditWindow: number = 100;
  public readonly taskCore: TaskCore;
  public readonly taskOperations: TaskOperations;
  public readonly taskTurns: TaskTurns;

  constructor(
    public readonly ctx: DurableObjectState,
    public readonly env: CloudflareCelldEnv,
  ) {
    const taskId = ctx.id.name ?? ctx.id.toString();
    initializeCellProtocol(this.ctx.storage, { type: "task", key: taskId }, [
      taskCoreMigration,
      taskOperationsMigration,
      taskTurnUploadMigration,
      taskTurnsMigration,
    ]);
    this.taskCore = new TaskCore(this.ctx.storage);
    this.taskOperations = new TaskOperations(this.ctx.storage);
    this.taskTurns = new TaskTurns(this.ctx.storage, new R2SnapshotBlockStore(env.ANSTS_CHECKPOINTS_R2), this.taskOperations);
    this.sagaGw = new SagaToolGateway(this.leaseCoord, this.taskOperations);
    this.nodeDaemon = new NodeDaemon(this.leaseCoord, this.cogRouter, this.sagaGw);
    this.task = new AgentTask({
      taskId,
      tenantId: "fin-enterprise",
      primaryModel: (env.PRIMARY_MODEL_POOL as ModelPoolName) ?? "DeepSeek",
      fastUtilityModel: (env.FAST_UTILITY_POOL as ModelPoolName) ?? "GLM-5.3",
      fallbackModels: [
        ((env.FALLBACK_MODEL_POOL as ModelPoolName) ?? "Qwen-3.8"),
        ((env.FAST_UTILITY_POOL as ModelPoolName) ?? "GLM-5.3"),
      ],
      state: TaskState.SUBMITTED,
      phase: SandboxPhase.IDLE,
    });
    this.workflowCtx = new DurableWorkflowContext(`wf-${taskId}`, 3600_000);

    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS turn_wal_frames (turn_seq INTEGER, epoch INTEGER, routed_model TEXT, effective_effort TEXT, tokens_used INTEGER, workspace_hash TEXT, pass_rate REAL, error_count INTEGER, diff_entropy REAL)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS saga_journal (turn_seq INTEGER, epoch INTEGER, tool_name TEXT, effect_class TEXT, compensation_tool TEXT, payload TEXT)",
    );

    // Rehydrate state from private SQLite if waking from HIBERNATED or INACTIVE
    this.rehydrateFromSql();
  }

  public rehydrateFromSql(): void {
    const taskId = this.task.taskId;
    const walRows = this.ctx.storage.sql.exec("SELECT * FROM turn_wal_frames").toArray();
    const execution = this.taskCore.get(taskId);
    if (walRows.length === 0) {
      if (execution) this.applyTaskExecution(execution);
      this.cellOwnershipEpoch = this.durabilityEngine.claimCellOwnershipCas(taskId, this.currentOwnerNodeId, Date.now()).epoch;
      return;
    }

    const frames: TurnWalFrame[] = [];
    let totalUsedTokens = 0;
    for (const r of walRows) {
      const f = new FenceToken(Number(r.epoch), Number(r.turn_seq), taskId);
      totalUsedTokens += Number(r.tokens_used ?? 0);
      frames.push({
        taskId,
        fence: f,
        routedModel: String(r.routed_model),
        effectiveEffort: (r.effective_effort as ReasoningEffort) ?? "high",
        tokensUsed: Number(r.tokens_used ?? 0),
        workspaceMerkleHash: String(r.workspace_hash),
        passRate: Number(r.pass_rate ?? 0.5),
        errorCount: Number(r.error_count ?? 0),
        diffEntropy: Number(r.diff_entropy ?? 0.5),
        contextMessages: [`turn_${r.turn_seq}`],
      });
    }
    this.nodeDaemon.walStore.set(taskId, frames);
    this.task.usedTokens = totalUsedTokens;

    const lastFrame = frames[frames.length - 1];
    const nextFence = new FenceToken(
      execution?.business_epoch ?? 0,
      (execution?.last_turn_seq ?? lastFrame.fence.turnSeq) + 1,
      taskId,
    );
    this.cellOwnershipEpoch = this.durabilityEngine.claimCellOwnershipCas(taskId, this.currentOwnerNodeId, Date.now()).epoch;
    this.task.fence = nextFence;
    if (execution) this.applyTaskExecution(execution);
    this.leaseCoord.restoreFence(taskId, this.currentOwnerNodeId, this.task.assignedModelPool ?? this.task.primaryModel,
      new FenceToken(nextFence.epoch, Math.max(0, nextFence.turnSeq - 1), taskId));

    const sagaRows = this.ctx.storage.sql.exec("SELECT * FROM saga_journal").toArray();
    for (const sr of sagaRows) {
      if (sr.effect_class === ToolEffectClass.SAGA_COMPENSATABLE && sr.compensation_tool) {
        this.sagaGw.compensator.registerForwardAction(
          taskId,
          new FenceToken(Number(sr.epoch), Number(sr.turn_seq), taskId),
          String(sr.tool_name),
          String(sr.compensation_tool),
          String(sr.payload),
        );
      }
    }
  }

  public async fetch(request: Request): Promise<Response> {
    try {
      return await this.handleRequest(request);
    } catch (error) {
      if (error instanceof TaskCoreError) return json({ error: error.code, message: error.message }, error.status);
      if (error instanceof TaskOperationError) return json({ error: error.code, message: error.message }, error.status);
      if (error instanceof TaskTurnError) return json({ error: error.code, message: error.message }, error.status);
      if (error instanceof CellOutboxConflictError) return json({ error: error.code, message: error.message }, 409);
      if (error instanceof MeteredProviderCallError || error instanceof CellAuthorityError) {
        return json({ error: error.code, message: error.message }, error.status);
      }
      throw error;
    }
  }

  private async handleRequest(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/execution/prepare") {
      const body = await request.json() as TaskDispatchRequest;
      const dispatch = await this.taskCore.prepareDispatch(body);
      this.applyTaskExecution(this.taskCore.get(this.task.taskId)!);
      return json({ dispatch });
    }

    if (request.method === "POST" && url.pathname === "/execution/reserved") {
      const body = await request.json() as TaskDispatchRequest;
      const dispatch = await this.taskCore.recordReservation(body);
      this.applyTaskExecution(this.taskCore.get(this.task.taskId)!);
      return json({ dispatch });
    }

    if (request.method === "POST" && url.pathname === "/execution/authorize") {
      const body = await request.json() as TaskDispatchRequest;
      await this.assertCurrentNodeReservation(body);
      const authorization = await this.taskCore.authorizeDispatch(body);
      this.applyTaskExecution(this.taskCore.get(this.task.taskId)!);
      return json(authorization);
    }

    const dispatchMatch = url.pathname.match(/^\/execution\/dispatches\/([^/]+)$/);
    if (request.method === "GET" && dispatchMatch) {
      const dispatch = this.taskCore.getDispatch(decodeURIComponent(dispatchMatch[1]));
      return dispatch ? json(dispatch) : json({ error: "TASK_DISPATCH_NOT_FOUND" }, 404);
    }

    if (request.method === "POST" && url.pathname === "/execution/started") {
      const body = await request.json() as { authorization: any; start_receipt: string };
      const execution = await this.taskCore.markStarted(body.authorization, body.start_receipt);
      this.applyTaskExecution(execution);
      this.nodeDaemon.applyAuthorizedExecution(
        this.task,
        execution.current_node_id ?? body.authorization.node_id,
        new FenceToken(execution.business_epoch, execution.last_turn_seq + 1, execution.task_id),
      );
      return json({ task_id: execution.task_id, state: execution.lifecycle_state, business_epoch: execution.business_epoch });
    }

    if (request.method === "POST" && url.pathname === "/execution/complete") {
      const body = await request.json() as { tenant_id?: string; epoch?: number; status?: "SUCCEEDED" | "FAILED" };
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id") ?? body.tenant_id ?? null);
      if (body.status !== "SUCCEEDED" && body.status !== "FAILED") {
        throw new TaskCoreError("TASK_TERMINAL_STATUS_INVALID", "status must be SUCCEEDED or FAILED.", 400);
      }
      const completed = await this.taskCore.finish(execution.task_id, execution.tenant_id, Number(body.epoch), body.status);
      this.applyTaskExecution(completed);
      return json({ task_id: completed.task_id, tenant_id: completed.tenant_id, state: completed.lifecycle_state,
        business_epoch: completed.business_epoch, execution_version: completed.execution_version });
    }

    if (request.method === "GET" && url.pathname === "/execution/state") {
      const execution = this.taskCore.get(this.task.taskId);
      return execution ? json(execution) : json({ error: "TASK_NOT_INITIALIZED" }, 404);
    }

    if (request.method === "POST" && url.pathname === "/race/winner") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      const body = await request.json() as {
        event_id?: string; decision_id?: string; task_id?: string; tenant_id?: string;
        race_id?: string; winner_branch_id?: string; aggregate_version?: number;
      };
      if (body.task_id !== execution.task_id || body.tenant_id !== execution.tenant_id) {
        throw new TaskOperationError("TASK_RACE_EVENT_OWNER_MISMATCH", "Race winner event does not match this TaskCell and tenant.", 403);
      }
      const winner = await this.taskOperations.recordRaceWinner({
        eventId: String(body.event_id ?? ""), decisionId: String(body.decision_id ?? ""),
        taskId: execution.task_id, tenantId: execution.tenant_id, raceId: String(body.race_id ?? ""),
        winnerBranchId: String(body.winner_branch_id ?? ""), aggregateVersion: Number(body.aggregate_version),
      });
      return json({ task_id: execution.task_id, race_id: body.race_id, winner_branch_id: winner, recorded: true });
    }

    if (url.pathname === "/turns/recovery" && request.method === "GET") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      return json(await this.taskTurns.recoverLatest(execution.task_id));
    }

    if (url.pathname === "/turns/snapshot-uploads" && request.method === "POST") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      const body = await request.json() as { epoch?: number; turn_seq?: number; upload_id?: string; chunks?: Array<{ index: number; sha256: string; size_bytes: number }> };
      const fence = new FenceToken(Number(body.epoch), Number(body.turn_seq), execution.task_id);
      assertBusinessEpoch(execution, fence.epoch, fence.turnSeq);
      const upload = await this.taskTurns.beginSnapshotUpload({
        taskId: execution.task_id,
        tenantId: execution.tenant_id,
        fence,
        uploadId: String(body.upload_id ?? ""),
        chunks: (body.chunks ?? []).map((chunk) => ({ index: Number(chunk.index), sha256: String(chunk.sha256), sizeBytes: Number(chunk.size_bytes) })),
      });
      return json({ upload_id: upload.uploadId, turn_seq: upload.turnSeq, status: upload.status, chunks: upload.chunks });
    }

    const snapshotChunkMatch = url.pathname.match(/^\/turns\/snapshot-uploads\/([^/]+)\/chunks\/([^/]+)$/);
    if (snapshotChunkMatch && request.method === "PUT") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      const uploadId = decodeURIComponent(snapshotChunkMatch[1]);
      const index = Number(decodeURIComponent(snapshotChunkMatch[2]));
      const uploaded = await this.taskTurns.uploadSnapshotChunk({
        taskId: execution.task_id,
        tenantId: execution.tenant_id,
        uploadId,
        index,
        bytes: new Uint8Array(await request.arrayBuffer()),
      });
      return json({ chunk: uploaded });
    }

    if (request.method === "GET" && url.pathname === "/execution/start-commands") {
      const commands = listDueCellOutbox(this.ctx.storage.sql)
        .filter((event) => event.aggregateId === this.task.taskId && event.eventType === "task.execution.start_requested");
      return json({ commands });
    }

    if (request.method === "GET" && url.pathname === "/events") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      const rawCursor = request.headers.get("last-event-id") ?? url.searchParams.get("after_cursor");
      let cursor;
      try { cursor = rawCursor ? decodeCellOutboxCursor(rawCursor) : undefined; }
      catch (error) { throw new TaskCoreError("TASK_EVENT_CURSOR_INVALID", error instanceof Error ? error.message : String(error), 400); }
      const limit = Number(url.searchParams.get("limit") ?? 100);
      if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
        throw new TaskCoreError("TASK_EVENT_LIMIT_INVALID", "limit must be an integer from 1 through 1000.", 400);
      }
      const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
      const page = listCellOutboxReplay(this.ctx.storage.sql, {
        tenantId: execution.tenant_id,
        aggregateType: "task-execution",
        aggregateId: execution.task_id,
        cutoff,
        after: cursor,
        limit,
      });
      if (page.cursorExpired) {
        return json({ error: "CURSOR_EXPIRED", resync_required: true, task_id: execution.task_id,
          resync_url: `/v1beta1/tasks/${encodeURIComponent(execution.task_id)}/turns/recovery?tenant_id=${encodeURIComponent(execution.tenant_id)}` }, 410);
      }
      return json({ task_id: execution.task_id, tenant_id: execution.tenant_id, window_days: 7,
        events: page.events, next_cursor: page.nextCursor, has_more: page.hasMore });
    }

    if (request.method === "POST" && url.pathname === "/events/ack") {
      const body = await request.json() as { tenant_id?: string; event_ids?: string[] };
      const execution = this.requireTenantExecution(body.tenant_id ?? null);
      const acknowledged = await markCellOutboxBatchDelivered(this.ctx.storage, {
        eventIds: body.event_ids ?? [],
        tenantId: execution.tenant_id,
        aggregateType: "task-execution",
        aggregateId: execution.task_id,
      });
      return json({ task_id: execution.task_id, tenant_id: execution.tenant_id, acknowledged_count: acknowledged });
    }

    if (request.method === "POST" && url.pathname === "/events/prune") {
      const body = await request.json() as { tenant_id?: string; now?: number };
      const execution = this.requireTenantExecution(body.tenant_id ?? null);
      const now = body.now ?? Date.now();
      const cutoff = now - 7 * 24 * 60 * 60 * 1000;
      if (!Number.isSafeInteger(now) || now < 0) throw new TaskCoreError("TASK_EVENT_RETENTION_TIME_INVALID", "now must be a valid timestamp.", 400);
      const pruned = await pruneDeliveredCellOutboxPayloads(this.ctx.storage, {
        tenantId: execution.tenant_id,
        aggregateType: "task-execution",
        aggregateId: execution.task_id,
        cutoff,
      });
      return json({ task_id: execution.task_id, tenant_id: execution.tenant_id, pruned_payloads: pruned, retained_window_days: 7 });
    }

    if (request.method === "POST" && url.pathname === "/execution/suspect") {
      const body = await request.json() as { epoch: number; reason: string };
      const execution = await this.taskCore.markSuspect(this.task.taskId, body.epoch, body.reason);
      if (execution.current_node_id && execution.current_dispatch_id) {
        await this.callNodeCapacity(execution.current_node_id, `/reservations/${encodeURIComponent(execution.current_dispatch_id)}/suspect`, "POST", { reason: body.reason });
      }
      this.applyTaskExecution(execution);
      return json({ task_id: execution.task_id, state: execution.lifecycle_state, business_epoch: execution.business_epoch });
    }

    if (request.method === "POST" && url.pathname === "/execution/suspend") {
      const body = await request.json() as { epoch: number; progress?: unknown };
      const execution = await this.taskCore.suspend(this.task.taskId, body.epoch, body.progress);
      this.applyTaskExecution(execution);
      return json({ task_id: execution.task_id, state: execution.lifecycle_state, progress: JSON.parse(execution.progress_json) });
    }

    if (request.method === "POST" && url.pathname === "/execution/resume") {
      const execution = await this.taskCore.resume(this.task.taskId);
      this.applyTaskExecution(execution);
      return json({
        task_id: execution.task_id,
        state: execution.lifecycle_state,
        task_spec: JSON.parse(execution.task_spec_json),
        execution_policy: JSON.parse(execution.execution_policy_json),
        progress: JSON.parse(execution.progress_json),
      });
    }

    if (url.pathname === "/operations" && request.method === "GET") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      return json({ operations: this.taskOperations.listOperations(execution.task_id) });
    }

    const operationApprovalMatch = url.pathname.match(/^\/operations\/([^/]+)\/approval$/);
    if (operationApprovalMatch && request.method === "POST") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      const operationId = decodeURIComponent(operationApprovalMatch[1]);
      const operation = this.taskOperations.getOperation(operationId);
      if (!operation || operation.tenantId !== execution.tenant_id) return json({ error: "TASK_OPERATION_NOT_FOUND" }, 404);
      const policy = taskToolPolicy(execution.task_spec_json, operation.toolName);
      if (policy.effect_class !== operation.effectClass) {
        throw new TaskOperationError("TASK_TOOL_POLICY_CHANGED", "The persisted operation no longer matches the immutable TaskSpec policy.", 409);
      }
      const body = await request.json() as { approval_id?: string; approved_by?: string; expires_at?: number };
      const approved = await this.taskOperations.approveOperation({
        approvalId: String(body.approval_id ?? ""),
        operationId,
        argumentDigest: operation.argumentDigest,
        scope: taskApprovalScope(policy),
        approvedBy: String(body.approved_by ?? ""),
        expiresAt: Number(body.expires_at),
      });
      return json({ operation: approved });
    }

    const operationMatch = url.pathname.match(/^\/operations\/([^/]+)$/);
    if (operationMatch && request.method === "GET") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      const operation = this.taskOperations.getOperation(decodeURIComponent(operationMatch[1]));
      if (!operation || operation.tenantId !== execution.tenant_id) return json({ error: "TASK_OPERATION_NOT_FOUND" }, 404);
      return json({ operation });
    }

    if (url.pathname === "/operations" && request.method === "POST") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      const body = await request.json() as DurableToolCallRequest;
      const result = await this.executeToolOperation(execution, body);
      return json(result.body, result.status);
    }

    if (url.pathname === "/compensations" && request.method === "GET") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      const afterTurnExclusive = Number(url.searchParams.get("after_turn_exclusive") ?? 0);
      if (!Number.isSafeInteger(afterTurnExclusive) || afterTurnExclusive < 0) {
        throw new TaskOperationError("TASK_COMPENSATION_CURSOR_INVALID", "after_turn_exclusive must be a non-negative integer.", 400);
      }
      return json({ compensations: this.taskOperations.listCompensations(execution.task_id, afterTurnExclusive) });
    }

    if (url.pathname === "/compensations/run" && request.method === "POST") {
      const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
      if (!this.env.TASK_OPERATION_EXECUTOR) return json({ error: "TASK_OPERATION_EXECUTOR_UNAVAILABLE" }, 503);
      const body = await request.json() as { epoch?: number; turn_seq?: number; after_turn_exclusive?: number };
      const afterTurnExclusive = Number(body.after_turn_exclusive);
      if (!Number.isSafeInteger(afterTurnExclusive) || afterTurnExclusive < 0) {
        throw new TaskOperationError("TASK_COMPENSATION_CURSOR_INVALID", "after_turn_exclusive must be a non-negative integer.", 400);
      }
      const fence = new FenceToken(Number(body.epoch), Number(body.turn_seq), execution.task_id);
      const provider = new CloudflareTaskOperationProvider(this.env.TASK_OPERATION_EXECUTOR, execution.tenant_id, execution.task_id);
      const results = await this.sagaGw.compensatePersisted(
        fence,
        afterTurnExclusive,
        (input) => provider.executeCompensation(input),
      );
      return json({ results });
    }

    if (request.method === "POST" && url.pathname.endsWith("/turn")) {
      const body = (await request.json()) as {
        epoch?: number;
        turn_seq?: number;
        is_read_only_turn?: boolean;
        consecutive_tool_errors?: number;
        deepseek_429_open?: boolean;
        tool_call?: DurableToolCallRequest & { requires_heavy_container?: boolean };
        workspace_merkle_hash?: string;
        snapshot_upload_id?: string;
        messages?: Array<{ role: "system" | "user" | "assistant"; content: string }>;
        max_output_tokens?: number;
        progress?: unknown;
        pass_rate?: number;
        error_count?: number;
        diff_entropy?: number;
      };

      if (!this.env.TEST_SIMULATOR_MODE) return this.executeDurableTurn(request, body);

      const incomingFence = new FenceToken(
        body.epoch ?? this.task.fence.epoch,
        body.turn_seq ?? this.task.fence.turnSeq,
        this.task.taskId,
      );

      try {
        const execution = this.taskCore.get(this.task.taskId);
        if (!execution) throw new TaskCoreError("TASK_NOT_INITIALIZED", "Task must be prepared and authorized before Turn execution.", 409);
        assertBusinessEpoch(execution, incomingFence.epoch, incomingFence.turnSeq);
        this.applyTaskExecution(execution);
        this.leaseCoord.restoreFence(
          this.task.taskId,
          execution.current_node_id ?? this.currentOwnerNodeId,
          this.task.assignedModelPool ?? this.task.primaryModel,
          new FenceToken(execution.business_epoch, execution.last_turn_seq, this.task.taskId),
        );
        if (body.deepseek_429_open) {
          this.cogRouter.tripPoolBreaker("DeepSeek", 0.48);
        }

        this.nodeDaemon.transitionToCognitivePhase(this.task);
        const routeRes = this.cogRouter.routeTurn(
          this.task,
          incomingFence,
          Boolean(body.is_read_only_turn),
          body.consecutive_tool_errors ?? 0,
        );

        let containerBurstTriggered = false;
        let toolResult: Record<string, any> | null = null;

        if (body.tool_call) {
          if (body.tool_call.requires_heavy_container && this.ctx.container) {
            await this.ctx.container.start();
            containerBurstTriggered = true;
          }
          let operationResult: { status: number; body: unknown };
          try {
            operationResult = await this.executeToolOperation(execution, body.tool_call, incomingFence);
          } finally {
            if (containerBurstTriggered && this.ctx.container) await this.ctx.container.stop();
          }
          toolResult = operationResult.body as Record<string, any>;
          if (operationResult.status !== 200) {
            return json({ task_id: this.task.taskId, tool_result: toolResult, turn_committed: false }, operationResult.status);
          }
        }

        const wsHash = body.workspace_merkle_hash ?? `merkle_t${incomingFence.turnSeq}`;
        const passRate = body.pass_rate ?? 0.75;
        const errCount = body.error_count ?? 0;
        const diffEntropy = body.diff_entropy ?? 0.5;

        this.ctx.storage.sql.exec(
          "INSERT INTO turn_wal_frames (turn_seq, epoch, routed_model, effective_effort, tokens_used, workspace_hash, pass_rate, error_count, diff_entropy) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
          incomingFence.turnSeq,
          incomingFence.epoch,
          routeRes.routedModel,
          routeRes.effectiveEffort,
          routeRes.tokensConsumed,
          wsHash,
          passRate,
          errCount,
          diffEntropy,
        );

        // TASK-49: Enforce RPO=0 Durability Proof before emitting output to caller
        const rpoProof = this.durabilityEngine.commitAndProveRpoZero({
          cellScope: this.task.taskId,
          nodeId: this.currentOwnerNodeId,
          fence: incomingFence,
          ownershipEpoch: this.cellOwnershipEpoch,
          ltxPayload: JSON.stringify({ wsHash, tokens: routeRes.tokensConsumed }),
          peerFollowerIds: [],
          writeCursorConsumed: true,
        });

        const audit = this.nodeDaemon.recordTurnAndAuditVelocity({
          task: this.task,
          routedModel: routeRes.routedModel,
          effectiveEffort: routeRes.effectiveEffort as ReasoningEffort,
          tokensUsed: routeRes.tokensConsumed,
          workspaceMerkleHash: wsHash,
          passRate,
          errorCount: errCount,
          diffEntropy,
        });

        const committedExecution = await this.taskCore.markTurnCommitted(
          this.task.taskId,
          incomingFence.epoch,
          incomingFence.turnSeq,
          { workspace_merkle_hash: wsHash, tokens_used: this.task.usedTokens },
        );
        this.applyTaskExecution(committedExecution);

        if (audit.action === "BACKTRACKED") {
          this.ctx.storage.sql.exec(
            "DELETE FROM turn_wal_frames WHERE turn_seq > ?",
            audit.restoredTurnSeq,
          );
        }

        await this.ctx.storage.setAlarm(Date.now() + 20000);

        return new Response(
          JSON.stringify({
            task_id: this.task.taskId,
            routed_model: routeRes.routedModel,
            effective_effort: routeRes.effectiveEffort,
            tokens_consumed: routeRes.tokensConsumed,
            container_burst_triggered: containerBurstTriggered,
            tool_result: toolResult,
            velocity_audit: audit,
            rpo_zero_proof: rpoProof,
            sqlite_wal_rows: this.ctx.storage.sql.exec("SELECT * FROM turn_wal_frames").toArray().length,
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      } catch (err: any) {
        if (err instanceof StaleFenceError || err instanceof TaskCoreError) {
          return new Response(JSON.stringify({ error: err instanceof TaskCoreError ? err.code : "StaleFenceRejected", message: err.message }), {
            status: 409,
            headers: { "content-type": "application/json" },
          });
        }
        throw err;
      }
    }

    // TASK-49: Multi-step Durable Workflow execution on AgentTaskHarnessCell
    if (request.method === "POST" && url.pathname.endsWith("/workflow")) {
      const body = (await request.json()) as {
        event_to_deliver?: { type: string; payload: any };
        sleep_duration_ms?: number;
      };
      if (body.event_to_deliver) {
        this.workflowCtx.sendEvent(body.event_to_deliver.type, body.event_to_deliver.payload);
      }

      const step1 = await this.workflowCtx.do("allocate-sandbox", async () => {
        return { sandboxId: `sbx-${this.task.taskId}`, allocatedAtEpoch: this.task.fence.epoch };
      });
      const step2 = await this.workflowCtx.waitForEvent("sandbox.ready");
      const step3 = await this.workflowCtx.do("execute-agent-turn", async () => {
        return { executed: true, model: this.task.primaryModel };
      });
      await this.workflowCtx.sleep("post-turn-cooldown", body.sleep_duration_ms ?? 5_000);

      return new Response(
        JSON.stringify({
          task_id: this.task.taskId,
          workflow_steps: {
            step1,
            step2,
            step3,
            sleepInfo: { hibernatedIsolate: this.workflowCtx.isolateHibernated },
          },
          cached_step_count: this.workflowCtx.completedSteps.size,
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    if (request.method === "POST" && url.pathname.endsWith("/replay")) {
      const body = (await request.json()) as { target_turn_seq: number };
      const history = this.nodeDaemon.walStore.get(this.task.taskId) ?? [];
      const rep = this.replayer.replayToTurn(this.task.taskId, history, body.target_turn_seq);
      return new Response(
        JSON.stringify({
          task_id: rep.taskId,
          replayed_up_to_turn: rep.replayedUpToTurn,
          tokens_billed_during_replay: rep.tokensBilledDuringReplay,
          restored_workspace_hash: rep.restoredWorkspaceHash,
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    if (request.method === "POST" && url.pathname.endsWith("/failover")) {
      const body = (await request.json()) as {
        new_node_id: string;
        node_incarnation?: string;
        capacity?: NodeResourceVector;
        region?: string;
        resource_pool?: string;
      };
      const execution = this.taskCore.get(this.task.taskId);
      if (!execution) return json({ error: "TASK_NOT_INITIALIZED" }, 409);
      if (execution.current_node_id && execution.current_dispatch_id) {
        await this.callNodeCapacity(execution.current_node_id, `/reservations/${encodeURIComponent(execution.current_dispatch_id)}/suspect`, "POST", { reason: "heartbeat-loss-investigation" });
      }
      await this.taskCore.markSuspect(this.task.taskId, execution.business_epoch, "heartbeat-loss-investigation");
      const region = body.region ?? execution.region;
      const resourcePool = body.resource_pool ?? execution.resource_pool;
      const scheduler = persistenceCellStub(this.env, "scheduler-pool", schedulerPoolCellKey(region, resourcePool));
      const requestCandidate = {
        task_id: this.task.taskId,
        tenant_id: execution.tenant_id,
        dispatch_id: `recovery:${this.task.taskId}:${execution.business_epoch + 1}`,
        node_id: body.new_node_id,
        node_incarnation: body.node_incarnation,
        region,
        resource_pool: resourcePool,
        kind: "recovery",
        task_spec: JSON.parse(execution.task_spec_json),
        execution_policy: JSON.parse(execution.execution_policy_json),
        resources: demandForTask(this.task),
      };
      const response = await scheduler.fetch("http://scheduler.internal/scheduler/dispatch", {
        method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(requestCandidate),
      });
      const dispatchResult = await response.json() as any;
      if (!response.ok || dispatchResult.stage !== "AUTHORIZED") {
        return json({ error: dispatchResult.error ?? dispatchResult.status ?? "RECOVERY_DISPATCH_BLOCKED", dispatch: dispatchResult }, response.status >= 400 ? response.status : 409);
      }
      const newFence = new FenceToken(Number(dispatchResult.business_epoch), this.taskCore.get(this.task.taskId)!.last_turn_seq + 1, this.task.taskId);
      this.task.fence = newFence;
      this.applyTaskExecution(this.taskCore.get(this.task.taskId)!);
      return new Response(
        JSON.stringify({
          task_id: this.task.taskId,
          new_epoch: newFence.epoch,
          business_epoch: newFence.epoch,
          next_turn_seq: newFence.turnSeq,
          assigned_node_id: this.task.assignedNodeId,
          node_id: body.new_node_id,
          state: "STARTING",
          region,
          resource_pool: resourcePool,
          dispatch_id: dispatchResult.dispatch_id,
          node_incarnation: dispatchResult.node_incarnation,
          authorization_token: dispatchResult.authorization_token,
        }),
        { status: 202, headers: { "content-type": "application/json" } },
      );
    }

      return json({ error: "NotFound" }, 404);
  }

  private async executeDurableTurn(
    request: Request,
    body: {
      epoch?: number;
      turn_seq?: number;
      is_read_only_turn?: boolean;
      consecutive_tool_errors?: number;
      tool_call?: DurableToolCallRequest & { requires_heavy_container?: boolean };
      workspace_merkle_hash?: string;
      snapshot_upload_id?: string;
      messages?: Array<{ role: "system" | "user" | "assistant"; content: string }>;
      max_output_tokens?: number;
      progress?: unknown;
      pass_rate?: number;
      error_count?: number;
      diff_entropy?: number;
    },
  ): Promise<Response> {
    const url = new URL(request.url);
    const execution = this.requireTenantExecution(url.searchParams.get("tenant_id"));
    const fence = new FenceToken(Number(body.epoch), Number(body.turn_seq), execution.task_id);
    let messages: Array<{ role: "system" | "user" | "assistant"; content: string }>;
    try {
      messages = normalizeChatMessages(body.messages);
    } catch (error) {
      throw new TaskTurnError("TURN_MESSAGES_INVALID", error instanceof Error ? error.message : String(error), 400);
    }
    const uploadId = String(body.snapshot_upload_id ?? "").trim();
    if (!uploadId) throw new TaskTurnError("SNAPSHOT_UPLOAD_REQUIRED", "snapshot_upload_id is required before a metered Turn can start.", 400);

    const requestDigest = (await digestJson({
      taskId: execution.task_id,
      tenantId: execution.tenant_id,
      epoch: fence.epoch,
      turnSeq: fence.turnSeq,
      snapshotUploadId: uploadId,
      messages,
      isReadOnlyTurn: Boolean(body.is_read_only_turn),
      consecutiveToolErrors: body.consecutive_tool_errors ?? 0,
      maxOutputTokens: body.max_output_tokens ?? null,
      toolCall: body.tool_call ?? null,
      workspaceMerkleHash: body.workspace_merkle_hash ?? null,
      passRate: body.pass_rate ?? null,
      errorCount: body.error_count ?? null,
      diffEntropy: body.diff_entropy ?? null,
      progress: body.progress ?? null,
    })).sha256;

    const existingTurn = this.taskTurns.getTurn(execution.task_id, fence.turnSeq);
    if (existingTurn) {
      if (existingTurn.requestDigest !== requestDigest) {
        throw new TaskTurnError("TASK_TURN_REQUEST_CONFLICT", `Turn ${fence.turnSeq} was already committed with different input.`, 409);
      }
      return json({ ...(existingTurn.response as Record<string, unknown>), turn_committed: true, replayed: true });
    }

    assertBusinessEpoch(execution, fence.epoch, fence.turnSeq);
    this.applyTaskExecution(execution);
    this.leaseCoord.restoreFence(
      this.task.taskId,
      execution.current_node_id ?? this.currentOwnerNodeId,
      this.task.assignedModelPool ?? this.task.primaryModel,
      new FenceToken(execution.business_epoch, execution.last_turn_seq, this.task.taskId),
    );
    await this.taskTurns.assertSnapshotReady({ uploadId, taskId: execution.task_id, tenantId: execution.tenant_id, fence });

    const callId = `turn:${fence.turnSeq}:model:0`;
    const executor = createTaskMeteredProviderCallExecutor(this.env, execution.tenant_id, this.modelCallResultJournal(fence.turnSeq));
    const routed = await this.cogRouter.routeMeteredTurn(this.task, fence, {
      callId,
      periodKey: currentBillingPeriodKey(),
      requestedMaxOutputTokens: body.max_output_tokens,
      payload: messages,
      isReadOnlyTurn: Boolean(body.is_read_only_turn),
      consecutiveToolErrors: body.consecutive_tool_errors ?? 0,
    }, executor);

    if (routed.metering.status !== "SETTLED") {
      return json({
        task_id: execution.task_id,
        turn_seq: fence.turnSeq,
        model_call: routed.metering,
        outcome: routed.metering.status,
        turn_committed: false,
      }, routed.metering.status === "NOT_STARTED" ? 429 : 503);
    }
    if (routed.metering.response === undefined) {
      return json({ error: "MODEL_RESULT_UNAVAILABLE", outcome: "UNKNOWN", call_id: callId, turn_committed: false }, 503);
    }

    let containerBurstTriggered = false;
    let toolResult: Record<string, unknown> | null = null;
    if (body.tool_call) {
      if (body.tool_call.requires_heavy_container && this.ctx.container) {
        await this.ctx.container.start();
        containerBurstTriggered = true;
      }
      let operationResult: { status: number; body: unknown };
      try {
        // One tool slot per Turn has a deterministic operation identity, independent of caller retries.
        const durableToolCall = { ...body.tool_call, tool_call_id: `turn:${fence.turnSeq}:tool:0` };
        operationResult = await this.executeToolOperation(execution, durableToolCall, fence);
      } finally {
        if (containerBurstTriggered && this.ctx.container) await this.ctx.container.stop();
      }
      toolResult = operationResult.body as Record<string, unknown>;
      if (operationResult.status !== 200) {
        return json({
          task_id: execution.task_id,
          turn_seq: fence.turnSeq,
          model_response: routed.metering.response,
          tool_result: toolResult,
          turn_committed: false,
        }, operationResult.status);
      }
    }

    const response = {
      task_id: execution.task_id,
      turn_seq: fence.turnSeq,
      routed_model: routed.routedModel,
      effective_effort: routed.effectiveEffort,
      model_response: routed.metering.response,
      usage: routed.metering.usage,
      reserved_minor: routed.metering.reservedMinor,
      actual_minor: routed.metering.actualMinor,
      price_version: routed.metering.priceVersion,
      currency: routed.metering.currency,
      container_burst_triggered: containerBurstTriggered,
      tool_result: toolResult,
      workspace_merkle_hash: body.workspace_merkle_hash,
      turn_committed: true,
    };
    const committed = await this.taskTurns.commitTurn({
      taskId: execution.task_id,
      tenantId: execution.tenant_id,
      fence,
      uploadId,
      requestDigest,
      response,
      modelCallRefs: [callId],
      settlementRefs: [`settle:${callId}`],
      ...(body.progress === undefined ? {} : { progress: body.progress }),
    });
    const committedExecution = this.taskCore.get(execution.task_id);
    if (committedExecution) this.applyTaskExecution(committedExecution);
    await this.ctx.storage.setAlarm(Date.now() + 20000);
    return json({ ...(committed.response as Record<string, unknown>), turn_id: committed.turnId, snapshot_manifest_sha256: committed.manifestSha256 });
  }

  private modelCallResultJournal(turnSeq: number): ProviderCallResultJournal {
    return {
      getModelCallResult: async ({ taskId, tenantId, callId }) => {
        try {
          const record = await this.taskTurns.getConfirmedModelCallResult(taskId, callId);
          if (record.tenantId !== tenantId) throw new TaskTurnError("MODEL_CALL_TENANT_MISMATCH", "Model call result belongs to another tenant.", 404);
          return {
            response: record.response,
            usage: record.usage as import("../persistence/provider-quota").ProviderUsageEvidence,
            routeRefs: record.routeRefs as Record<string, unknown>,
          };
        } catch (error) {
          if (error && typeof error === "object" && "code" in error && error.code === "MODEL_CALL_RESULT_MISSING") return null;
          throw error;
        }
      },
      recordModelCallResult: async (input) => this.taskTurns.recordModelCallResult({ ...input, turnSeq }),
    };
  }

  public async webSocketMessage(ws: WebSocketLike, message: string): Promise<void> {
    const parsed = JSON.parse(message) as { type: string; credits?: number };
    if (parsed.type === "CREDIT_ACK" && typeof parsed.credits === "number") {
      this.clientCreditWindow = Math.min(100, this.clientCreditWindow + parsed.credits);
      ws.send(
        JSON.stringify({
          type: "CREDIT_WINDOW_UPDATED",
          task_id: this.task.taskId,
          available_credits: this.clientCreditWindow,
        }),
      );
    }
  }

  public async alarm(): Promise<void> {
    const execution = this.taskCore.get(this.task.taskId);
    this.task.state = TaskState.HIBERNATED;
    this.task.phase = SandboxPhase.IDLE;
    this.task.activeSessions = 0;
    if (this.ctx.container && this.ctx.container.running) {
      await this.ctx.container.stop();
    }
    if (execution && execution.lifecycle_state === "RUNNING") {
      await this.taskCore.suspend(this.task.taskId, execution.business_epoch, JSON.parse(execution.progress_json));
    }
  }

  private applyTaskExecution(execution: NonNullable<ReturnType<TaskCore["get"]>>): void {
    this.task.tenantId = execution.tenant_id;
    const taskSpec = parseTaskSpec(execution.task_spec_json);
    const cognitivePolicy = taskSpec?.spec?.cognitive_policy ?? taskSpec?.cognitive_policy ?? {};
    const quotaPolicy = taskSpec?.spec?.quota_and_dag_policy ?? taskSpec?.quota_and_dag_policy ?? {};
    this.task.primaryModel = modelPool(cognitivePolicy.primary_model) ?? modelPool(this.env.PRIMARY_MODEL_POOL) ?? "DeepSeek";
    this.task.fastUtilityModel = modelPool(cognitivePolicy.fast_utility_model) ?? modelPool(this.env.FAST_UTILITY_POOL) ?? "GLM-5.3";
    const configuredFallbacks = Array.isArray(cognitivePolicy.fallback_models)
      ? cognitivePolicy.fallback_models.map(modelPool).filter((value: ModelPoolName | undefined): value is ModelPoolName => value !== undefined)
      : [];
    const envFallback = modelPool(this.env.FALLBACK_MODEL_POOL);
    this.task.fallbackModels = [...new Set(configuredFallbacks.length > 0 ? configuredFallbacks : [envFallback ?? "Qwen-3.8", this.task.fastUtilityModel])];
    this.task.effort = reasoningEffort(cognitivePolicy.default_reasoning_effort) ?? this.task.effort;
    this.task.maxTotalTokens = positiveSafeInteger(quotaPolicy.max_total_tokens) ?? this.task.maxTotalTokens;
    this.task.fence = new FenceToken(execution.business_epoch, execution.last_turn_seq + 1, this.task.taskId);
    this.task.assignedNodeId = execution.current_node_id ?? undefined;
    this.task.state = taskState(execution.lifecycle_state);
    this.task.phase = execution.lifecycle_state === "RUNNING" || execution.lifecycle_state === "STARTING"
      ? SandboxPhase.COGNITIVE
      : SandboxPhase.IDLE;
    this.leaseCoord.restoreFence(
      this.task.taskId,
      execution.current_node_id ?? this.currentOwnerNodeId,
      this.task.assignedModelPool ?? this.task.primaryModel,
      new FenceToken(execution.business_epoch, execution.last_turn_seq, this.task.taskId),
    );
  }

  private requireTenantExecution(tenantId: string | null): NonNullable<ReturnType<TaskCore["get"]>> {
    if (!tenantId) throw new TaskOperationError("TENANT_REQUIRED", "tenant_id is required.", 400);
    const execution = this.taskCore.get(this.task.taskId);
    if (!execution) throw new TaskCoreError("TASK_NOT_INITIALIZED", "Task has not been initialized.", 404);
    if (execution.tenant_id !== tenantId) throw new TaskOperationError("TASK_OPERATION_NOT_FOUND", "Task was not found for this tenant.", 404);
    return execution;
  }

  private async executeToolOperation(
    execution: NonNullable<ReturnType<TaskCore["get"]>>,
    request: DurableToolCallRequest,
    fenceOverride?: FenceToken,
  ): Promise<{ status: number; body: unknown }> {
    const toolName = String(request.tool_name ?? "");
    const policy = taskToolPolicy(execution.task_spec_json, toolName);
    if (request.effect_class !== undefined && request.effect_class !== policy.effect_class) {
      throw new TaskOperationError("TASK_TOOL_POLICY_MISMATCH", "Tool effect class must match the immutable TaskSpec policy.", 403);
    }
    if (request.compensation_tool !== undefined && request.compensation_tool !== policy.compensation_tool) {
      throw new TaskOperationError("TASK_TOOL_POLICY_MISMATCH", "Compensation tool must match the immutable TaskSpec policy.", 403);
    }
    const toolCallId = String(request.tool_call_id ?? "");
    if (toolCallId.trim().length === 0) throw new TaskOperationError("TASK_TOOL_CALL_ID_REQUIRED", "tool_call_id is required.", 400);
    const fence = fenceOverride ?? new FenceToken(Number(request.epoch), Number(request.turn_seq), execution.task_id);
    const args = Object.prototype.hasOwnProperty.call(request, "args") ? request.args : request.payload;
    if (args === undefined) throw new TaskOperationError("TASK_TOOL_ARGUMENTS_REQUIRED", "args is required.", 400);
    let race: { raceId: string; branchId: string } | undefined;
    if (request.race) {
      const raceId = String(request.race.race_id ?? "");
      const branchId = String(request.race.branch_id ?? "");
      if (!raceId.trim() || !branchId.trim() || !this.env.DAG_CELL) {
        throw new TaskOperationError("TASK_OPERATION_RACE_AUTHORITY_UNAVAILABLE", "A durable DagCell and race branch identity are required for raced operations.", 503);
      }
      const dagStub = this.env.DAG_CELL.get(this.env.DAG_CELL.idFromName(raceId));
      const authorityResponse = await dagStub.fetch("http://dag.internal/dags/branch-authority", {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ tenant_id: execution.tenant_id, task_id: execution.task_id, race_id: raceId, branch_id: branchId }),
      });
      if (!authorityResponse.ok) {
        let authority: any = {};
        try { authority = await authorityResponse.json(); } catch { /* stable fallback */ }
        throw new TaskOperationError(String(authority.error ?? "TASK_OPERATION_RACE_AUTHORITY_DENIED"),
          String(authority.message ?? "DagCell did not authorize this task branch."), authorityResponse.status);
      }
      race = { raceId, branchId };
    }
    const operationRequest = {
      tenantId: execution.tenant_id,
      fence,
      toolName,
      effectClass: policy.effect_class,
      args,
      ...(policy.effect_class === ToolEffectClass.SAGA_COMPENSATABLE
        ? { compensation: { toolName: String(policy.compensation_tool), args } }
        : {}),
      ...(policy.effect_class === ToolEffectClass.STAGED_COMMIT
        ? { approval: { scope: taskApprovalScope(policy) } }
        : {}),
      ...(race ? { race } : {}),
    };
    const prepared = await this.taskOperations.prepareOperation({
      ...operationRequest,
      operationId: stableTaskOperationId(execution.task_id, toolCallId),
    });
    if (prepared.status === "APPROVAL_REQUIRED") {
      return { status: 202, body: { status: prepared.status, operation: prepared } };
    }
    if (!this.env.TASK_OPERATION_EXECUTOR) {
      return {
        status: 503,
        body: { error: "TASK_OPERATION_EXECUTOR_UNAVAILABLE", status: prepared.status, operation: prepared },
      };
    }
    const result = await this.sagaGw.executeDurableTool({
      ...operationRequest,
      toolCallId,
      provider: new CloudflareTaskOperationProvider(this.env.TASK_OPERATION_EXECUTOR, execution.tenant_id, execution.task_id),
    });
    return { status: result.status === "SUCCEEDED" || result.status === "FAILED" ? 200 : 202, body: result };
  }

  private async assertCurrentNodeReservation(body: TaskDispatchRequest): Promise<void> {
    if (!body.node_id || !body.reservation_id) throw new TaskCoreError("TASK_RESERVATION_INVALID", "Node and reservation identity are required.");
    const result = await this.callNodeCapacity(body.node_id, `/reservations/${encodeURIComponent(body.dispatch_id)}`, "GET");
    if (!result.ok) throw new TaskCoreError("NODE_RESERVATION_UNAVAILABLE", "NodeCapacity reservation is unavailable.", 503);
    const reservation = result.body;
    if (
      reservation.dispatch_id !== body.dispatch_id || reservation.task_id !== body.task_id ||
      reservation.node_incarnation !== body.node_incarnation || reservation.status !== "RESERVED" ||
      reservation.reservation_token !== body.reservation_token
    ) throw new TaskCoreError("TASK_RESERVATION_INVALID", "TaskCell authorization requires the current matching NodeCapacity reservation.");
  }

  private async callNodeCapacity(nodeId: string, path: string, method: string, body?: unknown): Promise<{ ok: boolean; body: any }> {
    const namespace = this.env.NODE_CAPACITY_CELL;
    if (!namespace) throw new TaskCoreError("NODE_CAPACITY_UNAVAILABLE", "NODE_CAPACITY_CELL binding is required.", 503);
    const stub = namespace.get(namespace.idFromName(nodeId));
    const response = await stub.fetch(`http://node-capacity.internal${path}`, {
      method,
      headers: body === undefined ? undefined : { "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    let parsed: any = null;
    try { parsed = await response.json(); } catch { parsed = null; }
    return { ok: response.ok, body: parsed };
  }
}

function taskState(status: string): TaskState {
  switch (status) {
    case "RUNNING": case "STARTING": return TaskState.RUNNING;
    case "RESERVED": return TaskState.PROVISIONING;
    case "SUSPENDED": return TaskState.HIBERNATED;
    case "SUSPECT": return TaskState.RECOVERING;
    case "SUCCEEDED": return TaskState.SUCCEEDED;
    case "FAILED": return TaskState.CIRCUIT_BROKEN;
    default: return TaskState.QUEUED;
  }
}

function parseTaskSpec(taskSpecJson: string): Record<string, any> {
  try {
    const value = JSON.parse(taskSpecJson);
    if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("TaskSpec must be an object.");
    return value as Record<string, any>;
  } catch (error) {
    throw new TaskCoreError("TASK_SPEC_INVALID", error instanceof Error ? error.message : String(error), 500);
  }
}

function modelPool(value: unknown): ModelPoolName | undefined {
  return value === "DeepSeek" || value === "GLM-5.3" || value === "Qwen-3.8" ? value : undefined;
}

function reasoningEffort(value: unknown): ReasoningEffort | undefined {
  return value === "minimal" || value === "low" || value === "medium" || value === "high" || value === "max"
    ? value
    : undefined;
}

function positiveSafeInteger(value: unknown): number | undefined {
  return Number.isSafeInteger(value) && Number(value) > 0 ? Number(value) : undefined;
}

function currentBillingPeriodKey(now: number = Date.now()): string {
  const date = new Date(now);
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}`;
}

interface DurableToolCallRequest {
  tool_call_id?: string;
  tool_name?: string;
  args?: unknown;
  payload?: unknown;
  epoch?: number;
  turn_seq?: number;
  effect_class?: ToolEffectClass;
  compensation_tool?: string;
  race?: { race_id?: string; branch_id?: string };
}

interface PersistedTaskToolPolicy {
  tool_name: string;
  effect_class: ToolEffectClass;
  compensation_tool?: string;
  require_human_approval?: boolean;
  [key: string]: unknown;
}

function taskToolPolicy(taskSpecJson: string, toolName: string): PersistedTaskToolPolicy {
  if (typeof toolName !== "string" || toolName.trim().length === 0) {
    throw new TaskOperationError("TASK_TOOL_NAME_REQUIRED", "tool_name is required.", 400);
  }
  let policies: unknown;
  try {
    policies = (JSON.parse(taskSpecJson) as any)?.spec?.tools_and_saga_config?.transactional_policies;
  } catch {
    throw new TaskOperationError("TASK_SPEC_INVALID", "Persisted TaskSpec is not valid JSON.", 500);
  }
  if (!Array.isArray(policies)) {
    throw new TaskOperationError("TASK_TOOL_POLICY_NOT_FOUND", `TaskSpec has no transactional policy for ${toolName}.`, 403);
  }
  const matches = policies.filter((policy) => policy && policy.tool_name === toolName);
  if (matches.length !== 1) {
    throw new TaskOperationError(
      matches.length === 0 ? "TASK_TOOL_POLICY_NOT_FOUND" : "TASK_TOOL_POLICY_AMBIGUOUS",
      matches.length === 0 ? `TaskSpec has no transactional policy for ${toolName}.` : `TaskSpec has multiple policies for ${toolName}.`,
      403,
    );
  }
  const policy = matches[0] as PersistedTaskToolPolicy;
  if (!Object.values(ToolEffectClass).includes(policy.effect_class)) {
    throw new TaskOperationError("TASK_TOOL_POLICY_INVALID", `TaskSpec policy for ${toolName} has an invalid effect class.`, 500);
  }
  if (policy.effect_class === ToolEffectClass.SAGA_COMPENSATABLE &&
      (typeof policy.compensation_tool !== "string" || policy.compensation_tool.trim().length === 0)) {
    throw new TaskOperationError("TASK_TOOL_POLICY_INVALID", `TaskSpec policy for ${toolName} lacks its compensation tool.`, 500);
  }
  return policy;
}

function taskApprovalScope(policy: PersistedTaskToolPolicy): unknown {
  return { task_tool_policy: policy };
}

function demandForTask(task: AgentTask): NodeResourceVector {
  const demand = task.demandVector();
  return {
    cpu: demand.steadyCpu,
    memory_mb: demand.steadyMemMb,
    burst_cpu: demand.burstCpu,
    sessions: demand.sessions,
    tps: demand.effectiveTps,
  };
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}
