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
import { listDueCellOutbox } from "../persistence/outbox";
import { persistenceCellStub } from "../persistence/registry";
import {
  assertBusinessEpoch,
  TaskCore,
  TaskCoreError,
  TaskDispatchRequest,
  taskCoreMigration,
} from "../persistence/task-core";
import { NodeResourceVector } from "../persistence/node-capacity";
import { schedulerPoolCellKey } from "../persistence/scheduler-pool";

export class AgentTaskHarnessCell {
  public readonly leaseCoord = new LeaseCoordinator();
  public readonly cogRouter = new CognitiveRouter(this.leaseCoord);
  public readonly sagaGw = new SagaToolGateway(this.leaseCoord);
  public readonly nodeDaemon = new NodeDaemon(this.leaseCoord, this.cogRouter, this.sagaGw);
  public readonly replayer = new DeterministicTimeTravelReplayer();
  public readonly durabilityEngine = new RpoZeroDurabilityEngine("bucket", 10000);
  /** Local Celld ownership lease epoch; distinct from TaskCore's business epoch. */
  public cellOwnershipEpoch: number = 0;
  public readonly workflowCtx: DurableWorkflowContext;
  public currentOwnerNodeId: string = "celld-node-01";
  public task: AgentTask;
  public clientCreditWindow: number = 100;
  public readonly taskCore: TaskCore;

  constructor(
    public readonly ctx: DurableObjectState,
    public readonly env: CloudflareCelldEnv,
  ) {
    const taskId = ctx.id.name ?? ctx.id.toString();
    initializeCellProtocol(this.ctx.storage.sql, { type: "task", key: taskId }, [taskCoreMigration]);
    this.taskCore = new TaskCore(this.ctx.storage);
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

    if (request.method === "GET" && url.pathname === "/execution/state") {
      const execution = this.taskCore.get(this.task.taskId);
      return execution ? json(execution) : json({ error: "TASK_NOT_INITIALIZED" }, 404);
    }

    if (request.method === "GET" && url.pathname === "/execution/start-commands") {
      const commands = listDueCellOutbox(this.ctx.storage.sql)
        .filter((event) => event.aggregateId === this.task.taskId && event.eventType === "task.execution.start_requested");
      return json({ commands });
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

    if (request.method === "POST" && url.pathname.endsWith("/turn")) {
      const body = (await request.json()) as {
        epoch?: number;
        turn_seq?: number;
        is_read_only_turn?: boolean;
        consecutive_tool_errors?: number;
        deepseek_429_open?: boolean;
        tool_call?: {
          tool_call_id: string;
          tool_name: string;
          effect_class: ToolEffectClass;
          payload: string;
          compensation_tool?: string;
          requires_heavy_container?: boolean;
        };
        workspace_merkle_hash?: string;
        pass_rate?: number;
        error_count?: number;
        diff_entropy?: number;
      };

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
          toolResult = this.sagaGw.executeTool({
            fence: incomingFence,
            toolCallId: body.tool_call.tool_call_id,
            toolName: body.tool_call.tool_name,
            effectClass: body.tool_call.effect_class,
            payload: body.tool_call.payload,
            compensationTool: body.tool_call.compensation_tool,
          });
          this.ctx.storage.sql.exec(
            "INSERT INTO saga_journal (turn_seq, epoch, tool_name, effect_class, compensation_tool, payload) VALUES (?, ?, ?, ?, ?, ?)",
            incomingFence.turnSeq,
            incomingFence.epoch,
            body.tool_call.tool_name,
            body.tool_call.effect_class,
            body.tool_call.compensation_tool ?? "",
            body.tool_call.payload,
          );
          if (containerBurstTriggered && this.ctx.container) {
            await this.ctx.container.stop();
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
