/**
 * AgentTaskHarnessCell: Brain-Body Disaggregated Durable Object / celld Cell (~0.47 MB RAM).
 * - Stores Turn-WAL frames and 4-Tier Saga Journal in private SQLite (`ctx.storage.sql`, LTX RPO=0).
 * - Stays in V8 Isolate during Cognitive Phase (0 MicroVM overhead) and bursts companion container
 *   (`ctx.container`) only during Execution Phase.
 * - Supports Hibernatable WebSockets (`acceptWebSocket`, `webSocketMessage`) & `alarm()`.
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
import { CloudflareCelldEnv, DurableObjectState, WebSocketLike } from "./env";

export class AgentTaskHarnessCell {
  public readonly leaseCoord = new LeaseCoordinator();
  public readonly cogRouter = new CognitiveRouter(this.leaseCoord);
  public readonly sagaGw = new SagaToolGateway(this.leaseCoord);
  public readonly nodeDaemon = new NodeDaemon(this.leaseCoord, this.cogRouter, this.sagaGw);
  public readonly replayer = new DeterministicTimeTravelReplayer();
  public task: AgentTask;
  public clientCreditWindow: number = 100;

  constructor(
    public readonly ctx: DurableObjectState,
    public readonly env: CloudflareCelldEnv,
  ) {
    const taskId = ctx.id.name ?? ctx.id.toString();
    this.task = new AgentTask({
      taskId,
      tenantId: "fin-enterprise",
      primaryModel: (env.PRIMARY_MODEL_POOL as ModelPoolName) ?? "DeepSeek",
      fastUtilityModel: (env.FAST_UTILITY_POOL as ModelPoolName) ?? "GLM-5.3",
      fallbackModels: [
        ((env.FALLBACK_MODEL_POOL as ModelPoolName) ?? "Qwen-3.8"),
        ((env.FAST_UTILITY_POOL as ModelPoolName) ?? "GLM-5.3"),
      ],
      state: TaskState.RUNNING,
      phase: SandboxPhase.COGNITIVE,
    });
    this.task.fence = this.leaseCoord.acquireOrIncrementEpoch(taskId, "celld-node-01", this.task.primaryModel);

    // Initialize private SQLite tables replicated by celld LTX
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS turn_wal_frames (turn_seq INTEGER, epoch INTEGER, routed_model TEXT, effective_effort TEXT, tokens_used INTEGER, workspace_hash TEXT, pass_rate REAL, error_count INTEGER, diff_entropy REAL)",
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS saga_journal (turn_seq INTEGER, epoch INTEGER, tool_name TEXT, effect_class TEXT, compensation_tool TEXT, payload TEXT)",
    );
  }

  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // 1. Execute a Cognitive + Tool Turn inside the Cell (Brain-Body Disaggregation)
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
        if (body.deepseek_429_open) {
          this.cogRouter.tripPoolBreaker("DeepSeek", 0.48);
        }

        // Cognitive Phase: Runs purely inside the 0.47MB V8 Cell (Companion container stopped/idle)
        this.nodeDaemon.transitionToCognitivePhase(this.task);
        const routeRes = this.cogRouter.routeTurn(
          this.task,
          incomingFence,
          Boolean(body.is_read_only_turn),
          body.consecutive_tool_errors ?? 0,
        );

        let containerBurstTriggered = false;
        let toolResult: Record<string, any> | null = null;

        // Execution Phase: Only start/burst companion container if heavy tool execution is requested
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

        // Persist Turn-WAL to private SQLite & audit Progress Velocity V_k
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

        if (audit.action === "BACKTRACKED") {
          this.ctx.storage.sql.exec(
            "DELETE FROM turn_wal_frames WHERE turn_seq > ?",
            audit.restoredTurnSeq,
          );
        }

        // Schedule idle hibernation alarm 20s in the future
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
            sqlite_wal_rows: this.ctx.storage.sql.exec("SELECT * FROM turn_wal_frames").toArray().length,
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      } catch (err: any) {
        if (err instanceof StaleFenceError) {
          return new Response(JSON.stringify({ error: "StaleFenceRejected", message: err.message }), {
            status: 409,
            headers: { "content-type": "application/json" },
          });
        }
        throw err;
      }
    }

    // 2. Zero-Token Deterministic Time-Travel Replay from Cell SQLite WAL
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

    // 3. Failover / Epoch Bump
    if (request.method === "POST" && url.pathname.endsWith("/failover")) {
      const body = (await request.json()) as { new_node_id: string };
      const newFence = this.nodeDaemon.failoverTaskToNode(this.task, body.new_node_id);
      return new Response(
        JSON.stringify({
          task_id: this.task.taskId,
          new_epoch: newFence.epoch,
          next_turn_seq: newFence.turnSeq,
          assigned_node_id: this.task.assignedNodeId,
        }),
        { status: 200, headers: { "content-type": "application/json" } },
      );
    }

    return new Response(JSON.stringify({ error: "NotFound" }), { status: 404 });
  }

  /**
   * Hibernatable WebSocket handler (`celld` & Cloudflare Workers standard API).
   * Wakes up the Cell in ~4ms when client sends a credit ACK or command frame.
   */
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

  /**
   * Durable Object Persistent Alarm handler for idle hibernation & companion container teardown.
   */
  public async alarm(): Promise<void> {
    this.task.state = TaskState.HIBERNATED;
    this.task.phase = SandboxPhase.IDLE;
    this.task.activeSessions = 0;
    if (this.ctx.container && this.ctx.container.running) {
      await this.ctx.container.stop();
    }
  }
}
