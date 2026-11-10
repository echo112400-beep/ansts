/**
 * @fileoverview M3 TASK-11 & M53-1: Unified Idempotent Tool Proxy & Saga Transaction Gateway
 * with Post-Rollback Idempotency Cache Invalidation.
 */
import * as crypto from "node:crypto";
import { ToolCallEffectClass, ToolExecutionLog } from "./models.js";
import { SagaCompensatorEngine, SagaStepRecord } from "./saga/compensator.js";
import { StagingCommitAndApprovalGate } from "./saga/staging.js";

export type ToolHandler = (args: Record<string, unknown>) => Record<string, unknown>;

export class SagaToolGateway {
  private readonly executionJournal: Map<string, ToolExecutionLog> = new Map();
  private readonly toolRegistry: Map<string, [ToolCallEffectClass, ToolHandler, ToolHandler | undefined]> = new Map();
  public readonly compensator: SagaCompensatorEngine;
  public readonly stagingGate: StagingCommitAndApprovalGate;

  constructor(
    compensator = new SagaCompensatorEngine(),
    stagingGate = new StagingCommitAndApprovalGate()
  ) {
    this.compensator = compensator;
    this.stagingGate = stagingGate;
    this.compensator.setOnStepCompensated((step) => {
      if (step.idempotencyKey) {
        this.executionJournal.delete(step.idempotencyKey);
      }
      this.invalidateTaskTurnsFrom(step.taskId, step.turnIndex);
    });
  }

  public registerTool(
    toolName: string,
    effectClass: ToolCallEffectClass,
    handler: ToolHandler,
    compensateHandler?: ToolHandler
  ): void {
    this.toolRegistry.set(toolName, [effectClass, handler, compensateHandler]);
  }

  public static generateIdempotencyKey(
    taskId: string,
    turnIndex: number,
    toolName: string,
    canonicalArgs: Record<string, unknown>
  ): string {
    const sortedKeys = Object.keys(canonicalArgs).sort();
    const sortedObj: Record<string, unknown> = {};
    for (const k of sortedKeys) {
      sortedObj[k] = canonicalArgs[k];
    }
    const payload = `${taskId}:${turnIndex}:${toolName}:${JSON.stringify(sortedObj)}`;
    return crypto.createHash("sha256").update(payload, "utf-8").digest("hex");
  }

  /**
   * [M53-1] Purges stale idempotency journal records for `taskId` at or after `fromTurnInclusive`
   * when a task is rolled back or time-travel forked.
   */
  public invalidateTaskTurnsFrom(taskId: string, fromTurnInclusive = 0): number {
    let invalidated = 0;
    for (const [k, log] of this.executionJournal.entries()) {
      if (log.task_id === taskId && log.turn_index >= fromTurnInclusive) {
        this.executionJournal.delete(k);
        invalidated += 1;
      }
    }
    return invalidated;
  }

  /**
   * [M53-1] Inline tool execution helper that registers or invokes a tool with idempotency caching and Saga registration.
   */
  public executeTool(params: {
    taskId: string;
    turnIndex: number;
    toolName: string;
    effectClass: ToolCallEffectClass;
    args: Record<string, unknown>;
    actionFn: () => Record<string, unknown>;
    compensationFn?: () => void;
  }): Record<string, unknown> {
    this.registerTool(
      params.toolName,
      params.effectClass,
      () => params.actionFn(),
      params.compensationFn
        ? () => {
            params.compensationFn!();
            return { compensated: true };
          }
        : undefined
    );
    const [result] = this.invokeTool(params.taskId, params.turnIndex, params.toolName, params.args);
    return result;
  }

  /**
   * [M53-1] Rolls back all Saga steps for `taskId` at or after `upToTurnInclusive` in LIFO order,
   * isolates per-step compensation errors, and invalidates corresponding `executionJournal` idempotency keys.
   */
  public rollbackAndInvalidateTask(
    taskId: string,
    upToTurnInclusive = 0
  ): {
    rolledBackSteps: SagaStepRecord[];
    invalidatedIdemKeys: string[];
    compensationErrors: Array<{ stepId: string; turnIndex: number; error: string }>;
  } {
    const invalidatedIdemKeys: string[] = [];
    for (const [k, log] of this.executionJournal.entries()) {
      if (log.task_id === taskId && log.turn_index >= upToTurnInclusive) {
        invalidatedIdemKeys.push(k);
      }
    }
    const rolledBackSteps = this.compensator.rollbackTask(taskId, upToTurnInclusive);
    this.invalidateTaskTurnsFrom(taskId, upToTurnInclusive);
    const compensationErrors = rolledBackSteps
      .filter((s) => Boolean(s.compensationError))
      .map((s) => ({
        stepId: s.stepId,
        turnIndex: s.turnIndex,
        error: s.compensationError!,
      }));
    return {
      rolledBackSteps,
      invalidatedIdemKeys,
      compensationErrors,
    };
  }

  public invokeTool(
    taskId: string,
    turnIndex: number,
    toolName: string,
    args: Record<string, unknown>
  ): [Record<string, unknown>, boolean] {
    const entry = this.toolRegistry.get(toolName);
    if (!entry) {
      throw new Error(`Tool '${toolName}' is not registered in SagaToolGateway`);
    }
    const [effectClass, handler, compensateHandler] = entry;
    const idemKey = SagaToolGateway.generateIdempotencyKey(taskId, turnIndex, toolName, args);

    if (effectClass !== ToolCallEffectClass.READ_ONLY) {
      const existing = this.executionJournal.get(idemKey);
      if (existing && existing.status === "SUCCESS") {
        return [existing.result_payload, true];
      }
    }

    const result = handler(args);
    const logEntry: ToolExecutionLog = {
      idempotency_key: idemKey,
      task_id: taskId,
      turn_index: turnIndex,
      tool_name: toolName,
      effect_class: effectClass,
      status: "SUCCESS",
      result_payload: result,
      compensation_tool: compensateHandler ? `${toolName}_compensate` : undefined,
      executed_at: Date.now() / 1000,
    };
    this.executionJournal.set(idemKey, logEntry);

    if (compensateHandler) {
      this.compensator.recordForwardStep({
        stepId: idemKey.slice(0, 16),
        taskId,
        turnIndex,
        toolName,
        idempotencyKey: idemKey,
        forwardPayload: { args, result },
        compensateHandler,
      });
    }

    return [result, false];
  }
}
