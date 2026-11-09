/**
 * @fileoverview M3 TASK-11: Unified Idempotent Tool Proxy & Saga Transaction Gateway.
 */
import * as crypto from "node:crypto";
import { ToolCallEffectClass, ToolExecutionLog } from "./models.js";
import { SagaCompensatorEngine } from "./saga/compensator.js";
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
        forwardPayload: { args, result },
        compensateHandler,
      });
    }

    return [result, false];
  }
}
