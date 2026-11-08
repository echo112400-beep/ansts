/**
 * @fileoverview M3 TASK-11: Unified Idempotent Tool Proxy & Saga Transaction Gateway.
 */
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { FenceToken, ToolCallEffectClass, ToolExecutionLog } from "./models.js";
import { SagaCompensatorEngine } from "./saga/compensator.js";
import { StagingCommitAndApprovalGate } from "./saga/staging.js";

export type ToolHandler = (args: Record<string, unknown>) => Record<string, unknown>;

export class SagaToolGateway {
  private readonly executionJournal: Map<string, ToolExecutionLog> = new Map();
  private readonly toolRegistry: Map<string, [ToolCallEffectClass, ToolHandler, ToolHandler | undefined]> = new Map();
  public readonly compensator: SagaCompensatorEngine;
  public readonly stagingGate: StagingCommitAndApprovalGate;

  private readonly workspaceDir?: string;
  private readonly compensations: Array<{ tool?: string; filePath: string }> = [];

  constructor(
    compensatorOrLease?: SagaCompensatorEngine | { acquireLease?: unknown },
    stagingGateOrWorkspace?: StagingCommitAndApprovalGate | string,
  ) {
    if (
      compensatorOrLease &&
      typeof (compensatorOrLease as { acquireLease?: unknown }).acquireLease === "function" &&
      typeof stagingGateOrWorkspace === "string"
    ) {
      this.compensator = new SagaCompensatorEngine();
      this.stagingGate = new StagingCommitAndApprovalGate();
      this.workspaceDir = stagingGateOrWorkspace;
      fs.mkdirSync(this.workspaceDir, { recursive: true });
      return;
    }
    this.compensator = (compensatorOrLease as SagaCompensatorEngine | undefined) ?? new SagaCompensatorEngine();
    this.stagingGate = (stagingGateOrWorkspace as StagingCommitAndApprovalGate | undefined) ?? new StagingCommitAndApprovalGate();
  }

  /** Filesystem tools addressed by a fencing token. Compensation is LIFO. */
  public executeTool(request: {
    fence: FenceToken;
    toolCallId: string;
    toolName: string;
    effectClass: ToolCallEffectClass | string;
    compensationTool?: string;
    payload: string;
  }): { status: "OK"; result?: string } {
    if (!this.workspaceDir) throw new Error("SagaToolGateway has no workspace directory");
    const args = JSON.parse(request.payload) as { filePath?: string; content?: string };
    const filePath = path.resolve(this.workspaceDir, args.filePath ?? "");
    if (!filePath.startsWith(path.resolve(this.workspaceDir))) {
      throw new Error("tool path escapes the workspace");
    }
    if (request.toolName === "fs_write_file") {
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, args.content ?? "", "utf8");
      this.compensations.push({ tool: request.compensationTool, filePath });
      return { status: "OK" };
    }
    if (request.toolName === "fs_read_file") {
      return { status: "OK", result: fs.readFileSync(filePath, "utf8") };
    }
    if (request.toolName === "fs_delete_file" && fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
    return { status: "OK" };
  }

  public rollbackAndCompensate(_fence: FenceToken, _fromIndex = 0): void {
    for (let i = this.compensations.length - 1; i >= 0; i -= 1) {
      const step = this.compensations[i];
      if (step.tool === "fs_delete_file" && fs.existsSync(step.filePath)) fs.unlinkSync(step.filePath);
    }
    this.compensations.length = 0;
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

    if (effectClass === ToolCallEffectClass.COMPENSATABLE && compensateHandler) {
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
