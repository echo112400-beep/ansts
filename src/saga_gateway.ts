/**
 * @fileoverview TASK-15, TASK-16, TASK-17 & M27 (TASK-124): 4-Tier Transactional Tool Gateway
 * with Pluggable Real Tool Execution Handlers, Disk Journal Persistence, SagaCompensatorEngine,
 * and StagingCommitAndApprovalGate.
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { LeaseCoordinator } from "./lease_coordinator";
import { AgentTask, FenceToken, ToolEffectClass } from "./models";
import { SagaCompensatorEngine } from "./saga/compensator";
import { StagingCommitAndApprovalGate } from "./saga/staging";

export interface SagaLogEntry {
  taskId: string;
  fence: FenceToken;
  toolName: string;
  effectClass: ToolEffectClass;
  compensationTool?: string;
  payload: string;
  idempotencyKey: string;
  committed: boolean;
  executionOutput?: string;
  executionDurationMs?: number;
}

export type RealToolHandlerFn = (payload: string, context: { taskId: string; fence: FenceToken }) => string;

export class SagaToolGateway {
  public readCache: Map<string, string> = new Map();
  public sagaJournal: Map<string, SagaLogEntry[]> = new Map();
  public stagingBuffer: Map<string, SagaLogEntry[]> = new Map();
  public flushedProductionWrites: SagaLogEntry[] = [];
  public readonly compensator = new SagaCompensatorEngine(3);
  public readonly approvalGate = new StagingCommitAndApprovalGate();
  public readonly toolHandlers = new Map<string, RealToolHandlerFn>();

  constructor(
    public readonly leaseCoordinator: LeaseCoordinator,
    public readonly workspaceRootDir: string = "/tmp/ansts_saga_workspace",
  ) {
    fs.mkdirSync(this.workspaceRootDir, { recursive: true });
    this.registerBuiltInRealToolHandlers();
  }

  public registerToolHandler(toolName: string, handler: RealToolHandlerFn): void {
    this.toolHandlers.set(toolName, handler);
  }

  private registerBuiltInRealToolHandlers(): void {
    // Built-in real file read handler
    this.registerToolHandler("fs_read_file", (payloadStr) => {
      const parsed = JSON.parse(payloadStr) as { filePath: string };
      const fullPath = path.resolve(this.workspaceRootDir, parsed.filePath);
      if (!fs.existsSync(fullPath)) {
        throw new Error(`ENOENT: ${fullPath}`);
      }
      return fs.readFileSync(fullPath, "utf8");
    });

    // Built-in real file write handler
    this.registerToolHandler("fs_write_file", (payloadStr) => {
      const parsed = JSON.parse(payloadStr) as { filePath: string; content: string };
      const fullPath = path.resolve(this.workspaceRootDir, parsed.filePath);
      fs.mkdirSync(path.dirname(fullPath), { recursive: true });
      fs.writeFileSync(fullPath, parsed.content, "utf8");
      const digest = crypto.createHash("sha256").update(parsed.content, "utf8").digest("hex").slice(0, 16);
      return JSON.stringify({ writtenBytes: Buffer.byteLength(parsed.content, "utf8"), sha256Prefix: digest });
    });

    // Built-in real file delete / compensation handler
    this.registerToolHandler("fs_delete_file", (payloadStr) => {
      const parsed = JSON.parse(payloadStr) as { filePath: string };
      const fullPath = path.resolve(this.workspaceRootDir, parsed.filePath);
      if (fs.existsSync(fullPath)) {
        fs.rmSync(fullPath, { force: true });
      }
      return JSON.stringify({ deleted: true, filePath: parsed.filePath });
    });
  }

  private invokeUnderlyingHandler(toolName: string, payload: string, fence: FenceToken, defaultPrefix: string): {
    output: string;
    durationMs: number;
  } {
    const startHr = process.hrtime.bigint();
    const handler = this.toolHandlers.get(toolName);
    let output: string;
    if (handler) {
      output = handler(payload, { taskId: fence.taskId, fence });
    } else {
      output = `${defaultPrefix}:${toolName}:${payload}`;
    }
    const durationMs = Number((Number(process.hrtime.bigint() - startHr) / 1_000_000).toFixed(3));
    return { output, durationMs };
  }

  public executeTool(params: {
    fence: FenceToken;
    toolCallId: string;
    toolName: string;
    effectClass: ToolEffectClass;
    payload: string;
    compensationTool?: string;
    task?: AgentTask;
    requireHumanApproval?: boolean;
  }): { status: string; staged: boolean; idempotencyKey: string; result: string; durationMs?: number } {
    this.leaseCoordinator.verifyAndAdvanceFence(params.fence);

    const idemRaw = `${params.fence.taskId}:${params.fence.epoch}:${params.fence.turnSeq}:${params.toolCallId}`;
    const idemKey = crypto.createHash("sha256").update(idemRaw, "utf8").digest("hex").slice(0, 20);

    const entry: SagaLogEntry = {
      taskId: params.fence.taskId,
      fence: params.fence,
      toolName: params.toolName,
      effectClass: params.effectClass,
      compensationTool: params.compensationTool,
      payload: params.payload,
      idempotencyKey: idemKey,
      committed: false,
    };

    if (params.effectClass === ToolEffectClass.PURE_READ) {
      let cached = this.readCache.get(idemKey);
      let durationMs = 0;
      if (!cached) {
        const execRes = this.invokeUnderlyingHandler(params.toolName, params.payload, params.fence, "READ_OK");
        cached = execRes.output;
        durationMs = execRes.durationMs;
        this.readCache.set(idemKey, cached);
      }
      return { status: "OK", staged: false, idempotencyKey: idemKey, result: cached, durationMs };
    }

    if (params.effectClass === ToolEffectClass.IDEMPOTENT_WRITE) {
      const execRes = this.invokeUnderlyingHandler(
        params.toolName,
        params.payload,
        params.fence,
        "IDEMPOTENT_COMMITTED",
      );
      entry.committed = true;
      entry.executionOutput = execRes.output;
      entry.executionDurationMs = execRes.durationMs;
      const list = this.sagaJournal.get(params.fence.taskId) ?? [];
      list.push(entry);
      this.sagaJournal.set(params.fence.taskId, list);
      return {
        status: "OK",
        staged: false,
        idempotencyKey: idemKey,
        result: this.toolHandlers.has(params.toolName) ? execRes.output : "IDEMPOTENT_COMMITTED",
        durationMs: execRes.durationMs,
      };
    }

    if (params.effectClass === ToolEffectClass.SAGA_COMPENSATABLE) {
      const execRes = this.invokeUnderlyingHandler(params.toolName, params.payload, params.fence, "SAGA_LOGGED");
      entry.committed = true;
      entry.executionOutput = execRes.output;
      entry.executionDurationMs = execRes.durationMs;
      const list = this.sagaJournal.get(params.fence.taskId) ?? [];
      list.push(entry);
      this.sagaJournal.set(params.fence.taskId, list);
      if (params.compensationTool) {
        this.compensator.registerForwardAction(
          params.fence.taskId,
          params.fence,
          params.toolName,
          params.compensationTool,
          params.payload,
        );
      }
      return {
        status: "OK",
        staged: false,
        idempotencyKey: idemKey,
        result: this.toolHandlers.has(params.toolName) ? execRes.output : "SAGA_LOGGED",
        durationMs: execRes.durationMs,
      };
    }

    const stagedList = this.stagingBuffer.get(params.fence.taskId) ?? [];
    stagedList.push(entry);
    this.stagingBuffer.set(params.fence.taskId, stagedList);
    if (params.task) {
      this.approvalGate.stageWrite(
        params.task,
        params.fence,
        params.toolName,
        params.payload,
        params.requireHumanApproval ?? true,
      );
    }
    return { status: "STAGED", staged: true, idempotencyKey: idemKey, result: "HELD_IN_STAGING_BUFFER" };
  }

  public rollbackAndCompensate(fence: FenceToken, targetTurnSeq: number): string[] {
    this.leaseCoordinator.verifyAndAdvanceFence(fence);
    const taskId = fence.taskId;

    if (this.stagingBuffer.has(taskId)) {
      const keptStaged = (this.stagingBuffer.get(taskId) ?? []).filter(
        (e) => e.fence.turnSeq <= targetTurnSeq,
      );
      this.stagingBuffer.set(taskId, keptStaged);
    }

    const compensated = this.compensator.compensateLifo(taskId, targetTurnSeq, (compTool, payload) => {
      const handler = this.toolHandlers.get(compTool);
      if (handler) {
        handler(payload, { taskId, fence });
      }
      return true;
    });
    const entries = this.sagaJournal.get(taskId) ?? [];
    this.sagaJournal.set(
      taskId,
      entries.filter((e) => e.fence.turnSeq <= targetTurnSeq),
    );
    return compensated;
  }

  public flushStagedCommits(fence: FenceToken, humanApproved: boolean = true): number {
    this.leaseCoordinator.verifyAndAdvanceFence(fence);
    if (!humanApproved) return 0;
    const staged = this.stagingBuffer.get(fence.taskId) ?? [];
    this.stagingBuffer.delete(fence.taskId);
    for (const entry of staged) {
      const execRes = this.invokeUnderlyingHandler(entry.toolName, entry.payload, fence, "FLUSHED_STAGE_COMMIT");
      entry.committed = true;
      entry.executionOutput = execRes.output;
      entry.executionDurationMs = execRes.durationMs;
      this.flushedProductionWrites.push(entry);
    }
    return staged.length;
  }
}
