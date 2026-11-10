/**
 * @fileoverview M7 TASK-27 & M54-1: AgentTaskHarnessCell (Per-Task Stateful Actor with R2/RustFS LTX Snapshot Sync
 * & Cold-Start SQLite Fencing Watermark Rehydration).
 */
import { DurableObjectStateLike, R2BucketLike } from "./env.js";

export interface HarnessTurnRecord {
  turnIndex: number;
  fencingToken: number;
  toolName: string;
  toolOutput: Record<string, unknown>;
  r2SnapshotKey: string;
}

export class AgentTaskHarnessCell {
  private readonly ctx: DurableObjectStateLike;
  private readonly r2Bucket: R2BucketLike;
  private readonly taskId: string;
  private highestFencingToken = 0;
  private highestTurnIndex = 0;

  constructor(ctx: DurableObjectStateLike, r2Bucket: R2BucketLike, taskId = "task-default") {
    this.ctx = ctx;
    this.r2Bucket = r2Bucket;
    this.taskId = taskId;
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS harness_turns (turn_index INTEGER PRIMARY KEY, fencing_token INTEGER, tool_name TEXT, output_json TEXT, r2_key TEXT)"
    );
    this.rehydrateWatermarksFromSql();
  }

  /**
   * [M54-1] Rehydrate `highestFencingToken` and `highestTurnIndex` from persisted `harness_turns` rows
   * on Cell cold-start or live migration so zombie workers holding stale fencing tokens cannot overwrite turns.
   */
  public rehydrateWatermarksFromSql(): { highestFencingToken: number; highestTurnIndex: number } {
    const rows = this.ctx.storage.sql
      .exec<Record<string, unknown>>("SELECT * FROM harness_turns")
      .toArray();
    for (const r of rows) {
      const fToken = Number(r.fencing_token ?? 0);
      const tIdx = Number(r.turn_index ?? 0);
      if (fToken > this.highestFencingToken) {
        this.highestFencingToken = fToken;
      }
      if (tIdx > this.highestTurnIndex) {
        this.highestTurnIndex = tIdx;
      }
    }
    return {
      highestFencingToken: this.highestFencingToken,
      highestTurnIndex: this.highestTurnIndex,
    };
  }

  public getHighestFencingToken(): number {
    return this.highestFencingToken;
  }

  public async executeAndCheckpointTurn(params: {
    turnIndex: number;
    fencingToken: number;
    toolName: string;
    toolOutput: Record<string, unknown>;
    enforceTurnMonotonicity?: boolean;
  }): Promise<HarnessTurnRecord> {
    if (params.fencingToken < this.highestFencingToken) {
      throw new Error(
        `Stale fencing token ${params.fencingToken} < highest ${this.highestFencingToken} in AgentTaskHarnessCell`
      );
    }
    if (
      (params.enforceTurnMonotonicity ?? false) &&
      params.fencingToken === this.highestFencingToken &&
      params.turnIndex <= this.highestTurnIndex
    ) {
      throw new Error(
        `Stale or duplicate turnIndex ${params.turnIndex} <= highestTurnIndex ${this.highestTurnIndex} under fencing token ${params.fencingToken}`
      );
    }
    this.highestFencingToken = params.fencingToken;
    if (params.turnIndex > this.highestTurnIndex) {
      this.highestTurnIndex = params.turnIndex;
    }

    const r2Key = `cells/${this.taskId}/turn-${params.turnIndex}-f${params.fencingToken}.ltx`;
    const payloadStr = JSON.stringify({
      taskId: this.taskId,
      turnIndex: params.turnIndex,
      fencingToken: params.fencingToken,
      toolOutput: params.toolOutput,
    });

    await this.r2Bucket.put(r2Key, payloadStr, {
      customMetadata: {
        taskId: this.taskId,
        turnIndex: String(params.turnIndex),
        fencingToken: String(params.fencingToken),
      },
    });

    this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO harness_turns (turn_index, fencing_token, tool_name, output_json, r2_key) VALUES (?, ?, ?, ?, ?)",
      params.turnIndex,
      params.fencingToken,
      params.toolName,
      JSON.stringify(params.toolOutput),
      r2Key
    );

    return {
      turnIndex: params.turnIndex,
      fencingToken: params.fencingToken,
      toolName: params.toolName,
      toolOutput: params.toolOutput,
      r2SnapshotKey: r2Key,
    };
  }

  /**
   * M16 TASK-81: Auto-Healing Container Execution across Cell Migration (`OperationInterruptedError` recovery).
   */
  public async executeInCompanionContainerWithAutoHeal(
    action: () => Promise<Record<string, unknown>>,
    onContainerRestartAndRehydrate: (latestTurn: HarnessTurnRecord | null) => Promise<void>
  ): Promise<{ output: Record<string, unknown>; recoveredFromMigrationInterrupt: boolean }> {
    try {
      const output = await action();
      return { output, recoveredFromMigrationInterrupt: false };
    } catch (err) {
      if ((err as Error).name === "OperationInterruptedError" || (err as Error).message.includes("OperationInterruptedError")) {
        const history = this.getTurnHistory();
        const latestTurn = history.length > 0 ? history[history.length - 1] : null;
        await onContainerRestartAndRehydrate(latestTurn);
        const retryOutput = await action();
        return { output: retryOutput, recoveredFromMigrationInterrupt: true };
      }
      throw err;
    }
  }

  public getTurnHistory(): HarnessTurnRecord[] {
    const rows = this.ctx.storage.sql
      .exec<Record<string, unknown>>("SELECT * FROM harness_turns")
      .toArray();
    return rows
      .map((r) => ({
        turnIndex: Number(r.turn_index),
        fencingToken: Number(r.fencing_token),
        toolName: String(r.tool_name),
        toolOutput: JSON.parse(String(r.output_json ?? "{}")) as Record<string, unknown>,
        r2SnapshotKey: String(r.r2_key),
      }))
      .sort((a, b) => a.turnIndex - b.turnIndex);
  }
}
