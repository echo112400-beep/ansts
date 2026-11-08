"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.AgentTaskHarnessCell = void 0; /**
 * @fileoverview M7 TASK-27: AgentTaskHarnessCell (Per-Task Stateful Actor with R2/RustFS LTX Snapshot Sync).
 */










class AgentTaskHarnessCell {
  ctx;
  r2Bucket;
  taskId;
  highestFencingToken = 0;

  constructor(ctx, r2Bucket, taskId = "task-default") {
    this.ctx = ctx;
    this.r2Bucket = r2Bucket;
    this.taskId = taskId;
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS harness_turns (turn_index INTEGER PRIMARY KEY, fencing_token INTEGER, tool_name TEXT, output_json TEXT, r2_key TEXT)"
    );
  }

  async executeAndCheckpointTurn(params)




  {
    if (params.fencingToken < this.highestFencingToken) {
      throw new Error(
        `Stale fencing token ${params.fencingToken} < highest ${this.highestFencingToken} in AgentTaskHarnessCell`
      );
    }
    this.highestFencingToken = params.fencingToken;

    const r2Key = `cells/${this.taskId}/turn-${params.turnIndex}-f${params.fencingToken}.ltx`;
    const payloadStr = JSON.stringify({
      taskId: this.taskId,
      turnIndex: params.turnIndex,
      fencingToken: params.fencingToken,
      toolOutput: params.toolOutput
    });

    await this.r2Bucket.put(r2Key, payloadStr, {
      customMetadata: {
        taskId: this.taskId,
        turnIndex: String(params.turnIndex),
        fencingToken: String(params.fencingToken)
      }
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
      r2SnapshotKey: r2Key
    };
  }

  /**
   * M16 TASK-81: Auto-Healing Container Execution across Cell Migration (`OperationInterruptedError` recovery).
   */
  async executeInCompanionContainerWithAutoHeal(
  action,
  onContainerRestartAndRehydrate)
  {
    try {
      const output = await action();
      return { output, recoveredFromMigrationInterrupt: false };
    } catch (err) {
      if (err.name === "OperationInterruptedError" || err.message.includes("OperationInterruptedError")) {
        const history = this.getTurnHistory();
        const latestTurn = history.length > 0 ? history[history.length - 1] : null;
        await onContainerRestartAndRehydrate(latestTurn);
        const retryOutput = await action();
        return { output: retryOutput, recoveredFromMigrationInterrupt: true };
      }
      throw err;
    }
  }

  getTurnHistory() {
    const rows = this.ctx.storage.sql.
    exec("SELECT * FROM harness_turns").
    toArray();
    return rows.map((r) => ({
      turnIndex: Number(r.turn_index),
      fencingToken: Number(r.fencing_token),
      toolName: String(r.tool_name),
      toolOutput: JSON.parse(String(r.output_json ?? "{}")),
      r2SnapshotKey: String(r.r2_key)
    }));
  }
}exports.AgentTaskHarnessCell = AgentTaskHarnessCell;