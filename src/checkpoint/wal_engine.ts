/**
 * @fileoverview M5 TASK-19 & M53-4: Turn-Level Incremental OverlayFS Snapshot & SQLite WAL Writer (`TurnWalEngine` & `TurnWalFrame`)
 * with Dual-Tuple `(fencingToken, turnIndex)` Monotonicity & Full Chain Integrity Verification.
 */
import * as crypto from "node:crypto";
import { CheckpointRecord, FenceToken, TurnWalFrame } from "../models.js";
import { StaleFenceError } from "../lease_coordinator.js";

export interface WalDeltaEntry {
  walSeq: number;
  taskId: string;
  turnIndex: number;
  fencingToken: number;
  changedFiles: string[];
  sqliteWalPages: number;
  checksumSha256: string;
  checkpoint: CheckpointRecord;
  frame: TurnWalFrame;
}

export class TurnWalEngine {
  private readonly logsByTask: Map<string, WalDeltaEntry[]> = new Map();
  private globalSeq = 0;

  public appendTurnDelta(params: {
    taskId: string;
    turnIndex: number;
    fencingToken: number;
    changedFiles: string[];
    sqliteWalPages: number;
    highestSubmittedFencingToken: number;
    enforceTurnMonotonicity?: boolean;
  }): WalDeltaEntry {
    if (params.fencingToken < params.highestSubmittedFencingToken) {
      throw new StaleFenceError(
        params.taskId,
        params.fencingToken,
        params.highestSubmittedFencingToken
      );
    }

    const list = this.logsByTask.get(params.taskId) ?? [];
    if (list.length > 0) {
      const lastEntry = list[list.length - 1];
      if (params.fencingToken < lastEntry.fencingToken) {
        throw new StaleFenceError(params.taskId, params.fencingToken, lastEntry.fencingToken);
      }
      if (
        (params.enforceTurnMonotonicity ?? false) &&
        params.fencingToken === lastEntry.fencingToken &&
        params.turnIndex <= lastEntry.turnIndex
      ) {
        throw new StaleFenceError(
          params.taskId,
          params.fencingToken,
          lastEntry.fencingToken
        );
      }
    }

    this.globalSeq += 1;
    const raw = `${params.taskId}:${params.turnIndex}:${params.fencingToken}:${params.changedFiles.join(",")}`;
    const checksumSha256 = crypto.createHash("sha256").update(raw, "utf-8").digest("hex");

    const frame = new TurnWalFrame({
      frameId: `frame-${params.taskId}-t${params.turnIndex}-s${this.globalSeq}`,
      taskId: params.taskId,
      turnIndex: params.turnIndex,
      fence: new FenceToken(params.fencingToken, params.turnIndex, params.taskId),
      changedFiles: params.changedFiles,
      sqliteWalPages: params.sqliteWalPages,
    });

    const checkpoint: CheckpointRecord = {
      checkpoint_id: `wal-${params.taskId}-t${params.turnIndex}-s${this.globalSeq}`,
      task_id: params.taskId,
      turnIndex: params.turnIndex,
      fencing_token: params.fencingToken,
      state_hash: checksumSha256.slice(0, 16),
      overlayfs_diff_uri: `s3://ansts-wal/${params.taskId}/turn-${params.turnIndex}.zst`,
      created_at: Date.now() / 1000,
    };
    const entry: WalDeltaEntry = {
      walSeq: this.globalSeq,
      taskId: params.taskId,
      turnIndex: params.turnIndex,
      fencingToken: params.fencingToken,
      changedFiles: [...params.changedFiles],
      sqliteWalPages: params.sqliteWalPages,
      checksumSha256,
      checkpoint,
      frame,
    };
    list.push(entry);
    this.logsByTask.set(params.taskId, list);
    return entry;
  }

  public getTaskWalEntries(taskId: string): WalDeltaEntry[] {
    return [...(this.logsByTask.get(taskId) ?? [])];
  }

  public getTaskWalFrames(taskId: string): TurnWalFrame[] {
    return (this.logsByTask.get(taskId) ?? []).map((e) => e.frame);
  }

  /**
   * [M53-4] Verify full WAL chain CRC32 integrity and `(fencingToken, turnIndex)` monotonicity for a task.
   */
  public verifyTaskWalChainIntegrity(taskId: string): {
    valid: boolean;
    frameCount: number;
    corruptedFrameIds: string[];
    monotonicFenceOrdered: boolean;
  } {
    const entries = this.logsByTask.get(taskId) ?? [];
    const corruptedFrameIds: string[] = [];
    let monotonicFenceOrdered = true;

    for (let i = 0; i < entries.length; i++) {
      const cur = entries[i];
      if (!cur.frame.verifyIntegrity()) {
        corruptedFrameIds.push(cur.frame.frameId);
      }
      if (i > 0) {
        const prev = entries[i - 1];
        if (
          cur.fencingToken < prev.fencingToken ||
          (cur.fencingToken === prev.fencingToken && cur.turnIndex < prev.turnIndex)
        ) {
          monotonicFenceOrdered = false;
        }
      }
    }

    return {
      valid: corruptedFrameIds.length === 0 && monotonicFenceOrdered,
      frameCount: entries.length,
      corruptedFrameIds,
      monotonicFenceOrdered,
    };
  }
}
