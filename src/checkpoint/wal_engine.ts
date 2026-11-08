/**
 * @fileoverview M5 TASK-19: Turn-Level Incremental OverlayFS Snapshot & SQLite WAL Writer (`TurnWalEngine` & `TurnWalFrame`).
 */
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
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
  /** Every frame persisted for a task, in append order (`appendTurnDelta` + `writeWalFrameIfNoneMatch`). */
  private readonly framesByTask: Map<string, TurnWalFrame[]> = new Map();
  /** `objectKey -> etag` of every frame persisted through `writeWalFrameIfNoneMatch`. */
  private readonly etagByObjectKey: Map<string, string> = new Map();
  /** Object keys whose conditional write was skipped because the object already existed (`If-None-Match: *`). */
  public readonly idempotentSkipObjectKeys: string[] = [];
  private globalSeq = 0;

  constructor(public readonly walDir: string = "/tmp/ansts_turnwal") {
    fs.mkdirSync(this.walDir, { recursive: true });
  }

  public appendTurnDelta(params: {
    taskId: string;
    turnIndex: number;
    fencingToken: number;
    changedFiles: string[];
    sqliteWalPages: number;
    highestSubmittedFencingToken: number;
  }): WalDeltaEntry {
    if (params.fencingToken < params.highestSubmittedFencingToken) {
      throw new StaleFenceError(
        params.taskId,
        params.fencingToken,
        params.highestSubmittedFencingToken
      );
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
      turn_index: params.turnIndex,
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
    const list = this.logsByTask.get(params.taskId) ?? [];
    list.push(entry);
    this.logsByTask.set(params.taskId, list);
    this.recordFrame(frame);
    return entry;
  }

  public getTaskWalEntries(taskId: string): WalDeltaEntry[] {
    return [...(this.logsByTask.get(taskId) ?? [])];
  }

  public getTaskWalFrames(taskId: string): TurnWalFrame[] {
    return [...(this.framesByTask.get(taskId) ?? [])];
  }

  private recordFrame(frame: TurnWalFrame): void {
    const frames = this.framesByTask.get(frame.taskId) ?? [];
    frames.push(frame);
    this.framesByTask.set(frame.taskId, frames);
  }

  /** Alias of `getTaskWalFrames` used by the TurnWAL timeline / catch-up BFF endpoints. */
  public listTaskFrames(taskId: string): TurnWalFrame[] {
    return this.getTaskWalFrames(taskId);
  }

  /**
   * Appends a `TurnWalFrame` under an `If-None-Match: *` guard (S3/RustFS CAS semantics): the frame is
   * written to `<walDir>/<taskId>_turn<turnSeq>_ep<epoch>.wal` only when the object key is still absent
   * or `ifNoneMatchEtag` differs from the persisted ETag. An already-present frame is never rewritten;
   * its object key is recorded in `idempotentSkipObjectKeys` and returned.
   */
  public writeWalFrameIfNoneMatch(frame: TurnWalFrame, ifNoneMatchEtag?: string): string {
    const objectKey = `${frame.taskId}_turn${frame.fence.turnSeq}_ep${frame.fence.epoch}.wal`;
    const filePath = path.join(this.walDir, objectKey);
    const serialized = JSON.stringify({
      frameId: frame.frameId,
      taskId: frame.taskId,
      turnIndex: frame.turnIndex,
      fence: frame.fence.toProto(),
      changedFiles: frame.changedFiles,
      sqliteWalPages: frame.sqliteWalPages,
      crc32Hex: frame.crc32Hex,
      state: frame.state,
      routedModel: frame.routedModel,
      tokensUsed: frame.tokensUsed,
      workspaceMerkleHash: frame.workspaceMerkleHash,
      diffSnapshot: frame.diffSnapshot,
      createdAtMs: frame.createdAtMs,
    });
    const etag = crypto.createHash("sha256").update(serialized, "utf-8").digest("hex");

    const existingEtag = this.etagByObjectKey.get(objectKey);
    const alreadyPresent = fs.existsSync(filePath) || existingEtag !== undefined;
    if (alreadyPresent && (ifNoneMatchEtag === undefined || ifNoneMatchEtag === existingEtag)) {
      this.idempotentSkipObjectKeys.push(objectKey);
      return objectKey;
    }

    fs.writeFileSync(filePath, serialized, "utf8");
    this.etagByObjectKey.set(objectKey, etag);
    this.recordFrame(frame);
    return objectKey;
  }
}
