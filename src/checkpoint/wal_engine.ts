/**
 * @fileoverview M5 TASK-19: Turn-Level Incremental OverlayFS Snapshot & SQLite WAL Writer (`TurnWalEngine` & `TurnWalFrame`).
 */
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { CheckpointRecord, FenceToken, TaskState, TurnWalFrame } from "../models.js";
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
    this.hydrateFramesFromDisk();
  }

  /**
   * Loads frames already persisted in `walDir` so a restarted gateway can still replay them
   * (`listTaskFrames` / `resumeAndCatchUpFromWal`) instead of reporting an empty WAL.
   */
  private hydrateFramesFromDisk(): void {
    let entries: string[];
    try {
      entries = fs.readdirSync(this.walDir);
    } catch {
      return;
    }
    const frames: TurnWalFrame[] = [];
    for (const entry of entries) {
      if (!entry.endsWith(".wal")) continue;
      try {
        const raw = fs.readFileSync(path.join(this.walDir, entry), "utf8");
        const parsed = JSON.parse(raw) as {
          frameId: string;
          taskId: string;
          turnIndex: number;
          fence: { epoch: number; turn_seq: number; task_id: string };
          changedFiles: string[];
          sqliteWalPages: number;
          crc32Hex: string;
          state: TaskState;
          routedModel: string;
          tokensUsed: number;
          workspaceMerkleHash: string;
          diffSnapshot: string;
          createdAtMs: number;
        };
        frames.push(
          new TurnWalFrame({
            frameId: parsed.frameId,
            taskId: parsed.taskId,
            turnIndex: parsed.turnIndex,
            fence: new FenceToken(parsed.fence.epoch, parsed.fence.turn_seq, parsed.fence.task_id),
            changedFiles: parsed.changedFiles ?? [],
            sqliteWalPages: parsed.sqliteWalPages ?? 0,
            crc32Hex: parsed.crc32Hex,
            state: parsed.state,
            routedModel: parsed.routedModel,
            tokensUsed: parsed.tokensUsed,
            workspaceMerkleHash: parsed.workspaceMerkleHash,
            diffSnapshot: parsed.diffSnapshot,
            createdAtMs: parsed.createdAtMs,
          }),
        );
      } catch {
        // A partially written or corrupt frame is skipped; it must not block the rest of the WAL.
      }
    }
    frames.sort((a, b) => a.fence.turnSeq - b.fence.turnSeq || a.createdAtMs - b.createdAtMs);
    for (const frame of frames) {
      const existing = this.framesByTask.get(frame.taskId) ?? [];
      const alreadyIndexed = existing.some(
        (f) => f.frameId === frame.frameId || f.fence.turnSeq === frame.fence.turnSeq,
      );
      if (alreadyIndexed) continue;
      existing.push(frame);
      this.framesByTask.set(frame.taskId, existing);
    }
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
    const existingIndex = frames.findIndex((f) => f.fence.turnSeq === frame.fence.turnSeq);
    if (existingIndex >= 0) {
      // One WAL frame per turn: a replayed/persisted frame replaces the indexed one instead of
      // appending a duplicate that catch-up replay would emit twice.
      frames[existingIndex] = frame;
    } else {
      frames.push(frame);
      frames.sort((a, b) => a.fence.turnSeq - b.fence.turnSeq);
    }
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
      // The file is already durable. Index it once so catch-up still sees the frame after a restart,
      // without appending a duplicate when this process already recorded the turn.
      const known = this.getTaskWalFrames(frame.taskId).some(
        (existing) => existing.fence.epoch === frame.fence.epoch && existing.fence.turnSeq === frame.fence.turnSeq,
      );
      if (!known) this.recordFrame(frame);
      return objectKey;
    }

    fs.writeFileSync(filePath, serialized, "utf8");
    this.etagByObjectKey.set(objectKey, etag);
    this.recordFrame(frame);
    return objectKey;
  }
}
