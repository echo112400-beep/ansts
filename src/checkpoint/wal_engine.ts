/**
 * @fileoverview M5 TASK-19, M53-4 & M60-1/M60-4: Turn-Level Incremental OverlayFS Snapshot & SQLite WAL Writer (`TurnWalEngine` & `TurnWalFrame`)
 * with Dual-Tuple `(fencingToken, turnIndex)` Monotonicity, Idempotent Retry, SQLite Durability & Deterministic Breakpoint Recovery.
 */
import * as crypto from "node:crypto";
import { CheckpointRecord, FenceToken, TurnWalFrame } from "../models.js";
import { StaleFenceError } from "../lease_coordinator.js";
import { SqlStorageLike } from "../cloudflare/env.js";

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

export interface RecoveredWalBreakpoint {
  taskId: string;
  hasBreakpoint: boolean;
  restoredTurnIndex: number;
  restoredFencingToken: number;
  stateHash: string;
  overlayfsDiffUri: string;
  accumulatedFiles: string[];
  canonicalEntries: WalDeltaEntry[];
  discardedFrameIds: string[];
}

export class TurnWalEngine {
  private readonly logsByTask: Map<string, WalDeltaEntry[]> = new Map();
  private globalSeq = 0;
  private sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M60-1] Binds SQLite storage so WAL frames survive process restarts and cold-start rehydration.
   */
  public bindSqlStorage(sql: SqlStorageLike): number {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS turn_wal_entries (task_id TEXT, wal_seq INTEGER, turn_index INTEGER, fencing_token INTEGER, changed_files_json TEXT, sqlite_wal_pages INTEGER, checksum_sha256 TEXT, frame_crc32 TEXT, created_at REAL, PRIMARY KEY (task_id, wal_seq))"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM turn_wal_entries")
      .toArray()
      .sort((a, b) => Number(a.wal_seq ?? 0) - Number(b.wal_seq ?? 0));
    let restored = 0;
    for (const r of rows) {
      const taskId = String(r.task_id);
      const walSeq = Number(r.wal_seq ?? 0);
      const turnIndex = Number(r.turn_index ?? 0);
      const fencingToken = Number(r.fencing_token ?? 0);
      const changedFiles = JSON.parse(String(r.changed_files_json ?? "[]")) as string[];
      const sqliteWalPages = Number(r.sqlite_wal_pages ?? 1);
      const checksumSha256 = String(r.checksum_sha256 ?? "");
      const frameCrc32 = r.frame_crc32 ? String(r.frame_crc32) : undefined;
      const createdAt = Number(r.created_at ?? Date.now() / 1000);
      if (walSeq > this.globalSeq) {
        this.globalSeq = walSeq;
      }
      const frame = new TurnWalFrame({
        frameId: `frame-${taskId}-t${turnIndex}-s${walSeq}`,
        taskId,
        turnIndex,
        fence: new FenceToken(fencingToken, turnIndex, taskId),
        changedFiles,
        sqliteWalPages,
        crc32Hex: frameCrc32,
        createdAtMs: Math.floor(createdAt * 1000),
      });
      const checkpoint: CheckpointRecord = {
        checkpoint_id: `wal-${taskId}-t${turnIndex}-s${walSeq}`,
        task_id: taskId,
        turn_index: turnIndex,
        fencing_token: fencingToken,
        state_hash: checksumSha256.slice(0, 16),
        overlayfs_diff_uri: `s3://ansts-wal/${taskId}/turn-${turnIndex}.zst`,
        created_at: createdAt,
      };
      const entry: WalDeltaEntry = {
        walSeq,
        taskId,
        turnIndex,
        fencingToken,
        changedFiles,
        sqliteWalPages,
        checksumSha256,
        checkpoint,
        frame,
      };
      const list = this.logsByTask.get(taskId) ?? [];
      if (!list.some((e) => e.walSeq === walSeq)) {
        list.push(entry);
        this.logsByTask.set(taskId, list);
      }
      restored += 1;
    }
    return restored;
  }

  public appendTurnDelta(params: {
    taskId: string;
    turnIndex: number;
    fencingToken: number;
    changedFiles: string[];
    sqliteWalPages: number;
    highestSubmittedFencingToken: number;
    enforceTurnMonotonicity?: boolean;
    allowIdempotentRetry?: boolean;
  }): WalDeltaEntry {
    if (params.fencingToken < params.highestSubmittedFencingToken) {
      throw new StaleFenceError(
        params.taskId,
        params.fencingToken,
        params.highestSubmittedFencingToken
      );
    }

    const raw = `${params.taskId}:${params.turnIndex}:${params.fencingToken}:${params.changedFiles.join(",")}`;
    const checksumSha256 = crypto.createHash("sha256").update(raw, "utf-8").digest("hex");

    const list = this.logsByTask.get(params.taskId) ?? [];
    if (list.length > 0) {
      const lastEntry = list[list.length - 1];
      if (params.fencingToken < lastEntry.fencingToken) {
        throw new StaleFenceError(params.taskId, params.fencingToken, lastEntry.fencingToken);
      }
      // [M60-4] Idempotent retry of the exact same (turnIndex, fencingToken, checksumSha256)
      if (
        (params.allowIdempotentRetry ?? false) &&
        params.fencingToken === lastEntry.fencingToken &&
        params.turnIndex === lastEntry.turnIndex &&
        lastEntry.checksumSha256 === checksumSha256 &&
        lastEntry.sqliteWalPages === params.sqliteWalPages
      ) {
        return lastEntry;
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
    const frame = new TurnWalFrame({
      frameId: `frame-${params.taskId}-t${params.turnIndex}-s${this.globalSeq}`,
      taskId: params.taskId,
      turnIndex: params.turnIndex,
      fence: new FenceToken(params.fencingToken, params.turnIndex, params.taskId),
      changedFiles: params.changedFiles,
      sqliteWalPages: params.sqliteWalPages,
    });

    const createdAt = Date.now() / 1000;
    const checkpoint: CheckpointRecord = {
      checkpoint_id: `wal-${params.taskId}-t${params.turnIndex}-s${this.globalSeq}`,
      task_id: params.taskId,
      turn_index: params.turnIndex,
      fencing_token: params.fencingToken,
      state_hash: checksumSha256.slice(0, 16),
      overlayfs_diff_uri: `s3://ansts-wal/${params.taskId}/turn-${params.turnIndex}.zst`,
      created_at: createdAt,
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
    if (this.sql) {
      this.sql.exec(
        "INSERT OR REPLACE INTO turn_wal_entries (task_id, wal_seq, turn_index, fencing_token, changed_files_json, sqlite_wal_pages, checksum_sha256, frame_crc32, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        entry.taskId,
        entry.walSeq,
        entry.turnIndex,
        entry.fencingToken,
        JSON.stringify(entry.changedFiles),
        entry.sqliteWalPages,
        entry.checksumSha256,
        entry.frame.crc32Hex,
        createdAt
      );
    }
    return entry;
  }

  public getTaskWalEntries(taskId: string): WalDeltaEntry[] {
    return [...(this.logsByTask.get(taskId) ?? [])];
  }

  public getTaskWalFrames(taskId: string): TurnWalFrame[] {
    return (this.logsByTask.get(taskId) ?? []).map((e) => e.frame);
  }

  /**
   * [M60-4] Deterministic WAL Breakpoint Recovery:
   * Scans a task's WAL chain, discards corrupted CRC32 frames or frames exceeding `maxValidFencingToken`,
   * deduplicates overlapping turn indices by choosing the highest `(fencingToken, walSeq)`, and returns
   * the canonical recovery breakpoint so interrupted tasks resume cleanly from `restoredTurnIndex + 1`.
   */
  public recoverTaskBreakpoint(taskId: string, maxValidFencingToken?: number): RecoveredWalBreakpoint {
    const entries = this.logsByTask.get(taskId) ?? [];
    const discardedFrameIds: string[] = [];
    const canonicalByTurn = new Map<number, WalDeltaEntry>();

    for (const entry of entries) {
      const crcOk = entry.frame.verifyIntegrity();
      const fenceOk =
        maxValidFencingToken === undefined || entry.fencingToken <= maxValidFencingToken;
      if (!crcOk || !fenceOk) {
        discardedFrameIds.push(entry.frame.frameId);
        continue;
      }
      const existing = canonicalByTurn.get(entry.turnIndex);
      if (!existing) {
        canonicalByTurn.set(entry.turnIndex, entry);
      } else if (
        entry.fencingToken > existing.fencingToken ||
        (entry.fencingToken === existing.fencingToken && entry.walSeq > existing.walSeq)
      ) {
        discardedFrameIds.push(existing.frame.frameId);
        canonicalByTurn.set(entry.turnIndex, entry);
      } else {
        discardedFrameIds.push(entry.frame.frameId);
      }
    }

    const canonicalEntries = Array.from(canonicalByTurn.values()).sort(
      (a, b) => a.turnIndex - b.turnIndex
    );

    if (discardedFrameIds.length > 0) {
      this.logsByTask.set(taskId, canonicalEntries);
      if (this.sql) {
        this.sql.exec("DELETE FROM turn_wal_entries WHERE task_id = ?", taskId);
        for (const e of canonicalEntries) {
          this.sql.exec(
            "INSERT OR REPLACE INTO turn_wal_entries (task_id, wal_seq, turn_index, fencing_token, changed_files_json, sqlite_wal_pages, checksum_sha256, frame_crc32, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            e.taskId,
            e.walSeq,
            e.turnIndex,
            e.fencingToken,
            JSON.stringify(e.changedFiles),
            e.sqliteWalPages,
            e.checksumSha256,
            e.frame.crc32Hex,
            e.checkpoint.created_at
          );
        }
      }
    }

    if (canonicalEntries.length === 0) {
      return {
        taskId,
        hasBreakpoint: false,
        restoredTurnIndex: 0,
        restoredFencingToken: 0,
        stateHash: "",
        overlayfsDiffUri: "",
        accumulatedFiles: [],
        canonicalEntries: [],
        discardedFrameIds,
      };
    }

    const accumulatedFilesSet = new Set<string>();
    for (const e of canonicalEntries) {
      for (const f of e.changedFiles) {
        accumulatedFilesSet.add(f);
      }
    }
    const latest = canonicalEntries[canonicalEntries.length - 1];
    return {
      taskId,
      hasBreakpoint: true,
      restoredTurnIndex: latest.turnIndex,
      restoredFencingToken: latest.fencingToken,
      stateHash: latest.checkpoint.state_hash,
      overlayfsDiffUri: latest.checkpoint.overlayfs_diff_uri,
      accumulatedFiles: [...accumulatedFilesSet],
      canonicalEntries,
      discardedFrameIds,
    };
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
