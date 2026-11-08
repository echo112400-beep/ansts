"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TurnWalEngine = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));
var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));
var _models = require("../models.js");
var _lease_coordinator = require("../lease_coordinator.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M5 TASK-19: Turn-Level Incremental OverlayFS Snapshot & SQLite WAL Writer (`TurnWalEngine` & `TurnWalFrame`).
 */











class TurnWalEngine {
  logsByTask = new Map();
  /** Every frame persisted for a task, in append order (`appendTurnDelta` + `writeWalFrameIfNoneMatch`). */
  framesByTask = new Map();
  /** `objectKey -> etag` of every frame persisted through `writeWalFrameIfNoneMatch`. */
  etagByObjectKey = new Map();
  /** Object keys whose conditional write was skipped because the object already existed (`If-None-Match: *`). */
  idempotentSkipObjectKeys = [];
  globalSeq = 0;

  constructor(walDir = "/tmp/ansts_turnwal") {this.walDir = walDir;
    fs.mkdirSync(this.walDir, { recursive: true });
    this.hydrateFramesFromDisk();
  }

  /**
   * Loads frames already persisted in `walDir` so a restarted gateway can still replay them
   * (`listTaskFrames` / `resumeAndCatchUpFromWal`) instead of reporting an empty WAL.
   */
  hydrateFramesFromDisk() {
    let entries;
    try {
      entries = fs.readdirSync(this.walDir);
    } catch {
      return;
    }
    const frames = [];
    for (const entry of entries) {
      if (!entry.endsWith(".wal")) continue;
      try {
        const raw = fs.readFileSync(path.join(this.walDir, entry), "utf8");
        const parsed = JSON.parse(raw);














        frames.push(
          new _models.TurnWalFrame({
            frameId: parsed.frameId,
            taskId: parsed.taskId,
            turnIndex: parsed.turnIndex,
            fence: new _models.FenceToken(parsed.fence.epoch, parsed.fence.turn_seq, parsed.fence.task_id),
            changedFiles: parsed.changedFiles ?? [],
            sqliteWalPages: parsed.sqliteWalPages ?? 0,
            crc32Hex: parsed.crc32Hex,
            state: parsed.state,
            routedModel: parsed.routedModel,
            tokensUsed: parsed.tokensUsed,
            workspaceMerkleHash: parsed.workspaceMerkleHash,
            diffSnapshot: parsed.diffSnapshot,
            createdAtMs: parsed.createdAtMs
          })
        );
      } catch {

        // A partially written or corrupt frame is skipped; it must not block the rest of the WAL.
      }}
    frames.sort((a, b) => a.fence.turnSeq - b.fence.turnSeq || a.createdAtMs - b.createdAtMs);
    for (const frame of frames) {
      const existing = this.framesByTask.get(frame.taskId) ?? [];
      const alreadyIndexed = existing.some(
        (f) => f.frameId === frame.frameId || f.fence.turnSeq === frame.fence.turnSeq
      );
      if (alreadyIndexed) continue;
      existing.push(frame);
      this.framesByTask.set(frame.taskId, existing);
    }
  }

  appendTurnDelta(params)






  {
    if (params.fencingToken < params.highestSubmittedFencingToken) {
      throw new _lease_coordinator.StaleFenceError(
        params.taskId,
        params.fencingToken,
        params.highestSubmittedFencingToken
      );
    }
    this.globalSeq += 1;
    const raw = `${params.taskId}:${params.turnIndex}:${params.fencingToken}:${params.changedFiles.join(",")}`;
    const checksumSha256 = crypto.createHash("sha256").update(raw, "utf-8").digest("hex");

    const frame = new _models.TurnWalFrame({
      frameId: `frame-${params.taskId}-t${params.turnIndex}-s${this.globalSeq}`,
      taskId: params.taskId,
      turnIndex: params.turnIndex,
      fence: new _models.FenceToken(params.fencingToken, params.turnIndex, params.taskId),
      changedFiles: params.changedFiles,
      sqliteWalPages: params.sqliteWalPages
    });

    const checkpoint = {
      checkpoint_id: `wal-${params.taskId}-t${params.turnIndex}-s${this.globalSeq}`,
      task_id: params.taskId,
      turn_index: params.turnIndex,
      fencing_token: params.fencingToken,
      state_hash: checksumSha256.slice(0, 16),
      overlayfs_diff_uri: `s3://ansts-wal/${params.taskId}/turn-${params.turnIndex}.zst`,
      created_at: Date.now() / 1000
    };
    const entry = {
      walSeq: this.globalSeq,
      taskId: params.taskId,
      turnIndex: params.turnIndex,
      fencingToken: params.fencingToken,
      changedFiles: [...params.changedFiles],
      sqliteWalPages: params.sqliteWalPages,
      checksumSha256,
      checkpoint,
      frame
    };
    const list = this.logsByTask.get(params.taskId) ?? [];
    list.push(entry);
    this.logsByTask.set(params.taskId, list);
    this.recordFrame(frame);
    return entry;
  }

  getTaskWalEntries(taskId) {
    return [...(this.logsByTask.get(taskId) ?? [])];
  }

  getTaskWalFrames(taskId) {
    return [...(this.framesByTask.get(taskId) ?? [])];
  }

  recordFrame(frame) {
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
  listTaskFrames(taskId) {
    return this.getTaskWalFrames(taskId);
  }

  /**
   * Appends a `TurnWalFrame` under an `If-None-Match: *` guard (S3/RustFS CAS semantics): the frame is
   * written to `<walDir>/<taskId>_turn<turnSeq>_ep<epoch>.wal` only when the object key is still absent
   * or `ifNoneMatchEtag` differs from the persisted ETag. An already-present frame is never rewritten;
   * its object key is recorded in `idempotentSkipObjectKeys` and returned.
   */
  writeWalFrameIfNoneMatch(frame, ifNoneMatchEtag) {
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
      createdAtMs: frame.createdAtMs
    });
    const etag = crypto.createHash("sha256").update(serialized, "utf-8").digest("hex");

    const existingEtag = this.etagByObjectKey.get(objectKey);
    const alreadyPresent = fs.existsSync(filePath) || existingEtag !== undefined;
    if (alreadyPresent && (ifNoneMatchEtag === undefined || ifNoneMatchEtag === existingEtag)) {
      this.idempotentSkipObjectKeys.push(objectKey);
      // The frame is already durable, but it still belongs in the in-memory index or catch-up replay
      // would silently drop it.
      this.recordFrame(frame);
      return objectKey;
    }

    fs.writeFileSync(filePath, serialized, "utf8");
    this.etagByObjectKey.set(objectKey, etag);
    this.recordFrame(frame);
    return objectKey;
  }
}exports.TurnWalEngine = TurnWalEngine;