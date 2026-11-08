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
    frames.push(frame);
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
      return objectKey;
    }

    fs.writeFileSync(filePath, serialized, "utf8");
    this.etagByObjectKey.set(objectKey, etag);
    this.recordFrame(frame);
    return objectKey;
  }
}exports.TurnWalEngine = TurnWalEngine;