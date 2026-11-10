"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TurnWalEngine = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));
var _models = require("../models.js");
var _lease_coordinator = require("../lease_coordinator.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M5 TASK-19 & M53-4: Turn-Level Incremental OverlayFS Snapshot & SQLite WAL Writer (`TurnWalEngine` & `TurnWalFrame`)
 * with Dual-Tuple `(fencingToken, turnIndex)` Monotonicity & Full Chain Integrity Verification.
 */










class TurnWalEngine {
  logsByTask = new Map();
  globalSeq = 0;

  appendTurnDelta(params)







  {
    if (params.fencingToken < params.highestSubmittedFencingToken) {
      throw new _lease_coordinator.StaleFenceError(
      params.taskId,
      params.fencingToken,
      params.highestSubmittedFencingToken);

    }

    const list = this.logsByTask.get(params.taskId) ?? [];
    if (list.length > 0) {
      const lastEntry = list[list.length - 1];
      if (params.fencingToken < lastEntry.fencingToken) {
        throw new _lease_coordinator.StaleFenceError(params.taskId, params.fencingToken, lastEntry.fencingToken);
      }
      if (
      (params.enforceTurnMonotonicity ?? false) &&
      params.fencingToken === lastEntry.fencingToken &&
      params.turnIndex <= lastEntry.turnIndex)
      {
        throw new _lease_coordinator.StaleFenceError(
        params.taskId,
        params.fencingToken,
        lastEntry.fencingToken);

      }
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
      turnIndex: params.turnIndex,
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
    list.push(entry);
    this.logsByTask.set(params.taskId, list);
    return entry;
  }

  getTaskWalEntries(taskId) {
    return [...(this.logsByTask.get(taskId) ?? [])];
  }

  getTaskWalFrames(taskId) {
    return (this.logsByTask.get(taskId) ?? []).map((e) => e.frame);
  }

  /**
   * [M53-4] Verify full WAL chain CRC32 integrity and `(fencingToken, turnIndex)` monotonicity for a task.
   */
  verifyTaskWalChainIntegrity(taskId)




  {
    const entries = this.logsByTask.get(taskId) ?? [];
    const corruptedFrameIds = [];
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
        cur.fencingToken === prev.fencingToken && cur.turnIndex < prev.turnIndex)
        {
          monotonicFenceOrdered = false;
        }
      }
    }

    return {
      valid: corruptedFrameIds.length === 0 && monotonicFenceOrdered,
      frameCount: entries.length,
      corruptedFrameIds,
      monotonicFenceOrdered
    };
  }
}exports.TurnWalEngine = TurnWalEngine;