"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TurnWalEngine = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));
var _models = require("../models.js");
var _lease_coordinator = require("../lease_coordinator.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M5 TASK-19: Turn-Level Incremental OverlayFS Snapshot & SQLite WAL Writer (`TurnWalEngine` & `TurnWalFrame`).
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
    const list = this.logsByTask.get(params.taskId) ?? [];
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
}exports.TurnWalEngine = TurnWalEngine;