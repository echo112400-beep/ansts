"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LayerFsStalePushRefusedError = exports.LayerFsRemoteStoreSynchronizer = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M17 TASK-88, M55-3, M61-5 & M65-1: LayerFS Store RPO=0 Remote Storage & CAS Pull/Push Synchronizer
 * with Pull-to-Push Generation & ETag State Synchronization, Idempotent Same-Payload Push Retry & SQLite Persistence.
 */class LayerFsStalePushRefusedError extends Error {
  expectedRemoteGeneration;
  actualRemoteGeneration;
  constructor(expectedRemoteGeneration, actualRemoteGeneration) {
    super(
    `LayerFS push refused (Rule D9): remote Store changed since last pull (expected gen=${expectedRemoteGeneration}, actual=${actualRemoteGeneration})`);

    this.name = "LayerFsStalePushRefusedError";
    this.expectedRemoteGeneration = expectedRemoteGeneration;
    this.actualRemoteGeneration = actualRemoteGeneration;
  }
}exports.LayerFsStalePushRefusedError = LayerFsStalePushRefusedError;

class LayerFsRemoteStoreSynchronizer {
  bucket;
  projectId;
  remoteGeneration = 0;
  lastKnownEtag = null;
  lastCommittedDigest = null;
  lastCommittedMainHead = null;
  lastCommittedIdempotencyKey = null;
  sql;

  constructor(bucket, projectId = "proj-default", sql) {
    this.bucket = bucket;
    this.projectId = projectId;
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M65-1] Binds SQLite storage (`layerfs_remote_sync_state`) so `remoteGeneration`, `lastKnownEtag`,
   * `lastCommittedDigest`, `lastCommittedMainHead`, and `lastCommittedIdempotencyKey` survive restarts.
   */
  bindSqlStorage(sql) {
    this.sql = sql;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS layerfs_remote_sync_state (project_id TEXT PRIMARY KEY, payload_json TEXT)");

    const row = this.sql.
    exec(
    "SELECT * FROM layerfs_remote_sync_state WHERE project_id = ?",
    this.projectId).

    one();
    if (!row || !row.payload_json) return false;
    try {
      const parsed = JSON.parse(String(row.payload_json));
      if (parsed.remoteGeneration !== undefined) {
        this.remoteGeneration = Number(parsed.remoteGeneration);
      }
      this.lastKnownEtag = parsed.lastKnownEtag ? String(parsed.lastKnownEtag) : null;
      this.lastCommittedDigest = parsed.lastCommittedDigest ? String(parsed.lastCommittedDigest) : null;
      this.lastCommittedMainHead = parsed.lastCommittedMainHead ?
      String(parsed.lastCommittedMainHead) :
      null;
      this.lastCommittedIdempotencyKey = parsed.lastCommittedIdempotencyKey ?
      String(parsed.lastCommittedIdempotencyKey) :
      null;
      return true;
    } catch {
      return false;
    }
  }

  persistState() {
    if (!this.sql) return;
    this.sql.exec(
    "INSERT OR REPLACE INTO layerfs_remote_sync_state (project_id, payload_json) VALUES (?, ?)",
    this.projectId,
    JSON.stringify({
      projectId: this.projectId,
      remoteGeneration: this.remoteGeneration,
      lastKnownEtag: this.lastKnownEtag,
      lastCommittedDigest: this.lastCommittedDigest,
      lastCommittedMainHead: this.lastCommittedMainHead,
      lastCommittedIdempotencyKey: this.lastCommittedIdempotencyKey
    }));

  }

  getRemoteGeneration() {
    return this.remoteGeneration;
  }

  /**
   * [M61-5 & M65-1] Supports idempotent push retry when `idempotencyKey` is provided and matches the
   * just-committed generation (`basedOnGeneration === this.remoteGeneration - 1`, matching `mainHeadCommitId`
   * and `sha256(storeSqliteBytes)`), while strictly enforcing Rule D9 on unkeyed stale pushes.
   */
  async pushStoreSnapshot(params)





  {
    const r2Key = `layerfs-stores/${this.projectId}/store.db`;
    const digest = crypto.createHash("sha256").update(params.storeSqliteBytes).digest("hex");

    if (params.basedOnGeneration !== this.remoteGeneration) {
      if (
      params.idempotencyKey &&
      params.idempotencyKey === this.lastCommittedIdempotencyKey &&
      params.basedOnGeneration === this.remoteGeneration - 1 &&
      this.lastCommittedDigest === digest &&
      this.lastCommittedMainHead === params.mainHeadCommitId &&
      this.lastKnownEtag)
      {
        return {
          remoteGeneration: this.remoteGeneration,
          r2Key,
          etag: this.lastKnownEtag,
          deduplicated: true
        };
      }
      throw new LayerFsStalePushRefusedError(params.basedOnGeneration, this.remoteGeneration);
    }

    const nextGen = this.remoteGeneration + 1;
    const putRes = await this.bucket.put(r2Key, params.storeSqliteBytes, {
      onlyIf: this.lastKnownEtag ? { etagMatches: this.lastKnownEtag } : { etagDoesNotMatch: "*" },
      customMetadata: {
        projectId: this.projectId,
        machineId: params.machineId,
        generation: String(nextGen),
        mainHeadCommitId: params.mainHeadCommitId,
        sha256: digest
      }
    });
    if (!putRes) {
      const latest = await this.bucket.get(r2Key);
      const actualGen = latest ?
      Number(latest.customMetadata.generation ?? this.remoteGeneration + 1) :
      this.remoteGeneration + 1;
      throw new LayerFsStalePushRefusedError(params.basedOnGeneration, actualGen);
    }
    this.remoteGeneration = nextGen;
    this.lastKnownEtag = putRes.etag;
    this.lastCommittedDigest = digest;
    this.lastCommittedMainHead = params.mainHeadCommitId;
    this.lastCommittedIdempotencyKey = params.idempotencyKey ?? null;
    this.persistState();
    return {
      remoteGeneration: this.remoteGeneration,
      r2Key,
      etag: putRes.etag,
      deduplicated: false
    };
  }

  /**
   * [M55-3 & M65-1] Pulls latest remote SQLite Store metadata and synchronizes `this.remoteGeneration`
   * and `this.lastKnownEtag` so a subsequent `pushStoreSnapshot({ basedOnGeneration: pulled.remoteGeneration })`
   * succeeds cleanly without a false Rule D9 conflict.
   */
  async pullStoreSnapshot()




  {
    const r2Key = `layerfs-stores/${this.projectId}/store.db`;
    const obj = await this.bucket.get(r2Key);
    if (!obj) {
      this.remoteGeneration = 0;
      this.lastKnownEtag = null;
      this.persistState();
      return { remoteGeneration: 0, mainHeadCommitId: "C0", sizeBytes: 0, etag: null };
    }
    const pulledGen = Number(obj.customMetadata.generation ?? this.remoteGeneration);
    this.remoteGeneration = pulledGen;
    this.lastKnownEtag = obj.etag;
    this.lastCommittedMainHead = String(obj.customMetadata.mainHeadCommitId ?? "C0");
    if (obj.customMetadata.sha256) {
      this.lastCommittedDigest = String(obj.customMetadata.sha256);
    }
    this.persistState();
    return {
      remoteGeneration: pulledGen,
      mainHeadCommitId: String(obj.customMetadata.mainHeadCommitId ?? "C0"),
      sizeBytes: obj.size,
      etag: obj.etag
    };
  }
}exports.LayerFsRemoteStoreSynchronizer = LayerFsRemoteStoreSynchronizer;