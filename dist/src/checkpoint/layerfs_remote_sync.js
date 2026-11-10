"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LayerFsStalePushRefusedError = exports.LayerFsRemoteStoreSynchronizer = void 0; /**
 * @fileoverview M17 TASK-88 & M55-3: LayerFS Store RPO=0 Remote Storage & CAS Pull/Push Synchronizer
 * with Pull-to-Push Generation & ETag State Synchronization (eliminating post-pull Rule D9 deadlock).
 */


class LayerFsStalePushRefusedError extends Error {
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

  constructor(bucket, projectId = "proj-default") {
    this.bucket = bucket;
    this.projectId = projectId;
  }

  getRemoteGeneration() {
    return this.remoteGeneration;
  }

  async pushStoreSnapshot(params)




  {
    if (params.basedOnGeneration !== this.remoteGeneration) {
      throw new LayerFsStalePushRefusedError(params.basedOnGeneration, this.remoteGeneration);
    }
    const r2Key = `layerfs-stores/${this.projectId}/store.db`;
    const nextGen = this.remoteGeneration + 1;
    const putRes = await this.bucket.put(r2Key, params.storeSqliteBytes, {
      onlyIf: this.lastKnownEtag ? { etagMatches: this.lastKnownEtag } : { etagDoesNotMatch: "*" },
      customMetadata: {
        projectId: this.projectId,
        machineId: params.machineId,
        generation: String(nextGen),
        mainHeadCommitId: params.mainHeadCommitId
      }
    });
    if (!putRes) {
      // Refresh local awareness of remote generation if R2 CAS failed due to external writer
      const latest = await this.bucket.get(r2Key);
      const actualGen = latest ? Number(latest.customMetadata.generation ?? this.remoteGeneration + 1) : this.remoteGeneration + 1;
      throw new LayerFsStalePushRefusedError(params.basedOnGeneration, actualGen);
    }
    this.remoteGeneration = nextGen;
    this.lastKnownEtag = putRes.etag;
    return {
      remoteGeneration: this.remoteGeneration,
      r2Key,
      etag: putRes.etag
    };
  }

  /**
   * [M55-3] Pulls latest remote SQLite Store metadata and synchronizes `this.remoteGeneration`
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
      return { remoteGeneration: 0, mainHeadCommitId: "C0", sizeBytes: 0, etag: null };
    }
    const pulledGen = Number(obj.customMetadata.generation ?? this.remoteGeneration);
    this.remoteGeneration = pulledGen;
    this.lastKnownEtag = obj.etag;
    return {
      remoteGeneration: pulledGen,
      mainHeadCommitId: String(obj.customMetadata.mainHeadCommitId ?? "C0"),
      sizeBytes: obj.size,
      etag: obj.etag
    };
  }
}exports.LayerFsRemoteStoreSynchronizer = LayerFsRemoteStoreSynchronizer;