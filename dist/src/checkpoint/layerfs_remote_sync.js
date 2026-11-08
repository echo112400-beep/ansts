"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LayerFsStalePushRefusedError = exports.LayerFsRemoteStoreSynchronizer = void 0; /**
 * @fileoverview M17 TASK-88: LayerFS Store RPO=0 Remote Storage & CAS Pull/Push Synchronizer
 * (Addresses LayerFS 2.0 Issue #446 `R · Remote storage for the SQLite Store file` & `D9 · Pull/Push refusal`).
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
      throw new LayerFsStalePushRefusedError(params.basedOnGeneration, this.remoteGeneration + 1);
    }
    this.remoteGeneration = nextGen;
    this.lastKnownEtag = putRes.etag;
    return {
      remoteGeneration: this.remoteGeneration,
      r2Key,
      etag: putRes.etag
    };
  }

  async pullStoreSnapshot()



  {
    const r2Key = `layerfs-stores/${this.projectId}/store.db`;
    const obj = await this.bucket.get(r2Key);
    if (!obj) {
      return { remoteGeneration: 0, mainHeadCommitId: "C0", sizeBytes: 0 };
    }
    return {
      remoteGeneration: Number(obj.customMetadata.generation ?? this.remoteGeneration),
      mainHeadCommitId: String(obj.customMetadata.mainHeadCommitId ?? "C0"),
      sizeBytes: obj.size
    };
  }
}exports.LayerFsRemoteStoreSynchronizer = LayerFsRemoteStoreSynchronizer;