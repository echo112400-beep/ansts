/**
 * @fileoverview M17 TASK-88: LayerFS Store RPO=0 Remote Storage & CAS Pull/Push Synchronizer
 * (Addresses LayerFS 2.0 Issue #446 `R · Remote storage for the SQLite Store file` & `D9 · Pull/Push refusal`).
 */
import { R2BucketLike } from "../cloudflare/env.js";

export class LayerFsStalePushRefusedError extends Error {
  public readonly expectedRemoteGeneration: number;
  public readonly actualRemoteGeneration: number;
  constructor(expectedRemoteGeneration: number, actualRemoteGeneration: number) {
    super(
      `LayerFS push refused (Rule D9): remote Store changed since last pull (expected gen=${expectedRemoteGeneration}, actual=${actualRemoteGeneration})`
    );
    this.name = "LayerFsStalePushRefusedError";
    this.expectedRemoteGeneration = expectedRemoteGeneration;
    this.actualRemoteGeneration = actualRemoteGeneration;
  }
}

export class LayerFsRemoteStoreSynchronizer {
  private readonly bucket: R2BucketLike;
  private readonly projectId: string;
  private remoteGeneration = 0;
  private lastKnownEtag: string | null = null;

  constructor(bucket: R2BucketLike, projectId = "proj-default") {
    this.bucket = bucket;
    this.projectId = projectId;
  }

  public async pushStoreSnapshot(params: {
    machineId: string;
    basedOnGeneration: number;
    storeSqliteBytes: Uint8Array;
    mainHeadCommitId: string;
  }): Promise<{ remoteGeneration: number; r2Key: string; etag: string }> {
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
        mainHeadCommitId: params.mainHeadCommitId,
      },
    });
    if (!putRes) {
      throw new LayerFsStalePushRefusedError(params.basedOnGeneration, this.remoteGeneration + 1);
    }
    this.remoteGeneration = nextGen;
    this.lastKnownEtag = putRes.etag;
    return {
      remoteGeneration: this.remoteGeneration,
      r2Key,
      etag: putRes.etag,
    };
  }

  public async pullStoreSnapshot(): Promise<{
    remoteGeneration: number;
    mainHeadCommitId: string;
    sizeBytes: number;
  }> {
    const r2Key = `layerfs-stores/${this.projectId}/store.db`;
    const obj = await this.bucket.get(r2Key);
    if (!obj) {
      return { remoteGeneration: 0, mainHeadCommitId: "C0", sizeBytes: 0 };
    }
    return {
      remoteGeneration: Number(obj.customMetadata.generation ?? this.remoteGeneration),
      mainHeadCommitId: String(obj.customMetadata.mainHeadCommitId ?? "C0"),
      sizeBytes: obj.size,
    };
  }
}
