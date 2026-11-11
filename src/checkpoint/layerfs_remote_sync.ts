/**
 * @fileoverview M17 TASK-88, M55-3, M61-5 & M65-1: LayerFS Store RPO=0 Remote Storage & CAS Pull/Push Synchronizer
 * with Pull-to-Push Generation & ETag State Synchronization, Idempotent Same-Payload Push Retry & SQLite Persistence.
 */
import * as crypto from "node:crypto";
import { R2BucketLike, SqlStorageLike } from "../cloudflare/env.js";

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
  private lastCommittedDigest: string | null = null;
  private lastCommittedMainHead: string | null = null;
  private lastCommittedIdempotencyKey: string | null = null;
  private sql?: SqlStorageLike;

  constructor(bucket: R2BucketLike, projectId = "proj-default", sql?: SqlStorageLike) {
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
  public bindSqlStorage(sql: SqlStorageLike): boolean {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS layerfs_remote_sync_state (project_id TEXT PRIMARY KEY, payload_json TEXT)"
    );
    const row = this.sql
      .exec<Record<string, unknown>>(
        "SELECT * FROM layerfs_remote_sync_state WHERE project_id = ?",
        this.projectId
      )
      .one();
    if (!row || !row.payload_json) return false;
    try {
      const parsed = JSON.parse(String(row.payload_json)) as Record<string, unknown>;
      if (parsed.remoteGeneration !== undefined) {
        this.remoteGeneration = Number(parsed.remoteGeneration);
      }
      this.lastKnownEtag = parsed.lastKnownEtag ? String(parsed.lastKnownEtag) : null;
      this.lastCommittedDigest = parsed.lastCommittedDigest ? String(parsed.lastCommittedDigest) : null;
      this.lastCommittedMainHead = parsed.lastCommittedMainHead
        ? String(parsed.lastCommittedMainHead)
        : null;
      this.lastCommittedIdempotencyKey = parsed.lastCommittedIdempotencyKey
        ? String(parsed.lastCommittedIdempotencyKey)
        : null;
      return true;
    } catch {
      return false;
    }
  }

  private persistState(): void {
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
        lastCommittedIdempotencyKey: this.lastCommittedIdempotencyKey,
      })
    );
  }

  public getRemoteGeneration(): number {
    return this.remoteGeneration;
  }

  /**
   * [M61-5 & M65-1] Supports idempotent push retry when `idempotencyKey` is provided and matches the
   * just-committed generation (`basedOnGeneration === this.remoteGeneration - 1`, matching `mainHeadCommitId`
   * and `sha256(storeSqliteBytes)`), while strictly enforcing Rule D9 on unkeyed stale pushes.
   */
  public async pushStoreSnapshot(params: {
    machineId: string;
    basedOnGeneration: number;
    storeSqliteBytes: Uint8Array;
    mainHeadCommitId: string;
    idempotencyKey?: string;
  }): Promise<{ remoteGeneration: number; r2Key: string; etag: string; deduplicated?: boolean }> {
    const r2Key = `layerfs-stores/${this.projectId}/store.db`;
    const digest = crypto.createHash("sha256").update(params.storeSqliteBytes).digest("hex");

    if (params.basedOnGeneration !== this.remoteGeneration) {
      if (
        params.idempotencyKey &&
        params.idempotencyKey === this.lastCommittedIdempotencyKey &&
        params.basedOnGeneration === this.remoteGeneration - 1 &&
        this.lastCommittedDigest === digest &&
        this.lastCommittedMainHead === params.mainHeadCommitId &&
        this.lastKnownEtag
      ) {
        return {
          remoteGeneration: this.remoteGeneration,
          r2Key,
          etag: this.lastKnownEtag,
          deduplicated: true,
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
        sha256: digest,
      },
    });
    if (!putRes) {
      const latest = await this.bucket.get(r2Key);
      const actualGen = latest
        ? Number(latest.customMetadata.generation ?? this.remoteGeneration + 1)
        : this.remoteGeneration + 1;
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
      deduplicated: false,
    };
  }

  /**
   * [M55-3 & M65-1] Pulls latest remote SQLite Store metadata and synchronizes `this.remoteGeneration`
   * and `this.lastKnownEtag` so a subsequent `pushStoreSnapshot({ basedOnGeneration: pulled.remoteGeneration })`
   * succeeds cleanly without a false Rule D9 conflict.
   */
  public async pullStoreSnapshot(): Promise<{
    remoteGeneration: number;
    mainHeadCommitId: string;
    sizeBytes: number;
    etag: string | null;
  }> {
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
      etag: obj.etag,
    };
  }
}
