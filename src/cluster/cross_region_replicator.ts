/**
 * @fileoverview Cross-Region Asynchronous LTX Snapshot Replicator with RPO Lag Tracking & S3 Sync.
 */
export class CrossRegionAsyncReplicator {
  private readonly replicatedByRegion: Map<string, Set<string>> = new Map();
  private readonly lastReplicationLagMs: Map<string, number> = new Map();

  public replicateLtxObject(targetRegion: string, objectKey: string): {
    targetRegion: string;
    replicatedCount: number;
  } {
    let set = this.replicatedByRegion.get(targetRegion);
    if (!set) {
      set = new Set();
      this.replicatedByRegion.set(targetRegion, set);
    }
    set.add(objectKey);
    this.lastReplicationLagMs.set(targetRegion, 0);
    return { targetRegion, replicatedCount: set.size };
  }

  public async replicateToRemoteS3Region(params: {
    s3Endpoint: string;
    sourceRegion: string;
    targetRegion: string;
    objectKey: string;
    payload: Record<string, unknown>;
  }): Promise<{
    targetRegion: string;
    objectKey: string;
    replicatedCount: number;
    rpoLagMs: number;
    s3Stored: boolean;
  }> {
    const t0 = performance.now();
    let s3Stored = false;
    try {
      const resp = await fetch(
        `${params.s3Endpoint.replace(/\/$/, "")}/ansts-fleet/regions/${encodeURIComponent(params.targetRegion)}/${encodeURIComponent(params.objectKey)}.json`,
        {
          method: "PUT",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            sourceRegion: params.sourceRegion,
            targetRegion: params.targetRegion,
            objectKey: params.objectKey,
            replicatedAtMs: Date.now(),
            payload: params.payload,
          }),
        }
      );
      s3Stored = resp.ok;
    } catch {
      s3Stored = false;
    }
    const rpoLagMs = Number((performance.now() - t0).toFixed(3));
    const local = this.replicateLtxObject(params.targetRegion, params.objectKey);
    this.lastReplicationLagMs.set(params.targetRegion, rpoLagMs);
    return {
      targetRegion: params.targetRegion,
      objectKey: params.objectKey,
      replicatedCount: local.replicatedCount,
      rpoLagMs,
      s3Stored,
    };
  }
}
