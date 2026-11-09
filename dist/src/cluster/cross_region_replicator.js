"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CrossRegionAsyncReplicator = void 0; /**
 * @fileoverview Cross-Region Asynchronous LTX Snapshot Replicator with RPO Lag Tracking & S3 Sync.
 */
class CrossRegionAsyncReplicator {
  replicatedByRegion = new Map();
  lastReplicationLagMs = new Map();

  replicateLtxObject(targetRegion, objectKey)


  {
    let set = this.replicatedByRegion.get(targetRegion);
    if (!set) {
      set = new Set();
      this.replicatedByRegion.set(targetRegion, set);
    }
    set.add(objectKey);
    this.lastReplicationLagMs.set(targetRegion, 0);
    return { targetRegion, replicatedCount: set.size };
  }

  async replicateToRemoteS3Region(params)











  {
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
          payload: params.payload
        })
      });

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
      s3Stored
    };
  }
}exports.CrossRegionAsyncReplicator = CrossRegionAsyncReplicator;