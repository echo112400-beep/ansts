"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.CrossRegionAsyncReplicator = void 0;
/**
 * @fileoverview M11 & M62-5: Cross-Region Asynchronous LTX Snapshot Replicator
 * with RPO Lag Tracking, S3 Sync, URL Sanitization, Safe HTTP Status Validation & Non-Blocking Error Isolation.
 */
class CrossRegionAsyncReplicator {
    replicatedByRegion = new Map();
    lastReplicationLagMs = new Map();
    replicateLtxObject(targetRegion, objectKey) {
        let set = this.replicatedByRegion.get(targetRegion);
        if (!set) {
            set = new Set();
            this.replicatedByRegion.set(targetRegion, set);
        }
        set.add(objectKey);
        this.lastReplicationLagMs.set(targetRegion, 0);
        return { targetRegion, replicatedCount: set.size };
    }
    /**
     * [M62-5] DFX-Maintainability & Deployability: Sanitizes targetRegion/objectKey,
     * isolates network and HTTP fetch rejections without crashing the caller, and returns
     * safe structured RPO lag and storage telemetry.
     */
    async replicateToRemoteS3Region(params) {
        const t0 = performance.now();
        let s3Stored = false;
        const safeRegion = params.targetRegion.trim();
        const safeKey = params.objectKey.trim();
        try {
            const endpoint = params.s3Endpoint.trim().replace(/\/$/, "");
            if (endpoint.startsWith("http://") || endpoint.startsWith("https://")) {
                const resp = await fetch(`${endpoint}/ansts-fleet/regions/${encodeURIComponent(safeRegion)}/${encodeURIComponent(safeKey)}.json`, {
                    method: "PUT",
                    headers: { "content-type": "application/json" },
                    body: JSON.stringify({
                        sourceRegion: params.sourceRegion,
                        targetRegion: safeRegion,
                        objectKey: safeKey,
                        replicatedAtMs: Date.now(),
                        payload: params.payload,
                    }),
                });
                s3Stored = resp.ok;
            }
        }
        catch {
            s3Stored = false;
        }
        const rpoLagMs = Number(Math.max(0, performance.now() - t0).toFixed(3));
        const local = this.replicateLtxObject(safeRegion, safeKey);
        this.lastReplicationLagMs.set(safeRegion, rpoLagMs);
        return {
            targetRegion: safeRegion,
            objectKey: safeKey,
            replicatedCount: local.replicatedCount,
            rpoLagMs,
            s3Stored,
        };
    }
    getRegionLagMs(region) {
        return this.lastReplicationLagMs.get(region) ?? 0;
    }
}
exports.CrossRegionAsyncReplicator = CrossRegionAsyncReplicator;
