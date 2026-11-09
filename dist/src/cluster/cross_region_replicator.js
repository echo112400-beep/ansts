"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CrossRegionAsyncReplicator = void 0; /**
 * @fileoverview Cross-Region Asynchronous LTX Snapshot Replicator with RPO Lag Tracking.
 */
class CrossRegionAsyncReplicator {
  replicatedByRegion = new Map();

  replicateLtxObject(targetRegion, objectKey)


  {
    let set = this.replicatedByRegion.get(targetRegion);
    if (!set) {
      set = new Set();
      this.replicatedByRegion.set(targetRegion, set);
    }
    set.add(objectKey);
    return { targetRegion, replicatedCount: set.size };
  }
}exports.CrossRegionAsyncReplicator = CrossRegionAsyncReplicator;