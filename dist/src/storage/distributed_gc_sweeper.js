"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.DistributedCasGcSweeper = void 0; /**
 * @fileoverview Distributed Mark-and-Sweep CAS Object Store Garbage Collector.
 */
class DistributedCasGcSweeper {
  sweepUnreferencedBlobs(params)


  {
    const referenced = new Set();
    for (const blobs of params.activeCommitRoots.values()) {
      for (const id of blobs) referenced.add(id);
    }
    const retainedBlobIds = [];
    const sweptBlobIds = [];
    for (const id of params.allBlobIds) {
      if (referenced.has(id)) retainedBlobIds.push(id);else
      sweptBlobIds.push(id);
    }
    return { retainedBlobIds, sweptBlobIds };
  }
}exports.DistributedCasGcSweeper = DistributedCasGcSweeper;