"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.DistributedCasGcSweeper = void 0; /**
 * @fileoverview Distributed Mark-and-Sweep CAS Object Store Garbage Collector.
 * Connected to `LayerFsWorkspaceDriver` & `LayerFsContentStoreEngine` to sweep unreferenced blocks
 * from deleted/aborted branches while preserving all active commit & delta base objects.
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

  /**
   * Performs a live mark-and-sweep GC pass directly against a `LayerFsWorkspaceDriver`,
   * preserving all objects reachable from active branch commit chains (including DELTA base objects)
   * and physically deleting unreferenced orphan objects from `storeEngine`.
   */
  sweepLayerFsWorkspaceStore(driver)



  {
    const allBlobIds = driver.storeEngine.listAllObjectIds();
    const activeCommitRoots = driver.collectReachableBranchObjects();
    const { retainedBlobIds, sweptBlobIds } = this.sweepUnreferencedBlobs({
      allBlobIds,
      activeCommitRoots
    });
    let reclaimedBytes = 0;
    for (const orphanId of sweptBlobIds) {
      reclaimedBytes += driver.storeEngine.deleteObjectById(orphanId);
    }
    return { retainedBlobIds, sweptBlobIds, reclaimedBytes };
  }
}exports.DistributedCasGcSweeper = DistributedCasGcSweeper;