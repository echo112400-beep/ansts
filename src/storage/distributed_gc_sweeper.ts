/**
 * @fileoverview Distributed Mark-and-Sweep CAS Object Store Garbage Collector.
 * Connected to `LayerFsWorkspaceDriver` & `LayerFsContentStoreEngine` to sweep unreferenced blocks
 * from deleted/aborted branches while preserving all active commit & delta base objects.
 */
import { LayerFsWorkspaceDriver } from "../runtime/layerfs_workspace_driver.js";

export class DistributedCasGcSweeper {
  public sweepUnreferencedBlobs(params: {
    allBlobIds: string[];
    activeCommitRoots: Map<string, string[]>;
  }): { retainedBlobIds: string[]; sweptBlobIds: string[] } {
    const referenced = new Set<string>();
    for (const blobs of params.activeCommitRoots.values()) {
      for (const id of blobs) referenced.add(id);
    }
    const retainedBlobIds: string[] = [];
    const sweptBlobIds: string[] = [];
    for (const id of params.allBlobIds) {
      if (referenced.has(id)) retainedBlobIds.push(id);
      else sweptBlobIds.push(id);
    }
    return { retainedBlobIds, sweptBlobIds };
  }

  /**
   * Performs a live mark-and-sweep GC pass directly against a `LayerFsWorkspaceDriver`,
   * preserving all objects reachable from active branch commit chains (including DELTA base objects)
   * and physically deleting unreferenced orphan objects from `storeEngine`.
   *
   * [M57-2] Multi-pass topological leaf-first deletion ensures that when both a derived `DELTA`/`PREFIX`
   * object and its `FULL` base object become orphans in the same GC cycle, the derived leaf is deleted
   * first so the base object is unpinned and reclaimed in the same sweep pass.
   */
  public sweepLayerFsWorkspaceStore(driver: LayerFsWorkspaceDriver): {
    retainedBlobIds: string[];
    sweptBlobIds: string[];
    pinnedOrphanBlobIds: string[];
    reclaimedBytes: number;
  } {
    const allBlobIds = driver.storeEngine.listAllObjectIds();
    const activeCommitRoots = driver.collectReachableBranchObjects();
    const { retainedBlobIds, sweptBlobIds: candidateOrphans } = this.sweepUnreferencedBlobs({
      allBlobIds,
      activeCommitRoots,
    });
    let reclaimedBytes = 0;
    const actuallySwept: string[] = [];
    let remaining = [...candidateOrphans];

    // Multi-pass leaf-first sweep: delete unpinned leaves until fixed-point convergence
    while (remaining.length > 0) {
      const nextRemaining: string[] = [];
      let progressed = false;
      for (const orphanId of remaining) {
        const bytes = driver.storeEngine.deleteObjectById(orphanId);
        if (bytes > 0) {
          reclaimedBytes += bytes;
          actuallySwept.push(orphanId);
          progressed = true;
        } else {
          nextRemaining.push(orphanId);
        }
      }
      remaining = nextRemaining;
      if (!progressed) break;
    }

    return {
      retainedBlobIds: [...retainedBlobIds, ...remaining],
      sweptBlobIds: actuallySwept,
      pinnedOrphanBlobIds: remaining,
      reclaimedBytes,
    };
  }
}
