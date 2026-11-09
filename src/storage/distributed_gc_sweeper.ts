/**
 * @fileoverview Distributed Mark-and-Sweep CAS Object Store Garbage Collector.
 */
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
}
