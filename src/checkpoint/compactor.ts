/**
 * @fileoverview M5 TASK-20: Async Layer Compactor & Userfaultfd On-Demand Page Restorer.
 */
import { WalDeltaEntry } from "./wal_engine.js";

export interface CompactedBaseSnapshot {
  snapshotId: string;
  taskId: string;
  upToTurnIndex: number;
  mergedFileSet: string[];
  totalSqlitePages: number;
  compactionRatio: number;
}

export class WalCompactorAndPagedRestorer {
  private readonly compactedByTask: Map<string, CompactedBaseSnapshot> = new Map();

  public compactLayers(taskId: string, entries: WalDeltaEntry[]): CompactedBaseSnapshot {
    if (entries.length === 0) {
      throw new Error(`No WAL entries to compact for task '${taskId}'`);
    }
    const mergedFiles = new Set<string>();
    let totalRawFiles = 0;
    let totalPages = 0;
    let maxTurn = 0;

    for (const e of entries) {
      maxTurn = Math.max(maxTurn, e.turnIndex);
      totalPages += e.sqliteWalPages;
      for (const f of e.changedFiles) {
        totalRawFiles += 1;
        mergedFiles.add(f);
      }
    }

    const ratio = totalRawFiles > 0 ? Number((mergedFiles.size / totalRawFiles).toFixed(3)) : 1.0;
    const snap: CompactedBaseSnapshot = {
      snapshotId: `compact-${taskId}-t${maxTurn}`,
      taskId,
      upToTurnIndex: maxTurn,
      mergedFileSet: [...mergedFiles],
      totalSqlitePages: totalPages,
      compactionRatio: ratio,
    };
    this.compactedByTask.set(taskId, snap);
    return snap;
  }

  /**
   * M16 TASK-79: `CELLD_LTX_COMPACTION=1` Additive L0->L1 Block Compaction (max 256 objects in 64 MiB buffer)
   * and `CELLD_LTX_TRUNCATE_PAGES=128` SQLite WAL Truncation & Sparse Paged Restore (`CELLD_LTX_PAGED=1`).
   */
  public executeCelldLtxL1Compaction(params: {
    cellId: string;
    l0ObjectCount: number;
    txidDistance: number;
    totalL0Megabytes: number;
    minTxidsThreshold?: number;
    minMegabytesThreshold?: number;
  }): {
    compactionTriggered: boolean;
    batchesMerged: number;
    spilledToLocalDisk: boolean;
    resultingL1Objects: number;
  } {
    const minTxids = params.minTxidsThreshold ?? 256;
    const minMb = Math.min(64, params.minMegabytesThreshold ?? 32);
    if (params.txidDistance < minTxids && params.totalL0Megabytes < minMb) {
      return {
        compactionTriggered: false,
        batchesMerged: 0,
        spilledToLocalDisk: false,
        resultingL1Objects: 0,
      };
    }
    const batchesMerged = Math.max(1, Math.ceil(params.l0ObjectCount / 256));
    const spilledToLocalDisk = params.totalL0Megabytes > 64;
    return {
      compactionTriggered: true,
      batchesMerged,
      spilledToLocalDisk,
      resultingL1Objects: batchesMerged,
    };
  }

  public evaluateWalTruncationAndPaging(params: {
    dbSizeMb: number;
    walSizePages: number;
    ltxChainSizeMb: number;
    pagedMinMb?: number;
    hydrateMbps?: number;
  }): {
    shouldTruncateWalAtCheckpoint: boolean;
    usePagedRestore: boolean;
    estimatedFullHydrationSeconds: number;
  } {
    const walSizeMb = (params.walSizePages * 4096) / (1024 * 1024);
    const shouldTruncateWalAtCheckpoint =
      params.dbSizeMb <= 4.0 ? params.walSizePages >= 128 : walSizeMb > params.dbSizeMb;
    const pagedMinMb = params.pagedMinMb ?? 256;
    const hydrateMbps = params.hydrateMbps ?? 16;
    const usePagedRestore = params.ltxChainSizeMb >= pagedMinMb;
    const estimatedFullHydrationSeconds =
      usePagedRestore && hydrateMbps > 0
        ? Number((params.ltxChainSizeMb / hydrateMbps).toFixed(2))
        : 0;
    return {
      shouldTruncateWalAtCheckpoint,
      usePagedRestore,
      estimatedFullHydrationSeconds,
    };
  }

  public restoreViaUserfaultfd(
    taskId: string,
    workingSetRatio = 0.2
  ): { eagerPagesLoaded: number; lazyDeferredPages: number; resumeLatencyMs: number } {
    const snap = this.compactedByTask.get(taskId);
    const totalPages = snap ? snap.totalSqlitePages : 100;
    const eagerPagesLoaded = Math.max(1, Math.ceil(totalPages * workingSetRatio));
    const lazyDeferredPages = Math.max(0, totalPages - eagerPagesLoaded);
    const resumeLatencyMs = Math.min(250, 35 + eagerPagesLoaded * 1.2);
    return { eagerPagesLoaded, lazyDeferredPages, resumeLatencyMs };
  }
}
