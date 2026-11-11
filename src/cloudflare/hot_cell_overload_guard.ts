/**
 * @fileoverview M16 TASK-77, M54-4 & M67-2: denoland/celld Hot-Cell 64-Concurrent Fetch Admission Guard (`CELLD_MAX_CELL_REQUESTS=64`)
 * & Queue Broker 256-Producer Group-Commit Controller (`503 Retry-After: 1`, `X-Celld-Overload: cell`)
 * with Idempotent Reclaimed/Double-Release Lease Protection & Finite Producer Validation.
 */
export interface CellAdmissionDecision {
  admitted: boolean;
  statusCode: 200 | 503;
  headers: Record<string, string>;
  body?: { error: string };
  recommendSubShardSplit: boolean;
  leaseId?: string;
}

export class HotCellOverloadAndQueueGuard {
  private readonly maxCellConcurrentRequests: number;
  private readonly maxQueueConcurrentProducers: number;
  private readonly maxQueueBatchCommitSize: number;
  private readonly maxOverlappingTransactions: number;
  private readonly inFlightByCell: Map<string, number> = new Map();
  private readonly overloadCountByCell: Map<string, number> = new Map();
  private readonly activeLeasesByCell: Map<string, Map<string, number>> = new Map();
  private leaseSeq = 0;
  private activeQueueProducers = 0;

  constructor(options?: {
    maxCellConcurrentRequests?: number;
    maxQueueConcurrentProducers?: number;
    maxQueueBatchCommitSize?: number;
    maxOverlappingTransactions?: number;
  }) {
    const maxCell = Number(options?.maxCellConcurrentRequests ?? 64);
    const maxProd = Number(options?.maxQueueConcurrentProducers ?? 256);
    const maxBatch = Number(options?.maxQueueBatchCommitSize ?? 64);
    const maxTx = Number(options?.maxOverlappingTransactions ?? 4);
    this.maxCellConcurrentRequests = Number.isFinite(maxCell) && maxCell > 0 ? Math.floor(maxCell) : 64;
    this.maxQueueConcurrentProducers = Number.isFinite(maxProd) && maxProd > 0 ? Math.floor(maxProd) : 256;
    this.maxQueueBatchCommitSize = Number.isFinite(maxBatch) && maxBatch > 0 ? Math.floor(maxBatch) : 64;
    this.maxOverlappingTransactions = Number.isFinite(maxTx) && maxTx > 0 ? Math.floor(maxTx) : 4;
  }

  public tryEnterCellFetch(cellScope: string, nowMs = Date.now()): CellAdmissionDecision {
    const current = this.inFlightByCell.get(cellScope) ?? 0;
    if (current >= this.maxCellConcurrentRequests) {
      const overloads = (this.overloadCountByCell.get(cellScope) ?? 0) + 1;
      this.overloadCountByCell.set(cellScope, overloads);
      return {
        admitted: false,
        statusCode: 503,
        headers: {
          "Retry-After": "1",
          "X-Celld-Overload": "cell",
        },
        body: { error: "cell_overload_refused" },
        recommendSubShardSplit: overloads >= 3,
      };
    }
    this.inFlightByCell.set(cellScope, current + 1);
    this.leaseSeq += 1;
    const leaseId = `fetch-${cellScope}-${this.leaseSeq}`;
    let cellLeases = this.activeLeasesByCell.get(cellScope);
    if (!cellLeases) {
      cellLeases = new Map();
      this.activeLeasesByCell.set(cellScope, cellLeases);
    }
    cellLeases.set(leaseId, nowMs);
    return {
      admitted: true,
      statusCode: 200,
      headers: {},
      recommendSubShardSplit: false,
      leaseId,
    };
  }

  /**
   * [M67-2] Idempotent lease-aware release:
   * When `leaseId` is provided, verifies that `leaseId` is still active in `cellLeases` before
   * deleting it and decrementing `inFlightByCell`. If the lease was already released or reclaimed
   * by `reclaimStaleInFlightFetches()`, this call is a safe no-op and never evicts another caller's slot.
   */
  public leaveCellFetch(cellScope: string, leaseId?: string): void {
    const cellLeases = this.activeLeasesByCell.get(cellScope);
    if (leaseId !== undefined) {
      if (!cellLeases || !cellLeases.has(leaseId)) {
        return;
      }
      cellLeases.delete(leaseId);
      if (cellLeases.size === 0) {
        this.activeLeasesByCell.delete(cellScope);
      }
    } else if (cellLeases) {
      const firstKey = cellLeases.keys().next().value;
      if (firstKey !== undefined) {
        cellLeases.delete(firstKey);
      }
      if (cellLeases.size === 0) {
        this.activeLeasesByCell.delete(cellScope);
      }
    }
    const current = this.inFlightByCell.get(cellScope) ?? 0;
    if (current <= 1) {
      this.inFlightByCell.delete(cellScope);
    } else {
      this.inFlightByCell.set(cellScope, current - 1);
    }
  }

  /**
   * [M54-4] Reclaims leaked in-flight fetch slots whose handler hung or crashed beyond `maxFetchDurationMs`
   * so a cell never permanently locks up in HTTP 503 overload state.
   */
  public reclaimStaleInFlightFetches(nowMs = Date.now(), maxFetchDurationMs = 30_000): {
    reclaimedCount: number;
    affectedCells: string[];
  } {
    let reclaimedCount = 0;
    const affectedCells: string[] = [];
    for (const [cellScope, cellLeases] of Array.from(this.activeLeasesByCell.entries())) {
      let cellReclaimed = 0;
      for (const [leaseId, startedAt] of Array.from(cellLeases.entries())) {
        if (nowMs - startedAt >= maxFetchDurationMs) {
          cellLeases.delete(leaseId);
          cellReclaimed += 1;
          reclaimedCount += 1;
        }
      }
      if (cellReclaimed > 0) {
        affectedCells.push(cellScope);
        const cur = this.inFlightByCell.get(cellScope) ?? 0;
        const next = Math.max(0, cur - cellReclaimed);
        if (next === 0) {
          this.inFlightByCell.delete(cellScope);
          this.activeLeasesByCell.delete(cellScope);
        } else {
          this.inFlightByCell.set(cellScope, next);
        }
      }
    }
    return { reclaimedCount, affectedCells };
  }

  public acknowledgeSubShardSplit(cellScope: string): void {
    this.overloadCountByCell.delete(cellScope);
  }

  public getInFlightCount(cellScope: string): number {
    return this.inFlightByCell.get(cellScope) ?? 0;
  }

  public tryAdmitQueueProducerBatch(producerCount: number): {
    admitted: boolean;
    statusCode: 200 | 503;
    committedTransactions: number;
    groupCommitTimerMs: number;
    errorBody?: { error: string };
  } {
    if (!Number.isFinite(producerCount) || producerCount <= 0) {
      return {
        admitted: false,
        statusCode: 503,
        committedTransactions: 0,
        groupCommitTimerMs: 4,
        errorBody: { error: "invalid producerCount" },
      };
    }
    const safeProducers = Math.floor(producerCount);
    if (this.activeQueueProducers + safeProducers > this.maxQueueConcurrentProducers) {
      return {
        admitted: false,
        statusCode: 503,
        committedTransactions: 0,
        groupCommitTimerMs: 4,
        errorBody: { error: "cell admission refused" },
      };
    }
    this.activeQueueProducers += safeProducers;
    try {
      const txNeeded = Math.ceil(safeProducers / this.maxQueueBatchCommitSize);
      const committedTransactions = Math.min(this.maxOverlappingTransactions, Math.max(1, txNeeded));
      return {
        admitted: true,
        statusCode: 200,
        committedTransactions,
        groupCommitTimerMs: 4,
      };
    } finally {
      this.activeQueueProducers = Math.max(0, this.activeQueueProducers - safeProducers);
    }
  }
}
