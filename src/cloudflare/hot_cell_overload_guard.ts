/**
 * @fileoverview M16 TASK-77: denoland/celld Hot-Cell 64-Concurrent Fetch Admission Guard (`CELLD_MAX_CELL_REQUESTS=64`)
 * & Queue Broker 256-Producer Group-Commit Controller (`503 Retry-After: 1`, `X-Celld-Overload: cell`).
 */
export interface CellAdmissionDecision {
  admitted: boolean;
  statusCode: 200 | 503;
  headers: Record<string, string>;
  body?: { error: string };
  recommendSubShardSplit: boolean;
}

export class HotCellOverloadAndQueueGuard {
  private readonly maxCellConcurrentRequests: number;
  private readonly maxQueueConcurrentProducers: number;
  private readonly maxQueueBatchCommitSize: number;
  private readonly maxOverlappingTransactions: number;
  private readonly inFlightByCell: Map<string, number> = new Map();
  private readonly overloadCountByCell: Map<string, number> = new Map();
  private activeQueueProducers = 0;

  constructor(options?: {
    maxCellConcurrentRequests?: number;
    maxQueueConcurrentProducers?: number;
    maxQueueBatchCommitSize?: number;
    maxOverlappingTransactions?: number;
  }) {
    this.maxCellConcurrentRequests = options?.maxCellConcurrentRequests ?? 64;
    this.maxQueueConcurrentProducers = options?.maxQueueConcurrentProducers ?? 256;
    this.maxQueueBatchCommitSize = options?.maxQueueBatchCommitSize ?? 64;
    this.maxOverlappingTransactions = options?.maxOverlappingTransactions ?? 4;
  }

  public tryEnterCellFetch(cellScope: string): CellAdmissionDecision {
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
    return {
      admitted: true,
      statusCode: 200,
      headers: {},
      recommendSubShardSplit: false,
    };
  }

  public leaveCellFetch(cellScope: string): void {
    const current = this.inFlightByCell.get(cellScope) ?? 0;
    if (current <= 1) {
      this.inFlightByCell.delete(cellScope);
    } else {
      this.inFlightByCell.set(cellScope, current - 1);
    }
  }

  public tryAdmitQueueProducerBatch(producerCount: number): {
    admitted: boolean;
    statusCode: 200 | 503;
    committedTransactions: number;
    groupCommitTimerMs: number;
    errorBody?: { error: string };
  } {
    if (this.activeQueueProducers + producerCount > this.maxQueueConcurrentProducers) {
      return {
        admitted: false,
        statusCode: 503,
        committedTransactions: 0,
        groupCommitTimerMs: 4,
        errorBody: { error: "cell admission refused" },
      };
    }
    this.activeQueueProducers += producerCount;
    try {
      const txNeeded = Math.ceil(producerCount / this.maxQueueBatchCommitSize);
      const committedTransactions = Math.min(this.maxOverlappingTransactions, Math.max(1, txNeeded));
      return {
        admitted: true,
        statusCode: 200,
        committedTransactions,
        groupCommitTimerMs: 4,
      };
    } finally {
      this.activeQueueProducers = Math.max(0, this.activeQueueProducers - producerCount);
    }
  }
}
