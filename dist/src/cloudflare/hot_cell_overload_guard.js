"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.HotCellOverloadAndQueueGuard = void 0; /**
 * @fileoverview M16 TASK-77: denoland/celld Hot-Cell 64-Concurrent Fetch Admission Guard (`CELLD_MAX_CELL_REQUESTS=64`)
 * & Queue Broker 256-Producer Group-Commit Controller (`503 Retry-After: 1`, `X-Celld-Overload: cell`).
 */








class HotCellOverloadAndQueueGuard {
  maxCellConcurrentRequests;
  maxQueueConcurrentProducers;
  maxQueueBatchCommitSize;
  maxOverlappingTransactions;
  inFlightByCell = new Map();
  overloadCountByCell = new Map();
  activeQueueProducers = 0;

  constructor(options)




  {
    this.maxCellConcurrentRequests = options?.maxCellConcurrentRequests ?? 64;
    this.maxQueueConcurrentProducers = options?.maxQueueConcurrentProducers ?? 256;
    this.maxQueueBatchCommitSize = options?.maxQueueBatchCommitSize ?? 64;
    this.maxOverlappingTransactions = options?.maxOverlappingTransactions ?? 4;
  }

  tryEnterCellFetch(cellScope) {
    const current = this.inFlightByCell.get(cellScope) ?? 0;
    if (current >= this.maxCellConcurrentRequests) {
      const overloads = (this.overloadCountByCell.get(cellScope) ?? 0) + 1;
      this.overloadCountByCell.set(cellScope, overloads);
      return {
        admitted: false,
        statusCode: 503,
        headers: {
          "Retry-After": "1",
          "X-Celld-Overload": "cell"
        },
        body: { error: "cell_overload_refused" },
        recommendSubShardSplit: overloads >= 3
      };
    }
    this.inFlightByCell.set(cellScope, current + 1);
    return {
      admitted: true,
      statusCode: 200,
      headers: {},
      recommendSubShardSplit: false
    };
  }

  leaveCellFetch(cellScope) {
    const current = this.inFlightByCell.get(cellScope) ?? 0;
    if (current <= 1) {
      this.inFlightByCell.delete(cellScope);
    } else {
      this.inFlightByCell.set(cellScope, current - 1);
    }
  }

  tryAdmitQueueProducerBatch(producerCount)





  {
    if (this.activeQueueProducers + producerCount > this.maxQueueConcurrentProducers) {
      return {
        admitted: false,
        statusCode: 503,
        committedTransactions: 0,
        groupCommitTimerMs: 4,
        errorBody: { error: "cell admission refused" }
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
        groupCommitTimerMs: 4
      };
    } finally {
      this.activeQueueProducers = Math.max(0, this.activeQueueProducers - producerCount);
    }
  }
}exports.HotCellOverloadAndQueueGuard = HotCellOverloadAndQueueGuard;