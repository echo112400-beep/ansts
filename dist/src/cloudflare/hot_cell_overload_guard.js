"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.HotCellOverloadAndQueueGuard = void 0;
class HotCellOverloadAndQueueGuard {
    maxCellConcurrentRequests;
    maxQueueConcurrentProducers;
    maxQueueBatchCommitSize;
    maxOverlappingTransactions;
    inFlightByCell = new Map();
    overloadCountByCell = new Map();
    activeLeasesByCell = new Map();
    leaseSeq = 0;
    activeQueueProducers = 0;
    constructor(options) {
        const maxCell = Number(options?.maxCellConcurrentRequests ?? 64);
        const maxProd = Number(options?.maxQueueConcurrentProducers ?? 256);
        const maxBatch = Number(options?.maxQueueBatchCommitSize ?? 64);
        const maxTx = Number(options?.maxOverlappingTransactions ?? 4);
        this.maxCellConcurrentRequests = Number.isFinite(maxCell) && maxCell > 0 ? Math.floor(maxCell) : 64;
        this.maxQueueConcurrentProducers = Number.isFinite(maxProd) && maxProd > 0 ? Math.floor(maxProd) : 256;
        this.maxQueueBatchCommitSize = Number.isFinite(maxBatch) && maxBatch > 0 ? Math.floor(maxBatch) : 64;
        this.maxOverlappingTransactions = Number.isFinite(maxTx) && maxTx > 0 ? Math.floor(maxTx) : 4;
    }
    tryEnterCellFetch(cellScope, nowMs = Date.now()) {
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
    leaveCellFetch(cellScope, leaseId) {
        const cellLeases = this.activeLeasesByCell.get(cellScope);
        if (leaseId !== undefined) {
            if (!cellLeases || !cellLeases.has(leaseId)) {
                return;
            }
            cellLeases.delete(leaseId);
            if (cellLeases.size === 0) {
                this.activeLeasesByCell.delete(cellScope);
            }
        }
        else if (cellLeases) {
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
        }
        else {
            this.inFlightByCell.set(cellScope, current - 1);
        }
    }
    /**
     * [M54-4] Reclaims leaked in-flight fetch slots whose handler hung or crashed beyond `maxFetchDurationMs`
     * so a cell never permanently locks up in HTTP 503 overload state.
     */
    reclaimStaleInFlightFetches(nowMs = Date.now(), maxFetchDurationMs = 30_000) {
        let reclaimedCount = 0;
        const affectedCells = [];
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
                }
                else {
                    this.inFlightByCell.set(cellScope, next);
                }
            }
        }
        return { reclaimedCount, affectedCells };
    }
    acknowledgeSubShardSplit(cellScope) {
        this.overloadCountByCell.delete(cellScope);
    }
    getInFlightCount(cellScope) {
        return this.inFlightByCell.get(cellScope) ?? 0;
    }
    tryAdmitQueueProducerBatch(producerCount) {
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
        }
        finally {
            this.activeQueueProducers = Math.max(0, this.activeQueueProducers - safeProducers);
        }
    }
}
exports.HotCellOverloadAndQueueGuard = HotCellOverloadAndQueueGuard;
