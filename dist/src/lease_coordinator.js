"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.StaleFenceError = exports.LeaseCoordinator = void 0;



var _models = require("./models.js"); /**
 * @fileoverview M2 TASK-07, M52-1 & M68-3: Monotonic Fencing Token & Dual-Tuple `FenceToken` Lease Coordinator
 * with `StaleFenceError` (HTTP 409) Split-Brain Rejection, `advanceTurnFence` Epoch Watermark Synchronization & Completed-Task Fence Purge.
 */class StaleFenceError extends Error {statusCode = 409;
  taskId;
  attemptedFence;
  currentHighestFence;

  constructor(taskId, attemptedFence, currentHighestFence) {
    const att =
    attemptedFence instanceof _models.FenceToken ?
    `(${attemptedFence.epoch},${attemptedFence.turnSeq})` :
    String(attemptedFence);
    const cur =
    currentHighestFence instanceof _models.FenceToken ?
    `(${currentHighestFence.epoch},${currentHighestFence.turnSeq})` :
    String(currentHighestFence);
    super(
    `StaleFenceError [409]: Fencing token ${att} rejected for task ${taskId} (highest=${cur})`);

    this.name = "StaleFenceError";
    this.taskId = taskId;
    this.attemptedFence = att;
    this.currentHighestFence = cur;
  }
}exports.StaleFenceError = StaleFenceError;

class LeaseCoordinator {
  leaseTtlSeconds;
  activeLeases = new Map();
  highestTokenByTask = new Map();
  dualTupleFencesByTask = new Map();

  constructor(leaseTtlSeconds = 15.0) {
    this.leaseTtlSeconds =
    Number.isFinite(leaseTtlSeconds) && leaseTtlSeconds > 0 ? leaseTtlSeconds : 15.0;
  }

  activeLeaseCount() {
    return this.activeLeases.size;
  }

  /**
   * [M52-1] Restores monotonic fencing watermark on cell/node cold-start so rehydrated tasks never regress fencing tokens.
   */
  rehydrateFencingWatermark(taskId, token) {
    this.seedHighestFencingToken(taskId, token);
  }

  seedHighestFencingToken(taskId, token) {
    if (!Number.isFinite(token) || token <= 0) return;
    const safeToken = Math.floor(token);
    const curr = this.highestTokenByTask.get(taskId) ?? 0;
    if (safeToken > curr) {
      this.highestTokenByTask.set(taskId, safeToken);
      const prevDual = this.dualTupleFencesByTask.get(taskId);
      if (!prevDual || prevDual.epoch < safeToken) {
        this.dualTupleFencesByTask.set(taskId, new _models.FenceToken(safeToken, 0, taskId));
      }
    }
  }

  acquireLease(taskId, nodeId, nowSec = Date.now() / 1000) {
    const prevDual = this.dualTupleFencesByTask.get(taskId);
    const prevToken = Math.max(this.highestTokenByTask.get(taskId) ?? 0, prevDual?.epoch ?? 0);
    const nextToken = prevToken + 1;
    this.highestTokenByTask.set(taskId, nextToken);

    const nextDual = new _models.FenceToken(nextToken, 0, taskId);
    this.dualTupleFencesByTask.set(taskId, nextDual);

    const now = nowSec;
    const record = {
      task_id: taskId,
      node_id: nodeId,
      fencing_token: nextToken,
      acquired_at: now,
      expires_at: now + this.leaseTtlSeconds
    };
    this.activeLeases.set(taskId, record);
    return record;
  }

  getDualTupleFence(taskId) {
    return (
      this.dualTupleFencesByTask.get(taskId) ??
      new _models.FenceToken(this.highestTokenByTask.get(taskId) ?? 1, 0, taskId));

  }

  getHighestFencingToken(taskId) {
    return this.highestTokenByTask.get(taskId) ?? 0;
  }

  /**
   * [M68-3] Advances the turn-sequence component of the dual-tuple fence and synchronizes
   * `highestTokenByTask` whenever `callerFence.epoch` exceeds the recorded scalar watermark,
   * preventing subsequent `acquireLease` / `validateFencingToken` calls from regressing to a stale epoch.
   */
  advanceTurnFence(taskId, callerFence) {
    const current = this.getDualTupleFence(taskId);
    if (callerFence.isStrictlyOlderThan(current)) {
      throw new StaleFenceError(taskId, callerFence, current);
    }
    const next = callerFence.nextTurn();
    this.dualTupleFencesByTask.set(taskId, next);
    const prevHighest = this.highestTokenByTask.get(taskId) ?? 0;
    if (next.epoch > prevHighest) {
      this.highestTokenByTask.set(taskId, next.epoch);
    }
    return next;
  }

  assertValidFence(taskId, fencingTokenOrDual) {
    if (fencingTokenOrDual instanceof _models.FenceToken) {
      const current = this.getDualTupleFence(taskId);
      if (fencingTokenOrDual.isStrictlyOlderThan(current)) {
        throw new StaleFenceError(taskId, fencingTokenOrDual, current);
      }
      return;
    }
    if (!this.validateFencingToken(taskId, fencingTokenOrDual)) {
      const highest = this.highestTokenByTask.get(taskId) ?? 0;
      throw new StaleFenceError(taskId, fencingTokenOrDual, highest);
    }
  }

  renewLease(taskId, nodeId, fencingToken, nowSec = Date.now() / 1000) {
    const lease = this.activeLeases.get(taskId);
    if (!lease) return false;
    if (lease.node_id !== nodeId || lease.fencing_token !== fencingToken) {
      return false;
    }
    lease.expires_at = nowSec + this.leaseTtlSeconds;
    return true;
  }

  validateFencingToken(taskId, fencingToken) {
    const highest = this.highestTokenByTask.get(taskId) ?? 0;
    const active = this.activeLeases.get(taskId);
    if (!active) return false;
    return fencingToken === highest && active.fencing_token === fencingToken;
  }

  revokeLease(taskId) {
    this.activeLeases.delete(taskId);
  }

  /**
   * [M68-3] Purges all lease and fencing watermark records for a permanently archived/purged task.
   */
  purgeCompletedTaskFence(taskId) {
    const hadAny =
    this.activeLeases.has(taskId) ||
    this.highestTokenByTask.has(taskId) ||
    this.dualTupleFencesByTask.has(taskId);
    this.activeLeases.delete(taskId);
    this.highestTokenByTask.delete(taskId);
    this.dualTupleFencesByTask.delete(taskId);
    return hadAny;
  }

  getLease(taskId) {
    return this.activeLeases.get(taskId);
  }

  scanExpiredLeases(nowSec = Date.now() / 1000) {
    const expired = [];
    for (const [taskId, lease] of this.activeLeases.entries()) {
      if (nowSec >= lease.expires_at) {
        expired.push(lease);
        this.activeLeases.delete(taskId);
      }
    }
    return expired;
  }
}exports.LeaseCoordinator = LeaseCoordinator;