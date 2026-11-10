"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.StaleFenceError = exports.LeaseCoordinator = void 0;


var _models = require("./models.js"); /**
 * @fileoverview M2 TASK-07 & M52-1: Monotonic Fencing Token & Dual-Tuple `FenceToken` Lease Coordinator with `StaleFenceError` (HTTP 409) Split-Brain Rejection.
 */class StaleFenceError extends Error {
  statusCode = 409;
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
    this.leaseTtlSeconds = leaseTtlSeconds;
  }

  activeLeaseCount() {
    return this.activeLeases.size;
  }

  /**
   * [M52-1] Restores monotonic fencing watermark on cell/node cold-start so rehydrated tasks never regress fencing tokens.
   */
  seedHighestFencingToken(taskId, token) {
    const curr = this.highestTokenByTask.get(taskId) ?? 0;
    if (token > curr) {
      this.highestTokenByTask.set(taskId, token);
      this.dualTupleFencesByTask.set(taskId, new _models.FenceToken(token, 0, taskId));
    }
  }

  acquireLease(taskId, nodeId, nowSec = Date.now() / 1000) {
    const prevToken = this.highestTokenByTask.get(taskId) ?? 0;
    const nextToken = prevToken + 1;
    this.highestTokenByTask.set(taskId, nextToken);

    const prevDual = this.dualTupleFencesByTask.get(taskId);
    const nextDual = prevDual ? prevDual.nextEpoch() : new _models.FenceToken(nextToken, 0, taskId);
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

  advanceTurnFence(taskId, callerFence) {
    const current = this.getDualTupleFence(taskId);
    if (callerFence.isStrictlyOlderThan(current)) {
      throw new StaleFenceError(taskId, callerFence, current);
    }
    const next = callerFence.nextTurn();
    this.dualTupleFencesByTask.set(taskId, next);
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