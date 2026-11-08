"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.LeaseCoordinator = void 0; /**
 * @fileoverview M2 TASK-07: Monotonic Fencing Token Lease Coordinator with Split-Brain Rejection.
 */


class LeaseCoordinator {
  leaseTtlSeconds;
  activeLeases = new Map();
  highestTokenByTask = new Map();

  constructor(leaseTtlSeconds = 15.0) {
    this.leaseTtlSeconds = leaseTtlSeconds;
  }

  acquireLease(taskId, nodeId) {
    const prevToken = this.highestTokenByTask.get(taskId) ?? 0;
    const nextToken = prevToken + 1;
    this.highestTokenByTask.set(taskId, nextToken);

    const now = Date.now() / 1000;
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

  renewLease(taskId, nodeId, fencingToken) {
    const lease = this.activeLeases.get(taskId);
    if (!lease) return false;
    if (lease.node_id !== nodeId || lease.fencing_token !== fencingToken) {
      return false;
    }
    lease.expires_at = Date.now() / 1000 + this.leaseTtlSeconds;
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