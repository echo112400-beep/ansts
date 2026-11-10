/**
 * @fileoverview M2 TASK-07 & M52-1: Monotonic Fencing Token & Dual-Tuple `FenceToken` Lease Coordinator with `StaleFenceError` (HTTP 409) Split-Brain Rejection.
 */
import { FenceToken, LeaseRecord } from "./models.js";

export class StaleFenceError extends Error {
  public readonly statusCode = 409;
  public readonly taskId: string;
  public readonly attemptedFence: string;
  public readonly currentHighestFence: string;

  constructor(taskId: string, attemptedFence: FenceToken | number, currentHighestFence: FenceToken | number) {
    const att =
      attemptedFence instanceof FenceToken
        ? `(${attemptedFence.epoch},${attemptedFence.turnSeq})`
        : String(attemptedFence);
    const cur =
      currentHighestFence instanceof FenceToken
        ? `(${currentHighestFence.epoch},${currentHighestFence.turnSeq})`
        : String(currentHighestFence);
    super(
      `StaleFenceError [409]: Fencing token ${att} rejected for task ${taskId} (highest=${cur})`
    );
    this.name = "StaleFenceError";
    this.taskId = taskId;
    this.attemptedFence = att;
    this.currentHighestFence = cur;
  }
}

export class LeaseCoordinator {
  private readonly leaseTtlSeconds: number;
  private readonly activeLeases: Map<string, LeaseRecord> = new Map();
  private readonly highestTokenByTask: Map<string, number> = new Map();
  private readonly dualTupleFencesByTask: Map<string, FenceToken> = new Map();

  constructor(leaseTtlSeconds = 15.0) {
    this.leaseTtlSeconds = leaseTtlSeconds;
  }

  public activeLeaseCount(): number {
    return this.activeLeases.size;
  }

  /**
   * [M52-1] Restores monotonic fencing watermark on cell/node cold-start so rehydrated tasks never regress fencing tokens.
   */
  public rehydrateFencingWatermark(taskId: string, token: number): void {
    this.seedHighestFencingToken(taskId, token);
  }

  public seedHighestFencingToken(taskId: string, token: number): void {
    const curr = this.highestTokenByTask.get(taskId) ?? 0;
    if (token > curr) {
      this.highestTokenByTask.set(taskId, token);
      this.dualTupleFencesByTask.set(taskId, new FenceToken(token, 0, taskId));
    }
  }

  public acquireLease(taskId: string, nodeId: string, nowSec = Date.now() / 1000): LeaseRecord {
    const prevToken = this.highestTokenByTask.get(taskId) ?? 0;
    const nextToken = prevToken + 1;
    this.highestTokenByTask.set(taskId, nextToken);

    const prevDual = this.dualTupleFencesByTask.get(taskId);
    const nextDual = prevDual ? prevDual.nextEpoch() : new FenceToken(nextToken, 0, taskId);
    this.dualTupleFencesByTask.set(taskId, nextDual);

    const now = nowSec;
    const record: LeaseRecord = {
      task_id: taskId,
      node_id: nodeId,
      fencing_token: nextToken,
      acquired_at: now,
      expires_at: now + this.leaseTtlSeconds,
    };
    this.activeLeases.set(taskId, record);
    return record;
  }

  public getDualTupleFence(taskId: string): FenceToken {
    return (
      this.dualTupleFencesByTask.get(taskId) ??
      new FenceToken(this.highestTokenByTask.get(taskId) ?? 1, 0, taskId)
    );
  }

  public advanceTurnFence(taskId: string, callerFence: FenceToken): FenceToken {
    const current = this.getDualTupleFence(taskId);
    if (callerFence.isStrictlyOlderThan(current)) {
      throw new StaleFenceError(taskId, callerFence, current);
    }
    const next = callerFence.nextTurn();
    this.dualTupleFencesByTask.set(taskId, next);
    return next;
  }

  public assertValidFence(taskId: string, fencingTokenOrDual: number | FenceToken): void {
    if (fencingTokenOrDual instanceof FenceToken) {
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

  public renewLease(taskId: string, nodeId: string, fencingToken: number, nowSec = Date.now() / 1000): boolean {
    const lease = this.activeLeases.get(taskId);
    if (!lease) return false;
    if (lease.node_id !== nodeId || lease.fencing_token !== fencingToken) {
      return false;
    }
    lease.expires_at = nowSec + this.leaseTtlSeconds;
    return true;
  }

  public validateFencingToken(taskId: string, fencingToken: number): boolean {
    const highest = this.highestTokenByTask.get(taskId) ?? 0;
    const active = this.activeLeases.get(taskId);
    if (!active) return false;
    return fencingToken === highest && active.fencing_token === fencingToken;
  }

  public revokeLease(taskId: string): void {
    this.activeLeases.delete(taskId);
  }

  public getLease(taskId: string): LeaseRecord | undefined {
    return this.activeLeases.get(taskId);
  }

  public scanExpiredLeases(nowSec = Date.now() / 1000): LeaseRecord[] {
    const expired: LeaseRecord[] = [];
    for (const [taskId, lease] of this.activeLeases.entries()) {
      if (nowSec >= lease.expires_at) {
        expired.push(lease);
        this.activeLeases.delete(taskId);
      }
    }
    return expired;
  }
}
