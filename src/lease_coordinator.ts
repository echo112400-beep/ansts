/**
 * @fileoverview M2 TASK-07, M52-1 & M68-3: Monotonic Fencing Token & Dual-Tuple `FenceToken` Lease Coordinator
 * with `StaleFenceError` (HTTP 409) Split-Brain Rejection, `advanceTurnFence` Epoch Watermark Synchronization & Completed-Task Fence Purge.
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
    this.leaseTtlSeconds =
      Number.isFinite(leaseTtlSeconds) && leaseTtlSeconds > 0 ? leaseTtlSeconds : 15.0;
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
    if (!Number.isFinite(token) || token <= 0) return;
    const safeToken = Math.floor(token);
    const curr = this.highestTokenByTask.get(taskId) ?? 0;
    if (safeToken > curr) {
      this.highestTokenByTask.set(taskId, safeToken);
      const prevDual = this.dualTupleFencesByTask.get(taskId);
      if (!prevDual || prevDual.epoch < safeToken) {
        this.dualTupleFencesByTask.set(taskId, new FenceToken(safeToken, 0, taskId));
      }
    }
  }

  public acquireLease(taskId: string, nodeId: string, nowSec = Date.now() / 1000): LeaseRecord {
    const prevDual = this.dualTupleFencesByTask.get(taskId);
    const prevToken = Math.max(this.highestTokenByTask.get(taskId) ?? 0, prevDual?.epoch ?? 0);
    const nextToken = prevToken + 1;
    this.highestTokenByTask.set(taskId, nextToken);

    const nextDual = new FenceToken(nextToken, 0, taskId);
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

  public getHighestFencingToken(taskId: string): number {
    return this.highestTokenByTask.get(taskId) ?? 0;
  }

  /**
   * [M68-3] Advances the turn-sequence component of the dual-tuple fence and synchronizes
   * `highestTokenByTask` whenever `callerFence.epoch` exceeds the recorded scalar watermark,
   * preventing subsequent `acquireLease` / `validateFencingToken` calls from regressing to a stale epoch.
   */
  public advanceTurnFence(taskId: string, callerFence: FenceToken): FenceToken {
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

  /**
   * [M68-3] Purges all lease and fencing watermark records for a permanently archived/purged task.
   */
  public purgeCompletedTaskFence(taskId: string): boolean {
    const hadAny =
      this.activeLeases.has(taskId) ||
      this.highestTokenByTask.has(taskId) ||
      this.dualTupleFencesByTask.has(taskId);
    this.activeLeases.delete(taskId);
    this.highestTokenByTask.delete(taskId);
    this.dualTupleFencesByTask.delete(taskId);
    return hadAny;
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
