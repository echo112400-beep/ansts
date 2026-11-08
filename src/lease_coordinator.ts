/**
 * @fileoverview M2 TASK-07: Monotonic Fencing Token Lease Coordinator with Split-Brain Rejection.
 */
import { LeaseRecord } from "./models.js";

export class LeaseCoordinator {
  private readonly leaseTtlSeconds: number;
  private readonly activeLeases: Map<string, LeaseRecord> = new Map();
  private readonly highestTokenByTask: Map<string, number> = new Map();

  constructor(leaseTtlSeconds = 15.0) {
    this.leaseTtlSeconds = leaseTtlSeconds;
  }

  public acquireLease(taskId: string, nodeId: string): LeaseRecord {
    const prevToken = this.highestTokenByTask.get(taskId) ?? 0;
    const nextToken = prevToken + 1;
    this.highestTokenByTask.set(taskId, nextToken);

    const now = Date.now() / 1000;
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

  public renewLease(taskId: string, nodeId: string, fencingToken: number): boolean {
    const lease = this.activeLeases.get(taskId);
    if (!lease) return false;
    if (lease.node_id !== nodeId || lease.fencing_token !== fencingToken) {
      return false;
    }
    lease.expires_at = Date.now() / 1000 + this.leaseTtlSeconds;
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
