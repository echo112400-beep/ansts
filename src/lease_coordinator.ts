/**
 * TASK-11: Coordinator-Free CAS Leases, Dual-Tuple Fencing (epoch, turnSeq) & Heartbeat Failover.
 */

import { FenceToken } from "./models";

export class StaleFenceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StaleFenceError";
  }
}

export interface TaskLeaseRecord {
  taskId: string;
  nodeId: string;
  modelPool: string;
  fence: FenceToken;
}

export class LeaseCoordinator {
  private leases: Map<string, TaskLeaseRecord> = new Map();
  private nodeHeartbeats: Map<string, number> = new Map();

  constructor(public readonly heartbeatTimeoutSec: number = 12.0) {}

  public recordNodeHeartbeat(nodeId: string, now: number): void {
    this.nodeHeartbeats.set(nodeId, now);
  }

  public detectDeadNodes(now: number): string[] {
    const dead: string[] = [];
    for (const [nid, lastTs] of this.nodeHeartbeats.entries()) {
      if (now - lastTs > this.heartbeatTimeoutSec) {
        dead.push(nid);
      }
    }
    return dead;
  }

  public tryCasAcquire(
    taskId: string,
    nodeId: string,
    modelPool: string,
    expectedEpoch: number,
  ): FenceToken | null {
    const existing = this.leases.get(taskId);
    const currEpoch = existing ? existing.fence.epoch : 0;
    const currTurn = existing ? existing.fence.turnSeq : 1;
    if (currEpoch !== expectedEpoch) {
      return null;
    }
    const newFence = new FenceToken(expectedEpoch + 1, currTurn, taskId);
    this.leases.set(taskId, { taskId, nodeId, modelPool, fence: newFence });
    return newFence;
  }

  public acquireOrIncrementEpoch(taskId: string, nodeId: string, modelPool: string): FenceToken {
    const existing = this.leases.get(taskId);
    const expectedEpoch = existing ? existing.fence.epoch : 0;
    const currTurn = existing ? existing.fence.turnSeq : 1;
    const newFence = new FenceToken(expectedEpoch + 1, currTurn, taskId);
    this.leases.set(taskId, { taskId, nodeId, modelPool, fence: newFence });
    return newFence;
  }

  public verifyAndAdvanceFence(incomingFence: FenceToken): FenceToken {
    const record = this.leases.get(incomingFence.taskId);
    if (!record) {
      throw new StaleFenceError(`409 StaleFenceRejected: Unknown task ${incomingFence.taskId}`);
    }
    if (!incomingFence.dominates(record.fence)) {
      throw new StaleFenceError(
        `409 StaleFenceRejected: incoming (${incomingFence.epoch}, ${incomingFence.turnSeq}) < current (${record.fence.epoch}, ${record.fence.turnSeq})`,
      );
    }
    record.fence = incomingFence;
    return record.fence;
  }

  public getLease(taskId: string): TaskLeaseRecord | undefined {
    return this.leases.get(taskId);
  }

  /** Drops a lease (task completion / terminal state): no later fence can validate against it. */
  public releaseLease(taskId: string): void {
    this.leases.delete(taskId);
  }

  /** Detached copy of every live lease, used to roll back a failed scheduling transaction. */
  public snapshotLeases(): TaskLeaseRecord[] {
    return Array.from(this.leases.values()).map((r) => ({
      taskId: r.taskId,
      nodeId: r.nodeId,
      modelPool: r.modelPool,
      fence: new FenceToken(r.fence.epoch, r.fence.turnSeq, r.fence.taskId),
    }));
  }

  /** Restores a lease snapshot by replacing the whole map. */
  public restoreLeases(records: TaskLeaseRecord[]): void {
    this.leases.clear();
    for (const r of records) {
      this.leases.set(r.taskId, {
        taskId: r.taskId,
        nodeId: r.nodeId,
        modelPool: r.modelPool,
        fence: new FenceToken(r.fence.epoch, r.fence.turnSeq, r.fence.taskId),
      });
    }
  }
}
