/**
 * TASK-06, TASK-07, TASK-08: 3-Tier H-WFQ Queue Tree, Aging+EDF Priority Queue, and DAG Escrow.
 */

import { DagEscrowCoordinator } from "./dag/escrow";
import { AgentTask, EscrowVoucher, ResourceVector5D, SandboxPhase, TaskState } from "./models";

export interface TenantQueueQuota {
  tenantId: string;
  tierGroup?: string;
  weight?: number;
  guaranteedCpu?: number;
  burstCeilingCpu?: number;
  burstCeilingTps?: number;
  maxQueueWaitSec?: number;
}

export class SubAgentDagGroup {
  public stragglerTriggerRatio: number = 0.75;
  public boostTpsMultiplier: number = 2.0;

  constructor(
    public readonly dagId: string,
    public readonly parentTask: AgentTask,
    public readonly children: AgentTask[],
    public readonly speculativeRaceMode: boolean = false,
  ) {}

  public completionRatio(): number {
    if (this.children.length === 0) return 0.0;
    const done = this.children.filter((c) => c.state === TaskState.SUCCEEDED).length;
    return done / this.children.length;
  }
}

export class MultiTenantQueueManager {
  public tenantQuotas: Map<string, Required<TenantQueueQuota>> = new Map();
  public tenantQueues: Map<string, AgentTask[]> = new Map();
  public expiredTasks: AgentTask[] = [];
  public escrowCoordinator: DagEscrowCoordinator = new DagEscrowCoordinator();
  public escrowFastQueue: Array<{ child: AgentTask; voucher: EscrowVoucher }> = [];
  public activeDags: Map<string, SubAgentDagGroup> = new Map();

  constructor(
    public readonly agingBeta: number = 65.0,
    public readonly agingT: number = 10.0,
    public readonly edfGamma: number = 180.0,
  ) {}

  public configureTenantQuota(q: TenantQueueQuota): void {
    this.tenantQuotas.set(q.tenantId, {
      tenantId: q.tenantId,
      tierGroup: q.tierGroup ?? "Enterprise-Dedicated",
      weight: q.weight ?? 1.0,
      guaranteedCpu: q.guaranteedCpu ?? 4.0,
      burstCeilingCpu: q.burstCeilingCpu ?? 32.0,
      burstCeilingTps: q.burstCeilingTps ?? 2400,
      maxQueueWaitSec: q.maxQueueWaitSec ?? 300.0,
    });
  }

  public enqueue(task: AgentTask): void {
    task.state = TaskState.QUEUED;
    const q = this.tenantQueues.get(task.tenantId) ?? [];
    q.push(task);
    this.tenantQueues.set(task.tenantId, q);
  }

  public sweepExpiredTasks(now: number): AgentTask[] {
    const newlyExpired: AgentTask[] = [];
    for (const [tid, q] of this.tenantQueues.entries()) {
      const maxWait = this.tenantQuotas.get(tid)?.maxQueueWaitSec ?? 300.0;
      const kept: AgentTask[] = [];
      for (const t of q) {
        if (now - t.enqueueTs > maxWait) {
          t.state = TaskState.CIRCUIT_BROKEN;
          this.expiredTasks.push(t);
          newlyExpired.push(t);
        } else {
          kept.push(t);
        }
      }
      this.tenantQueues.set(tid, kept);
    }
    return newlyExpired;
  }

  public exceedsBurstCeiling(tenantId: string, currentUsage: ResourceVector5D, candidate: AgentTask): boolean {
    const quota = this.tenantQuotas.get(tenantId);
    if (!quota) return false;
    const candVec = candidate.demandVector();
    if (currentUsage.steadyCpu + candVec.steadyCpu > quota.burstCeilingCpu) return true;
    if (currentUsage.effectiveTps + candVec.effectiveTps > quota.burstCeilingTps) return true;
    return false;
  }

  public computeDynamicUrgency(task: AgentTask, now: number): number {
    const waitSec = Math.max(0.0, now - task.enqueueTs);
    const agingBonus = this.agingBeta * Math.log(1.0 + waitSec / this.agingT);
    const remDeadline = Math.max(1.0, task.deadlineTs - now);
    const edfBonus = this.edfGamma * (60.0 / remDeadline);
    return Number((task.baseScore + agingBonus + edfBonus).toFixed(2));
  }

  public popBestForTenant(tenantId: string, now: number): AgentTask | undefined {
    const q = this.tenantQueues.get(tenantId);
    if (!q || q.length === 0) return undefined;
    q.sort((a, b) => this.computeDynamicUrgency(b, now) - this.computeDynamicUrgency(a, now));
    return q.shift();
  }

  public spawnDagWithEscrow(
    dagId: string,
    parent: AgentTask,
    children: AgentTask[],
    raceMode: boolean = false,
  ): EscrowVoucher[] {
    const vouchers = this.escrowCoordinator.yieldParentAndIssueVouchers(parent, children);
    children.forEach((child, i) => {
      this.escrowFastQueue.push({ child, voucher: vouchers[i] });
    });
    this.activeDags.set(dagId, new SubAgentDagGroup(dagId, parent, children, raceMode));
    return vouchers;
  }

  public evaluateDagBarriers(): string[] {
    const actions: string[] = [];
    for (const dag of this.activeDags.values()) {
      if (dag.speculativeRaceMode) {
        const winners = dag.children.filter((c) => c.state === TaskState.SUCCEEDED);
        if (winners.length > 0) {
          for (const c of dag.children) {
            if (c.taskId !== winners[0].taskId && c.state === TaskState.RUNNING) {
              c.state = TaskState.CIRCUIT_BROKEN;
              c.activeSessions = 0;
              actions.push(`RACE_PRUNED:${c.taskId}`);
            }
          }
          dag.parentTask.state = TaskState.RUNNING;
          dag.parentTask.phase = SandboxPhase.COGNITIVE;
          actions.push(`PARENT_RESUMED:${dag.parentTask.taskId}`);
        }
      } else {
        const ratio = dag.completionRatio();
        if (ratio >= 1.0) {
          dag.parentTask.state = TaskState.RUNNING;
          dag.parentTask.phase = SandboxPhase.COGNITIVE;
          actions.push(`JOIN_COMPLETED:${dag.parentTask.taskId}`);
        } else if (ratio >= dag.stragglerTriggerRatio) {
          const unusedTps = dag.children
            .filter((c) => c.state === TaskState.SUCCEEDED)
            .reduce((acc, c) => acc + c.reservedTps, 0);
          const running = dag.children.filter((c) => c.state === TaskState.RUNNING);
          for (const straggler of running) {
            straggler.reservedTps = Math.floor(
              Math.min(
                straggler.reservedTps * dag.boostTpsMultiplier,
                straggler.reservedTps + Math.floor(unusedTps / Math.max(1, running.length)),
              ),
            );
            straggler.baseScore += 150;
            actions.push(`STRAGGLER_BOOSTED:${straggler.taskId}`);
          }
        }
      }
    }
    return actions;
  }
}
