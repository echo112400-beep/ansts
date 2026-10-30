/**
 * TASK-09 & TASK-10: 5D Phase & Effort-Aware W-DRF Scheduler, Graceful PreemptToHibernate,
 * and Automatic Wake-on-Headroom for Preempted / PSI-Hibernated Sandboxes.
 */

import { CognitiveRouter } from "./cognitive_router";
import { LeaseCoordinator } from "./lease_coordinator";
import { AgentTask, FenceToken, IsolationDriver, ModelPoolName, ResourceVector5D, SandboxPhase, TaskState } from "./models";
import { BayesianTaskProfiler } from "./profiler";
import { MultiTenantQueueManager } from "./queue_manager";

export interface TenantQuotaConfig {
  tenantId: string;
  weight: number;
  dominantShare: number;
}

export interface WorkerNodeState {
  nodeId: string;
  driver: IsolationDriver;
  warmPools: string[];
  cpuCap: number;
  burstSemCap: number;
  memCapMb: number;
  alive: boolean;
}

export class CoreScheduler5D {
  public tenants: Map<string, TenantQuotaConfig> = new Map();
  public nodes: Map<string, WorkerNodeState> = new Map();
  public tasks: Map<string, AgentTask> = new Map();
  public hibernatedResumeQueue: AgentTask[] = [];
  public clusterCapacity: ResourceVector5D = new ResourceVector5D(64.0, 64.0, 131072.0, 64, 4800);

  constructor(
    public readonly queueManager: MultiTenantQueueManager,
    public readonly profiler: BayesianTaskProfiler,
    public readonly leaseCoordinator: LeaseCoordinator,
    public readonly cognitiveRouter: CognitiveRouter,
  ) {}

  public registerTenant(tenantId: string, weight: number): void {
    this.tenants.set(tenantId, { tenantId, weight, dominantShare: 0.0 });
  }

  public registerNode(node: Partial<WorkerNodeState> & { nodeId: string }): void {
    this.nodes.set(node.nodeId, {
      nodeId: node.nodeId,
      driver: node.driver ?? "firecracker",
      warmPools: node.warmPools ?? ["pool-py312-ds", "pool-rust-toolchains"],
      cpuCap: node.cpuCap ?? 16.0,
      burstSemCap: node.burstSemCap ?? 4,
      memCapMb: node.memCapMb ?? 32768.0,
      alive: node.alive ?? true,
    });
  }

  public submitTask(task: AgentTask): number {
    const softTokens = this.profiler.assignInitialP50Reservation(task);
    this.tasks.set(task.taskId, task);
    this.queueManager.enqueue(task);
    return softTokens;
  }

  public getTenantUsageVector(tenantId: string): ResourceVector5D {
    const running = Array.from(this.tasks.values()).filter(
      (t) => t.tenantId === tenantId && t.state === TaskState.RUNNING,
    );
    let sCpu = 0;
    let bCpu = 0;
    let sMem = 0;
    let sess = 0;
    let tps = 0;
    for (const t of running) {
      const dv = t.demandVector();
      sCpu += dv.steadyCpu;
      bCpu += dv.burstCpu;
      sMem += dv.steadyMemMb;
      sess += dv.sessions;
      tps += dv.effectiveTps;
    }
    return new ResourceVector5D(sCpu, bCpu, sMem, sess, tps);
  }

  public compute5dDominantShares(): Record<string, number> {
    const shares: Record<string, number> = {};
    for (const [tid, tcfg] of this.tenants.entries()) {
      const u = this.getTenantUsageVector(tid);
      const norm = [
        u.steadyCpu / this.clusterCapacity.steadyCpu,
        u.burstCpu / this.clusterCapacity.burstCpu,
        u.steadyMemMb / this.clusterCapacity.steadyMemMb,
        u.sessions / this.clusterCapacity.sessions,
        u.effectiveTps / this.clusterCapacity.effectiveTps,
      ];
      tcfg.dominantShare = Number((Math.max(...norm) / tcfg.weight).toFixed(4));
      shares[tid] = tcfg.dominantShare;
    }
    return shares;
  }

  public nodeUsedCpu(nodeId: string): number {
    return Array.from(this.tasks.values())
      .filter((t) => t.assignedNodeId === nodeId && t.state === TaskState.RUNNING)
      .reduce((acc, t) => acc + t.execCpu, 0.0);
  }

  public scoreNodeAndModelPool(task: AgentTask, node: WorkerNodeState): { score: number; poolName: ModelPoolName } {
    const warmScore = node.warmPools.includes(task.warmPoolId) ? 1.0 : 0.3;
    const usedCpu = this.nodeUsedCpu(node.nodeId);
    const headroomScore = Math.max(0.0, (node.cpuCap - usedCpu) / node.cpuCap);

    const primaryPool = this.cognitiveRouter.pools[task.primaryModel];
    let chosenPool: ModelPoolName;
    let poolScore: number;
    if (primaryPool.breakerState === "CLOSED") {
      chosenPool = task.primaryModel;
      poolScore = primaryPool.quotaHeadroomRatio - 0.2 * primaryPool.ttftSec;
    } else {
      chosenPool = task.fallbackModels[0];
      const fbPool = this.cognitiveRouter.pools[chosenPool];
      poolScore = fbPool.quotaHeadroomRatio - 0.2 * fbPool.ttftSec;
    }
    const score = Number((0.45 * warmScore + 0.25 * headroomScore + 0.30 * poolScore).toFixed(4));
    return { score, poolName: chosenPool };
  }

  public tryPreemptToHibernate(highPrioTask: AgentTask): AgentTask | null {
    if (!["P0", "P1", "critical-p0", "high-interactive"].includes(highPrioTask.priorityClass)) {
      return null;
    }
    const victims = Array.from(this.tasks.values()).filter(
      (t) =>
        t.state === TaskState.RUNNING &&
        ["P3", "P4", "low-background", "spot-opportunistic"].includes(t.priorityClass),
    );
    if (victims.length === 0) return null;
    victims.sort((a, b) => a.baseScore - b.baseScore);
    const victim = victims[0];
    victim.state = TaskState.HIBERNATED;
    victim.hibernationReason = "PREEMPTED";
    victim.phase = SandboxPhase.IDLE;
    victim.activeSessions = 0;
    if (!this.hibernatedResumeQueue.some((x) => x.taskId === victim.taskId)) {
      this.hibernatedResumeQueue.push(victim);
    }
    return victim;
  }

  public scheduleTick(now: number): AgentTask[] {
    const dispatched: AgentTask[] = [];
    this.queueManager.sweepExpiredTasks(now);

    // 1. Drain deadlock-free Escrow Fast Queue first
    while (this.queueManager.escrowFastQueue.length > 0) {
      const item = this.queueManager.escrowFastQueue.shift()!;
      const child = item.child;
      this.tasks.set(child.taskId, child);
      const aliveNodes = Array.from(this.nodes.values()).filter((n) => n.alive);
      if (aliveNodes.length > 0) {
        aliveNodes.sort(
          (a, b) => this.scoreNodeAndModelPool(child, b).score - this.scoreNodeAndModelPool(child, a).score,
        );
        const bestNode = aliveNodes[0];
        const { poolName } = this.scoreNodeAndModelPool(child, bestNode);
        const fence = this.leaseCoordinator.acquireOrIncrementEpoch(child.taskId, bestNode.nodeId, poolName);
        child.fence = fence;
        child.assignedNodeId = bestNode.nodeId;
        child.assignedModelPool = poolName;
        child.state = TaskState.RUNNING;
        child.phase = SandboxPhase.COGNITIVE;
        child.activeSessions = 1;
        dispatched.append ? dispatched.push(child) : dispatched.push(child);
      }
    }

    // 2. 5D W-DRF Multi-Tenant Scheduling Loop for Queued Tasks
    const blockedTenants = new Set<string>();
    while (true) {
      this.compute5dDominantShares();
      const eligibleTenants = Array.from(this.tenants.values()).filter(
        (tcfg) =>
          (this.queueManager.tenantQueues.get(tcfg.tenantId)?.length ?? 0) > 0 &&
          !blockedTenants.has(tcfg.tenantId),
      );
      if (eligibleTenants.length === 0) break;

      eligibleTenants.sort((a, b) => a.dominantShare - b.dominantShare);
      const chosenTenant = eligibleTenants[0];
      const q = this.queueManager.tenantQueues.get(chosenTenant.tenantId)!;
      q.sort((a, b) => this.queueManager.computeDynamicUrgency(b, now) - this.queueManager.computeDynamicUrgency(a, now));
      const candidate = q[0];

      const uVec = this.getTenantUsageVector(chosenTenant.tenantId);
      if (this.queueManager.exceedsBurstCeiling(chosenTenant.tenantId, uVec, candidate)) {
        blockedTenants.add(chosenTenant.tenantId);
        continue;
      }

      let feasibleNodes = Array.from(this.nodes.values()).filter(
        (n) => n.alive && this.nodeUsedCpu(n.nodeId) + candidate.execCpu <= n.cpuCap,
      );
      if (feasibleNodes.length === 0) {
        const victim = this.tryPreemptToHibernate(candidate);
        if (victim) {
          feasibleNodes = Array.from(this.nodes.values()).filter(
            (n) => n.alive && this.nodeUsedCpu(n.nodeId) + candidate.execCpu <= n.cpuCap,
          );
        }
        if (feasibleNodes.length === 0) {
          blockedTenants.add(chosenTenant.tenantId);
          continue;
        }
      }

      q.shift();
      feasibleNodes.sort(
        (a, b) => this.scoreNodeAndModelPool(candidate, b).score - this.scoreNodeAndModelPool(candidate, a).score,
      );
      const bestNode = feasibleNodes[0];
      const { poolName } = this.scoreNodeAndModelPool(candidate, bestNode);
      const fence = this.leaseCoordinator.acquireOrIncrementEpoch(candidate.taskId, bestNode.nodeId, poolName);
      candidate.fence = fence;
      candidate.assignedNodeId = bestNode.nodeId;
      candidate.assignedModelPool = poolName;
      candidate.state = TaskState.RUNNING;
      candidate.hibernationReason = "NONE";
      candidate.phase = SandboxPhase.COGNITIVE;
      candidate.activeSessions = Math.min(2, candidate.maxSessions);
      dispatched.push(candidate);
    }

    // 3. Auto-Resume Preempted / PSI-Hibernated Sandboxes when Node Headroom Returns
    if (this.hibernatedResumeQueue.length > 0) {
      this.hibernatedResumeQueue.sort(
        (a, b) => this.queueManager.computeDynamicUrgency(b, now) - this.queueManager.computeDynamicUrgency(a, now),
      );
      const stillHibernated: AgentTask[] = [];
      for (const hTask of this.hibernatedResumeQueue) {
        if (hTask.state !== TaskState.HIBERNATED || hTask.hibernationReason === "HUMAN_APPROVAL_WAIT") {
          continue;
        }
        const feasibleNodes = Array.from(this.nodes.values()).filter(
          (n) => n.alive && this.nodeUsedCpu(n.nodeId) + hTask.execCpu <= n.cpuCap,
        );
        if (feasibleNodes.length > 0) {
          feasibleNodes.sort(
            (a, b) => this.scoreNodeAndModelPool(hTask, b).score - this.scoreNodeAndModelPool(hTask, a).score,
          );
          const bestNode = feasibleNodes[0];
          const { poolName } = this.scoreNodeAndModelPool(hTask, bestNode);
          const prevTurn = Math.max(1, hTask.fence.turnSeq);
          const newEpochFence = this.leaseCoordinator.acquireOrIncrementEpoch(
            hTask.taskId,
            bestNode.nodeId,
            poolName,
          );
          hTask.fence = new FenceToken(newEpochFence.epoch, prevTurn, hTask.taskId);
          this.leaseCoordinator.verifyAndAdvanceFence(hTask.fence);
          hTask.assignedNodeId = bestNode.nodeId;
          hTask.assignedModelPool = poolName;
          hTask.state = TaskState.RUNNING;
          hTask.hibernationReason = "NONE";
          hTask.phase = SandboxPhase.COGNITIVE;
          hTask.activeSessions = Math.min(2, hTask.maxSessions);
          dispatched.push(hTask);
        } else {
          stillHibernated.push(hTask);
        }
      }
      this.hibernatedResumeQueue = stillHibernated;
    }

    return dispatched;
  }
}
