/**
 * ansts-node-daemon: Bi-Modal Phase-Elastic Runtime, TurnWAL Checkpointing & Velocity Backtracking.
 */

import { CognitiveRouter } from "./cognitive_router";
import { LeaseCoordinator } from "./lease_coordinator";
import { AgentTask, FenceToken, ReasoningEffort, SandboxPhase, TaskState, TurnWalFrame } from "./models";
import { SagaToolGateway } from "./saga_gateway";

export class NodeDaemon {
  public activeBurstCount: number = 0;
  public burstSemaphoreLimit: number = 4;
  public psiMemoryPressurePct: number = 12.0;
  public walStore: Map<string, TurnWalFrame[]> = new Map();
  public cgroupCpuAllocation: Map<string, number> = new Map();

  constructor(
    public readonly leaseCoordinator: LeaseCoordinator,
    public readonly cognitiveRouter: CognitiveRouter,
    public readonly sagaGateway: SagaToolGateway,
  ) {}

  public transitionToCognitivePhase(task: AgentTask): number {
    task.phase = SandboxPhase.COGNITIVE;
    this.cgroupCpuAllocation.set(task.taskId, task.cogCpu);
    return task.cogCpu;
  }

  public transitionToExecutionPhase(task: AgentTask): { allocatedCpu: number; microQueued: boolean } {
    task.phase = SandboxPhase.EXECUTION;
    if (this.activeBurstCount < this.burstSemaphoreLimit) {
      this.activeBurstCount += 1;
      this.cgroupCpuAllocation.set(task.taskId, task.burstCpu);
      return { allocatedCpu: task.burstCpu, microQueued: false };
    }
    this.cgroupCpuAllocation.set(task.taskId, task.execCpu);
    return { allocatedCpu: task.execCpu, microQueued: true };
  }

  public computeProgressVelocity(frame: TurnWalFrame, history: TurnWalFrame[]): number {
    if (history.length === 0) return 0.50;
    const prev = history[history.length - 1];
    const deltaPass = Math.max(0.0, frame.passRate - prev.passRate);
    const errRed =
      prev.errorCount > 0
        ? Math.max(0.0, (prev.errorCount - frame.errorCount) / Math.max(1, prev.errorCount))
        : 0.2;
    const oscillation = history
      .slice(0, -1)
      .some((h) => h.workspaceMerkleHash === frame.workspaceMerkleHash);
    const oscPenalty = oscillation ? 0.8 : 0.0;
    const velocity = 0.4 * deltaPass + 0.3 * errRed + 0.3 * frame.diffEntropy - oscPenalty;
    return Number(velocity.toFixed(4));
  }

  public recordTurnAndAuditVelocity(params: {
    task: AgentTask;
    routedModel: string;
    effectiveEffort: ReasoningEffort;
    tokensUsed: number;
    workspaceMerkleHash: string;
    passRate: number;
    errorCount: number;
    diffEntropy: number;
    windowSize?: number;
    minVelocity?: number;
  }): Record<string, any> {
    const { task } = params;
    const windowSize = params.windowSize ?? 4;
    const minVelocity = params.minVelocity ?? 0.10;

    this.leaseCoordinator.verifyAndAdvanceFence(task.fence);
    const history = this.walStore.get(task.taskId) ?? [];

    const frame: TurnWalFrame = {
      taskId: task.taskId,
      fence: task.fence,
      routedModel: params.routedModel,
      effectiveEffort: params.effectiveEffort,
      tokensUsed: params.tokensUsed,
      workspaceMerkleHash: params.workspaceMerkleHash,
      passRate: params.passRate,
      errorCount: params.errorCount,
      diffEntropy: params.diffEntropy,
      contextMessages: [`turn_${task.fence.turnSeq}`],
    };

    const vk = this.computeProgressVelocity(frame, history);
    history.push(frame);
    this.walStore.set(task.taskId, history);

    if (history.length >= windowSize) {
      const recent = history.slice(-windowSize);
      let sumV = 0.0;
      for (let i = 0; i < windowSize; i++) {
        const prefix = history.slice(0, history.length - windowSize + i);
        sumV += this.computeProgressVelocity(recent[i], prefix);
      }
      const avgV = sumV / windowSize;
      if (avgV < minVelocity) {
        if (task.backtrackCount < 2) {
          const targetIdx = Math.max(0, history.length - windowSize);
          const targetTurn = history[targetIdx].fence.turnSeq;
          const compensated = this.sagaGateway.rollbackAndCompensate(task.fence, targetTurn);
          this.walStore.set(task.taskId, history.slice(0, targetIdx + 1));
          task.backtrackCount += 1;
          task.effort = "max";
          task.negativeMemories.push(`Avoid stagnant trajectory around hash=${params.workspaceMerkleHash}`);
          return {
            action: "BACKTRACKED",
            avgVelocity: Number(avgV.toFixed(4)),
            restoredTurnSeq: targetTurn,
            compensatedTools: compensated,
          };
        }
        task.state = TaskState.CIRCUIT_BROKEN;
        return { action: "CIRCUIT_BROKEN", avgVelocity: Number(avgV.toFixed(4)) };
      }
    }

    task.fence = task.fence.nextTurn();
    this.leaseCoordinator.verifyAndAdvanceFence(task.fence);
    return { action: "CONTINUE", velocity: vk, nextFence: task.fence };
  }

  public failoverTaskToNode(task: AgentTask, newNodeId: string): FenceToken {
    const newFence = this.leaseCoordinator.acquireOrIncrementEpoch(
      task.taskId,
      newNodeId,
      task.assignedModelPool ?? task.primaryModel,
    );
    const history = this.walStore.get(task.taskId) ?? [];
    const nextTurnToRun = history.length > 0 ? history[history.length - 1].fence.turnSeq + 1 : 1;
    task.fence = new FenceToken(newFence.epoch, nextTurnToRun, task.taskId);
    this.leaseCoordinator.verifyAndAdvanceFence(task.fence);
    task.assignedNodeId = newNodeId;
    task.state = TaskState.RUNNING;
    return task.fence;
  }
}
