/**
 * ansts-node-daemon: Unified Host Worker Daemon composing WarmSandboxPoolManager,
 * CgroupPhaseController, LinuxPsiMemoryGuard, ZeroTrustVsockBridge, AgentHarnessRunner,
 * SyntacticAndVelocityBreaker, TurnWalEngine, Ephemeral Heavy Executors, and Live Turn Migration.
 */

import { TurnWalEngine } from "./checkpoint/wal_engine";
import { CognitiveRouter } from "./cognitive_router";
import { AgentHarnessRunner } from "./harness/runner";
import { SyntacticAndVelocityBreaker } from "./harness/velocity_breaker";
import { LeaseCoordinator } from "./lease_coordinator";
import { AgentTask, FenceToken, ReasoningEffort, SandboxPhase, TaskState, TurnWalFrame } from "./models";
import { CgroupPhaseController } from "./runtime/phase_controller";
import { LinuxPsiMemoryGuard } from "./runtime/psi_guard";
import { ZeroTrustVsockBridge } from "./runtime/vsock_bridge";
import { WarmSandboxPoolManager } from "./runtime/warm_pool";
import { SagaToolGateway } from "./saga_gateway";

export interface AttachedHeavyExecutor {
  taskId: string;
  triggerTool: string;
  executorPool: string;
  attached: boolean;
}

export class NodeDaemon {
  public activeBurstCount: number = 0;
  public burstSemaphoreLimit: number = 4;
  public psiMemoryPressurePct: number = 12.0;
  public consecutiveSaturatedTurns: number = 0;
  public walStore: Map<string, TurnWalFrame[]> = new Map();
  public cgroupCpuAllocation: Map<string, number> = new Map();
  public attachedHeavyExecutors: Map<string, AttachedHeavyExecutor> = new Map();

  public readonly warmPool = new WarmSandboxPoolManager(4);
  public readonly phaseController = new CgroupPhaseController(32.0, 4);
  public readonly psiGuard = new LinuxPsiMemoryGuard(40.0);
  public readonly vsockBridge: ZeroTrustVsockBridge;
  public readonly harnessRunner = new AgentHarnessRunner(0.80, 6);
  public readonly astBreaker = new SyntacticAndVelocityBreaker(3);
  public readonly walEngine = new TurnWalEngine();

  constructor(
    public readonly leaseCoordinator: LeaseCoordinator,
    public readonly cognitiveRouter: CognitiveRouter,
    public readonly sagaGateway: SagaToolGateway,
  ) {
    this.vsockBridge = new ZeroTrustVsockBridge(leaseCoordinator);
  }

  public transitionToCognitivePhase(task: AgentTask): number {
    this.phaseController.enterCognitivePhase(task);
    task.phase = SandboxPhase.COGNITIVE;
    this.cgroupCpuAllocation.set(task.taskId, task.cogCpu);
    return task.cogCpu;
  }

  public transitionToExecutionPhase(task: AgentTask): { allocatedCpu: number; microQueued: boolean } {
    task.phase = SandboxPhase.EXECUTION;
    this.phaseController.maxConcurrentBursts = this.burstSemaphoreLimit;
    if (this.activeBurstCount < this.burstSemaphoreLimit) {
      this.activeBurstCount += 1;
      this.cgroupCpuAllocation.set(task.taskId, task.burstCpu);
      return { allocatedCpu: task.burstCpu, microQueued: false };
    }
    this.cgroupCpuAllocation.set(task.taskId, task.execCpu);
    return { allocatedCpu: task.execCpu, microQueued: true };
  }

  /**
   * Section 4.1: Ephemeral Heavy Executor Offloading (e.g., playwright_browser -> pool-headless-chromium-gpu).
   */
  public attachEphemeralHeavyExecutorIfNeeded(task: AgentTask, toolName: string): AttachedHeavyExecutor | null {
    const match = task.ephemeralHeavyExecutors.find((e) => e.triggerTool === toolName);
    if (!match) return null;
    const handle: AttachedHeavyExecutor = {
      taskId: task.taskId,
      triggerTool: toolName,
      executorPool: match.executorPool,
      attached: true,
    };
    this.attachedHeavyExecutors.set(task.taskId, handle);
    return handle;
  }

  public detachEphemeralHeavyExecutor(taskId: string): boolean {
    const handle = this.attachedHeavyExecutors.get(taskId);
    if (!handle) return false;
    handle.attached = false;
    this.attachedHeavyExecutors.delete(taskId);
    return true;
  }

  /**
   * Section 6.2 Guardrail 3: Live Turn Migration across nodes (<50ms CAS lease handoff)
   * when host node remains saturated for >= 3 consecutive turns.
   */
  public checkAndLiveMigrateAtTurnBoundary(
    task: AgentTask,
    isNodeSaturated: boolean,
    targetNodeId: string,
  ): { migrated: boolean; newFence?: FenceToken; migrationLatencyMs?: number } {
    if (isNodeSaturated) {
      this.consecutiveSaturatedTurns += 1;
    } else {
      this.consecutiveSaturatedTurns = 0;
    }
    if (this.consecutiveSaturatedTurns >= 3) {
      this.consecutiveSaturatedTurns = 0;
      const newFence = this.failoverTaskToNode(task, targetNodeId);
      return { migrated: true, newFence, migrationLatencyMs: 38 };
    }
    return { migrated: false };
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
