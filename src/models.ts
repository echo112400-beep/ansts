/**
 * Core Domain Models, 5D Resource Vectors, Dual-Tuple Fencing, Dual-Clock Budgets & Turn-WAL Structures.
 * 100% aligned with TaskSpec v1beta1 and Google Engineering Design Doc v2.1.
 */

export type ReasoningEffort = "minimal" | "low" | "medium" | "high" | "max";
export type ModelPoolName = "DeepSeek" | "GLM-5.3" | "Qwen-3.8";
export type IsolationDriver = "firecracker" | "kata" | "gvisor" | "runc";
export type PreemptionPolicy = "PreemptToHibernate" | "Never" | "YieldOnPressure";
export type HibernationReason =
  | "NONE"
  | "PREEMPTED"
  | "PSI_MEMORY_PRESSURE"
  | "HUMAN_APPROVAL_WAIT"
  | "IDLE_TIMEOUT"
  | "DAG_PARENT_YIELD";

export const EFFORT_MULTIPLIERS: Record<ReasoningEffort, number> = {
  minimal: 1.0,
  low: 1.2,
  medium: 1.8,
  high: 3.0,
  max: 5.0,
};

export enum TaskState {
  SUBMITTED = "SUBMITTED",
  QUEUED = "QUEUED",
  PROVISIONING = "PROVISIONING",
  RUNNING = "RUNNING",
  GRACEFUL_DRAINING = "GRACEFUL_DRAINING",
  HIBERNATED = "HIBERNATED",
  HIBERNATED_YIELD = "HIBERNATED_YIELD",
  BACKTRACKING = "BACKTRACKING",
  RECOVERING = "RECOVERING",
  SUCCEEDED = "SUCCEEDED",
  CIRCUIT_BROKEN = "CIRCUIT_BROKEN",
}

export enum SandboxPhase {
  COGNITIVE = "COGNITIVE_0.1C",
  EXECUTION = "EXECUTION_BURST",
  IDLE = "IDLE_0C",
}

export enum ToolEffectClass {
  PURE_READ = "PURE_READ",
  IDEMPOTENT_WRITE = "IDEMPOTENT_WRITE",
  SAGA_COMPENSATABLE = "SAGA_COMPENSATABLE",
  STAGED_COMMIT = "STAGED_COMMIT",
}

export interface EphemeralHeavyExecutorSpec {
  triggerTool: string;
  executorPool: string;
  maxAttachSec: number;
}

export class FenceToken {
  public readonly epoch: number;
  public readonly turnSeq: number;
  public readonly taskId: string;

  constructor(epoch: number, turnSeq: number, taskId: string = "") {
    this.epoch = epoch;
    this.turnSeq = turnSeq;
    this.taskId = taskId;
  }

  public dominates(other: FenceToken): boolean {
    if (this.epoch !== other.epoch) {
      return this.epoch > other.epoch;
    }
    return this.turnSeq >= other.turnSeq;
  }

  public nextTurn(): FenceToken {
    return new FenceToken(this.epoch, this.turnSeq + 1, this.taskId);
  }

  public nextEpoch(): FenceToken {
    return new FenceToken(this.epoch + 1, this.turnSeq, this.taskId);
  }

  public toHeader(): string {
    return `${this.epoch}:${this.turnSeq}`;
  }
}

export class ResourceVector5D {
  constructor(
    public readonly steadyCpu: number,
    public readonly burstCpu: number,
    public readonly steadyMemMb: number,
    public readonly sessions: number,
    public readonly effectiveTps: number,
  ) {}

  public static fromTaskParams(params: {
    cogCpu: number;
    execCpu: number;
    burstCpu: number;
    cogMemMb: number;
    execMemMb: number;
    sessions: number;
    reservedTps: number;
    effort: ReasoningEffort;
    rhoCog?: number;
  }): ResourceVector5D {
    const rhoCog = params.rhoCog ?? 0.75;
    const rhoExec = 1.0 - rhoCog;
    const steadyCpu = Number((rhoCog * params.cogCpu + rhoExec * params.execCpu).toFixed(4));
    const steadyMem = Number((rhoCog * params.cogMemMb + rhoExec * params.execMemMb).toFixed(2));
    const mult = EFFORT_MULTIPLIERS[params.effort] ?? 1.0;
    const effTps = Math.round(params.reservedTps * mult);
    return new ResourceVector5D(steadyCpu, params.burstCpu, steadyMem, params.sessions, effTps);
  }
}

export interface EscrowVoucher {
  parentTaskId: string;
  parentFence: FenceToken;
  childTaskId: string;
  delegatedTps: number;
  delegatedTokens: number;
}

export interface TurnWalFrame {
  taskId: string;
  fence: FenceToken;
  routedModel: string;
  effectiveEffort: ReasoningEffort;
  tokensUsed: number;
  workspaceMerkleHash: string;
  passRate: number;
  errorCount: number;
  diffEntropy: number;
  contextMessages: string[];
}

export interface AgentTaskInit {
  taskId: string;
  tenantId: string;
  priorityClass?: string;
  baseScore?: number;
  enqueueTs?: number;
  deadlineTs?: number;
  preemptionPolicy?: PreemptionPolicy;
  localityRegion?: string;
  primaryModel?: ModelPoolName;
  fastUtilityModel?: ModelPoolName;
  fallbackModels?: ModelPoolName[];
  effort?: ReasoningEffort;
  thinkingBudgetTokens?: number;
  temperature?: number;
  maxContextWindow?: number;
  reservedTps?: number;
  maxBurstTps?: number;
  maxRpm?: number;
  maxTotalTokens?: number;
  maxInputTokens?: number;
  maxOutputTokens?: number;
  softReservedTokens?: number;
  usedTokens?: number;
  usedInputTokens?: number;
  usedOutputTokens?: number;
  maxSessions?: number;
  maxTotalSessions?: number;
  activeSessions?: number;
  totalSpawnedSessions?: number;
  cogCpu?: number;
  execCpu?: number;
  burstCpu?: number;
  cogMemMb?: number;
  execMemMb?: number;
  maxPids?: number;
  isolationDriver?: IsolationDriver;
  warmPoolId?: string;
  maxTurns?: number;
  maxWallClockSec?: number;
  maxActiveComputeSec?: number;
  ephemeralHeavyExecutors?: EphemeralHeavyExecutorSpec[];
  denyCommands?: string[];
  state?: TaskState;
  phase?: SandboxPhase;
  customMetadata?: Record<string, string>;
}

export class AgentTask {
  public taskId: string;
  public tenantId: string;
  public priorityClass: string;
  public baseScore: number;
  public enqueueTs: number;
  public deadlineTs: number;
  public preemptionPolicy: PreemptionPolicy;
  public localityRegion: string;
  public primaryModel: ModelPoolName;
  public fastUtilityModel: ModelPoolName;
  public fallbackModels: ModelPoolName[];
  public effort: ReasoningEffort;
  public thinkingBudgetTokens: number;
  public temperature: number;
  public maxContextWindow: number;
  public reservedTps: number;
  public maxBurstTps: number;
  public maxRpm: number;
  public maxTotalTokens: number;
  public maxInputTokens: number;
  public maxOutputTokens: number;
  public softReservedTokens: number;
  public usedTokens: number;
  public usedInputTokens: number;
  public usedOutputTokens: number;
  public maxSessions: number;
  public maxTotalSessions: number;
  public activeSessions: number;
  public totalSpawnedSessions: number;
  public cogCpu: number;
  public execCpu: number;
  public burstCpu: number;
  public cogMemMb: number;
  public execMemMb: number;
  public maxPids: number;
  public isolationDriver: IsolationDriver;
  public warmPoolId: string;
  public maxTurns: number;
  public maxWallClockSec: number;
  public maxActiveComputeSec: number;
  public activeComputeSec: number = 0.0;
  public wallClockSec: number = 0.0;
  public hibernationReason: HibernationReason = "NONE";
  public drainSummary: string | null = null;
  public cgroupFrozen: boolean = false;
  public ephemeralHeavyExecutors: EphemeralHeavyExecutorSpec[];
  public denyCommands: string[];
  public state: TaskState;
  public phase: SandboxPhase;
  public fence: FenceToken;
  public assignedNodeId: string | null = null;
  public assignedModelPool: string | null = null;
  public backtrackCount: number = 0;
  public negativeMemories: string[] = [];
  public customMetadata: Record<string, string>;

  constructor(init: AgentTaskInit) {
    this.taskId = init.taskId;
    this.tenantId = init.tenantId;
    this.priorityClass = init.priorityClass ?? "P1";
    this.baseScore = init.baseScore ?? 800;
    this.enqueueTs = init.enqueueTs ?? 0.0;
    this.deadlineTs = init.deadlineTs ?? 300.0;
    this.preemptionPolicy = init.preemptionPolicy ?? "PreemptToHibernate";
    this.localityRegion = init.localityRegion ?? "us-west-2";
    this.primaryModel = init.primaryModel ?? "DeepSeek";
    this.fastUtilityModel = init.fastUtilityModel ?? "GLM-5.3";
    this.fallbackModels = init.fallbackModels ?? ["Qwen-3.8", "GLM-5.3"];
    this.effort = init.effort ?? "high";
    this.thinkingBudgetTokens = init.thinkingBudgetTokens ?? 16384;
    this.temperature = init.temperature ?? 0.2;
    this.maxContextWindow = init.maxContextWindow ?? 128000;
    this.reservedTps = init.reservedTps ?? 120;
    this.maxBurstTps = init.maxBurstTps ?? 400;
    this.maxRpm = init.maxRpm ?? 60;
    this.maxTotalTokens = init.maxTotalTokens ?? 500000;
    this.maxInputTokens = init.maxInputTokens ?? 400000;
    this.maxOutputTokens = init.maxOutputTokens ?? 100000;
    this.softReservedTokens = init.softReservedTokens ?? 150000;
    this.usedTokens = init.usedTokens ?? 0;
    this.usedInputTokens = init.usedInputTokens ?? 0;
    this.usedOutputTokens = init.usedOutputTokens ?? 0;
    this.maxSessions = init.maxSessions ?? 4;
    this.maxTotalSessions = init.maxTotalSessions ?? 16;
    this.activeSessions = init.activeSessions ?? 0;
    this.totalSpawnedSessions = init.totalSpawnedSessions ?? 1;
    this.cogCpu = init.cogCpu ?? 0.1;
    this.execCpu = init.execCpu ?? 2.0;
    this.burstCpu = init.burstCpu ?? 8.0;
    this.cogMemMb = init.cogMemMb ?? 256.0;
    this.execMemMb = init.execMemMb ?? 4096.0;
    this.maxPids = init.maxPids ?? 256;
    this.isolationDriver = init.isolationDriver ?? "firecracker";
    this.warmPoolId = init.warmPoolId ?? "pool-py312-ds";
    this.maxTurns = init.maxTurns ?? 15;
    this.maxWallClockSec = init.maxWallClockSec ?? 1800;
    this.maxActiveComputeSec = init.maxActiveComputeSec ?? 600;
    this.ephemeralHeavyExecutors = init.ephemeralHeavyExecutors ?? [
      {
        triggerTool: "playwright_browser",
        executorPool: "pool-headless-chromium-gpu",
        maxAttachSec: 120,
      },
    ];
    this.denyCommands = init.denyCommands ?? ["rm -rf /", "mkfs", "dd", "shutdown"];
    this.state = init.state ?? TaskState.QUEUED;
    this.phase = init.phase ?? SandboxPhase.IDLE;
    this.fence = new FenceToken(0, 0, init.taskId);
    this.customMetadata = init.customMetadata ?? {};
  }

  public demandVector(rhoCog: number = 0.75): ResourceVector5D {
    return ResourceVector5D.fromTaskParams({
      cogCpu: this.cogCpu,
      execCpu: this.execCpu,
      burstCpu: this.burstCpu,
      cogMemMb: this.cogMemMb,
      execMemMb: this.execMemMb,
      sessions: Math.max(1, this.activeSessions || this.maxSessions),
      reservedTps: this.reservedTps,
      effort: this.effort,
      rhoCog,
    });
  }

  public advanceClocks(deltaWallSec: number, deltaActiveComputeSec: number): boolean {
    this.wallClockSec = Number((this.wallClockSec + deltaWallSec).toFixed(3));
    if (this.state === TaskState.RUNNING) {
      this.activeComputeSec = Number((this.activeComputeSec + deltaActiveComputeSec).toFixed(3));
    }
    if (
      this.activeComputeSec >= this.maxActiveComputeSec ||
      this.wallClockSec >= this.maxWallClockSec ||
      this.usedTokens >= this.maxTotalTokens ||
      this.usedInputTokens >= this.maxInputTokens ||
      this.usedOutputTokens >= this.maxOutputTokens
    ) {
      this.state = TaskState.GRACEFUL_DRAINING;
      this.drainSummary = `Graceful drain triggered at turn=${this.fence.turnSeq} (activeCompute=${this.activeComputeSec}s, tokens=${this.usedTokens})`;
      return true;
    }
    return false;
  }
}
