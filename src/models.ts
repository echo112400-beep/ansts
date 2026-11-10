/**
 * @fileoverview Core Domain Models, TaskSpec v1beta1, 5D Resource Profiles, Monotonic Lease Fencing & LlmEndpointMetrics.
 */
export enum TaskPriority {
  P0_EMERGENCY = 0,
  P1_HIGH = 1,
  P2_STANDARD = 2,
  P3_BATCH = 3,
}

export enum TaskState {
  SUBMITTED = "SUBMITTED",
  QUEUED = "QUEUED",
  LEASED = "LEASED",
  RUNNING_THINK = "RUNNING_THINK",
  RUNNING_ACT = "RUNNING_ACT",
  WAITING_SUB_AGENT = "WAITING_SUB_AGENT",
  SUSPENDED_APPROVAL = "SUSPENDED_APPROVAL",
  PREEMPTING = "PREEMPTING",
  COMPLETED = "COMPLETED",
  FAILED = "FAILED",
  DEAD_LETTER = "DEAD_LETTER",
}

export enum ToolCallEffectClass {
  READ_ONLY = "READ_ONLY",
  PURE_READ = "READ_ONLY",
  IDEMPOTENT_WRITE = "IDEMPOTENT_WRITE",
  COMPENSATABLE = "COMPENSATABLE",
  SAGA_COMPENSATABLE = "COMPENSATABLE",
  STAGED_COMMIT = "STAGED_COMMIT",
  IRREVERSIBLE = "IRREVERSIBLE",
}

export type PreemptionPolicy = "PreemptToHibernate" | "Never" | "YieldOnPressure";

/**
 * Dual-Tuple Monotonic Fencing Token `(epoch, turnSeq, taskId)` aligned with `FenceTokenProto` in `proto/ansts_rpc.proto`.
 */
export class FenceToken {
  public readonly epoch: number;
  public readonly turnSeq: number;
  public readonly taskId: string;

  constructor(epoch: number, turnSeq: number, taskId: string) {
    if (!Number.isInteger(epoch) || epoch < 0) {
      throw new Error(`FenceToken epoch must be a non-negative integer, got ${epoch}`);
    }
    if (!Number.isInteger(turnSeq) || turnSeq < 0) {
      throw new Error(`FenceToken turnSeq must be a non-negative integer, got ${turnSeq}`);
    }
    this.epoch = epoch;
    this.turnSeq = turnSeq;
    this.taskId = taskId;
  }

  public nextEpoch(): FenceToken {
    return new FenceToken(this.epoch + 1, 0, this.taskId);
  }

  public nextTurn(): FenceToken {
    return new FenceToken(this.epoch, this.turnSeq + 1, this.taskId);
  }

  public compareTo(other: FenceToken): number {
    if (this.epoch !== other.epoch) {
      return this.epoch > other.epoch ? 1 : -1;
    }
    if (this.turnSeq !== other.turnSeq) {
      return this.turnSeq > other.turnSeq ? 1 : -1;
    }
    return 0;
  }

  public isStrictlyOlderThan(other: FenceToken): boolean {
    return this.compareTo(other) < 0;
  }

  public toScalarToken(): number {
    return this.epoch * 1_000_000 + this.turnSeq;
  }

  public toProto(): { epoch: number; turn_seq: number; task_id: string } {
    return { epoch: this.epoch, turn_seq: this.turnSeq, task_id: this.taskId };
  }
}

/**
 * Turn-Level Write-Ahead Log Frame (`TurnWalFrame`) with CRC32 integrity verification and dual-tuple `FenceToken`.
 */
export class TurnWalFrame {
  public readonly frameId: string;
  public readonly taskId: string;
  public readonly turnIndex: number;
  public readonly fence: FenceToken;
  public readonly changedFiles: string[];
  public readonly sqliteWalPages: number;
  public readonly crc32Hex: string;
  public readonly createdAtMs: number;

  constructor(params: {
    frameId: string;
    taskId: string;
    turnIndex: number;
    fence: FenceToken;
    changedFiles: string[];
    sqliteWalPages: number;
    crc32Hex?: string;
    createdAtMs?: number;
  }) {
    this.frameId = params.frameId;
    this.taskId = params.taskId;
    this.turnIndex = params.turnIndex;
    this.fence = params.fence;
    this.changedFiles = [...params.changedFiles];
    this.sqliteWalPages = params.sqliteWalPages;
    this.createdAtMs = params.createdAtMs ?? Date.now();
    this.crc32Hex = params.crc32Hex ?? TurnWalFrame.computeCrc32Hex(this.taskId, this.turnIndex, this.fence, this.changedFiles);
  }

  public static computeCrc32Hex(
    taskId: string,
    turnIndex: number,
    fence: FenceToken,
    changedFiles: string[]
  ): string {
    const raw = `${taskId}:${turnIndex}:${fence.epoch}:${fence.turnSeq}:${changedFiles.join(",")}`;
    let crc = 0xffffffff;
    for (let i = 0; i < raw.length; i++) {
      crc ^= raw.charCodeAt(i);
      for (let j = 0; j < 8; j++) {
        crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
      }
    }
    return ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0");
  }

  public verifyIntegrity(): boolean {
    const expected = TurnWalFrame.computeCrc32Hex(
      this.taskId,
      this.turnIndex,
      this.fence,
      this.changedFiles
    );
    return this.crc32Hex === expected;
  }
}

export interface ResourceProfile {
  think_cpu_cores: number;
  act_cpu_cores: number;
  act_memory_mb: number;
  disk_mb: number;
}

export interface TaskSpecInit {
  task_id: string;
  tenant_id: string;
  priority?: TaskPriority;
  prompt?: string;
  max_turns?: number;
  timeout_seconds?: number;
  resource_profile?: Partial<ResourceProfile>;
  dependencies?: string[];
  idempotency_key?: string;
  parent_task_id?: string;
  expected_SubAgent_count?: number;
  prefix_Cache_hash?: string;
}

export class TaskSpec {
  public task_id: string;
  public tenant_id: string;
  public priority: TaskPriority;
  public prompt: string;
  public max_turns: number;
  public current_turn = 0;
  public timeout_seconds: number;
  public resource_profile: ResourceProfile;
  public dependencies: string[];
  public idempotency_key?: string;
  public parent_task_id?: string;
  public expected_SubAgent_count: number;
  public prefix_Cache_hash = "";
  public state: TaskState = TaskState.SUBMITTED;
  public assigned_node_id?: string;
  public lease_fencing_token = 0;
  public retry_count = 0;
  public preempted_count = 0;
  public last_preempted_at = 0;
  public dead_letter_reason?: string;
  public created_at: number;

  constructor(init: TaskSpecInit) {
    this.task_id = init.task_id;
    this.tenant_id = init.tenant_id;
    this.priority = init.priority ?? TaskPriority.P2_STANDARD;
    this.prompt = init.prompt ?? "";
    this.max_turns = init.max_turns ?? 30;
    this.timeout_seconds = init.timeout_seconds ?? 1800;
    this.resource_profile = {
      think_cpu_cores: init.resource_profile?.think_cpu_cores ?? 0.25,
      act_cpu_cores: init.resource_profile?.act_cpu_cores ?? 4.0,
      act_memory_mb: init.resource_profile?.act_memory_mb ?? 4096,
      disk_mb: init.resource_profile?.disk_mb ?? 10240,
    };
    this.dependencies = init.dependencies ? [...init.dependencies] : [];
    this.idempotency_key = init.idempotency_key;
    this.parent_task_id = init.parent_task_id;
    this.expected_SubAgent_count = init.expected_SubAgent_count ?? 0;
    this.prefix_Cache_hash = init.prefix_Cache_hash ?? "";
    this.created_at = Date.now() / 1000;
  }
}

export interface NodeHeartbeatInit {
  node_id: string;
  total_cpu_cores?: number;
  free_cpu_cores?: number;
  total_memory_mb?: number;
  free_memory_mb?: number;
  warm_pool_ready?: number;
  psi_memory_some_avg10?: number;
  running_tasks?: number;
  healthy?: boolean;
  cached_images?: Iterable<string>;
  timestamp?: number;
}

export class NodeHeartbeat {
  public node_id: string;
  public total_cpu_cores: number;
  public free_cpu_cores: number;
  public total_memory_mb: number;
  public free_memory_mb: number;
  public warm_pool_ready: number;
  public psi_memory_some_avg10: number;
  public running_tasks: number;
  public healthy: boolean;
  public cached_images: Set<string>;
  public timestamp: number;

  constructor(init: NodeHeartbeatInit) {
    this.node_id = init.node_id;
    this.total_cpu_cores = init.total_cpu_cores ?? 32.0;
    this.free_cpu_cores = init.free_cpu_cores ?? this.total_cpu_cores;
    this.total_memory_mb = init.total_memory_mb ?? 65536;
    this.free_memory_mb = init.free_memory_mb ?? this.total_memory_mb;
    this.warm_pool_ready = init.warm_pool_ready ?? 8;
    this.psi_memory_some_avg10 = init.psi_memory_some_avg10 ?? 1.5;
    this.running_tasks = init.running_tasks ?? 0;
    this.healthy = init.healthy ?? true;
    this.cached_images = new Set(init.cached_images ?? []);
    this.timestamp = init.timestamp ?? Date.now() / 1000;
  }
}

export interface LlmEndpointMetricsInit {
  endpoint_id: string;
  provider: string;
  model_name: string;
  ttft_ewma_ms?: number;
  tpm_limit?: number;
  tpm_used?: number;
  rpm_limit?: number;
  rpm_used?: number;
  max_concurrency?: number;
  active_requests?: number;
  healthy?: boolean;
}

export class LlmEndpointMetrics {
  public endpoint_id: string;
  public provider: string;
  public model_name: string;
  public ttft_ewma_ms: number;
  public tpm_limit: number;
  public tpm_used: number;
  public rpm_limit: number;
  public rpm_used: number;
  public max_concurrency: number;
  public active_requests: number;
  public rate_limit_hits_last_min = 0;
  public healthy: boolean;

  constructor(init: LlmEndpointMetricsInit) {
    this.endpoint_id = init.endpoint_id;
    this.provider = init.provider;
    this.model_name = init.model_name;
    this.ttft_ewma_ms = init.ttft_ewma_ms ?? 250.0;
    this.tpm_limit = init.tpm_limit ?? 1_000_000;
    this.tpm_used = init.tpm_used ?? 0;
    this.rpm_limit = init.rpm_limit ?? 2000;
    this.rpm_used = init.rpm_used ?? 0;
    this.max_concurrency = init.max_concurrency ?? 200;
    this.active_requests = init.active_requests ?? 0;
    this.healthy = init.healthy ?? true;
  }

  public get tpm_remaining(): number {
    return Math.max(0, this.tpm_limit - this.tpm_used);
  }
}

export interface LeaseRecord {
  task_id: string;
  node_id: string;
  fencing_token: number;
  acquired_at: number;
  expires_at: number;
}

export interface CheckpointRecord {
  checkpoint_id: string;
  task_id: string;
  turn_index: number;
  fencing_token: number;
  state_hash: string;
  overlayfs_diff_uri: string;
  created_at: number;
}

export interface ToolExecutionLog {
  idempotency_key: string;
  task_id: string;
  turn_index: number;
  tool_name: string;
  effect_class: ToolCallEffectClass;
  status: "SUCCESS" | "FAILED";
  result_payload: Record<string, unknown>;
  compensation_tool?: string;
  executed_at: number;
}
