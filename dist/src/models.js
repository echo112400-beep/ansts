"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TurnWalFrame = exports.ToolCallEffectClass = exports.TaskState = exports.TaskSpec = exports.TaskPriority = exports.NodeHeartbeat = exports.LlmEndpointMetrics = exports.FenceToken = exports.AgentTask = void 0; /**
 * @fileoverview Core Domain Models, TaskSpec v1beta1, 5D Resource Profiles, Monotonic Lease Fencing & LlmEndpointMetrics.
 */let
TaskPriority = exports.TaskPriority = /*#__PURE__*/function (TaskPriority) {TaskPriority[TaskPriority["P0_EMERGENCY"] = 0] = "P0_EMERGENCY";TaskPriority[TaskPriority["P1_HIGH"] = 1] = "P1_HIGH";TaskPriority[TaskPriority["P2_STANDARD"] = 2] = "P2_STANDARD";TaskPriority[TaskPriority["P3_BATCH"] = 3] = "P3_BATCH";return TaskPriority;}({});let






TaskState = exports.TaskState = /*#__PURE__*/function (TaskState) {TaskState["SUBMITTED"] = "SUBMITTED";TaskState["QUEUED"] = "QUEUED";TaskState["LEASED"] = "LEASED";TaskState["RUNNING"] = "RUNNING";TaskState["RUNNING_THINK"] = "RUNNING_THINK";TaskState["THINKING_GEN"] = "THINKING_GEN";TaskState["RUNNING_ACT"] = "RUNNING_ACT";TaskState["TOOL_EXEC"] = "TOOL_EXEC";TaskState["WAITING_SUB_AGENT"] = "WAITING_SUB_AGENT";TaskState["SUSPENDED_APPROVAL"] = "SUSPENDED_APPROVAL";TaskState["PREEMPTING"] = "PREEMPTING";TaskState["HIBERNATED"] = "HIBERNATED";TaskState["COMPLETED"] = "COMPLETED";TaskState["SUCCEEDED"] = "COMPLETED";TaskState["FAILED"] = "FAILED";TaskState["DEAD_LETTER"] = "DEAD_LETTER";return TaskState;}({});


















/** Inference model pools that requests can be routed to (`PrefixCacheAffinityRouter`). */let


ToolCallEffectClass = exports.ToolCallEffectClass = /*#__PURE__*/function (ToolCallEffectClass) {ToolCallEffectClass["READ_ONLY"] = "READ_ONLY";ToolCallEffectClass["PURE_READ"] = "READ_ONLY";ToolCallEffectClass["IDEMPOTENT_WRITE"] = "IDEMPOTENT_WRITE";ToolCallEffectClass["COMPENSATABLE"] = "COMPENSATABLE";ToolCallEffectClass["SAGA_COMPENSATABLE"] = "COMPENSATABLE";ToolCallEffectClass["STAGED_COMMIT"] = "STAGED_COMMIT";ToolCallEffectClass["IRREVERSIBLE"] = "IRREVERSIBLE";return ToolCallEffectClass;}({});











/**
 * Dual-Tuple Monotonic Fencing Token `(epoch, turnSeq, taskId)` aligned with `FenceTokenProto` in `proto/ansts_rpc.proto`.
 */
class FenceToken {
  epoch;
  turnSeq;
  taskId;

  constructor(epoch, turnSeq, taskId) {
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

  nextEpoch() {
    return new FenceToken(this.epoch + 1, 0, this.taskId);
  }

  nextTurn() {
    return new FenceToken(this.epoch, this.turnSeq + 1, this.taskId);
  }

  compareTo(other) {
    if (this.epoch !== other.epoch) {
      return this.epoch > other.epoch ? 1 : -1;
    }
    if (this.turnSeq !== other.turnSeq) {
      return this.turnSeq > other.turnSeq ? 1 : -1;
    }
    return 0;
  }

  isStrictlyOlderThan(other) {
    return this.compareTo(other) < 0;
  }

  toScalarToken() {
    return this.epoch * 1_000_000 + this.turnSeq;
  }

  toProto() {
    return { epoch: this.epoch, turn_seq: this.turnSeq, task_id: this.taskId };
  }
}

/**
 * Turn-Level Write-Ahead Log Frame (`TurnWalFrame`) with CRC32 integrity verification and dual-tuple `FenceToken`.
 */exports.FenceToken = FenceToken;
class TurnWalFrame {
  frameId;
  taskId;
  turnIndex;
  fence;
  changedFiles;
  sqliteWalPages;
  crc32Hex;
  createdAtMs;
  /** Task state at the moment the frame was written (`THINKING_GEN` / `TOOL_EXEC` / ...). */
  state;
  routedModel;
  tokensUsed;
  /** Evaluation telemetry persisted for shadow replay / scorecards. */
  passRate;
  errorCount;
  diffEntropy;
  workspaceMerkleHash;
  diffSnapshot;
  contextMessages;

  constructor(params)



















  {
    this.frameId = params.frameId;
    this.taskId = params.taskId;
    this.turnIndex = params.turnIndex;
    this.fence = params.fence;
    this.changedFiles = [...params.changedFiles];
    this.sqliteWalPages = params.sqliteWalPages;
    this.createdAtMs = params.createdAtMs ?? params.timestampMs ?? Date.now();
    this.state = params.state ?? TaskState.RUNNING_ACT;
    this.routedModel = params.routedModel ?? "";
    this.tokensUsed = params.tokensUsed ?? 0;
    this.passRate = params.passRate ?? 1;
    this.errorCount = params.errorCount ?? 0;
    this.diffEntropy = params.diffEntropy ?? 0;
    this.diffSnapshot = params.diffSnapshot ?? "";
    this.contextMessages = params.contextMessages ? [...params.contextMessages] : [];
    this.crc32Hex = params.crc32Hex ?? TurnWalFrame.computeCrc32Hex(this.taskId, this.turnIndex, this.fence, this.changedFiles);
    this.workspaceMerkleHash = params.workspaceMerkleHash ?? `merkle_${this.crc32Hex}`;
  }

  /**
   * Builds a frame from the sparse trajectory literals used by shadow replay / stream publishing:
   * frame identity (`frameId`, `turnIndex`, `changedFiles`, `sqliteWalPages`) is derived from the
   * `FenceToken` and every telemetry field falls back to the constructor default.
   */
  static fromPartial(params)


















  {
    const turnIndex = params.turnIndex ?? params.fence.turnSeq;
    return new TurnWalFrame({
      ...params,
      frameId: params.frameId ?? `frame-${params.taskId}-t${turnIndex}`,
      turnIndex,
      changedFiles: params.changedFiles ?? [],
      sqliteWalPages: params.sqliteWalPages ?? 0
    });
  }

  static computeCrc32Hex(
  taskId,
  turnIndex,
  fence,
  changedFiles)
  {
    const raw = `${taskId}:${turnIndex}:${fence.epoch}:${fence.turnSeq}:${changedFiles.join(",")}`;
    let crc = 0xffffffff;
    for (let i = 0; i < raw.length; i++) {
      crc ^= raw.charCodeAt(i);
      for (let j = 0; j < 8; j++) {
        crc = crc >>> 1 ^ (crc & 1 ? 0xedb88320 : 0);
      }
    }
    return ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, "0");
  }

  verifyIntegrity() {
    const expected = TurnWalFrame.computeCrc32Hex(
      this.taskId,
      this.turnIndex,
      this.fence,
      this.changedFiles
    );
    return this.crc32Hex === expected;
  }
}

/**
 * Mutable per-task runtime record used by the console BFF / HITL approval gate.
 * It is intentionally thin: the durable task specification lives in `TaskSpec`.
 */exports.TurnWalFrame = TurnWalFrame;









class AgentTask {
  taskId;
  tenantId;
  state;
  priority;
  fence;
  createdAtMs;

  constructor(params) {
    if (!params.taskId) {
      throw new Error("AgentTask requires a non-empty taskId");
    }
    this.taskId = params.taskId;
    this.tenantId = params.tenantId;
    this.state = params.state ?? TaskState.SUBMITTED;
    this.priority = params.priority ?? TaskPriority.P2_STANDARD;
    this.fence = params.fence ?? null;
    this.createdAtMs = params.createdAtMs ?? Date.now();
  }

  transitionTo(nextState) {
    this.state = nextState;
  }
}exports.AgentTask = AgentTask;























class TaskSpec {
  task_id;
  tenant_id;
  priority;
  prompt;
  max_turns;
  current_turn = 0;
  timeout_seconds;
  resource_profile;
  dependencies;
  idempotency_key;
  parent_task_id;
  expected_SubAgent_count;
  prefix_Cache_hash = "";
  state = TaskState.SUBMITTED;
  assigned_node_id;
  lease_fencing_token = 0;
  retry_count = 0;
  created_at;

  constructor(init) {
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
      disk_mb: init.resource_profile?.disk_mb ?? 10240
    };
    this.dependencies = init.dependencies ? [...init.dependencies] : [];
    this.idempotency_key = init.idempotency_key;
    this.parent_task_id = init.parent_task_id;
    this.expected_SubAgent_count = init.expected_SubAgent_count ?? 0;
    this.prefix_Cache_hash = init.prefix_Cache_hash ?? "";
    this.created_at = Date.now() / 1000;
  }
}exports.TaskSpec = TaskSpec;














class NodeHeartbeat {
  node_id;
  total_cpu_cores;
  free_cpu_cores;
  total_memory_mb;
  free_memory_mb;
  warm_pool_ready;
  psi_memory_some_avg10;
  running_tasks;
  healthy;
  cached_images;
  timestamp;

  constructor(init) {
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
    this.timestamp = Date.now() / 1000;
  }
}exports.NodeHeartbeat = NodeHeartbeat;















class LlmEndpointMetrics {
  endpoint_id;
  provider;
  model_name;
  ttft_ewma_ms;
  tpm_limit;
  tpm_used;
  rpm_limit;
  rpm_used;
  max_concurrency;
  active_requests;
  rate_limit_hits_last_min = 0;
  healthy;

  constructor(init) {
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

  get tpm_remaining() {
    return Math.max(0, this.tpm_limit - this.tpm_used);
  }
}exports.LlmEndpointMetrics = LlmEndpointMetrics;