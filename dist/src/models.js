"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ToolCallEffectClass = exports.TaskState = exports.TaskSpec = exports.TaskPriority = exports.NodeHeartbeat = exports.LlmEndpointMetrics = void 0; /**
 * @fileoverview Core Domain Models, TaskSpec v1beta1, 5D Resource Profiles, Monotonic Lease Fencing & LlmEndpointMetrics.
 */let
TaskPriority;exports.TaskPriority = TaskPriority;(function (TaskPriority) {TaskPriority[TaskPriority["P0_EMERGENCY"] = 0] = "P0_EMERGENCY";TaskPriority[TaskPriority["P1_HIGH"] = 1] = "P1_HIGH";TaskPriority[TaskPriority["P2_STANDARD"] = 2] = "P2_STANDARD";TaskPriority[TaskPriority["P3_BATCH"] = 3] = "P3_BATCH";})(TaskPriority || (exports.TaskPriority = TaskPriority = {}));let






TaskState;exports.TaskState = TaskState;(function (TaskState) {TaskState["SUBMITTED"] = "SUBMITTED";TaskState["QUEUED"] = "QUEUED";TaskState["LEASED"] = "LEASED";TaskState["RUNNING_THINK"] = "RUNNING_THINK";TaskState["RUNNING_ACT"] = "RUNNING_ACT";TaskState["WAITING_SUB_AGENT"] = "WAITING_SUB_AGENT";TaskState["SUSPENDED_APPROVAL"] = "SUSPENDED_APPROVAL";TaskState["PREEMPTING"] = "PREEMPTING";TaskState["COMPLETED"] = "COMPLETED";TaskState["FAILED"] = "FAILED";TaskState["DEAD_LETTER"] = "DEAD_LETTER";})(TaskState || (exports.TaskState = TaskState = {}));let













ToolCallEffectClass;exports.ToolCallEffectClass = ToolCallEffectClass;(function (ToolCallEffectClass) {ToolCallEffectClass["READ_ONLY"] = "READ_ONLY";ToolCallEffectClass["IDEMPOTENT_WRITE"] = "IDEMPOTENT_WRITE";ToolCallEffectClass["COMPENSATABLE"] = "COMPENSATABLE";ToolCallEffectClass["IRREVERSIBLE"] = "IRREVERSIBLE";})(ToolCallEffectClass || (exports.ToolCallEffectClass = ToolCallEffectClass = {}));




























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