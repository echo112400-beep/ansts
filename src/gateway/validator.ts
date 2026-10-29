/**
 * TASK-01: Strict TaskSpec v1beta1 Validator and Default Value Injector across 7 spec blocks.
 */

export class TaskSpecValidationError extends Error {
  constructor(public readonly fieldPath: string, message: string) {
    super(`[${fieldPath}] ${message}`);
    this.name = "TaskSpecValidationError";
  }
}

const VALID_PRIORITIES = new Set([
  "P0", "P1", "P2", "P3", "P4",
  "critical-p0", "high-interactive", "normal-batch", "low-background", "spot-opportunistic",
]);
const VALID_MODELS = new Set(["DeepSeek", "GLM-5.3", "Qwen-3.8"]);
const VALID_EFFORTS = new Set(["minimal", "low", "medium", "high", "max"]);
const VALID_DRIVERS = new Set(["firecracker", "kata", "gvisor", "runc"]);
const VALID_EFFECT_CLASSES = new Set(["PURE_READ", "IDEMPOTENT_WRITE", "SAGA_COMPENSATABLE", "STAGED_COMMIT"]);

export class TaskSpecValidator {
  public validateAndNormalize(rawDoc: Record<string, any>): Record<string, any> {
    if (!rawDoc || typeof rawDoc !== "object") {
      throw new TaskSpecValidationError("root", "TaskSpec document must be an object");
    }
    const doc = JSON.parse(JSON.stringify(rawDoc));
    if (doc.apiVersion !== "scheduler.agent.io/v1beta1") {
      throw new TaskSpecValidationError("apiVersion", "Must be 'scheduler.agent.io/v1beta1'");
    }
    if (doc.kind !== "AgentTask") {
      throw new TaskSpecValidationError("kind", "Must be 'AgentTask'");
    }

    const meta = doc.metadata;
    if (!meta || typeof meta !== "object") {
      throw new TaskSpecValidationError("metadata", "Missing metadata object");
    }
    for (const reqField of ["task_id", "tenant_id"]) {
      if (!meta[reqField]) {
        throw new TaskSpecValidationError(`metadata.${reqField}`, "Field is required");
      }
    }
    meta.idempotency_key = meta.idempotency_key ?? `idem-${meta.task_id}`;
    meta.priority_class = meta.priority_class ?? "P1";
    if (!VALID_PRIORITIES.has(meta.priority_class)) {
      throw new TaskSpecValidationError("metadata.priority_class", `Invalid priority: ${meta.priority_class}`);
    }
    meta.priority_score = meta.priority_score ?? 800;
    if (typeof meta.priority_score !== "number" || meta.priority_score < 0 || meta.priority_score > 1000) {
      throw new TaskSpecValidationError("metadata.priority_score", "Must be in [0, 1000]");
    }
    meta.custom_metadata = meta.custom_metadata ?? {};

    const spec = doc.spec;
    if (!spec || typeof spec !== "object") {
      throw new TaskSpecValidationError("spec", "Missing spec object");
    }

    spec.scheduling_policy = spec.scheduling_policy ?? {};
    spec.cognitive_policy = spec.cognitive_policy ?? {};
    spec.quota_and_dag_policy = spec.quota_and_dag_policy ?? {};
    spec.execution_and_sandbox = spec.execution_and_sandbox ?? {};
    spec.tools_and_saga_config = spec.tools_and_saga_config ?? {};
    spec.harness_and_resilience = spec.harness_and_resilience ?? {};
    spec.observability_and_notify = spec.observability_and_notify ?? { event_stream: { enabled: true } };

    // 1. scheduling_policy
    const sched = spec.scheduling_policy;
    sched.queue_name = sched.queue_name ?? "default";
    sched.max_queue_wait_sec = sched.max_queue_wait_sec ?? 300;
    if (sched.max_queue_wait_sec <= 0) {
      throw new TaskSpecValidationError("spec.scheduling_policy.max_queue_wait_sec", "Must be > 0");
    }
    sched.preemption_policy = sched.preemption_policy ?? "PreemptToHibernate";

    // 2. cognitive_policy
    const cog = spec.cognitive_policy;
    cog.primary_model = cog.primary_model ?? "DeepSeek";
    if (!VALID_MODELS.has(cog.primary_model)) {
      throw new TaskSpecValidationError("spec.cognitive_policy.primary_model", `Invalid model: ${cog.primary_model}`);
    }
    cog.fast_utility_model = cog.fast_utility_model ?? "GLM-5.3";
    if (!VALID_MODELS.has(cog.fast_utility_model)) {
      throw new TaskSpecValidationError("spec.cognitive_policy.fast_utility_model", `Invalid model: ${cog.fast_utility_model}`);
    }
    cog.fallback_models = cog.fallback_models ?? ["Qwen-3.8", "GLM-5.3"];
    if (!Array.isArray(cog.fallback_models) || cog.fallback_models.length === 0 || cog.fallback_models.some((m: string) => !VALID_MODELS.has(m))) {
      throw new TaskSpecValidationError("spec.cognitive_policy.fallback_models", "Invalid fallback models");
    }
    cog.default_reasoning_effort = cog.default_reasoning_effort ?? "high";
    if (!VALID_EFFORTS.has(cog.default_reasoning_effort)) {
      throw new TaskSpecValidationError("spec.cognitive_policy.default_reasoning_effort", "Invalid reasoning effort");
    }

    // 3. quota_and_dag_policy
    const quota = spec.quota_and_dag_policy;
    quota.max_total_tokens = quota.max_total_tokens ?? 500000;
    quota.reserved_tps = quota.reserved_tps ?? 120;
    quota.max_concurrent_sessions = quota.max_concurrent_sessions ?? 4;
    if (quota.max_total_tokens <= 0 || quota.reserved_tps <= 0 || quota.max_concurrent_sessions <= 0) {
      throw new TaskSpecValidationError("spec.quota_and_dag_policy", "Quotas must be > 0");
    }

    // 4. execution_and_sandbox
    const sbx = spec.execution_and_sandbox;
    sbx.isolation_driver = sbx.isolation_driver ?? "firecracker";
    if (!VALID_DRIVERS.has(sbx.isolation_driver)) {
      throw new TaskSpecValidationError("spec.execution_and_sandbox.isolation_driver", "Invalid driver");
    }
    sbx.warm_pool_id = sbx.warm_pool_id ?? "pool-py312-ds";
    sbx.max_turns = sbx.max_turns ?? 20;
    sbx.phase_elastic_resources = sbx.phase_elastic_resources ?? {};
    sbx.phase_elastic_resources.cognitive_phase = sbx.phase_elastic_resources.cognitive_phase ?? {
      cpu_cores: 0.1,
      memory_active_mb: 256,
    };
    sbx.phase_elastic_resources.execution_phase = sbx.phase_elastic_resources.execution_phase ?? {
      cpu_request_cores: 2.0,
      cpu_burst_limit_cores: 8.0,
      memory_limit_mb: 4096,
    };

    // 5. tools_and_saga_config
    const policies = spec.tools_and_saga_config.transactional_policies ?? [];
    policies.forEach((pol: any, idx: number) => {
      if (!VALID_EFFECT_CLASSES.has(pol.effect_class)) {
        throw new TaskSpecValidationError(`spec.tools_and_saga_config.transactional_policies[${idx}]`, "Invalid effect_class");
      }
      if (pol.effect_class === "SAGA_COMPENSATABLE" && !pol.compensation_tool) {
        throw new TaskSpecValidationError(`spec.tools_and_saga_config.transactional_policies[${idx}]`, "Requires compensation_tool");
      }
    });

    return doc;
  }
}
