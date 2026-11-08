/**
 * @fileoverview M1 TASK-01: TaskSpec v1beta1 Schema Validator & Semantic Sanitizer.
 */
import { ResourceProfile, TaskPriority, TaskSpec } from "../models.js";

export class TaskSpecValidationError extends Error {
  public readonly field: string;
  constructor(field: string, message: string) {
    super(`Invalid TaskSpec field '${field}': ${message}`);
    this.name = "TaskSpecValidationError";
    this.field = field;
  }
}

/** Canonical TaskSpec version. `agent.scheduler.io/v1beta1` is accepted and normalized to this value. */
export const CANONICAL_TASKSPEC_API_VERSION = "scheduler.agent.io/v1beta1";
const TASKSPEC_API_VERSION_ALIASES = new Set(["agent.scheduler.io/v1beta1"]);

export function canonicalTaskSpecApiVersion(value: unknown): string {
  const apiVersion = String(value ?? CANONICAL_TASKSPEC_API_VERSION);
  if (apiVersion === CANONICAL_TASKSPEC_API_VERSION || TASKSPEC_API_VERSION_ALIASES.has(apiVersion)) {
    return CANONICAL_TASKSPEC_API_VERSION;
  }
  throw new TaskSpecValidationError("apiVersion", `Unsupported apiVersion '${apiVersion}'`);
}

export class TaskSpecValidator {
  public static validateAndNormalize(payload: Record<string, unknown>): TaskSpec {
    if (!payload || typeof payload !== "object") {
      throw new TaskSpecValidationError("root", "Payload must be a JSON object");
    }
    canonicalTaskSpecApiVersion(payload.apiVersion);
    const taskId = String(payload.task_id ?? payload.taskId ?? "").trim();
    if (!taskId || !/^[a-zA-Z0-9_\-:.]{3,128}$/.test(taskId)) {
      throw new TaskSpecValidationError("task_id", "task_id must be 3-128 alphanumeric/dash characters");
    }
    const tenantId = String(payload.tenant_id ?? payload.tenantId ?? "").trim();
    if (!tenantId) {
      throw new TaskSpecValidationError("tenant_id", "tenant_id is required");
    }
    const priorityNum = Number(payload.priority ?? TaskPriority.P2_STANDARD);
    if (![0, 1, 2, 3].includes(priorityNum)) {
      throw new TaskSpecValidationError("priority", "priority must be 0, 1, 2, or 3");
    }
    const maxTurns = Number(payload.max_turns ?? 30);
    if (!Number.isInteger(maxTurns) || maxTurns < 1 || maxTurns > 500) {
      throw new TaskSpecValidationError("max_turns", "max_turns must be an integer in [1, 500]");
    }
    const timeoutSeconds = Number(payload.timeout_seconds ?? 1800);
    if (timeoutSeconds < 5 || timeoutSeconds > 86400) {
      throw new TaskSpecValidationError("timeout_seconds", "timeout_seconds must be in [5, 86400]");
    }
    const rawRp = (payload.resource_profile as Partial<ResourceProfile>) ?? {};
    const resourceProfile: ResourceProfile = {
      think_cpu_cores: Number(rawRp.think_cpu_cores ?? 0.25),
      act_cpu_cores: Number(rawRp.act_cpu_cores ?? 4.0),
      act_memory_mb: Number(rawRp.act_memory_mb ?? 8192),
      disk_mb: Number(rawRp.disk_mb ?? 10240),
    };
    if (resourceProfile.think_cpu_cores <= 0 || resourceProfile.act_cpu_cores < resourceProfile.think_cpu_cores) {
      throw new TaskSpecValidationError(
        "resource_profile.cpu",
        "act_cpu_cores must be >= think_cpu_cores and > 0"
      );
    }
    const deps = Array.isArray(payload.dependencies) ? payload.dependencies.map(String) : [];
    if (deps.includes(taskId)) {
      throw new TaskSpecValidationError("dependencies", "Task cannot depend on itself");
    }
    return new TaskSpec({
      task_id: taskId,
      tenant_id: tenantId,
      priority: priorityNum as TaskPriority,
      prompt: String(payload.prompt ?? ""),
      max_turns: maxTurns,
      timeout_seconds: timeoutSeconds,
      resource_profile: resourceProfile,
      dependencies: deps,
      idempotency_key: payload.idempotency_key ? String(payload.idempotency_key) : undefined,
      parent_task_id: payload.parent_task_id ? String(payload.parent_task_id) : undefined,
      expected_SubAgent_count: Number(payload.expected_SubAgent_count ?? 0),
    });
  }
}
