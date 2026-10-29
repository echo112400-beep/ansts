/**
 * TASK-02: API Gateway Server with 24h SHA-256 Idempotency Deduplication and Task Lifecycle Endpoints.
 */

import * as crypto from "crypto";
import { AgentTask, TaskState } from "../models";
import { CoreScheduler5D } from "../scheduler";
import { IngressBackpressureController } from "./backpressure";
import { TaskSpecValidator } from "./validator";

export class ApiGatewayServer {
  public readonly validator: TaskSpecValidator;
  public readonly bpController: IngressBackpressureController;
  private idempotencyTable: Map<string, string> = new Map();

  constructor(
    public readonly scheduler: CoreScheduler5D,
    validator?: TaskSpecValidator,
    bpController?: IngressBackpressureController,
  ) {
    this.validator = validator ?? new TaskSpecValidator();
    this.bpController = bpController ?? new IngressBackpressureController();
  }

  public submitTaskSpec(
    rawDoc: Record<string, any>,
    now: number = 0.0,
  ): { statusCode: number; body: Record<string, any>; headers: Record<string, string> } {
    const norm = this.validator.validateAndNormalize(rawDoc);
    const meta = norm.metadata;
    const spec = norm.spec;
    const tenantId: string = meta.tenant_id;
    const idemKey: string = meta.idempotency_key;
    const dedupHash = crypto.createHash("sha256").update(`${tenantId}:${idemKey}`, "utf8").digest("hex");

    const existingId = this.idempotencyTable.get(dedupHash);
    if (existingId && this.scheduler.tasks.has(existingId)) {
      const existingTask = this.scheduler.tasks.get(existingId)!;
      return {
        statusCode: 200,
        body: {
          task_id: existingId,
          deduplicated: true,
          state: existingTask.state,
          soft_reserved_tokens: existingTask.softReservedTokens,
        },
        headers: { "X-Idempotent-Replay": "true" },
      };
    }

    const qDepth = (this.scheduler.queueManager.tenantQueues.get(tenantId) ?? []).length;
    const decision = this.bpController.evaluateAdmission(tenantId, qDepth);
    if (!decision.allowed) {
      return {
        statusCode: decision.statusCode,
        body: { error: "TooManyRequests", reason: decision.reason },
        headers: decision.headers,
      };
    }

    const cog = spec.cognitive_policy;
    const quota = spec.quota_and_dag_policy;
    const sbx = spec.execution_and_sandbox;
    const pe = sbx.phase_elastic_resources;

    const task = new AgentTask({
      taskId: meta.task_id,
      tenantId,
      priorityClass: meta.priority_class,
      baseScore: meta.priority_score,
      enqueueTs: now,
      deadlineTs: now + Number(spec.scheduling_policy.sla_deadline_sec ?? 600),
      primaryModel: cog.primary_model,
      fastUtilityModel: cog.fast_utility_model,
      fallbackModels: cog.fallback_models,
      effort: cog.default_reasoning_effort,
      reservedTps: quota.reserved_tps,
      maxTotalTokens: quota.max_total_tokens,
      maxSessions: quota.max_concurrent_sessions,
      cogCpu: Number(pe.cognitive_phase.cpu_cores),
      execCpu: Number(pe.execution_phase.cpu_request_cores),
      burstCpu: Number(pe.execution_phase.cpu_burst_limit_cores),
      cogMemMb: Number(pe.cognitive_phase.memory_active_mb),
      execMemMb: Number(pe.execution_phase.memory_limit_mb),
      isolationDriver: sbx.isolation_driver,
      warmPoolId: sbx.warm_pool_id,
      maxTurns: sbx.max_turns,
      customMetadata: meta.custom_metadata ?? {},
    });

    if (!this.scheduler.tenants.has(tenantId)) {
      this.scheduler.registerTenant(tenantId, 1.0);
    }

    const softTokens = this.scheduler.submitTask(task);
    this.idempotencyTable.set(dedupHash, task.taskId);
    return {
      statusCode: 202,
      body: {
        task_id: task.taskId,
        deduplicated: false,
        state: task.state,
        soft_reserved_tokens: softTokens,
      },
      headers: decision.headers,
    };
  }
}
