/**
 * @fileoverview M1 TASK-02: Ingress Backpressure & HTTP 429 Retry-After Controller.
 */
import { TaskPriority } from "../models.js";

export interface BackpressureDecision {
  allowed: boolean;
  statusCode: number;
  retryAfterSeconds?: number;
  reason?: string;
}

export class IngressBackpressureController {
  private readonly maxGlobalQueueDepth: number;
  private readonly maxTenantActiveTasks: number;
  private readonly p3ShedThresholdRatio: number;

  constructor(options?: {
    maxGlobalQueueDepth?: number;
    maxTenantActiveTasks?: number;
    p3ShedThresholdRatio?: number;
  }) {
    this.maxGlobalQueueDepth = options?.maxGlobalQueueDepth ?? 5000;
    this.maxTenantActiveTasks = options?.maxTenantActiveTasks ?? 200;
    this.p3ShedThresholdRatio = options?.p3ShedThresholdRatio ?? 0.8;
  }

  public evaluateAdmission(params: {
    priority: TaskPriority;
    globalQueueDepth: number;
    tenantActiveTasks: number;
    clusterPsiPressurePct: number;
  }): BackpressureDecision {
    if (params.clusterPsiPressurePct >= 85.0 && params.priority >= TaskPriority.P2_STANDARD) {
      return {
        allowed: false,
        statusCode: 429,
        retryAfterSeconds: 15,
        reason: `Cluster PSI pressure (${params.clusterPsiPressurePct.toFixed(1)}%) exceeds threshold`,
      };
    }
    if (params.globalQueueDepth >= this.maxGlobalQueueDepth) {
      return {
        allowed: false,
        statusCode: 429,
        retryAfterSeconds: 30,
        reason: `Global queue depth (${params.globalQueueDepth}) reached capacity (${this.maxGlobalQueueDepth})`,
      };
    }
    if (
      params.priority === TaskPriority.P3_BATCH &&
      params.globalQueueDepth >= this.maxGlobalQueueDepth * this.p3ShedThresholdRatio
    ) {
      return {
        allowed: false,
        statusCode: 429,
        retryAfterSeconds: 20,
        reason: "P3_BATCH traffic shed due to high global queue watermark",
      };
    }
    if (params.tenantActiveTasks >= this.maxTenantActiveTasks && params.priority > TaskPriority.P0_EMERGENCY) {
      return {
        allowed: false,
        statusCode: 429,
        retryAfterSeconds: 10,
        reason: `Tenant active task quota (${this.maxTenantActiveTasks}) exceeded`,
      };
    }
    return { allowed: true, statusCode: 202 };
  }
}
