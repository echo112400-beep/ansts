/**
 * @fileoverview M1 TASK-02 & M56-4: Ingress Backpressure & HTTP 429 Retry-After Controller
 * with Reserved P0_EMERGENCY Queue Headroom so life-safety/emergency tasks can always enter and preempt P3_BATCH.
 */
import { TaskPriority } from "../models.js";

export interface BackpressureDecision {
  allowed: boolean;
  statusCode: number;
  retryAfterSeconds?: number;
  reason?: string;
  usedEmergencyHeadroom?: boolean;
}

export class IngressBackpressureController {
  private readonly maxGlobalQueueDepth: number;
  private readonly maxTenantActiveTasks: number;
  private readonly p3ShedThresholdRatio: number;
  private readonly p0EmergencyReserveRatio: number;

  constructor(options?: {
    maxGlobalQueueDepth?: number;
    maxTenantActiveTasks?: number;
    p3ShedThresholdRatio?: number;
    p0EmergencyReserveRatio?: number;
  }) {
    this.maxGlobalQueueDepth = options?.maxGlobalQueueDepth ?? 5000;
    this.maxTenantActiveTasks = options?.maxTenantActiveTasks ?? 200;
    this.p3ShedThresholdRatio = options?.p3ShedThresholdRatio ?? 0.8;
    this.p0EmergencyReserveRatio = options?.p0EmergencyReserveRatio ?? 0.1;
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

    // [M56-4] Reserve +10% emergency burst headroom for P0_EMERGENCY tasks so they are not blocked
    // when P1/P2/P3 tasks fill maxGlobalQueueDepth, allowing P0_EMERGENCY to enter and preempt P3_BATCH.
    const effectiveMaxDepth =
      params.priority === TaskPriority.P0_EMERGENCY
        ? Math.ceil(this.maxGlobalQueueDepth * (1 + this.p0EmergencyReserveRatio))
        : this.maxGlobalQueueDepth;

    if (params.globalQueueDepth >= effectiveMaxDepth) {
      return {
        allowed: false,
        statusCode: 429,
        retryAfterSeconds: 30,
        reason: `Global queue depth (${params.globalQueueDepth}) reached capacity (${effectiveMaxDepth})`,
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
    return {
      allowed: true,
      statusCode: 202,
      usedEmergencyHeadroom:
        params.priority === TaskPriority.P0_EMERGENCY && params.globalQueueDepth >= this.maxGlobalQueueDepth,
    };
  }
}
