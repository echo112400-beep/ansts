/**
 * TASK-03: L3 Client Admission Backpressure & HTTP 429 Header Injector.
 */

export interface AdmissionDecision {
  allowed: boolean;
  statusCode: number;
  headers: Record<string, string>;
  reason: string;
}

export class IngressBackpressureController {
  public clusterStage: string = "NORMAL";

  constructor(public readonly maxTenantQueueDepth: number = 50) {}

  public evaluateAdmission(tenantId: string, currentQueueDepth: number): AdmissionDecision {
    const queuePos = currentQueueDepth + 1;
    if (this.clusterStage === "STAGE3_QUEUE_FREEZE") {
      return {
        allowed: false,
        statusCode: 429,
        headers: {
          "Retry-After": "30",
          "X-Queue-Position": String(queuePos),
          "X-Backpressure-Stage": this.clusterStage,
        },
        reason: "Cluster saturation freeze (Stage 3)",
      };
    }
    if (currentQueueDepth >= this.maxTenantQueueDepth) {
      const retrySec = Math.min(60, 5 + (currentQueueDepth - this.maxTenantQueueDepth) * 2);
      return {
        allowed: false,
        statusCode: 429,
        headers: {
          "Retry-After": String(retrySec),
          "X-Queue-Position": String(queuePos),
          "X-Backpressure-Stage": "TENANT_QUEUE_FULL",
        },
        reason: `Tenant ${tenantId} queue depth ${currentQueueDepth} >= limit ${this.maxTenantQueueDepth}`,
      };
    }
    return {
      allowed: true,
      statusCode: 202,
      headers: {
        "X-Queue-Position": String(queuePos),
        "X-Backpressure-Stage": this.clusterStage,
      },
      reason: "Accepted",
    };
  }
}
