"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.IngressBackpressureController = void 0;



var _models = require("../models.js"); /**
 * @fileoverview M1 TASK-02 & M56-4: Ingress Backpressure & HTTP 429 Retry-After Controller
 * with Reserved P0_EMERGENCY Queue Headroom so life-safety/emergency tasks can always enter and preempt P3_BATCH.
 */






class IngressBackpressureController {
  maxGlobalQueueDepth;
  maxTenantActiveTasks;
  p3ShedThresholdRatio;
  p0EmergencyReserveRatio;

  constructor(options)




  {
    this.maxGlobalQueueDepth = options?.maxGlobalQueueDepth ?? 5000;
    this.maxTenantActiveTasks = options?.maxTenantActiveTasks ?? 200;
    this.p3ShedThresholdRatio = options?.p3ShedThresholdRatio ?? 0.8;
    this.p0EmergencyReserveRatio = options?.p0EmergencyReserveRatio ?? 0.1;
  }

  evaluateAdmission(params)




  {
    if (params.clusterPsiPressurePct >= 85.0 && params.priority >= _models.TaskPriority.P2_STANDARD) {
      return {
        allowed: false,
        statusCode: 429,
        retryAfterSeconds: 15,
        reason: `Cluster PSI pressure (${params.clusterPsiPressurePct.toFixed(1)}%) exceeds threshold`
      };
    }

    // [M56-4] Reserve +10% emergency burst headroom for P0_EMERGENCY tasks so they are not blocked
    // when P1/P2/P3 tasks fill maxGlobalQueueDepth, allowing P0_EMERGENCY to enter and preempt P3_BATCH.
    const effectiveMaxDepth =
    params.priority === _models.TaskPriority.P0_EMERGENCY ?
    Math.ceil(this.maxGlobalQueueDepth * (1 + this.p0EmergencyReserveRatio)) :
    this.maxGlobalQueueDepth;

    if (params.globalQueueDepth >= effectiveMaxDepth) {
      return {
        allowed: false,
        statusCode: 429,
        retryAfterSeconds: 30,
        reason: `Global queue depth (${params.globalQueueDepth}) reached capacity (${effectiveMaxDepth})`
      };
    }
    if (
    params.priority === _models.TaskPriority.P3_BATCH &&
    params.globalQueueDepth >= this.maxGlobalQueueDepth * this.p3ShedThresholdRatio)
    {
      return {
        allowed: false,
        statusCode: 429,
        retryAfterSeconds: 20,
        reason: "P3_BATCH traffic shed due to high global queue watermark"
      };
    }
    if (params.tenantActiveTasks >= this.maxTenantActiveTasks && params.priority > _models.TaskPriority.P0_EMERGENCY) {
      return {
        allowed: false,
        statusCode: 429,
        retryAfterSeconds: 10,
        reason: `Tenant active task quota (${this.maxTenantActiveTasks}) exceeded`
      };
    }
    return {
      allowed: true,
      statusCode: 202,
      usedEmergencyHeadroom:
      params.priority === _models.TaskPriority.P0_EMERGENCY && params.globalQueueDepth >= this.maxGlobalQueueDepth
    };
  }
}exports.IngressBackpressureController = IngressBackpressureController;