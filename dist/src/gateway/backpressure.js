"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.IngressBackpressureController = void 0;


var _models = require("../models.js"); /**
 * @fileoverview M1 TASK-02: Ingress Backpressure & HTTP 429 Retry-After Controller.
 */






class IngressBackpressureController {
  maxGlobalQueueDepth;
  maxTenantActiveTasks;
  p3ShedThresholdRatio;

  constructor(options)



  {
    this.maxGlobalQueueDepth = options?.maxGlobalQueueDepth ?? 5000;
    this.maxTenantActiveTasks = options?.maxTenantActiveTasks ?? 200;
    this.p3ShedThresholdRatio = options?.p3ShedThresholdRatio ?? 0.8;
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
    if (params.globalQueueDepth >= this.maxGlobalQueueDepth) {
      return {
        allowed: false,
        statusCode: 429,
        retryAfterSeconds: 30,
        reason: `Global queue depth (${params.globalQueueDepth}) reached capacity (${this.maxGlobalQueueDepth})`
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
    return { allowed: true, statusCode: 202 };
  }
}exports.IngressBackpressureController = IngressBackpressureController;