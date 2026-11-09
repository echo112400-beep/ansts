"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.CapacitySlaEnforcer = void 0; /**
 * @fileoverview M17 Cluster Capacity SLA Enforcer & Headroom Reservation Guard.
 */
class CapacitySlaEnforcer {
  evaluateSlaCompliance(params)



  {
    const breachedMetrics = [];
    if (params.p0QueueWaitP99Ms > 250) breachedMetrics.push("p0QueueWaitP99Ms");
    if (params.warmPoolHitRatio < 0.85) breachedMetrics.push("warmPoolHitRatio");
    if (params.freeClusterCpuRatio < 0.1) breachedMetrics.push("freeClusterCpuRatio");
    return {
      compliant: breachedMetrics.length === 0,
      breachedMetrics,
      reserveEmergencyHeadroom: params.freeClusterCpuRatio < 0.15
    };
  }
}exports.CapacitySlaEnforcer = CapacitySlaEnforcer;