"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.BidirectionalBackpressureController = void 0;


var _backpressure = require("../gateway/backpressure.js");
var _models = require("../models.js"); /**
 * @fileoverview M6 TASK-24: End-to-End Bidirectional Backpressure Closed-Loop Coordinator.
 */






class BidirectionalBackpressureController {
  ingressController;

  constructor(ingressController = new _backpressure.IngressBackpressureController()) {
    this.ingressController = ingressController;
  }

  computeClusterHealthState(snapshot)



  {
    const llmUnhealthyRatio =
    snapshot.totalEndpoints > 0 ?
    snapshot.globalRateLimitedEndpoints / snapshot.totalEndpoints :
    0;

    if (snapshot.maxNodePsiSomePct >= 75.0 || llmUnhealthyRatio >= 0.75) {
      const p3Check = this.ingressController.evaluateAdmission({
        priority: _models.TaskPriority.P3_BATCH,
        globalQueueDepth: snapshot.globalQueueDepth,
        tenantActiveTasks: 0,
        clusterPsiPressurePct: 90.0
      });
      return {
        mode: "RED_SHEDDING",
        recommendedDispatchRateScale: 0.2,
        allowBatchAdmission: p3Check.allowed
      };
    }

    if (snapshot.maxNodePsiSomePct >= 45.0 || llmUnhealthyRatio >= 0.35) {
      return {
        mode: "YELLOW_DEGRADED",
        recommendedDispatchRateScale: 0.55,
        allowBatchAdmission: false
      };
    }

    return {
      mode: "GREEN",
      recommendedDispatchRateScale: 1.0,
      allowBatchAdmission: true
    };
  }
}exports.BidirectionalBackpressureController = BidirectionalBackpressureController;