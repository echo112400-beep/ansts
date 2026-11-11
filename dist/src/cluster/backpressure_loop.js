"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BidirectionalBackpressureController = void 0;
/**
 * @fileoverview M6 TASK-24 & M63-2: End-to-End Bidirectional Backpressure Closed-Loop Coordinator
 * with Zero-Endpoint Outage Protection & NaN/Out-of-Range Snapshot Sanitization.
 */
const backpressure_js_1 = require("../gateway/backpressure.js");
const models_js_1 = require("../models.js");
class BidirectionalBackpressureController {
    ingressController;
    constructor(ingressController = new backpressure_js_1.IngressBackpressureController()) {
        this.ingressController = ingressController;
    }
    /**
     * [M63-2] DFX-Availability:
     * - Treats `totalEndpoints <= 0` with `globalRateLimitedEndpoints > 0` (or negative/NaN PSI) safely:
     *   if `maxNodePsiSomePct` is NaN, defaults to conservative degraded evaluation;
     *   clamps `llmUnhealthyRatio` to `[0, 1]`.
     */
    computeClusterHealthState(snapshot) {
        const safePsi = Number.isFinite(snapshot.maxNodePsiSomePct)
            ? Math.max(0, snapshot.maxNodePsiSomePct)
            : 80.0;
        const safeTotalEp = Number.isFinite(snapshot.totalEndpoints)
            ? Math.max(0, Math.floor(snapshot.totalEndpoints))
            : 0;
        const safeLimitedEp = Number.isFinite(snapshot.globalRateLimitedEndpoints)
            ? Math.max(0, Math.floor(snapshot.globalRateLimitedEndpoints))
            : 0;
        const safeQueueDepth = Number.isFinite(snapshot.globalQueueDepth)
            ? Math.max(0, Math.floor(snapshot.globalQueueDepth))
            : 0;
        const llmUnhealthyRatio = safeTotalEp > 0
            ? Math.min(1.0, safeLimitedEp / safeTotalEp)
            : safeLimitedEp > 0
                ? 1.0
                : 0;
        if (safePsi >= 75.0 || llmUnhealthyRatio >= 0.75) {
            const p3Check = this.ingressController.evaluateAdmission({
                priority: models_js_1.TaskPriority.P3_BATCH,
                globalQueueDepth: safeQueueDepth,
                tenantActiveTasks: 0,
                clusterPsiPressurePct: Math.max(90.0, safePsi),
            });
            return {
                mode: "RED_SHEDDING",
                recommendedDispatchRateScale: 0.2,
                allowBatchAdmission: p3Check.allowed,
            };
        }
        if (safePsi >= 45.0 || llmUnhealthyRatio >= 0.35) {
            return {
                mode: "YELLOW_DEGRADED",
                recommendedDispatchRateScale: 0.55,
                allowBatchAdmission: false,
            };
        }
        return {
            mode: "GREEN",
            recommendedDispatchRateScale: 1.0,
            allowBatchAdmission: true,
        };
    }
}
exports.BidirectionalBackpressureController = BidirectionalBackpressureController;
