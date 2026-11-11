"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.PriorityLoadShedder = void 0;
/**
 * @fileoverview M16 & M63-2: Priority-Aware Progressive Load Shedder (P3 -> P2 -> P1 under Saturation)
 * with Fail-Safe NaN/Out-of-Bounds Utilization Guard (admitting only P0_EMERGENCY when telemetry is corrupted).
 */
const models_js_1 = require("../models.js");
class PriorityLoadShedder {
    shouldAdmitUnderLoad(priority, utilizationRatio) {
        if (!Number.isFinite(utilizationRatio) || utilizationRatio < 0) {
            // [M63-2] Fail-safe under corrupted/NaN utilization telemetry: only allow P0_EMERGENCY
            return priority === models_js_1.TaskPriority.P0_EMERGENCY;
        }
        if (utilizationRatio >= 0.98) {
            return priority === models_js_1.TaskPriority.P0_EMERGENCY;
        }
        if (utilizationRatio >= 0.9) {
            return priority <= models_js_1.TaskPriority.P1_HIGH;
        }
        if (utilizationRatio >= 0.8) {
            return priority <= models_js_1.TaskPriority.P2_STANDARD;
        }
        return true;
    }
}
exports.PriorityLoadShedder = PriorityLoadShedder;
