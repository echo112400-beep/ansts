"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.PriorityLoadShedder = void 0;


var _models = require("../models.js"); /**
 * @fileoverview Priority-Aware Progressive Load Shedder (P3 -> P2 -> P1 under Saturation).
 */class PriorityLoadShedder {
  shouldAdmitUnderLoad(priority, utilizationRatio) {
    if (utilizationRatio >= 0.98) {
      return priority === _models.TaskPriority.P0_EMERGENCY;
    }
    if (utilizationRatio >= 0.9) {
      return priority <= _models.TaskPriority.P1_HIGH;
    }
    if (utilizationRatio >= 0.8) {
      return priority <= _models.TaskPriority.P2_STANDARD;
    }
    return true;
  }
}exports.PriorityLoadShedder = PriorityLoadShedder;