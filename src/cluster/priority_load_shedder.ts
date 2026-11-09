/**
 * @fileoverview Priority-Aware Progressive Load Shedder (P3 -> P2 -> P1 under Saturation).
 */
import { TaskPriority } from "../models.js";

export class PriorityLoadShedder {
  public shouldAdmitUnderLoad(priority: TaskPriority, utilizationRatio: number): boolean {
    if (utilizationRatio >= 0.98) {
      return priority === TaskPriority.P0_EMERGENCY;
    }
    if (utilizationRatio >= 0.9) {
      return priority <= TaskPriority.P1_HIGH;
    }
    if (utilizationRatio >= 0.8) {
      return priority <= TaskPriority.P2_STANDARD;
    }
    return true;
  }
}
