/**
 * @fileoverview M16 & M63-2: Priority-Aware Progressive Load Shedder (P3 -> P2 -> P1 under Saturation)
 * with Fail-Safe NaN/Out-of-Bounds Utilization Guard (admitting only P0_EMERGENCY when telemetry is corrupted).
 */
import { TaskPriority } from "../models.js";

export class PriorityLoadShedder {
  public shouldAdmitUnderLoad(priority: TaskPriority, utilizationRatio: number): boolean {
    if (!Number.isFinite(utilizationRatio) || utilizationRatio < 0) {
      // [M63-2] Fail-safe under corrupted/NaN utilization telemetry: only allow P0_EMERGENCY
      return priority === TaskPriority.P0_EMERGENCY;
    }
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
