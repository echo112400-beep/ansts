/**
 * @fileoverview Priority load shedder that keeps P0 work when the cluster is saturated.
 */
export type TaskPriorityClass = "P3_BACKGROUND" | "P2_ASYNC_INDEX" | "P1_STANDARD" | "P0_CRITICAL";
export interface LoadSheddingReport {
  ingressTotalCount: number;
  admittedCount: number;
  shedCount: number;
  shedDetails: Record<TaskPriorityClass, number>;
  clusterLoadRatio: number;
}
export class PriorityLoadShedder {
  constructor(public readonly maxConcurrencyLimit = 1000) {}
  public evaluateAdmissionAndShed(incomingRequests: Array<{ taskId: string; priorityClass: TaskPriorityClass }>, currentInFlightCount: number): LoadSheddingReport {
    const loadRatio = currentInFlightCount / Math.max(1, this.maxConcurrencyLimit);
    const shedDetails: Record<TaskPriorityClass, number> = { P3_BACKGROUND: 0, P2_ASYNC_INDEX: 0, P1_STANDARD: 0, P0_CRITICAL: 0 };
    let admitted = 0;
    let shed = 0;
    for (const req of incomingRequests) {
      let allow = true;
      if (loadRatio >= 0.90 && (req.priorityClass === "P3_BACKGROUND" || req.priorityClass === "P2_ASYNC_INDEX")) allow = false;
      if (loadRatio >= 0.98 && req.priorityClass === "P1_STANDARD") allow = false;
      if (allow) admitted += 1; else { shed += 1; shedDetails[req.priorityClass] += 1; }
    }
    return { ingressTotalCount: incomingRequests.length, admittedCount: admitted, shedCount: shed, shedDetails, clusterLoadRatio: Number(loadRatio.toFixed(3)) };
  }
}
