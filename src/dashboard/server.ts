/**
 * TASK-31: Minimalist Simulation Dashboard Real-Time API & SSE Bridge.
 */

import { AnstsClusterRuntime } from "../cli/anstsctl";

export class DashboardBridgeService {
  public readonly runtime: AnstsClusterRuntime;

  constructor(runtime?: AnstsClusterRuntime) {
    this.runtime = runtime ?? new AnstsClusterRuntime();
  }

  public getClusterStateJson(): string {
    return JSON.stringify(this.runtime.clusterTopSnapshot());
  }

  public formatSseFrame(eventEnvelope: Record<string, any>): string {
    const eid = eventEnvelope.id ?? "evt_0";
    const etype = eventEnvelope.type ?? "agent.event";
    const dataStr = JSON.stringify(eventEnvelope.data ?? {});
    return `id: ${eid}\nevent: ${etype}\ndata: ${dataStr}\n\n`;
  }
}
