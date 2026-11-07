/**
 * @fileoverview TASK-88: TanStack Management Console BFF API Controller (`ConsoleBffController`).
 *
 * Provides typed JSON endpoints for the ANSTS v2.3 TanStack Management Console:
 * - `GET /v1beta1/console/overview`: Real-time cluster health, RPO=0 status, active sandboxes, and SLA metrics
 * - `GET /v1beta1/console/tunnels`: Dual-homed edge `NodeDaemon` route table and gateway health
 * - `POST /v1beta1/console/tunnels/drain`: Triggers Make-Before-Break graceful drain on a gateway
 * - `POST /v1beta1/console/wal/catchup`: Executes `Last-Event-ID` incremental TurnWAL replay
 */

import { CapacitySlaEnforcer } from "../cluster/capacity_sla_enforcer";
import { MultiRegionGeoRouter } from "../cluster/geo_routing";
import { AgentStreamQueuePublisher } from "../gateway/stream_queue_publisher";
import { EdgeTunnelHaDynamicRouter } from "../runtime/edge_tunnel_ha_router";

export class ConsoleBffController {
  public readonly slaEnforcer = new CapacitySlaEnforcer();

  constructor(
    public readonly tunnelRouter: EdgeTunnelHaDynamicRouter,
    public readonly geoRouter: MultiRegionGeoRouter,
    public readonly streamPublisher: AgentStreamQueuePublisher,
  ) {}

  public getClusterOverview(params: {
    clientRegion: string;
    activeSandboxes: number;
    maxSandboxes: number;
    heapUsedMb: number;
    activeCells: number;
    failedRustFsDrives?: number;
  }) {
    const geoRoute = this.geoRouter.routeRequest(params.clientRegion);
    const brainVerdict = this.slaEnforcer.evaluateBrainIsolateAdmission({
      isolateId: "celld-brain-primary",
      heapUsedMb: params.heapUsedMb,
      heapLimitMb: 128,
      activeCells: params.activeCells,
      lastRehydrateRtoMs: 420,
    });
    const rustFsVerdict = this.slaEnforcer.evaluateRustFsEc4Reliability({
      totalDrives: 12,
      dataBlocks: 8,
      parityBlocks: 4,
      healthyDrives: 12 - (params.failedRustFsDrives ?? 0),
      failedDrives: params.failedRustFsDrives ?? 0,
      lastEtagCasLatencyMs: 6.4,
    });

    return {
      version: "v2.3.0",
      clusterHealthy: brainVerdict.allowed && rustFsVerdict.rpoZeroGuaranteed,
      rpoZeroGuaranteed: rustFsVerdict.rpoZeroGuaranteed,
      selectedRegion: geoRoute.selectedRegion,
      geoFailoverActive: geoRoute.isFailover,
      sandboxes: {
        active: params.activeSandboxes,
        limit: params.maxSandboxes,
      },
      brainIsolate: brainVerdict,
      storageEc4: rustFsVerdict,
      activeTunnelRoutes: Array.from(this.tunnelRouter.routeTable.values()),
    };
  }

  public triggerGatewayMakeBeforeBreakDrain(gatewayId: string, gracePeriodMs = 15000) {
    const notices = this.tunnelRouter.initiateMakeBeforeBreakDrain(gatewayId, gracePeriodMs);
    return {
      drainingGatewayId: gatewayId,
      notifiedHostsCount: notices.size,
      notices: Object.fromEntries(notices.entries()),
    };
  }

  public replayMissedWalFrames(clientId: string, taskId: string, lastReceivedTurnSeq: number) {
    return this.streamPublisher.resumeAndCatchUpFromWal({
      clientId,
      taskId,
      lastReceivedTurnSeq,
      replenishCredits: 50,
    });
  }
}
