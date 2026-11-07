/**
 * @fileoverview TASK-87: `TunnelGatewayCell` (`celld` System Cell for Reverse Tunnel Hosting)
 * & Adaptive Edge Reconnection Backoff Guard with Full Jitter.
 *
 * Implements the co-located `celld` System Cell model (`idFromName(hostId)`) so any Brain node
 * can route commands via 1-Hop internal RPC without external Redis/Etcd, while edge `NodeDaemon`s
 * use bounded exponential backoff + full jitter to prevent thundering-herd storms on recovery.
 */

import { FenceToken } from "../models";
import { StaleFenceError } from "../lease_coordinator";
import { DisaggregatedControlCommand, DisaggregatedControlResponse } from "../runtime/mtls_tunnel_bridge";
import { EdgeTunnelHaDynamicRouter, RelayExecutionResult, TunnelRole } from "../runtime/edge_tunnel_ha_router";

export interface EdgeReconnectBackoffConfig {
  baseDelayMs: number;
  maxDelayMs: number;
  maxMissedHeartbeats: number;
  heartbeatIntervalMs: number;
}

/**
 * Prevents thundering-herd reconnection storms when thousands of edge `NodeDaemon`s
 * recover from a regional network partition simultaneously.
 */
export class EdgeReconnectBackoffGuard {
  public readonly config: EdgeReconnectBackoffConfig;

  constructor(config?: Partial<EdgeReconnectBackoffConfig>) {
    this.config = {
      baseDelayMs: config?.baseDelayMs ?? 200,
      maxDelayMs: config?.maxDelayMs ?? 10000,
      maxMissedHeartbeats: config?.maxMissedHeartbeats ?? 3,
      heartbeatIntervalMs: config?.heartbeatIntervalMs ?? 5000,
    };
  }

  /**
   * Computes AWS-style Full Jitter exponential backoff:
   * delay = random(0, min(maxDelayMs, baseDelayMs * 2^attempt))
   */
  public computeNextBackoffMs(attempt: number, deterministicSeedRatio?: number): number {
    const expCap = Math.min(
      this.config.maxDelayMs,
      this.config.baseDelayMs * Math.pow(2, Math.max(0, attempt)),
    );
    const rand =
      deterministicSeedRatio !== undefined
        ? Math.max(0, Math.min(1, deterministicSeedRatio))
        : Math.random();
    return Math.floor(rand * expCap);
  }

  public isSessionTimedOut(lastHeartbeatMs: number, nowMs: number = Date.now()): boolean {
    const timeoutWindowMs = this.config.heartbeatIntervalMs * this.config.maxMissedHeartbeats;
    return nowMs - lastHeartbeatMs > timeoutWindowMs;
  }
}

/**
 * `celld` System Cell (`TunnelGatewayCell`) that hosts reverse mTLS/WebSocket streams
 * for edge `NodeDaemon`s and processes 1-Hop internal relays from `AgentTaskHarnessCell`s.
 */
export class TunnelGatewayCell {
  public readonly haRouter: EdgeTunnelHaDynamicRouter;
  public readonly backoffGuard: EdgeReconnectBackoffGuard;
  private readonly sessionHeartbeats = new Map<string, number>();

  constructor(
    public readonly gatewayCellId: string,
    haRouter?: EdgeTunnelHaDynamicRouter,
  ) {
    this.haRouter = haRouter ?? new EdgeTunnelHaDynamicRouter(gatewayCellId);
    this.backoffGuard = new EdgeReconnectBackoffGuard();
  }

  /**
   * Registers an inbound reverse tunnel stream from an edge `NodeDaemon` into this System Cell.
   */
  public acceptDaemonTunnelStream(params: {
    hostId: string;
    gatewayId?: string;
    role: TunnelRole;
    connGeneration: number;
    epoch: number;
    nowMs?: number;
  }) {
    const nowMs = params.nowMs ?? Date.now();
    const targetGwId = params.gatewayId ?? this.gatewayCellId;
    this.sessionHeartbeats.set(`${targetGwId}:${params.hostId}`, nowMs);
    return this.haRouter.registerInboundDaemonStream({
      hostId: params.hostId,
      gatewayId: targetGwId,
      role: params.role,
      connGeneration: params.connGeneration,
      epoch: params.epoch,
      nowMs,
    });
  }

  /**
   * Processes a periodic `HEARTBEAT` frame from an edge `NodeDaemon` and returns `HEARTBEAT_ACK`.
   */
  public recordDaemonHeartbeat(hostId: string, gatewayId = this.gatewayCellId, nowMs = Date.now()): {
    type: "HEARTBEAT_ACK";
    hostId: string;
    gatewayId: string;
    timestampMs: number;
  } {
    const key = `${gatewayId}:${hostId}`;
    this.sessionHeartbeats.set(key, nowMs);
    const binding = this.haRouter.activeStreamsByGateway.get(key);
    if (binding) {
      binding.lastHeartbeatMs = nowMs;
      binding.streamHealthy = true;
    }
    return {
      type: "HEARTBEAT_ACK",
      hostId,
      gatewayId,
      timestampMs: nowMs,
    };
  }

  /**
   * Sweeps expired sessions that missed >3 heartbeats (>15s) and marks their stream unhealthy
   * so subsequent commands immediately trigger `<15ms` secondary failover.
   */
  public evictTimedOutStreams(nowMs = Date.now()): string[] {
    const evicted: string[] = [];
    for (const [key, lastSeen] of this.sessionHeartbeats.entries()) {
      if (this.backoffGuard.isSessionTimedOut(lastSeen, nowMs)) {
        const binding = this.haRouter.activeStreamsByGateway.get(key);
        if (binding) {
          binding.streamHealthy = false;
        }
        this.sessionHeartbeats.delete(key);
        evicted.push(key);
      }
    }
    return evicted;
  }

  /**
   * Dispatches a Brain command through the System Cell (0-Hop local or 1-Hop relay with Secondary Failover).
   */
  public relayBrainCommand(cmd: DisaggregatedControlCommand): RelayExecutionResult {
    return this.haRouter.routeAndForwardCommand(cmd);
  }
}
