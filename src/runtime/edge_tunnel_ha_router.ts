/**
 * @fileoverview M32 & M62-5: Edge Reverse-Tunnel High-Availability Failover Router.
 * Supports synchronous metadata routing, asynchronous NAT-traversal reverse-tunnel command dispatch,
 * and Idempotent Request Passthrough.
 */
import { TunnelGatewayCell } from "../cloudflare/tunnel_gateway_cell.js";
import type { ReverseTunnelBinding } from "../cloudflare/tunnel_gateway_cell.js";
import type { DrainPreMigrateNotice } from "../cluster/slot_canary_rollout_planner.js";

interface EdgeGateway {
  gatewayId: string;
  internalRelayUrl: string;
  publicEdgeIngressUrl: string;
  healthy: boolean;
  draining: boolean;
}

interface EdgeRoute {
  hostId: string;
  connGeneration: number;
  epoch: number;
  primaryGatewayId: string;
  primaryRelayUrl: string;
  secondaryGatewayId?: string;
  secondaryRelayUrl?: string;
  updatedAtMs: number;
}

export class EdgeTunnelRoutePlanner {
  public readonly gateways = new Map<string, EdgeGateway>();

  constructor(public readonly localNodeGatewayId: string, public readonly tunnelCell = new TunnelGatewayCell()) {}

  public registerTunnelGateway(gateway: EdgeGateway): void {
    this.gateways.set(gateway.gatewayId, { ...gateway });
  }

  public registerInboundDaemonStream(params: Omit<ReverseTunnelBinding, "healthy" | "lastHeartbeatMs"> & { nowMs?: number }): ReverseTunnelBinding {
    const { nowMs, ...binding } = params;
    const gateway = this.gateways.get(binding.gatewayId);
    if (!gateway?.healthy || gateway.draining) throw new Error("TUNNEL_GATEWAY_UNAVAILABLE");
    return this.tunnelCell.registerTunnel(binding, nowMs);
  }

  public get activeStreamsByGateway(): ReadonlyMap<string, ReverseTunnelBinding & { streamHealthy: boolean }> {
    return new Map(this.tunnelCell.listAllTunnels().map(binding => [
      JSON.stringify([binding.gatewayId, binding.hostId]),
      { ...binding, streamHealthy: binding.healthy && this.gateways.get(binding.gatewayId)?.healthy === true },
    ]));
  }

  public get routeTable(): ReadonlyMap<string, EdgeRoute> {
    const byHost = new Map<string, ReverseTunnelBinding[]>();
    for (const binding of this.activeStreamsByGateway.values()) {
      if (!binding.streamHealthy) continue;
      const entries = byHost.get(binding.hostId) ?? [];
      entries.push(binding);
      byHost.set(binding.hostId, entries);
    }
    const routes = new Map<string, EdgeRoute>();
    for (const [hostId, entries] of byHost) {
      entries.sort((left, right) => right.epoch - left.epoch || Number(right.role === "PRIMARY") -
        Number(left.role === "PRIMARY") || right.connGeneration - left.connGeneration || left.gatewayId.localeCompare(right.gatewayId));
      const primary = entries[0];
      const secondary = entries.find(binding => binding.epoch === primary.epoch && binding.gatewayId !== primary.gatewayId);
      routes.set(hostId, {
        hostId, connGeneration: primary.connGeneration, epoch: primary.epoch, primaryGatewayId: primary.gatewayId,
        primaryRelayUrl: this.gateways.get(primary.gatewayId)!.internalRelayUrl,
        secondaryGatewayId: secondary?.gatewayId,
        secondaryRelayUrl: secondary ? this.gateways.get(secondary.gatewayId)?.internalRelayUrl : undefined,
        updatedAtMs: Math.max(primary.lastHeartbeatMs ?? 0, secondary?.lastHeartbeatMs ?? 0),
      });
    }
    return routes;
  }

  public initiateMakeBeforeBreakDrain(gatewayId: string, gracePeriodMs = 15_000): Map<string, DrainPreMigrateNotice> {
    const gateway = this.gateways.get(gatewayId);
    if (!gateway) throw new Error("TUNNEL_GATEWAY_NOT_FOUND");
    if (!Number.isSafeInteger(gracePeriodMs) || gracePeriodMs < 0) throw new RangeError("INVALID_TUNNEL_DRAIN_GRACE");
    gateway.draining = true;
    const recommendedCandidateGateways = [...this.gateways.values()]
      .filter(candidate => candidate.gatewayId !== gatewayId && candidate.healthy && !candidate.draining)
      .map(candidate => ({ gatewayId: candidate.gatewayId, publicEdgeIngressUrl: candidate.publicEdgeIngressUrl }));
    const notices = new Map<string, DrainPreMigrateNotice>();
    for (const binding of this.activeStreamsByGateway.values()) {
      if (binding.gatewayId === gatewayId && binding.streamHealthy) notices.set(binding.hostId, {
        type: "TUNNEL_DRAIN_PRE_MIGRATE", drainingGatewayId: gatewayId, recommendedCandidateGateways, gracePeriodMs,
      });
    }
    return notices;
  }
}

export class EdgeTunnelHaRouter {
  public readonly tunnelCell: TunnelGatewayCell;

  constructor(tunnelCell = new TunnelGatewayCell()) {
    this.tunnelCell = tunnelCell;
  }

  public routeRpcToBodyHost(hostId: string, payload: Record<string, unknown>): {
    routedViaGatewayId: string;
    role: "PRIMARY" | "SECONDARY";
    delivered: boolean;
    payloadEcho: Record<string, unknown>;
  } {
    const active = this.tunnelCell.selectActiveTunnel(hostId);
    return {
      routedViaGatewayId: active.gatewayId,
      role: active.role,
      delivered: true,
      payloadEcho: payload,
    };
  }

  /**
   * [M62-5] DFX-Maintainability: Supports optional `options.idempotencyKey` forwarding to `tunnelCell`
   * ensuring retryable reverse tunnel dispatches coalesce onto existing in-flight/completed commands.
   */
  public async dispatchOverReverseTunnel(
    hostId: string,
    payload: Record<string, unknown>,
    timeoutMs = 5000,
    options?: { idempotencyKey?: string }
  ): Promise<{
    commandId: string;
    routedViaGatewayId: string;
    role: "PRIMARY" | "SECONDARY";
    executionResult: Record<string, unknown>;
    deduplicated?: boolean;
  }> {
    const { command, completionPromise, deduplicated } = this.tunnelCell.enqueueCommandForHost(
      hostId,
      payload,
      timeoutMs,
      options
    );
    const executionResult = await completionPromise;
    return {
      commandId: command.commandId,
      routedViaGatewayId: command.gatewayId,
      role: command.role,
      executionResult,
      deduplicated,
    };
  }
}
