/**
 * @fileoverview Edge Reverse-Tunnel High-Availability Failover Router.
 */
import { TunnelGatewayCell } from "../cloudflare/tunnel_gateway_cell.js";

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
}
