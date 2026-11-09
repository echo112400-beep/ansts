/**
 * @fileoverview Edge Reverse-Tunnel High-Availability Failover Router.
 * Supports synchronous metadata routing and asynchronous NAT-traversal reverse-tunnel command dispatch.
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

  public async dispatchOverReverseTunnel(
    hostId: string,
    payload: Record<string, unknown>,
    timeoutMs = 5000
  ): Promise<{
    commandId: string;
    routedViaGatewayId: string;
    role: "PRIMARY" | "SECONDARY";
    executionResult: Record<string, unknown>;
  }> {
    const { command, completionPromise } = this.tunnelCell.enqueueCommandForHost(hostId, payload, timeoutMs);
    const executionResult = await completionPromise;
    return {
      commandId: command.commandId,
      routedViaGatewayId: command.gatewayId,
      role: command.role,
      executionResult,
    };
  }
}
