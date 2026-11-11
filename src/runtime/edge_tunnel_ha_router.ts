/**
 * @fileoverview M32 & M62-5: Edge Reverse-Tunnel High-Availability Failover Router.
 * Supports synchronous metadata routing, asynchronous NAT-traversal reverse-tunnel command dispatch,
 * and Idempotent Request Passthrough.
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
