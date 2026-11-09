"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.EdgeTunnelHaRouter = void 0;



var _tunnel_gateway_cell = require("../cloudflare/tunnel_gateway_cell.js"); /**
 * @fileoverview Edge Reverse-Tunnel High-Availability Failover Router.
 * Supports synchronous metadata routing and asynchronous NAT-traversal reverse-tunnel command dispatch.
 */class EdgeTunnelHaRouter {tunnelCell;

  constructor(tunnelCell = new _tunnel_gateway_cell.TunnelGatewayCell()) {
    this.tunnelCell = tunnelCell;
  }

  routeRpcToBodyHost(hostId, payload)




  {
    const active = this.tunnelCell.selectActiveTunnel(hostId);
    return {
      routedViaGatewayId: active.gatewayId,
      role: active.role,
      delivered: true,
      payloadEcho: payload
    };
  }

  async dispatchOverReverseTunnel(
  hostId,
  payload,
  timeoutMs = 5000)





  {
    const { command, completionPromise } = this.tunnelCell.enqueueCommandForHost(hostId, payload, timeoutMs);
    const executionResult = await completionPromise;
    return {
      commandId: command.commandId,
      routedViaGatewayId: command.gatewayId,
      role: command.role,
      executionResult
    };
  }
}exports.EdgeTunnelHaRouter = EdgeTunnelHaRouter;