"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.EdgeTunnelHaRouter = void 0;


var _tunnel_gateway_cell = require("../cloudflare/tunnel_gateway_cell.js"); /**
 * @fileoverview Edge Reverse-Tunnel High-Availability Failover Router.
 */class EdgeTunnelHaRouter {
  tunnelCell;

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
}exports.EdgeTunnelHaRouter = EdgeTunnelHaRouter;