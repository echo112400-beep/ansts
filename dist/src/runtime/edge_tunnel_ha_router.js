"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.EdgeTunnelHaRouter = void 0;
/**
 * @fileoverview M32 & M62-5: Edge Reverse-Tunnel High-Availability Failover Router.
 * Supports synchronous metadata routing, asynchronous NAT-traversal reverse-tunnel command dispatch,
 * and Idempotent Request Passthrough.
 */
const tunnel_gateway_cell_js_1 = require("../cloudflare/tunnel_gateway_cell.js");
class EdgeTunnelHaRouter {
    tunnelCell;
    constructor(tunnelCell = new tunnel_gateway_cell_js_1.TunnelGatewayCell()) {
        this.tunnelCell = tunnelCell;
    }
    routeRpcToBodyHost(hostId, payload) {
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
    async dispatchOverReverseTunnel(hostId, payload, timeoutMs = 5000, options) {
        const { command, completionPromise, deduplicated } = this.tunnelCell.enqueueCommandForHost(hostId, payload, timeoutMs, options);
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
exports.EdgeTunnelHaRouter = EdgeTunnelHaRouter;
