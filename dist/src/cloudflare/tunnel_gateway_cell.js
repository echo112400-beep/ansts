"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TunnelGatewayCell = void 0; /**
 * @fileoverview v2.5 Dual-Homed Reverse Tunnel Gateway Cell (`TunnelGatewayCell`).
 */









class TunnelGatewayCell {
  tunnelsByHost = new Map();

  registerTunnel(binding) {
    const full = { ...binding, healthy: true };
    const list = (this.tunnelsByHost.get(binding.hostId) ?? []).filter(
    (b) => b.gatewayId !== binding.gatewayId);

    list.push(full);
    this.tunnelsByHost.set(binding.hostId, list);
    return full;
  }

  markGatewayHealth(hostId, gatewayId, healthy) {
    const list = this.tunnelsByHost.get(hostId) ?? [];
    for (const b of list) {
      if (b.gatewayId === gatewayId) {
        b.healthy = healthy;
      }
    }
  }

  selectActiveTunnel(hostId) {
    const list = (this.tunnelsByHost.get(hostId) ?? []).filter((b) => b.healthy);
    if (list.length === 0) {
      throw new Error(`No healthy reverse tunnel available for body host '${hostId}'`);
    }
    const primary = list.find((b) => b.role === "PRIMARY");
    return primary ?? list[0];
  }
}exports.TunnelGatewayCell = TunnelGatewayCell;