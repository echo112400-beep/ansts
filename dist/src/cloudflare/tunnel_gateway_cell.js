"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TunnelGatewayCell = void 0; /**
 * @fileoverview v2.5 Dual-Homed Reverse Tunnel Gateway Cell (`TunnelGatewayCell`).
 * Supports NAT-traversal outbound reverse tunnel command queueing, long-poll retrieval,
 * command execution acknowledgement, and automatic PRIMARY -> SECONDARY failover.
 */



















class TunnelGatewayCell {
  tunnelsByHost = new Map();
  pendingCommandsByHost = new Map();
  commandWaiters =


  new Map();
  cmdSeq = 0;

  registerTunnel(binding) {
    const full = {
      ...binding,
      healthy: true,
      lastHeartbeatMs: Date.now()
    };
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
        if (healthy) b.lastHeartbeatMs = Date.now();
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

  enqueueCommandForHost(
  hostId,
  payload,
  timeoutMs = 5000)



  {
    const active = this.selectActiveTunnel(hostId);
    this.cmdSeq += 1;
    const commandId = `tcmd-${Date.now()}-${this.cmdSeq}`;
    const command = {
      commandId,
      hostId,
      gatewayId: active.gatewayId,
      role: active.role,
      payload,
      enqueuedAtMs: Date.now()
    };
    const q = this.pendingCommandsByHost.get(hostId) ?? [];
    q.push(command);
    this.pendingCommandsByHost.set(hostId, q);

    const completionPromise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.commandWaiters.delete(commandId);
        reject(new Error(`Reverse tunnel command '${commandId}' for host '${hostId}' timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.commandWaiters.set(commandId, { resolve, timer });
    });

    return { command, completionPromise };
  }

  pollPendingCommands(hostId, gatewayId) {
    const list = this.tunnelsByHost.get(hostId) ?? [];
    for (const b of list) {
      if (!gatewayId || b.gatewayId === gatewayId) {
        b.lastHeartbeatMs = Date.now();
      }
    }
    const q = this.pendingCommandsByHost.get(hostId) ?? [];
    if (q.length === 0) return [];
    this.pendingCommandsByHost.set(hostId, []);
    return q;
  }

  acknowledgeCommand(commandId, result) {
    const waiter = this.commandWaiters.get(commandId);
    if (!waiter) return false;
    clearTimeout(waiter.timer);
    this.commandWaiters.delete(commandId);
    waiter.resolve(result);
    return true;
  }
}exports.TunnelGatewayCell = TunnelGatewayCell;