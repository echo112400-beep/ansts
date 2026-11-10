"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TunnelGatewayCell = void 0; /**
 * @fileoverview v2.5 & M52-4: Dual-Homed Reverse Tunnel Gateway Cell (`TunnelGatewayCell`).
 * Supports NAT-traversal outbound reverse tunnel command queueing, long-poll retrieval,
 * command execution acknowledgement, stale-heartbeat tunnel auto-demotion, and unacknowledged in-flight command re-queueing.
 */




















class TunnelGatewayCell {
  tunnelsByHost = new Map();
  pendingCommandsByHost = new Map();
  inFlightCommands = new Map();
  commandWaiters =


  new Map();
  cmdSeq = 0;

  registerTunnel(binding, nowMs = Date.now()) {
    const full = {
      ...binding,
      healthy: true,
      lastHeartbeatMs: binding.lastHeartbeatMs ?? nowMs
    };
    const list = (this.tunnelsByHost.get(binding.hostId) ?? []).filter(
    (b) => b.gatewayId !== binding.gatewayId);

    list.push(full);
    this.tunnelsByHost.set(binding.hostId, list);
    return full;
  }

  markGatewayHealth(hostId, gatewayId, healthy, nowMs = Date.now()) {
    const list = this.tunnelsByHost.get(hostId) ?? [];
    for (const b of list) {
      if (b.gatewayId === gatewayId) {
        b.healthy = healthy;
        if (healthy) b.lastHeartbeatMs = nowMs;
      }
    }
  }

  /**
   * [M52-4] Automatically demotes any tunnel binding whose `lastHeartbeatMs` has exceeded `heartbeatTtlMs`,
   * and re-queues any unacknowledged in-flight commands onto the surviving healthy tunnel.
   */
  evictStaleTunnelsAndRequeueInFlight(
  nowMs = Date.now(),
  heartbeatTtlMs = 30_000)
  {
    const demotedGateways = [];
    for (const [hostId, list] of this.tunnelsByHost.entries()) {
      for (const b of list) {
        if (b.healthy && b.lastHeartbeatMs !== undefined && nowMs - b.lastHeartbeatMs > heartbeatTtlMs) {
          b.healthy = false;
          demotedGateways.push({ hostId, gatewayId: b.gatewayId });
        }
      }
    }

    const requeuedCommandIds = [];
    for (const [cmdId, cmd] of this.inFlightCommands.entries()) {
      const hostTunnels = this.tunnelsByHost.get(cmd.hostId) ?? [];
      const origTunnel = hostTunnels.find((b) => b.gatewayId === cmd.gatewayId);
      if (!origTunnel || !origTunnel.healthy) {
        this.inFlightCommands.delete(cmdId);
        const active = this.selectActiveTunnel(cmd.hostId);
        const requeued = {
          ...cmd,
          gatewayId: active.gatewayId,
          role: active.role,
          retryCount: (cmd.retryCount ?? 0) + 1
        };
        const q = this.pendingCommandsByHost.get(cmd.hostId) ?? [];
        q.push(requeued);
        this.pendingCommandsByHost.set(cmd.hostId, q);
        requeuedCommandIds.push(cmdId);
      }
    }

    return { demotedGateways, requeuedCommandIds };
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
      enqueuedAtMs: Date.now(),
      retryCount: 0
    };
    const q = this.pendingCommandsByHost.get(hostId) ?? [];
    q.push(command);
    this.pendingCommandsByHost.set(hostId, q);

    const completionPromise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.commandWaiters.delete(commandId);
        this.inFlightCommands.delete(commandId);
        reject(new Error(`Reverse tunnel command '${commandId}' for host '${hostId}' timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.commandWaiters.set(commandId, { resolve, timer });
    });

    return { command, completionPromise };
  }

  pollPendingCommands(hostId, gatewayId, nowMs = Date.now()) {
    const list = this.tunnelsByHost.get(hostId) ?? [];
    for (const b of list) {
      if (!gatewayId || b.gatewayId === gatewayId) {
        b.lastHeartbeatMs = nowMs;
      }
    }
    const q = this.pendingCommandsByHost.get(hostId) ?? [];
    if (q.length === 0) return [];
    this.pendingCommandsByHost.set(hostId, []);
    for (const cmd of q) {
      this.inFlightCommands.set(cmd.commandId, cmd);
    }
    return q;
  }

  acknowledgeCommand(commandId, result) {
    this.inFlightCommands.delete(commandId);
    const waiter = this.commandWaiters.get(commandId);
    if (!waiter) return false;
    clearTimeout(waiter.timer);
    this.commandWaiters.delete(commandId);
    waiter.resolve(result);
    return true;
  }

  listHostTunnels(hostId) {
    return [...(this.tunnelsByHost.get(hostId) ?? [])];
  }
}exports.TunnelGatewayCell = TunnelGatewayCell;