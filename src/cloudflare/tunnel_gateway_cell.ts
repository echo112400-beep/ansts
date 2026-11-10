/**
 * @fileoverview v2.5 & M52-4: Dual-Homed Reverse Tunnel Gateway Cell (`TunnelGatewayCell`).
 * Supports NAT-traversal outbound reverse tunnel command queueing, long-poll retrieval,
 * command execution acknowledgement, stale-heartbeat tunnel auto-demotion, and unacknowledged in-flight command re-queueing.
 */
export interface ReverseTunnelBinding {
  hostId: string;
  gatewayId: string;
  role: "PRIMARY" | "SECONDARY";
  connGeneration: number;
  epoch: number;
  healthy: boolean;
  lastHeartbeatMs?: number;
}

export interface QueuedTunnelCommand {
  commandId: string;
  hostId: string;
  gatewayId: string;
  role: "PRIMARY" | "SECONDARY";
  payload: Record<string, unknown>;
  enqueuedAtMs: number;
  retryCount?: number;
}

export class TunnelGatewayCell {
  private readonly tunnelsByHost: Map<string, ReverseTunnelBinding[]> = new Map();
  private readonly pendingCommandsByHost: Map<string, QueuedTunnelCommand[]> = new Map();
  private readonly inFlightCommands: Map<string, QueuedTunnelCommand> = new Map();
  private readonly commandWaiters: Map<
    string,
    { resolve: (result: Record<string, unknown>) => void; timer: ReturnType<typeof setTimeout> }
  > = new Map();
  private cmdSeq = 0;

  public registerTunnel(binding: Omit<ReverseTunnelBinding, "healthy">, nowMs = Date.now()): ReverseTunnelBinding {
    const full: ReverseTunnelBinding = {
      ...binding,
      healthy: true,
      lastHeartbeatMs: binding.lastHeartbeatMs ?? nowMs,
    };
    const list = (this.tunnelsByHost.get(binding.hostId) ?? []).filter(
      (b) => b.gatewayId !== binding.gatewayId
    );
    list.push(full);
    this.tunnelsByHost.set(binding.hostId, list);
    return full;
  }

  public markGatewayHealth(hostId: string, gatewayId: string, healthy: boolean, nowMs = Date.now()): void {
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
  public evictStaleTunnelsAndRequeueInFlight(
    nowMs = Date.now(),
    heartbeatTtlMs = 30_000
  ): { demotedGateways: Array<{ hostId: string; gatewayId: string }>; requeuedCommandIds: string[] } {
    const demotedGateways: Array<{ hostId: string; gatewayId: string }> = [];
    for (const [hostId, list] of this.tunnelsByHost.entries()) {
      for (const b of list) {
        if (b.healthy && b.lastHeartbeatMs !== undefined && nowMs - b.lastHeartbeatMs > heartbeatTtlMs) {
          b.healthy = false;
          demotedGateways.push({ hostId, gatewayId: b.gatewayId });
        }
      }
    }

    const requeuedCommandIds: string[] = [];
    for (const [cmdId, cmd] of this.inFlightCommands.entries()) {
      const hostTunnels = this.tunnelsByHost.get(cmd.hostId) ?? [];
      const origTunnel = hostTunnels.find((b) => b.gatewayId === cmd.gatewayId);
      if (!origTunnel || !origTunnel.healthy) {
        this.inFlightCommands.delete(cmdId);
        const active = this.selectActiveTunnel(cmd.hostId);
        const requeued: QueuedTunnelCommand = {
          ...cmd,
          gatewayId: active.gatewayId,
          role: active.role,
          retryCount: (cmd.retryCount ?? 0) + 1,
        };
        const q = this.pendingCommandsByHost.get(cmd.hostId) ?? [];
        q.push(requeued);
        this.pendingCommandsByHost.set(cmd.hostId, q);
        requeuedCommandIds.push(cmdId);
      }
    }

    return { demotedGateways, requeuedCommandIds };
  }

  public selectActiveTunnel(hostId: string): ReverseTunnelBinding {
    const list = (this.tunnelsByHost.get(hostId) ?? []).filter((b) => b.healthy);
    if (list.length === 0) {
      throw new Error(`No healthy reverse tunnel available for body host '${hostId}'`);
    }
    const primary = list.find((b) => b.role === "PRIMARY");
    return primary ?? list[0];
  }

  public enqueueCommandForHost(
    hostId: string,
    payload: Record<string, unknown>,
    timeoutMs = 5000
  ): {
    command: QueuedTunnelCommand;
    completionPromise: Promise<Record<string, unknown>>;
  } {
    const active = this.selectActiveTunnel(hostId);
    this.cmdSeq += 1;
    const commandId = `tcmd-${Date.now()}-${this.cmdSeq}`;
    const command: QueuedTunnelCommand = {
      commandId,
      hostId,
      gatewayId: active.gatewayId,
      role: active.role,
      payload,
      enqueuedAtMs: Date.now(),
      retryCount: 0,
    };
    const q = this.pendingCommandsByHost.get(hostId) ?? [];
    q.push(command);
    this.pendingCommandsByHost.set(hostId, q);

    const completionPromise = new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.commandWaiters.delete(commandId);
        this.inFlightCommands.delete(commandId);
        reject(new Error(`Reverse tunnel command '${commandId}' for host '${hostId}' timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.commandWaiters.set(commandId, { resolve, timer });
    });

    return { command, completionPromise };
  }

  public pollPendingCommands(hostId: string, gatewayId?: string, nowMs = Date.now()): QueuedTunnelCommand[] {
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

  public acknowledgeCommand(commandId: string, result: Record<string, unknown>): boolean {
    this.inFlightCommands.delete(commandId);
    const waiter = this.commandWaiters.get(commandId);
    if (!waiter) return false;
    clearTimeout(waiter.timer);
    this.commandWaiters.delete(commandId);
    waiter.resolve(result);
    return true;
  }

  public listHostTunnels(hostId: string): ReverseTunnelBinding[] {
    return [...(this.tunnelsByHost.get(hostId) ?? [])];
  }
}
