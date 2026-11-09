/**
 * @fileoverview v2.5 Dual-Homed Reverse Tunnel Gateway Cell (`TunnelGatewayCell`).
 * Supports NAT-traversal outbound reverse tunnel command queueing, long-poll retrieval,
 * command execution acknowledgement, and automatic PRIMARY -> SECONDARY failover.
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
}

export class TunnelGatewayCell {
  private readonly tunnelsByHost: Map<string, ReverseTunnelBinding[]> = new Map();
  private readonly pendingCommandsByHost: Map<string, QueuedTunnelCommand[]> = new Map();
  private readonly commandWaiters: Map<
    string,
    { resolve: (result: Record<string, unknown>) => void; timer: ReturnType<typeof setTimeout> }
  > = new Map();
  private cmdSeq = 0;

  public registerTunnel(binding: Omit<ReverseTunnelBinding, "healthy">): ReverseTunnelBinding {
    const full: ReverseTunnelBinding = {
      ...binding,
      healthy: true,
      lastHeartbeatMs: Date.now(),
    };
    const list = (this.tunnelsByHost.get(binding.hostId) ?? []).filter(
      (b) => b.gatewayId !== binding.gatewayId
    );
    list.push(full);
    this.tunnelsByHost.set(binding.hostId, list);
    return full;
  }

  public markGatewayHealth(hostId: string, gatewayId: string, healthy: boolean): void {
    const list = this.tunnelsByHost.get(hostId) ?? [];
    for (const b of list) {
      if (b.gatewayId === gatewayId) {
        b.healthy = healthy;
        if (healthy) b.lastHeartbeatMs = Date.now();
      }
    }
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
    };
    const q = this.pendingCommandsByHost.get(hostId) ?? [];
    q.push(command);
    this.pendingCommandsByHost.set(hostId, q);

    const completionPromise = new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.commandWaiters.delete(commandId);
        reject(new Error(`Reverse tunnel command '${commandId}' for host '${hostId}' timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      this.commandWaiters.set(commandId, { resolve, timer });
    });

    return { command, completionPromise };
  }

  public pollPendingCommands(hostId: string, gatewayId?: string): QueuedTunnelCommand[] {
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

  public acknowledgeCommand(commandId: string, result: Record<string, unknown>): boolean {
    const waiter = this.commandWaiters.get(commandId);
    if (!waiter) return false;
    clearTimeout(waiter.timer);
    this.commandWaiters.delete(commandId);
    waiter.resolve(result);
    return true;
  }
}
