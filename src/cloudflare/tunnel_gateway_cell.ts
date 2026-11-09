/**
 * @fileoverview v2.5 Dual-Homed Reverse Tunnel Gateway Cell (`TunnelGatewayCell`).
 */
export interface ReverseTunnelBinding {
  hostId: string;
  gatewayId: string;
  role: "PRIMARY" | "SECONDARY";
  connGeneration: number;
  epoch: number;
  healthy: boolean;
}

export class TunnelGatewayCell {
  private readonly tunnelsByHost: Map<string, ReverseTunnelBinding[]> = new Map();

  public registerTunnel(binding: Omit<ReverseTunnelBinding, "healthy">): ReverseTunnelBinding {
    const full: ReverseTunnelBinding = { ...binding, healthy: true };
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
}
