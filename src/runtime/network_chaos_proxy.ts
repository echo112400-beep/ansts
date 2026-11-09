/**
 * @fileoverview Programmable Network Chaos Fault-Injection Proxy (Jitter, Packet Loss & Partition).
 */
export class NetworkChaosFaultInjectionProxy {
  private readonly partitionedTargets: Set<string> = new Set();
  private readonly latencyJitterByHost: Map<string, number> = new Map();

  public injectPartition(targetHost: string): void {
    this.partitionedTargets.add(targetHost);
  }

  public healPartition(targetHost: string): void {
    this.partitionedTargets.delete(targetHost);
    this.latencyJitterByHost.delete(targetHost);
  }

  public injectLatencyJitter(targetHost: string, jitterMs: number): void {
    this.latencyJitterByHost.set(targetHost, Math.max(0, jitterMs));
  }

  public assertReachable(targetHost: string): void {
    if (this.partitionedTargets.has(targetHost)) {
      throw new Error(`NetworkChaosFaultInjectionProxy: simulated network partition to '${targetHost}'`);
    }
  }

  public inspectActiveFaults(): { partitionedHosts: string[]; jitterByHost: Record<string, number> } {
    const jitterByHost: Record<string, number> = {};
    for (const [k, v] of this.latencyJitterByHost.entries()) {
      jitterByHost[k] = v;
    }
    return {
      partitionedHosts: Array.from(this.partitionedTargets),
      jitterByHost,
    };
  }
}
