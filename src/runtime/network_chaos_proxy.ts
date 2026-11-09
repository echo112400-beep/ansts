/**
 * @fileoverview Programmable Network Chaos Fault-Injection Proxy (Jitter, Packet Loss & Partition).
 */
export class NetworkChaosFaultInjectionProxy {
  private partitionedTargets: Set<string> = new Set();

  public injectPartition(targetHost: string): void {
    this.partitionedTargets.add(targetHost);
  }

  public healPartition(targetHost: string): void {
    this.partitionedTargets.delete(targetHost);
  }

  public assertReachable(targetHost: string): void {
    if (this.partitionedTargets.has(targetHost)) {
      throw new Error(`NetworkChaosFaultInjectionProxy: simulated network partition to '${targetHost}'`);
    }
  }
}
