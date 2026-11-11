/**
 * @fileoverview M16 & M63-1: Programmable Network Chaos Fault-Injection Proxy (Jitter, Packet Loss & Partition)
 * with Optional Auto-Expiring Partition TTL so injected faults never permanently brick a host if a drill aborts.
 */
export class NetworkChaosFaultInjectionProxy {
  private readonly partitionedTargets: Map<string, number | null> = new Map();
  private readonly latencyJitterByHost: Map<string, number> = new Map();

  public injectPartition(targetHost: string, ttlMs?: number, nowMs = Date.now()): void {
    const safeHost = targetHost.trim();
    if (!safeHost) return;
    const expiresAtMs =
      ttlMs !== undefined && Number.isFinite(ttlMs) && ttlMs > 0 ? nowMs + ttlMs : null;
    this.partitionedTargets.set(safeHost, expiresAtMs);
  }

  public healPartition(targetHost: string): void {
    const safeHost = targetHost.trim();
    this.partitionedTargets.delete(safeHost);
    this.latencyJitterByHost.delete(safeHost);
  }

  public healAllPartitions(): number {
    const count = this.partitionedTargets.size;
    this.partitionedTargets.clear();
    this.latencyJitterByHost.clear();
    return count;
  }

  public injectLatencyJitter(targetHost: string, jitterMs: number): void {
    const safeHost = targetHost.trim();
    const safeJitter = Number.isFinite(jitterMs) && jitterMs > 0 ? jitterMs : 0;
    this.latencyJitterByHost.set(safeHost, safeJitter);
  }

  public assertReachable(targetHost: string, nowMs = Date.now()): void {
    const safeHost = targetHost.trim();
    if (!this.partitionedTargets.has(safeHost)) return;
    const expiresAt = this.partitionedTargets.get(safeHost) ?? null;
    if (expiresAt !== null && nowMs >= expiresAt) {
      this.partitionedTargets.delete(safeHost);
      return;
    }
    throw new Error(`NetworkChaosFaultInjectionProxy: simulated network partition to '${safeHost}'`);
  }

  public inspectActiveFaults(nowMs = Date.now()): {
    partitionedHosts: string[];
    jitterByHost: Record<string, number>;
  } {
    for (const [host, exp] of Array.from(this.partitionedTargets.entries())) {
      if (exp !== null && nowMs >= exp) {
        this.partitionedTargets.delete(host);
      }
    }
    const jitterByHost: Record<string, number> = {};
    for (const [k, v] of this.latencyJitterByHost.entries()) {
      jitterByHost[k] = v;
    }
    return {
      partitionedHosts: Array.from(this.partitionedTargets.keys()),
      jitterByHost,
    };
  }
}
