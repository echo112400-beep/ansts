"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.NetworkChaosFaultInjectionProxy = void 0; /**
 * @fileoverview M16 & M63-1: Programmable Network Chaos Fault-Injection Proxy (Jitter, Packet Loss & Partition)
 * with Optional Auto-Expiring Partition TTL so injected faults never permanently brick a host if a drill aborts.
 */
class NetworkChaosFaultInjectionProxy {
  partitionedTargets = new Map();
  latencyJitterByHost = new Map();

  injectPartition(targetHost, ttlMs, nowMs = Date.now()) {
    const safeHost = targetHost.trim();
    if (!safeHost) return;
    const expiresAtMs =
    ttlMs !== undefined && Number.isFinite(ttlMs) && ttlMs > 0 ? nowMs + ttlMs : null;
    this.partitionedTargets.set(safeHost, expiresAtMs);
  }

  healPartition(targetHost) {
    const safeHost = targetHost.trim();
    this.partitionedTargets.delete(safeHost);
    this.latencyJitterByHost.delete(safeHost);
  }

  healAllPartitions() {
    const count = this.partitionedTargets.size;
    this.partitionedTargets.clear();
    this.latencyJitterByHost.clear();
    return count;
  }

  injectLatencyJitter(targetHost, jitterMs) {
    const safeHost = targetHost.trim();
    const safeJitter = Number.isFinite(jitterMs) && jitterMs > 0 ? jitterMs : 0;
    this.latencyJitterByHost.set(safeHost, safeJitter);
  }

  assertReachable(targetHost, nowMs = Date.now()) {
    const safeHost = targetHost.trim();
    if (!this.partitionedTargets.has(safeHost)) return;
    const expiresAt = this.partitionedTargets.get(safeHost) ?? null;
    if (expiresAt !== null && nowMs >= expiresAt) {
      this.partitionedTargets.delete(safeHost);
      return;
    }
    throw new Error(`NetworkChaosFaultInjectionProxy: simulated network partition to '${safeHost}'`);
  }

  inspectActiveFaults(nowMs = Date.now())


  {
    for (const [host, exp] of Array.from(this.partitionedTargets.entries())) {
      if (exp !== null && nowMs >= exp) {
        this.partitionedTargets.delete(host);
      }
    }
    const jitterByHost = {};
    for (const [k, v] of this.latencyJitterByHost.entries()) {
      jitterByHost[k] = v;
    }
    return {
      partitionedHosts: Array.from(this.partitionedTargets.keys()),
      jitterByHost
    };
  }
}exports.NetworkChaosFaultInjectionProxy = NetworkChaosFaultInjectionProxy;