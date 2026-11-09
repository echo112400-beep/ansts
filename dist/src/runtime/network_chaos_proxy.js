"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.NetworkChaosFaultInjectionProxy = void 0; /**
 * @fileoverview Programmable Network Chaos Fault-Injection Proxy (Jitter, Packet Loss & Partition).
 */
class NetworkChaosFaultInjectionProxy {
  partitionedTargets = new Set();
  latencyJitterByHost = new Map();

  injectPartition(targetHost) {
    this.partitionedTargets.add(targetHost);
  }

  healPartition(targetHost) {
    this.partitionedTargets.delete(targetHost);
    this.latencyJitterByHost.delete(targetHost);
  }

  injectLatencyJitter(targetHost, jitterMs) {
    this.latencyJitterByHost.set(targetHost, Math.max(0, jitterMs));
  }

  assertReachable(targetHost) {
    if (this.partitionedTargets.has(targetHost)) {
      throw new Error(`NetworkChaosFaultInjectionProxy: simulated network partition to '${targetHost}'`);
    }
  }

  inspectActiveFaults() {
    const jitterByHost = {};
    for (const [k, v] of this.latencyJitterByHost.entries()) {
      jitterByHost[k] = v;
    }
    return {
      partitionedHosts: Array.from(this.partitionedTargets),
      jitterByHost
    };
  }
}exports.NetworkChaosFaultInjectionProxy = NetworkChaosFaultInjectionProxy;