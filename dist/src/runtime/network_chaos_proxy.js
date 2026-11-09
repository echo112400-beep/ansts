"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.NetworkChaosFaultInjectionProxy = void 0; /**
 * @fileoverview Programmable Network Chaos Fault-Injection Proxy (Jitter, Packet Loss & Partition).
 */
class NetworkChaosFaultInjectionProxy {
  partitionedTargets = new Set();

  injectPartition(targetHost) {
    this.partitionedTargets.add(targetHost);
  }

  healPartition(targetHost) {
    this.partitionedTargets.delete(targetHost);
  }

  assertReachable(targetHost) {
    if (this.partitionedTargets.has(targetHost)) {
      throw new Error(`NetworkChaosFaultInjectionProxy: simulated network partition to '${targetHost}'`);
    }
  }
}exports.NetworkChaosFaultInjectionProxy = NetworkChaosFaultInjectionProxy;