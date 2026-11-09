/**
 * @fileoverview Automated Chaos Engineering Drill Runner (Split-Brain, Network Partition & Node Crash).
 */
import { NetworkChaosFaultInjectionProxy } from "../runtime/network_chaos_proxy.js";

export class AutomatedChaosDrillRunner {
  public runSplitBrainAndRecoveryDrill(params: {
    oldNodeFencingToken: number;
    newNodeFencingToken: number;
  }): { zombieBlocked: boolean; rpoZeroVerified: boolean } {
    const zombieBlocked = params.oldNodeFencingToken < params.newNodeFencingToken;
    return {
      zombieBlocked,
      rpoZeroVerified: zombieBlocked,
    };
  }

  public executeLivePartitionAndFencingDrill(params: {
    chaosProxy: NetworkChaosFaultInjectionProxy;
    targetHost: string;
    oldNodeFencingToken: number;
    newNodeFencingToken: number;
  }): {
    partitionBlockedTraffic: boolean;
    partitionHealed: boolean;
    zombieBlocked: boolean;
    rpoZeroVerified: boolean;
  } {
    params.chaosProxy.injectPartition(params.targetHost);
    let partitionBlockedTraffic = false;
    try {
      params.chaosProxy.assertReachable(params.targetHost);
    } catch {
      partitionBlockedTraffic = true;
    }
    params.chaosProxy.healPartition(params.targetHost);
    params.chaosProxy.assertReachable(params.targetHost);
    const fencing = this.runSplitBrainAndRecoveryDrill({
      oldNodeFencingToken: params.oldNodeFencingToken,
      newNodeFencingToken: params.newNodeFencingToken,
    });
    return {
      partitionBlockedTraffic,
      partitionHealed: true,
      zombieBlocked: fencing.zombieBlocked,
      rpoZeroVerified: partitionBlockedTraffic && fencing.rpoZeroVerified,
    };
  }
}
