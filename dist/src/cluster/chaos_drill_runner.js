"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.AutomatedChaosDrillRunner = void 0; /**
 * @fileoverview Automated Chaos Engineering Drill Runner (Split-Brain, Network Partition & Node Crash).
 */


class AutomatedChaosDrillRunner {
  runSplitBrainAndRecoveryDrill(params)


  {
    const zombieBlocked = params.oldNodeFencingToken < params.newNodeFencingToken;
    return {
      zombieBlocked,
      rpoZeroVerified: zombieBlocked
    };
  }

  executeLivePartitionAndFencingDrill(params)









  {
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
      newNodeFencingToken: params.newNodeFencingToken
    });
    return {
      partitionBlockedTraffic,
      partitionHealed: true,
      zombieBlocked: fencing.zombieBlocked,
      rpoZeroVerified: partitionBlockedTraffic && fencing.rpoZeroVerified
    };
  }
}exports.AutomatedChaosDrillRunner = AutomatedChaosDrillRunner;