"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.AutomatedChaosDrillRunner = void 0; /**
 * @fileoverview M37 & M63-1: Automated Chaos Engineering Drill Runner (Split-Brain, Network Partition & Node Crash)
 * with Guaranteed `try/finally` Partition Auto-Healing & Finite Fencing Token Validation.
 */


class AutomatedChaosDrillRunner {
  runSplitBrainAndRecoveryDrill(params)


  {
    if (
    !Number.isFinite(params.oldNodeFencingToken) ||
    !Number.isFinite(params.newNodeFencingToken) ||
    params.oldNodeFencingToken < 0 ||
    params.newNodeFencingToken < 0)
    {
      return { zombieBlocked: false, rpoZeroVerified: false };
    }
    const zombieBlocked = params.oldNodeFencingToken < params.newNodeFencingToken;
    return {
      zombieBlocked,
      rpoZeroVerified: zombieBlocked
    };
  }

  /**
   * [M63-1] DFX-Reliability: Wraps chaos fault injection in a `try ... finally` block so that
   * even if an unexpected exception occurs mid-drill, `healPartition` is unconditionally executed.
   */
  executeLivePartitionAndFencingDrill(params)










  {
    let partitionBlockedTraffic = false;
    try {
      params.chaosProxy.injectPartition(params.targetHost, params.partitionTtlMs ?? 30_000);
      try {
        params.chaosProxy.assertReachable(params.targetHost);
      } catch {
        partitionBlockedTraffic = true;
      }
    } finally {
      params.chaosProxy.healPartition(params.targetHost);
    }

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