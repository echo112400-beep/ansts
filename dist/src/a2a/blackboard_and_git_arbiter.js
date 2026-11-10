"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SharedBlackboardAndGitArbiter = void 0; /**
 * @fileoverview M23 & M54-4: A2A Shared Blackboard CRDT/CAS Store & Multi-Agent Git Merge Arbiter
 * with Conflict-Aging Priority Boost to prevent low-priority agent patch starvation.
 */








class SharedBlackboardAndGitArbiter {
  board = new Map();
  consecutiveConflictsByAgent = new Map();
  static CONFLICT_AGING_BOOST_PER_ROUND = 150;

  putCas(
  key,
  value,
  expectedVersion,
  authorAgentId)
  {
    const existing = this.board.get(key);
    const currentVersion = existing ? existing.version : 0;
    if (currentVersion !== expectedVersion) {
      throw new Error(
      `Blackboard CAS conflict on key '${key}': expected v${expectedVersion}, actual v${currentVersion}`);

    }
    const next = {
      key,
      value,
      version: currentVersion + 1,
      authorAgentId,
      updatedAtMs: Date.now()
    };
    this.board.set(key, next);
    return { committed: true, entry: next };
  }

  get(key) {
    return this.board.get(key);
  }

  /**
   * [M54-4] Arbitrates overlapping Git file patches across concurrent agents using
   * `effectivePriority = priorityScore + consecutiveConflicts * 150` so an agent whose patch
   * was rejected in earlier rounds gains priority and is never starved indefinitely.
   */
  arbitrateGitPatchTouches(
  patches)




  {
    const effectiveScores = {};
    for (const p of patches) {
      const priorConflicts = this.consecutiveConflictsByAgent.get(p.agentId) ?? 0;
      effectiveScores[p.agentId] =
      p.priorityScore + priorConflicts * SharedBlackboardAndGitArbiter.CONFLICT_AGING_BOOST_PER_ROUND;
    }

    const sorted = [...patches].sort(
    (a, b) => effectiveScores[b.agentId] - effectiveScores[a.agentId]);

    const claimedFiles = new Set();
    const acceptedAgentIds = [];
    const conflictedAgentIds = [];

    for (const p of sorted) {
      const hasOverlap = p.touchedFiles.some((f) => claimedFiles.has(f));
      if (hasOverlap) {
        conflictedAgentIds.push(p.agentId);
        this.consecutiveConflictsByAgent.set(
        p.agentId,
        (this.consecutiveConflictsByAgent.get(p.agentId) ?? 0) + 1);

      } else {
        acceptedAgentIds.push(p.agentId);
        this.consecutiveConflictsByAgent.set(p.agentId, 0);
        for (const f of p.touchedFiles) claimedFiles.add(f);
      }
    }
    return { acceptedAgentIds, conflictedAgentIds, effectiveScores };
  }

  getAgentConflictCount(agentId) {
    return this.consecutiveConflictsByAgent.get(agentId) ?? 0;
  }
}exports.SharedBlackboardAndGitArbiter = SharedBlackboardAndGitArbiter;