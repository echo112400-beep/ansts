"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SharedBlackboardAndGitArbiter = void 0; /**
 * @fileoverview M23 A2A Shared Blackboard CRDT/CAS Store & Multi-Agent Git Merge Arbiter.
 */








class SharedBlackboardAndGitArbiter {
  board = new Map();

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

  arbitrateGitPatchTouches(patches)


  {
    const sorted = [...patches].sort((a, b) => b.priorityScore - a.priorityScore);
    const claimedFiles = new Set();
    const acceptedAgentIds = [];
    const conflictedAgentIds = [];

    for (const p of sorted) {
      const hasOverlap = p.touchedFiles.some((f) => claimedFiles.has(f));
      if (hasOverlap) {
        conflictedAgentIds.push(p.agentId);
      } else {
        acceptedAgentIds.push(p.agentId);
        for (const f of p.touchedFiles) claimedFiles.add(f);
      }
    }
    return { acceptedAgentIds, conflictedAgentIds };
  }
}exports.SharedBlackboardAndGitArbiter = SharedBlackboardAndGitArbiter;