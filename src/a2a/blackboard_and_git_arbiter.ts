/**
 * @fileoverview M23 & M54-4: A2A Shared Blackboard CRDT/CAS Store & Multi-Agent Git Merge Arbiter
 * with Conflict-Aging Priority Boost to prevent low-priority agent patch starvation.
 */
export interface BlackboardEntry {
  key: string;
  value: unknown;
  version: number;
  authorAgentId: string;
  updatedAtMs: number;
}

export class SharedBlackboardAndGitArbiter {
  private readonly board: Map<string, BlackboardEntry> = new Map();
  private readonly consecutiveConflictsByAgent: Map<string, number> = new Map();
  public static readonly CONFLICT_AGING_BOOST_PER_ROUND = 150;

  public putCas(
    key: string,
    value: unknown,
    expectedVersion: number,
    authorAgentId: string
  ): { committed: boolean; entry: BlackboardEntry } {
    const existing = this.board.get(key);
    const currentVersion = existing ? existing.version : 0;
    if (currentVersion !== expectedVersion) {
      throw new Error(
        `Blackboard CAS conflict on key '${key}': expected v${expectedVersion}, actual v${currentVersion}`
      );
    }
    const next: BlackboardEntry = {
      key,
      value,
      version: currentVersion + 1,
      authorAgentId,
      updatedAtMs: Date.now(),
    };
    this.board.set(key, next);
    return { committed: true, entry: next };
  }

  public get(key: string): BlackboardEntry | undefined {
    return this.board.get(key);
  }

  /**
   * [M54-4] Arbitrates overlapping Git file patches across concurrent agents using
   * `effectivePriority = priorityScore + consecutiveConflicts * 150` so an agent whose patch
   * was rejected in earlier rounds gains priority and is never starved indefinitely.
   */
  public arbitrateGitPatchTouches(
    patches: Array<{ agentId: string; touchedFiles: string[]; priorityScore: number }>
  ): {
    acceptedAgentIds: string[];
    conflictedAgentIds: string[];
    effectiveScores: Record<string, number>;
  } {
    const effectiveScores: Record<string, number> = {};
    for (const p of patches) {
      const priorConflicts = this.consecutiveConflictsByAgent.get(p.agentId) ?? 0;
      effectiveScores[p.agentId] =
        p.priorityScore + priorConflicts * SharedBlackboardAndGitArbiter.CONFLICT_AGING_BOOST_PER_ROUND;
    }

    const sorted = [...patches].sort(
      (a, b) => effectiveScores[b.agentId] - effectiveScores[a.agentId]
    );
    const claimedFiles = new Set<string>();
    const acceptedAgentIds: string[] = [];
    const conflictedAgentIds: string[] = [];

    for (const p of sorted) {
      const hasOverlap = p.touchedFiles.some((f) => claimedFiles.has(f));
      if (hasOverlap) {
        conflictedAgentIds.push(p.agentId);
        this.consecutiveConflictsByAgent.set(
          p.agentId,
          (this.consecutiveConflictsByAgent.get(p.agentId) ?? 0) + 1
        );
      } else {
        acceptedAgentIds.push(p.agentId);
        this.consecutiveConflictsByAgent.set(p.agentId, 0);
        for (const f of p.touchedFiles) claimedFiles.add(f);
      }
    }
    return { acceptedAgentIds, conflictedAgentIds, effectiveScores };
  }

  public getAgentConflictCount(agentId: string): number {
    return this.consecutiveConflictsByAgent.get(agentId) ?? 0;
  }
}
