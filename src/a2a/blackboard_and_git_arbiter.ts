/**
 * @fileoverview M23 A2A Shared Blackboard CRDT/CAS Store & Multi-Agent Git Merge Arbiter.
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

  public arbitrateGitPatchTouches(patches: Array<{ agentId: string; touchedFiles: string[]; priorityScore: number }>): {
    acceptedAgentIds: string[];
    conflictedAgentIds: string[];
  } {
    const sorted = [...patches].sort((a, b) => b.priorityScore - a.priorityScore);
    const claimedFiles = new Set<string>();
    const acceptedAgentIds: string[] = [];
    const conflictedAgentIds: string[] = [];

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
}
