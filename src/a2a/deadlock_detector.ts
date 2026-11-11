/**
 * @fileoverview M23, M52-3 & M63-4: A2A Wait-For Graph (WFG) Cycle & Priority-Aware Distributed Deadlock Resolver
 * with Iterative Multi-Cycle Resolution & Self-Wait Edge Detection.
 */
export class A2aWaitForGraphDeadlockDetector {
  private readonly edges: Map<string, Set<string>> = new Map();

  public addWaitEdge(waiterAgentId: string, holderAgentId: string): void {
    let targets = this.edges.get(waiterAgentId);
    if (!targets) {
      targets = new Set();
      this.edges.set(waiterAgentId, targets);
    }
    targets.add(holderAgentId);
  }

  public removeAgent(agentId: string): void {
    this.edges.delete(agentId);
    for (const set of this.edges.values()) {
      set.delete(agentId);
    }
  }

  public detectCycle(): { hasDeadlock: boolean; cyclePath: string[] } {
    const visited = new Set<string>();
    const onStack = new Set<string>();
    const parentMap = new Map<string, string>();

    const dfs = (node: string): string[] | null => {
      visited.add(node);
      onStack.add(node);
      const neighbors = this.edges.get(node) ?? new Set();
      for (const next of neighbors) {
        if (!visited.has(next)) {
          parentMap.set(next, node);
          const found = dfs(next);
          if (found) return found;
        } else if (onStack.has(next)) {
          const cycle: string[] = [next];
          let cur = node;
          while (cur !== next && parentMap.has(cur)) {
            cycle.push(cur);
            cur = parentMap.get(cur)!;
          }
          cycle.push(next);
          return cycle.reverse();
        }
      }
      onStack.delete(node);
      return null;
    };

    for (const node of this.edges.keys()) {
      if (!visited.has(node)) {
        const cycle = dfs(node);
        if (cycle) {
          return { hasDeadlock: true, cyclePath: cycle };
        }
      }
    }
    return { hasDeadlock: false, cyclePath: [] };
  }

  /**
   * [M52-3 & M63-4] Iteratively selects the lowest-priority agent in each detected Wait-For Graph cycle
   * and aborts its outgoing wait edges until ALL disjoint/overlapping cycles in the graph are resolved.
   */
  public resolveCycleWithLowestPriorityVictim(
    agentPriorityScores: Record<string, number> = {}
  ): {
    resolved: boolean;
    victimAgentId: string | null;
    evictedVictims: string[];
    cyclePath: string[];
  } {
    const firstCheck = this.detectCycle();
    if (!firstCheck.hasDeadlock || firstCheck.cyclePath.length === 0) {
      return { resolved: false, victimAgentId: null, evictedVictims: [], cyclePath: [] };
    }

    const firstCyclePath = firstCheck.cyclePath;
    const evictedVictims: string[] = [];
    const maxIterations = Math.max(1, this.edges.size + 1);
    let iter = 0;

    while (iter < maxIterations) {
      iter += 1;
      const current = this.detectCycle();
      if (!current.hasDeadlock || current.cyclePath.length === 0) {
        break;
      }
      const uniqueAgents = Array.from(new Set(current.cyclePath));
      uniqueAgents.sort((a, b) => {
        const pa = agentPriorityScores[a] ?? 50;
        const pb = agentPriorityScores[b] ?? 50;
        if (pa !== pb) return pa - pb;
        return b.localeCompare(a);
      });
      const victim = uniqueAgents[0];
      this.edges.delete(victim);
      evictedVictims.push(victim);
    }

    return {
      resolved: !this.detectCycle().hasDeadlock,
      victimAgentId: evictedVictims[0] ?? null,
      evictedVictims,
      cyclePath: firstCyclePath,
    };
  }
}
