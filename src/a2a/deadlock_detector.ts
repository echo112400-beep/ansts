/**
 * @fileoverview M23 A2A Wait-For Graph (WFG) Cycle & Distributed Deadlock Detector.
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
}
