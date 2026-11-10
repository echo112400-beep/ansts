"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.A2aWaitForGraphDeadlockDetector = void 0; /**
 * @fileoverview M23 & M52-3: A2A Wait-For Graph (WFG) Cycle & Priority-Aware Distributed Deadlock Resolver.
 */
class A2aWaitForGraphDeadlockDetector {
  edges = new Map();

  addWaitEdge(waiterAgentId, holderAgentId) {
    let targets = this.edges.get(waiterAgentId);
    if (!targets) {
      targets = new Set();
      this.edges.set(waiterAgentId, targets);
    }
    targets.add(holderAgentId);
  }

  removeAgent(agentId) {
    this.edges.delete(agentId);
    for (const set of this.edges.values()) {
      set.delete(agentId);
    }
  }

  detectCycle() {
    const visited = new Set();
    const onStack = new Set();
    const parentMap = new Map();

    const dfs = (node) => {
      visited.add(node);
      onStack.add(node);
      const neighbors = this.edges.get(node) ?? new Set();
      for (const next of neighbors) {
        if (!visited.has(next)) {
          parentMap.set(next, node);
          const found = dfs(next);
          if (found) return found;
        } else if (onStack.has(next)) {
          const cycle = [next];
          let cur = node;
          while (cur !== next && parentMap.has(cur)) {
            cycle.push(cur);
            cur = parentMap.get(cur);
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
   * [M52-3] Automatically selects the lowest-priority agent in a detected Wait-For Graph cycle
   * and aborts only its outgoing wait edges to break the deadlock cleanly.
   */
  resolveCycleWithLowestPriorityVictim(
  agentPriorityScores = {})
  {
    const { hasDeadlock, cyclePath } = this.detectCycle();
    if (!hasDeadlock || cyclePath.length === 0) {
      return { resolved: false, victimAgentId: null, cyclePath: [] };
    }
    const uniqueAgents = Array.from(new Set(cyclePath));
    uniqueAgents.sort((a, b) => {
      const pa = agentPriorityScores[a] ?? 50;
      const pb = agentPriorityScores[b] ?? 50;
      if (pa !== pb) return pa - pb;
      return b.localeCompare(a);
    });
    const victimAgentId = uniqueAgents[0];
    this.edges.delete(victimAgentId);
    return {
      resolved: !this.detectCycle().hasDeadlock,
      victimAgentId,
      cyclePath
    };
  }
}exports.A2aWaitForGraphDeadlockDetector = A2aWaitForGraphDeadlockDetector;