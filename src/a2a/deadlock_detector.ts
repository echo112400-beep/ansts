
export type VectorClock = Record<string, number>;

export interface CausalMessageEvent {
  eventId: string;
  senderAgentId: string;
  recipientAgentId?: string;
  vectorClock: VectorClock;
  payloadType: string;
  logicalTurn: number;
  payloadDigest: string;
  timestampMs: number;
}

export interface DistributedConsistentCut {
  cutId: string;
  participatingAgents: string[];
  cutVectorClock: VectorClock;
  isCausallyConsistent: boolean;
  cutTurnsByAgent: Record<string, number>;
  violatedCausalInversions: string[];
}

/**
 * @fileoverview M23, M52-3 & M63-4: A2A Wait-For Graph (WFG) Cycle & Priority-Aware Distributed Deadlock Resolver
 * with Iterative Multi-Cycle Resolution & Self-Wait Edge Detection.
 */
export class A2aWaitForGraphDeadlockDetector {
  private readonly edges: Map<string, Set<string>> = new Map();
  private readonly agentVectorClocks: Map<string, VectorClock> = new Map();
  private readonly messageHistory: CausalMessageEvent[] = [];

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

  /**
   * [M74-3 / N3] Updates an agent's local Lamport/Vector Clock on local event or message receipt:
   * V_i[i] = V_i[i] + 1, and V_i[k] = max(V_i[k], V_msg[k]).
   */
  public advanceAgentVectorClock(
    agentId: string,
    receivedClock?: VectorClock
  ): VectorClock {
    const cur = { ...(this.agentVectorClocks.get(agentId) ?? { [agentId]: 0 }) };
    cur[agentId] = (cur[agentId] ?? 0) + 1;
    if (receivedClock) {
      for (const [k, v] of Object.entries(receivedClock)) {
        cur[k] = Math.max(cur[k] ?? 0, v);
      }
    }
    this.agentVectorClocks.set(agentId, cur);
    return { ...cur };
  }

  public getAgentVectorClock(agentId: string): VectorClock {
    return { ...(this.agentVectorClocks.get(agentId) ?? { [agentId]: 0 }) };
  }

  public recordCausalMessage(event: CausalMessageEvent): void {
    this.messageHistory.push(event);
    this.advanceAgentVectorClock(event.senderAgentId, event.vectorClock);
    if (event.recipientAgentId) {
      this.advanceAgentVectorClock(event.recipientAgentId, event.vectorClock);
    }
  }

  /**
   * [M74-3 / N3] Compares two vector clocks: returns -1 if a happens-before b (a < b),
   * 1 if b happens-before a (b < a), 0 if identical, or null if concurrent (incomparable).
   */
  public static compareVectorClocks(a: VectorClock, b: VectorClock): -1 | 0 | 1 | null {
    const allKeys = Array.from(new Set([...Object.keys(a), ...Object.keys(b)]));
    let aGreater = false;
    let bGreater = false;
    for (const k of allKeys) {
      const va = a[k] ?? 0;
      const vb = b[k] ?? 0;
      if (va > vb) aGreater = true;
      if (vb > va) bGreater = true;
    }
    if (!aGreater && !bGreater) return 0;
    if (aGreater && !bGreater) return 1;
    if (bGreater && !aGreater) return -1;
    return null; // concurrent
  }

  /**
   * [M74-3 / N3] Computes and verifies a Distributed Consistent Cut across multi-agent swarms:
   * A cut C is causally consistent iff for every message m, if receive(m) in C,
   * then send(m) in C (no message is received before it is sent).
   */
  public captureDistributedConsistentCut(params: {
    cutId: string;
    agentCutTurns: Record<string, number>;
  }): DistributedConsistentCut {
    const participatingAgents = Object.keys(params.agentCutTurns);
    const cutVectorClock: VectorClock = {};
    for (const ag of participatingAgents) {
      const clk = this.getAgentVectorClock(ag);
      for (const [k, v] of Object.entries(clk)) {
        cutVectorClock[k] = Math.max(cutVectorClock[k] ?? 0, v);
      }
    }

    const violatedCausalInversions: string[] = [];
    for (const msg of this.messageHistory) {
      if (
        msg.recipientAgentId &&
        params.agentCutTurns[msg.recipientAgentId] !== undefined &&
        params.agentCutTurns[msg.senderAgentId] !== undefined
      ) {
        const receivedAtCut = msg.logicalTurn <= params.agentCutTurns[msg.recipientAgentId];
        const sentAtCut = msg.logicalTurn <= params.agentCutTurns[msg.senderAgentId];
        if (receivedAtCut && !sentAtCut) {
          violatedCausalInversions.push(
            `Orphan message '${msg.eventId}': received by ${msg.recipientAgentId} at turn <= ${params.agentCutTurns[msg.recipientAgentId]}, but sender ${msg.senderAgentId} cut at earlier turn ${params.agentCutTurns[msg.senderAgentId]}`
          );
        }
      }
    }

    const isCausallyConsistent = violatedCausalInversions.length === 0;
    return {
      cutId: params.cutId,
      participatingAgents,
      cutVectorClock,
      isCausallyConsistent,
      cutTurnsByAgent: { ...params.agentCutTurns },
      violatedCausalInversions,
    };
  }
}
