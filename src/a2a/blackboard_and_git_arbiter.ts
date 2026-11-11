import * as crypto from "node:crypto";
/**
 * @fileoverview M23, M54-4 & M61-1: A2A Shared Blackboard CRDT/CAS Store & Multi-Agent Git Merge Arbiter
 * with Conflict-Aging Priority Boost, Idempotent CAS Retry, Duplicate Agent Patch Coalescing & SQLite Persistence.
 */
import { SqlStorageLike } from "../cloudflare/env.js";
import { SagaToolGateway } from "../saga_gateway.js";


export interface SwarmExpertAgentProfile {
  agentId: string;
  specialtyRole: string;
  supportedCapabilities: string[];
  historicalSuccessRate: number; // 0..1
  costPer1kTokensUsd: number;
  warmSandboxReady: boolean;
  activeTaskCount: number;
  reputationScore: number; // 0..100
}

export interface ContractNetBidSubmission {
  bidId: string;
  callForProposalId: string;
  agentId: string;
  estimatedDurationMs: number;
  bidPriceUsd: number;
  confidenceScore: number; // 0..1
}

export interface ContractNetTaskSpec {
  cfpId: string;
  requiredRole: string;
  requiredCapabilities: string[];
  maxBudgetUsd: number;
  slaDeadlineMs: number;
}

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
  private readonly swarmAgents: Map<string, SwarmExpertAgentProfile> = new Map();
  private readonly bftVoteLogs: Map<
    string,
    {
      key: string;
      targetVersion: number;
      candidateValue: unknown;
      votes: Array<{ agentId: string; reputationWeight: number; approve: boolean; signatureDigest: string }>;
      totalWeightedApprovals: number;
      totalWeightedRejections: number;
      quorumPassed: boolean;
    }
  > = new Map();
  public static readonly CONFLICT_AGING_BOOST_PER_ROUND = 150;
  private sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike) {
    if (sql) {
      this.bindSqlStorage(sql);
    }
  }

  /**
   * [M61-1] Binds SQLite storage so A2A Blackboard entries and conflict-aging counters survive restarts.
   */
  public bindSqlStorage(sql: SqlStorageLike): number {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS a2a_blackboard_entries (key TEXT PRIMARY KEY, value_json TEXT, version INTEGER, author_agent_id TEXT, updated_at_ms INTEGER)"
    );
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS a2a_agent_conflicts (agent_id TEXT PRIMARY KEY, conflict_count INTEGER)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM a2a_blackboard_entries")
      .toArray();
    let restored = 0;
    for (const r of rows) {
      const entry: BlackboardEntry = {
        key: String(r.key),
        value: JSON.parse(String(r.value_json ?? "null")),
        version: Number(r.version ?? 1),
        authorAgentId: String(r.author_agent_id ?? ""),
        updatedAtMs: Number(r.updated_at_ms ?? Date.now()),
      };
      this.board.set(entry.key, entry);
      restored += 1;
    }
    const cRows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM a2a_agent_conflicts")
      .toArray();
    for (const cr of cRows) {
      this.consecutiveConflictsByAgent.set(String(cr.agent_id), Number(cr.conflict_count ?? 0));
    }
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS a2a_swarm_agents (agent_id TEXT PRIMARY KEY, payload_json TEXT)"
    );
    for (const ar of this.sql.exec<Record<string, unknown>>("SELECT * FROM a2a_swarm_agents").toArray()) {
      const parsed = JSON.parse(String(ar.payload_json ?? "{}")) as SwarmExpertAgentProfile;
      if (parsed && parsed.agentId) {
        this.swarmAgents.set(parsed.agentId, parsed);
      }
    }
    return restored;
  }

  /**
   * [M61-1] Supports idempotent CAS retries: if `currentVersion === expectedVersion + 1`,
   * `existing.authorAgentId === authorAgentId`, and the canonicalized `value` matches `existing.value`,
   * returns the existing committed entry (`deduplicated: true`) instead of throwing a false CAS conflict.
   */
  public putCas(
    key: string,
    value: unknown,
    expectedVersion: number,
    authorAgentId: string
  ): { committed: boolean; deduplicated?: boolean; entry: BlackboardEntry } {
    const existing = this.board.get(key);
    const currentVersion = existing ? existing.version : 0;
    if (currentVersion !== expectedVersion) {
      if (
        existing &&
        currentVersion === expectedVersion + 1 &&
        existing.authorAgentId === authorAgentId &&
        JSON.stringify(SagaToolGateway.canonicalizeValue(existing.value)) ===
          JSON.stringify(SagaToolGateway.canonicalizeValue(value))
      ) {
        return { committed: true, deduplicated: true, entry: existing };
      }
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
    if (this.sql) {
      this.sql.exec(
        "INSERT OR REPLACE INTO a2a_blackboard_entries (key, value_json, version, author_agent_id, updated_at_ms) VALUES (?, ?, ?, ?, ?)",
        next.key,
        JSON.stringify(next.value),
        next.version,
        next.authorAgentId,
        next.updatedAtMs
      );
    }
    return { committed: true, deduplicated: false, entry: next };
  }

  public get(key: string): BlackboardEntry | undefined {
    return this.board.get(key);
  }

  /**
   * [M54-4 & M61-1] Arbitrates overlapping Git file patches across concurrent agents:
   * - Coalesces duplicate patch submissions from the same `agentId` within the same round
   *   (merging unique `touchedFiles` and taking `Math.max` priorityScore) so retried agent submissions
   *   never self-conflict or double-increment conflict counters.
   * - Applies `effectivePriority = priorityScore + consecutiveConflicts * 150` anti-starvation boost.
   */
  public arbitrateGitPatchTouches(
    patches: Array<{ agentId: string; touchedFiles: string[]; priorityScore: number }>
  ): {
    acceptedAgentIds: string[];
    conflictedAgentIds: string[];
    effectiveScores: Record<string, number>;
    coalescedDuplicateAgents: string[];
  } {
    const coalescedByAgent = new Map<
      string,
      { agentId: string; touchedFilesSet: Set<string>; priorityScore: number }
    >();
    const coalescedDuplicateAgents: string[] = [];

    for (const p of patches) {
      const existing = coalescedByAgent.get(p.agentId);
      if (existing) {
        if (!coalescedDuplicateAgents.includes(p.agentId)) {
          coalescedDuplicateAgents.push(p.agentId);
        }
        for (const f of p.touchedFiles) {
          existing.touchedFilesSet.add(f);
        }
        existing.priorityScore = Math.max(existing.priorityScore, p.priorityScore);
      } else {
        coalescedByAgent.set(p.agentId, {
          agentId: p.agentId,
          touchedFilesSet: new Set(p.touchedFiles),
          priorityScore: p.priorityScore,
        });
      }
    }

    const dedupedPatches = Array.from(coalescedByAgent.values()).map((item) => ({
      agentId: item.agentId,
      touchedFiles: Array.from(item.touchedFilesSet),
      priorityScore: item.priorityScore,
    }));

    const effectiveScores: Record<string, number> = {};
    for (const p of dedupedPatches) {
      const priorConflicts = this.consecutiveConflictsByAgent.get(p.agentId) ?? 0;
      effectiveScores[p.agentId] =
        p.priorityScore + priorConflicts * SharedBlackboardAndGitArbiter.CONFLICT_AGING_BOOST_PER_ROUND;
    }

    const sorted = [...dedupedPatches].sort(
      (a, b) => effectiveScores[b.agentId] - effectiveScores[a.agentId]
    );
    const claimedFiles = new Set<string>();
    const acceptedAgentIds: string[] = [];
    const conflictedAgentIds: string[] = [];

    for (const p of sorted) {
      const hasOverlap = p.touchedFiles.some((f) => claimedFiles.has(f));
      if (hasOverlap) {
        conflictedAgentIds.push(p.agentId);
        const nextCount = (this.consecutiveConflictsByAgent.get(p.agentId) ?? 0) + 1;
        this.consecutiveConflictsByAgent.set(p.agentId, nextCount);
        if (this.sql) {
          this.sql.exec(
            "INSERT OR REPLACE INTO a2a_agent_conflicts (agent_id, conflict_count) VALUES (?, ?)",
            p.agentId,
            nextCount
          );
        }
      } else {
        acceptedAgentIds.push(p.agentId);
        this.consecutiveConflictsByAgent.set(p.agentId, 0);
        if (this.sql) {
          this.sql.exec(
            "INSERT OR REPLACE INTO a2a_agent_conflicts (agent_id, conflict_count) VALUES (?, ?)",
            p.agentId,
            0
          );
        }
        for (const f of p.touchedFiles) claimedFiles.add(f);
      }
    }
    return {
      acceptedAgentIds,
      conflictedAgentIds,
      effectiveScores,
      coalescedDuplicateAgents,
    };
  }

  public getAgentConflictCount(agentId: string): number {
    return this.consecutiveConflictsByAgent.get(agentId) ?? 0;
  }

  public registerSwarmExpertAgent(profile: SwarmExpertAgentProfile): void {
    this.swarmAgents.set(profile.agentId, profile);
    if (this.sql) {
      this.sql.exec(
        "INSERT OR REPLACE INTO a2a_swarm_agents (agent_id, payload_json) VALUES (?, ?)",
        profile.agentId,
        JSON.stringify(profile)
      );
    }
  }

  public getSwarmExpertAgent(agentId: string): SwarmExpertAgentProfile | undefined {
    return this.swarmAgents.get(agentId);
  }

  public listSwarmAgents(): SwarmExpertAgentProfile[] {
    return Array.from(this.swarmAgents.values());
  }

  /**
   * [M74-1 / N1] Contract-Net Protocol (CNP) Multi-Agent Bidding & Task Allocation:
   * 1. Broadcasts CFP (Call-for-Proposal) to all swarm expert agents matching required capabilities.
   * 2. Evaluates incoming agent bids using Multi-Attribute Utility:
   *    Score = (CapabilityMatch * 30) + (Reputation * 25) + (Confidence * 20) + (BudgetFit * 15) + (WarmSandboxBonus * 10) - (LoadPenalty).
   * 3. Selects the highest-utility bidder, awards the contract, and returns matched dispatch telemetry.
   */
  public matchContractNetBidding(params: {
    task: ContractNetTaskSpec;
    bids: ContractNetBidSubmission[];
  }): {
    cfpId: string;
    awardedAgentId: string;
    winningBidId: string;
    utilityScore: number;
    awardedPriceUsd: number;
    bidsEvaluated: number;
    bidRankings: Array<{
      agentId: string;
      bidId: string;
      utilityScore: number;
      passedSlaAndBudget: boolean;
    }>;
  } {
    if (!Array.isArray(params.bids) || params.bids.length === 0) {
      throw new Error(`matchContractNetBidding requires at least 1 bid for CFP '${params.task.cfpId}'`);
    }

    const scoredBids = params.bids.map((bid) => {
      const agent = this.swarmAgents.get(bid.agentId) ?? {
        agentId: bid.agentId,
        specialtyRole: params.task.requiredRole,
        supportedCapabilities: [...params.task.requiredCapabilities],
        historicalSuccessRate: 0.85,
        costPer1kTokensUsd: 0.002,
        warmSandboxReady: false,
        activeTaskCount: 0,
        reputationScore: 70,
      };

      const matchedCaps = params.task.requiredCapabilities.filter((c) =>
        agent.supportedCapabilities.includes(c)
      ).length;
      const capRatio =
        params.task.requiredCapabilities.length > 0
          ? matchedCaps / params.task.requiredCapabilities.length
          : 1.0;

      const passedBudget = bid.bidPriceUsd <= params.task.maxBudgetUsd;
      const passedSla = bid.estimatedDurationMs <= params.task.slaDeadlineMs;
      const passedSlaAndBudget = passedBudget && passedSla && capRatio >= 0.5;

      const capScore = capRatio * 30.0;
      const repScore = (agent.reputationScore / 100.0) * 25.0;
      const confScore = Math.max(0, Math.min(1, bid.confidenceScore)) * 20.0;
      const budgetFit = Math.max(0, (1 - bid.bidPriceUsd / Math.max(0.01, params.task.maxBudgetUsd))) * 15.0;
      const warmBonus = agent.warmSandboxReady ? 10.0 : 0.0;
      const loadPenalty = Math.min(15.0, agent.activeTaskCount * 3.0);

      const rawUtility = passedSlaAndBudget
        ? capScore + repScore + confScore + budgetFit + warmBonus - loadPenalty
        : -100.0;
      const utilityScore = Number(rawUtility.toFixed(2));

      return {
        agentId: bid.agentId,
        bidId: bid.bidId,
        utilityScore,
        passedSlaAndBudget,
        bidPriceUsd: bid.bidPriceUsd,
      };
    });

    scoredBids.sort((a, b) => b.utilityScore - a.utilityScore);
    const winner = scoredBids[0];
    if (!winner || !winner.passedSlaAndBudget) {
      throw new Error(`No eligible swarm agent bid satisfied budget & SLA for CFP '${params.task.cfpId}'`);
    }

    return {
      cfpId: params.task.cfpId,
      awardedAgentId: winner.agentId,
      winningBidId: winner.bidId,
      utilityScore: winner.utilityScore,
      awardedPriceUsd: winner.bidPriceUsd,
      bidsEvaluated: scoredBids.length,
      bidRankings: scoredBids.map((b) => ({
        agentId: b.agentId,
        bidId: b.bidId,
        utilityScore: b.utilityScore,
        passedSlaAndBudget: b.passedSlaAndBudget,
      })),
    };
  }

  /**
   * [M74-2 / N2] Byzantine-Fault-Tolerant (BFT) Reputation-Weighted Blackboard Consensus:
   * Protects critical architecture/policy keys against single-agent hallucination or rogue writes.
   * Requires:
   * 1. Weighted approvals $\ge 2/3$ of total reputation weight across participating swarm agents.
   * 2. Zero vetoes from core authority agents ($reputationScore \ge 95$).
   * 3. Successful quorum commits to `a2a_blackboard_entries` with incremented version.
   */
  public putBftConsensusCas(params: {
    key: string;
    candidateValue: unknown;
    expectedVersion: number;
    initiatorAgentId: string;
    votes: Array<{ agentId: string; approve: boolean; rationale?: string }>;
  }): {
    committed: boolean;
    quorumPassed: boolean;
    key: string;
    newVersion: number;
    totalReputationWeight: number;
    weightedApprovalRatio: number;
    vetoedByCoreAuthority: boolean;
    dissentingAgents: string[];
    entry?: BlackboardEntry;
  } {
    const existing = this.board.get(params.key);
    const currentVersion = existing ? existing.version : 0;
    if (currentVersion !== params.expectedVersion) {
      throw new Error(
        `BFT Blackboard CAS conflict on key '${params.key}': expected v${params.expectedVersion}, actual v${currentVersion}`
      );
    }

    let totalWeight = 0;
    let approvalWeight = 0;
    let vetoedByCoreAuthority = false;
    const dissentingAgents: string[] = [];

    const voteDetails = params.votes.map((v) => {
      const agent = this.swarmAgents.get(v.agentId);
      const rep = agent ? agent.reputationScore : 50;
      totalWeight += rep;
      if (v.approve) {
        approvalWeight += rep;
      } else {
        dissentingAgents.push(v.agentId);
        if (rep >= 95) {
          vetoedByCoreAuthority = true;
        }
      }
      return {
        agentId: v.agentId,
        reputationWeight: rep,
        approve: v.approve,
        signatureDigest: crypto
          .createHash("sha256")
          .update(`${v.agentId}:${params.key}:${v.approve}:${currentVersion + 1}`)
          .digest("hex")
          .slice(0, 16),
      };
    });

    const approvalRatio = totalWeight > 0 ? Number((approvalWeight / totalWeight).toFixed(4)) : 0;
    const quorumPassed = approvalRatio >= 2 / 3 && !vetoedByCoreAuthority;

    this.bftVoteLogs.set(`${params.key}:v${currentVersion + 1}`, {
      key: params.key,
      targetVersion: currentVersion + 1,
      candidateValue: params.candidateValue,
      votes: voteDetails,
      totalWeightedApprovals: approvalWeight,
      totalWeightedRejections: totalWeight - approvalWeight,
      quorumPassed,
    });

    if (!quorumPassed) {
      return {
        committed: false,
        quorumPassed: false,
        key: params.key,
        newVersion: currentVersion,
        totalReputationWeight: totalWeight,
        weightedApprovalRatio: approvalRatio,
        vetoedByCoreAuthority,
        dissentingAgents,
      };
    }

    const nextEntry: BlackboardEntry = {
      key: params.key,
      value: params.candidateValue,
      version: currentVersion + 1,
      authorAgentId: params.initiatorAgentId,
      updatedAtMs: Date.now(),
    };
    this.board.set(params.key, nextEntry);
    if (this.sql) {
      this.sql.exec(
        "INSERT OR REPLACE INTO a2a_blackboard_entries (key, value_json, version, author_agent_id, updated_at_ms) VALUES (?, ?, ?, ?, ?)",
        nextEntry.key,
        JSON.stringify(nextEntry.value),
        nextEntry.version,
        nextEntry.authorAgentId,
        nextEntry.updatedAtMs
      );
    }

    return {
      committed: true,
      quorumPassed: true,
      key: params.key,
      newVersion: nextEntry.version,
      totalReputationWeight: totalWeight,
      weightedApprovalRatio: approvalRatio,
      vetoedByCoreAuthority,
      dissentingAgents,
      entry: nextEntry,
    };
  }

  /**
   * [M74-2 / N2] Semantic Export-Table Conflict Analyzer for Multi-Agent Code Merges:
   * Parses exported identifiers, functions, and interfaces across concurrent agents touching the same file,
   * distinguishing clean independent symbol additions from overlapping signature mutations.
   */
  public analyzeSemanticExportConflicts(params: {
    filePath: string;
    agentPatches: Array<{
      agentId: string;
      patchText: string;
    }>;
  }): {
    filePath: string;
    semanticClean: boolean;
    conflictingSymbols: string[];
    mergedExportSymbols: string[];
    agentContributions: Record<string, string[]>;
  } {
    const symbolMap = new Map<string, string[]>();
    const agentContributions: Record<string, string[]> = {};

    for (const ap of params.agentPatches) {
      const symbols: string[] = [];
      const regex = /export\s+(?:const|function|class|interface|type)\s+([A-Za-z0-9_$]+)/g;
      let match: RegExpExecArray | null;
      while ((match = regex.exec(ap.patchText)) !== null) {
        symbols.push(match[1]);
      }
      agentContributions[ap.agentId] = symbols;
      for (const sym of symbols) {
        const existing = symbolMap.get(sym) ?? [];
        existing.push(ap.agentId);
        symbolMap.set(sym, existing);
      }
    }

    const conflictingSymbols: string[] = [];
    const mergedExportSymbols: string[] = [];
    for (const [sym, authors] of symbolMap.entries()) {
      if (authors.length > 1) {
        conflictingSymbols.push(sym);
      } else {
        mergedExportSymbols.push(sym);
      }
    }

    return {
      filePath: params.filePath,
      semanticClean: conflictingSymbols.length === 0,
      conflictingSymbols,
      mergedExportSymbols,
      agentContributions,
    };
  }
}
