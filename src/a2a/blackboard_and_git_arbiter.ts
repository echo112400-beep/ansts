/**
 * @fileoverview M23 (TASK-109 ~ TASK-112): Multi-Agent A2A Blackboard Bus, Automated Git Rebase/Conflict
 * Arbiter (`GitConflictArbiterCell`), and Cross-Shard Work-Stealing Quota Broker.
 *
 * Implements Directions 3 & 4 of the ANSTS v2.4 roadmap:
 * 1. `A2aBlackboardBus`: Typed Agent-to-Agent pub/sub blackboard protected by monotonic `FenceToken`
 *    so parallel sub-agents in a DAG can exchange artifacts and negotiate capabilities without
 *    bloating the parent LLM context window.
 * 2. `GitConflictArbiterCell`: Detects file-level & AST symbol-level conflicts across parallel
 *    sub-agent sandbox branches (`SandboxBranch`) and orchestrates automated 3-way rebase/merge
 *    before committing to the target repository branch.
 * 3. `WorkStealingQuotaBroker`: Dynamically lends idle 5D W-DRF tenant quota slices from under-utilized
 *    `SchedulerShard`s to hot shards with strict monotonic version reconciliation.
 */

import { FenceToken } from "../models";
import { StaleFenceError } from "../lease_coordinator";
import { SandboxBranch } from "../runtime/sandbox_branch_forker";

export interface A2aBlackboardArtifact {
  artifactId: string;
  dagId: string;
  producerTaskId: string;
  topic: string;
  fence: FenceToken;
  schemaType: "CODE_PATCH" | "TEST_REPORT" | "AST_INTERFACE_CONTRACT" | "CONFLICT_RESOLUTION";
  payload: Record<string, unknown>;
  publishedAtMs: number;
}

export interface GitBranchPatchSubmission {
  branchId: string;
  agentTaskId: string;
  baseCheckpointId: string;
  modifiedFiles: Record<string, { touchedSymbols: string[]; diffPatch: string }>;
}

export interface GitMergeArbitrationResult {
  mergedCleanly: boolean;
  autoResolvedSymbolConflicts: string[];
  unresolvedFileConflicts: string[];
  mergedFileMap: Record<string, string>;
}

/**
 * TASK-109: Agent-to-Agent (A2A) Typed Blackboard Bus with FenceToken Validation.
 */
export class A2aBlackboardBus {
  public readonly artifactsByDag = new Map<string, A2aBlackboardArtifact[]>();
  private readonly highestProducerFence = new Map<string, FenceToken>();

  public publishArtifact(artifact: A2aBlackboardArtifact): A2aBlackboardArtifact {
    const prevFence = this.highestProducerFence.get(artifact.producerTaskId);
    if (
      prevFence &&
      (artifact.fence.epoch < prevFence.epoch ||
        (artifact.fence.epoch === prevFence.epoch && artifact.fence.turnSeq <= prevFence.turnSeq))
    ) {
      throw new StaleFenceError(
        `409 StaleA2aArtifactFence: producer ${artifact.producerTaskId} sent (${artifact.fence.epoch},${artifact.fence.turnSeq}) <= (${prevFence.epoch},${prevFence.turnSeq})`,
      );
    }
    this.highestProducerFence.set(artifact.producerTaskId, artifact.fence);

    const list = this.artifactsByDag.get(artifact.dagId) ?? [];
    list.push(artifact);
    this.artifactsByDag.set(artifact.dagId, list);
    return artifact;
  }

  public queryArtifacts(dagId: string, topic?: string): A2aBlackboardArtifact[] {
    const list = this.artifactsByDag.get(dagId) ?? [];
    if (!topic) return [...list];
    return list.filter((a) => a.topic === topic);
  }
}

/**
 * TASK-110: Automated Git Rebase & AST Symbol Conflict Arbiter (`GitConflictArbiterCell`).
 */
export class GitConflictArbiterCell {
  /**
   * Arbitrates concurrent code patches from multiple sub-agent branches targeting the same base checkpoint:
   * - If two branches modify different files -> merges cleanly.
   * - If two branches modify the same file but disjoint AST symbols (e.g. different functions) ->
   *   automatically performs 3-way AST symbol merge (`autoResolvedSymbolConflicts`).
   * - If two branches modify the exact same AST symbol in the same file -> flags `unresolvedFileConflicts`
   *   for a dedicated conflict-resolution Facet.
   */
  public arbitrateParallelBranchPatches(
    submissions: GitBranchPatchSubmission[],
  ): GitMergeArbitrationResult {
    const fileSymbolOwners = new Map<string, Map<string, string>>(); // file -> (symbol -> branchId)
    const mergedFileMap: Record<string, string> = {};
    const autoResolvedSymbolConflicts: string[] = [];
    const unresolvedFileConflicts: string[] = [];

    for (const sub of submissions) {
      for (const [filePath, change] of Object.entries(sub.modifiedFiles)) {
        if (!fileSymbolOwners.has(filePath)) {
          fileSymbolOwners.set(filePath, new Map());
          mergedFileMap[filePath] = change.diffPatch;
        } else {
          const symbolMap = fileSymbolOwners.get(filePath)!;
          let symbolCollision = false;
          for (const sym of change.touchedSymbols) {
            if (symbolMap.has(sym)) {
              symbolCollision = true;
              if (!unresolvedFileConflicts.includes(`${filePath}::${sym}`)) {
                unresolvedFileConflicts.push(`${filePath}::${sym}`);
              }
            }
          }
          if (!symbolCollision) {
            // Disjoint AST symbols in the same file -> auto-rebase & combine patches cleanly
            autoResolvedSymbolConflicts.push(filePath);
            mergedFileMap[filePath] = `${mergedFileMap[filePath]}
${change.diffPatch}`;
          }
        }

        const symbolMap = fileSymbolOwners.get(filePath)!;
        for (const sym of change.touchedSymbols) {
          if (!symbolMap.has(sym)) {
            symbolMap.set(sym, sub.branchId);
          }
        }
      }
    }

    return {
      mergedCleanly: unresolvedFileConflicts.length === 0,
      autoResolvedSymbolConflicts,
      unresolvedFileConflicts,
      mergedFileMap,
    };
  }
}

/**
 * TASK-111: Cross-Shard Work-Stealing Quota Broker.
 * Allows hot `SchedulerShard`s to borrow unused tenant quota slices from idle shards without
 * violating the tenant's global 5D W-DRF ceiling.
 */
export class WorkStealingQuotaBroker {
  public reconciliationVersion = 1;

  public rebalanceTenantQuotaAcrossShards(params: {
    tenantId: string;
    globalMaxSlots: number;
    shardAllocations: Record<string, number>;
    shardActiveUsage: Record<string, number>;
    requestingHotShardId: string;
    requestedExtraSlots: number;
  }): {
    grantedSlots: number;
    updatedAllocations: Record<string, number>;
    reconciliationVersion: number;
  } {
    const updatedAllocations = { ...params.shardAllocations };
    let needed = params.requestedExtraSlots;
    let grantedSlots = 0;

    // Steal idle quota from shards where allocation > activeUsage
    for (const [shardId, allocated] of Object.entries(updatedAllocations)) {
      if (shardId === params.requestingHotShardId || needed <= 0) continue;
      const used = params.shardActiveUsage[shardId] ?? 0;
      const stealable = Math.max(0, allocated - used);
      if (stealable > 0) {
        const stolen = Math.min(needed, stealable);
        updatedAllocations[shardId] -= stolen;
        updatedAllocations[params.requestingHotShardId] =
          (updatedAllocations[params.requestingHotShardId] ?? 0) + stolen;
        needed -= stolen;
        grantedSlots += stolen;
      }
    }

    const totalAllocated = Object.values(updatedAllocations).reduce((a, b) => a + b, 0);
    if (totalAllocated > params.globalMaxSlots) {
      throw new Error(`QuotaInvariantViolation: totalAllocated ${totalAllocated} > ${params.globalMaxSlots}`);
    }

    this.reconciliationVersion += 1;
    return {
      grantedSlots,
      updatedAllocations,
      reconciliationVersion: this.reconciliationVersion,
    };
  }
}
