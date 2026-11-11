/**
 * @fileoverview M5 TASK-21, M55-4 & M67-5: Deterministic Time-Travel Debugger & Turn Forking Engine
 * with WAL Frame CRC32 Integrity Verification, Highest-Epoch Turn Deduplication & Self-Overwrite Fork Guard.
 */
import { TaskSpec, TaskState } from "../models.js";
import { WalDeltaEntry } from "./wal_engine.js";
import { LayerFsWorkspaceDriver } from "../runtime/layerfs_workspace_driver.js";

export interface ForkedTaskBranch {
  forkedTask: TaskSpec;
  restoredTurnIndex: number;
  overriddenPrompt: string;
  inheritedFiles: string[];
  inheritedFencingToken: number;
}

export class DeterministicTimeTravelReplayer {
  /**
   * M17 TASK-86: Zero-Copy Recursive Commit Fork (`O(1)` metadata fork for MCTS/RL & Time-Travel Debugging).
   */
  public forkLayerFsCommitZeroCopy(
    driver: LayerFsWorkspaceDriver,
    sourceCommitId: string,
    newBranchName: string
  ): { branchName: string; headCommitId: string; copiedFilesCount: 0 } {
    return driver.forkBranch(sourceCommitId, newBranchName);
  }

  /**
   * [M55-4 & M67-5] Forks a task at `targetTurnIndex` after verifying every WAL frame's CRC32 integrity,
   * rejecting self-overwriting `newTaskId === originalTask.task_id`, and deduplicating multi-epoch turn entries
   * by selecting the highest `(fencingToken, walSeq)` per turn.
   */
  public forkTaskAtTurn(
    originalTask: TaskSpec,
    walEntries: WalDeltaEntry[],
    targetTurnIndex: number,
    newTaskId: string,
    overriddenPrompt?: string
  ): ForkedTaskBranch {
    if (!Number.isInteger(targetTurnIndex) || targetTurnIndex < 0) {
      throw new Error(`Invalid targetTurnIndex '${targetTurnIndex}' for time-travel fork`);
    }
    const cleanNewTaskId = String(newTaskId ?? "").trim();
    if (!cleanNewTaskId || cleanNewTaskId === originalTask.task_id) {
      throw new Error(
        `Cannot fork task '${originalTask.task_id}': newTaskId must be non-empty and distinct from originalTask.task_id`
      );
    }
    const candidateEntries = walEntries.filter((e) => e.turnIndex <= targetTurnIndex);
    if (candidateEntries.length === 0) {
      throw new Error(`Cannot fork task '${originalTask.task_id}' at turn ${targetTurnIndex}: no WAL found`);
    }

    // [M55-4] Deduplicate by turnIndex choosing highest (fencingToken, walSeq) and verify CRC32 integrity
    const canonicalByTurn = new Map<number, WalDeltaEntry>();
    let maxFencingToken = originalTask.lease_fencing_token;
    for (const entry of candidateEntries) {
      if (entry.frame && typeof entry.frame.verifyIntegrity === "function" && !entry.frame.verifyIntegrity()) {
        throw new Error(
          `Cannot fork task '${originalTask.task_id}' at turn ${targetTurnIndex}: corrupted WAL frame '${entry.frame.frameId}' (CRC32 mismatch at turn ${entry.turnIndex})`
        );
      }
      const existing = canonicalByTurn.get(entry.turnIndex);
      if (
        !existing ||
        entry.fencingToken > existing.fencingToken ||
        (entry.fencingToken === existing.fencingToken && entry.walSeq > existing.walSeq)
      ) {
        canonicalByTurn.set(entry.turnIndex, entry);
      }
      if (entry.fencingToken > maxFencingToken) {
        maxFencingToken = entry.fencingToken;
      }
    }

    const orderedTurns = Array.from(canonicalByTurn.values()).sort((a, b) => a.turnIndex - b.turnIndex);
    const inheritedFiles = new Set<string>();
    for (const e of orderedTurns) {
      for (const f of e.changedFiles) {
        inheritedFiles.add(f);
      }
    }

    const forkedTask = new TaskSpec({
      task_id: cleanNewTaskId,
      tenant_id: originalTask.tenant_id,
      priority: originalTask.priority,
      prompt: overriddenPrompt ?? originalTask.prompt,
      max_turns: originalTask.max_turns,
      timeout_seconds: originalTask.timeout_seconds,
      resource_profile: { ...originalTask.resource_profile },
      dependencies: [...originalTask.dependencies],
      parent_task_id: originalTask.task_id,
    });
    forkedTask.current_turn = targetTurnIndex;
    forkedTask.state = TaskState.QUEUED;
    forkedTask.lease_fencing_token = maxFencingToken;

    return {
      forkedTask,
      restoredTurnIndex: targetTurnIndex,
      overriddenPrompt: forkedTask.prompt,
      inheritedFiles: [...inheritedFiles],
      inheritedFencingToken: maxFencingToken,
    };
  }

  /**
   * [M72-3 / F6] Redacts PII, API secrets (`sk-...`), Bearer tokens, and email addresses
   * before replaying production TurnWalFrames in shadow canary evaluation.
   */
  public static redactSensitivePromptTokens(rawText: string): {
    redactedText: string;
    redactionCount: number;
  } {
    let count = 0;
    const redactedText = String(rawText ?? "")
      .replace(/\bsk-[A-Za-z0-9_\-]{8,}\b/g, () => {
        count += 1;
        return "[REDACTED_API_KEY]";
      })
      .replace(/Bearer\s+[A-Za-z0-9._\-]{8,}/gi, () => {
        count += 1;
        return "Bearer [REDACTED_TOKEN]";
      })
      .replace(/[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}/g, () => {
        count += 1;
        return "[REDACTED_EMAIL]";
      });
    return { redactedText, redactionCount: count };
  }

  /**
   * [M72-3 / F6] Production Shadow WAL Replay & Canary Regression Benchmark Harness:
   * Replays historical production turns against a candidate model/prompt version, verifies WAL CRC32 integrity,
   * redacts sensitive credentials/PII, and computes tool-selection accuracy, latency delta, token-cost delta,
   * and an automated Canary promotion/rollback gate decision.
   */
  public executeShadowWalRegressionSuite(params: {
    suiteId: string;
    baselineModel: string;
    candidateModel: string;
    maxRegressionRatePct?: number;
    maxCostIncreasePct?: number;
    traces: Array<{
      taskId: string;
      turnIndex: number;
      prompt: string;
      expectedToolName: string;
      expectedPathPrefix?: string;
      baselineLatencyMs: number;
      baselineTokens: number;
      candidateToolName: string;
      candidateArgs?: Record<string, unknown>;
      candidateLatencyMs: number;
      candidateTokens: number;
      walFrame?: WalDeltaEntry;
    }>;
  }): {
    suiteId: string;
    baselineModel: string;
    candidateModel: string;
    totalTraces: number;
    crc32VerifiedCount: number;
    totalRedactions: number;
    matchedTracesCount: number;
    regressionCount: number;
    toolMatchAccuracyPct: number;
    avgBaselineLatencyMs: number;
    avgCandidateLatencyMs: number;
    latencyDeltaPct: number;
    totalBaselineTokens: number;
    totalCandidateTokens: number;
    tokenCostDeltaPct: number;
    passedGate: boolean;
    canaryRecommendation: "PROMOTE_CANARY" | "AUTO_ROLLBACK_ON_REGRESSION";
    traceResults: Array<{
      taskId: string;
      turnIndex: number;
      redactedPrompt: string;
      expectedToolName: string;
      candidateToolName: string;
      matched: boolean;
      crc32Valid: boolean;
    }>;
  } {
    const traces = Array.isArray(params.traces) ? params.traces : [];
    if (traces.length === 0) {
      throw new Error("executeShadowWalRegressionSuite requires at least 1 production trace");
    }
    const maxRegressionRatePct = params.maxRegressionRatePct ?? 5.0;
    const maxCostIncreasePct = params.maxCostIncreasePct ?? 15.0;

    let crc32VerifiedCount = 0;
    let totalRedactions = 0;
    let matchedTracesCount = 0;
    let sumBaseLat = 0;
    let sumCandLat = 0;
    let totalBaselineTokens = 0;
    let totalCandidateTokens = 0;

    const traceResults = traces.map((tr) => {
      let crc32Valid = true;
      if (tr.walFrame?.frame && typeof tr.walFrame.frame.verifyIntegrity === "function") {
        crc32Valid = tr.walFrame.frame.verifyIntegrity();
        if (!crc32Valid) {
          throw new Error(
            `Shadow WAL replay aborted: corrupted WAL frame at task '${tr.taskId}' turn ${tr.turnIndex}`
          );
        }
      }
      crc32VerifiedCount += 1;

      const red = DeterministicTimeTravelReplayer.redactSensitivePromptTokens(tr.prompt);
      totalRedactions += red.redactionCount;

      const toolMatched = tr.candidateToolName === tr.expectedToolName;
      let pathMatched = true;
      if (tr.expectedPathPrefix) {
        const candPath = String(tr.candidateArgs?.path ?? tr.candidateArgs?.filePath ?? "");
        pathMatched = candPath.startsWith(tr.expectedPathPrefix);
      }
      const matched = toolMatched && pathMatched;
      if (matched) matchedTracesCount += 1;

      sumBaseLat += Math.max(1, tr.baselineLatencyMs);
      sumCandLat += Math.max(1, tr.candidateLatencyMs);
      totalBaselineTokens += Math.max(1, tr.baselineTokens);
      totalCandidateTokens += Math.max(1, tr.candidateTokens);

      return {
        taskId: tr.taskId,
        turnIndex: tr.turnIndex,
        redactedPrompt: red.redactedText,
        expectedToolName: tr.expectedToolName,
        candidateToolName: tr.candidateToolName,
        matched,
        crc32Valid,
      };
    });

    const regressionCount = traces.length - matchedTracesCount;
    const toolMatchAccuracyPct = Number(((matchedTracesCount / traces.length) * 100).toFixed(2));
    const avgBaselineLatencyMs = Number((sumBaseLat / traces.length).toFixed(2));
    const avgCandidateLatencyMs = Number((sumCandLat / traces.length).toFixed(2));
    const latencyDeltaPct = Number(
      (((avgCandidateLatencyMs - avgBaselineLatencyMs) / Math.max(1, avgBaselineLatencyMs)) * 100).toFixed(2)
    );
    const tokenCostDeltaPct = Number(
      (((totalCandidateTokens - totalBaselineTokens) / Math.max(1, totalBaselineTokens)) * 100).toFixed(2)
    );

    const minAccuracyPct = 100 - maxRegressionRatePct;
    const passedGate =
      toolMatchAccuracyPct >= minAccuracyPct && tokenCostDeltaPct <= maxCostIncreasePct;

    return {
      suiteId: params.suiteId,
      baselineModel: params.baselineModel,
      candidateModel: params.candidateModel,
      totalTraces: traces.length,
      crc32VerifiedCount,
      totalRedactions,
      matchedTracesCount,
      regressionCount,
      toolMatchAccuracyPct,
      avgBaselineLatencyMs,
      avgCandidateLatencyMs,
      latencyDeltaPct,
      totalBaselineTokens,
      totalCandidateTokens,
      tokenCostDeltaPct,
      passedGate,
      canaryRecommendation: passedGate ? "PROMOTE_CANARY" : "AUTO_ROLLBACK_ON_REGRESSION",
      traceResults,
    };
  }
}
