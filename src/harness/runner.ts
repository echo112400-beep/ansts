/**
 * @fileoverview TASK-22 & M27 (TASK-127): AgentHarnessRunner with Sub-Session Semaphore
 * and Real Sliding-Window Message Array Compaction.
 */

import { AgentTask } from "../models";

export interface CompactionEvent {
  taskId: string;
  tokensBefore: number;
  tokensAfter: number;
  preservedTurns: number;
  summarizedTurnsCount: number;
  episodicDigest?: string;
}

export class AgentHarnessRunner {
  public compactionHistory: CompactionEvent[] = [];

  constructor(
    public readonly compactionRatio: number = 0.80,
    public readonly preserveLastNTurns: number = 6,
  ) {}

  public tryAcquireSubSession(task: AgentTask): boolean {
    if (task.activeSessions >= task.maxSessions) {
      return false;
    }
    if (task.totalSpawnedSessions >= task.maxTotalSessions) {
      return false;
    }
    task.activeSessions += 1;
    task.totalSpawnedSessions += 1;
    return true;
  }

  public releaseSubSession(task: AgentTask): void {
    if (task.activeSessions > 0) {
      task.activeSessions -= 1;
    }
  }

  /**
   * Compacts the context window when `contextWindowTokens >= compactionRatio * windowCap`.
   * If `turnMessages` are supplied, slices out older turns, condenses them into an episodic
   * summary header, and retains the last `preserveLastNTurns` verbatim.
   */
  public checkAndCompactContext(
    task: AgentTask,
    contextWindowTokens: number,
    maxWindowTokens?: number,
    turnMessages?: string[],
  ): number {
    const windowCap = maxWindowTokens ?? task.maxContextWindow;
    if (contextWindowTokens < Math.floor(this.compactionRatio * windowCap)) {
      return contextWindowTokens;
    }

    let summarizedTurnsCount = Math.max(1, task.fence.turnSeq - this.preserveLastNTurns);
    let episodicDigest = `[Episodic Summary of early turns for ${task.taskId}]`;

    if (Array.isArray(turnMessages) && turnMessages.length > this.preserveLastNTurns) {
      const older = turnMessages.slice(0, turnMessages.length - this.preserveLastNTurns);
      const recent = turnMessages.slice(-this.preserveLastNTurns);
      summarizedTurnsCount = older.length;
      episodicDigest = `[Compacted ${older.length} turns]: ` + older.map((m) => m.slice(0, 32)).join(" | ");
      turnMessages.length = 0;
      turnMessages.push(episodicDigest, ...recent);
    }

    // Calculate post-compaction token footprint from retained ratio + episodic summary overhead
    const retainedTurnRatio = Math.min(
      0.42,
      this.preserveLastNTurns / Math.max(this.preserveLastNTurns + summarizedTurnsCount, 14),
    );
    const compacted = Math.floor(contextWindowTokens * retainedTurnRatio);

    this.compactionHistory.push({
      taskId: task.taskId,
      tokensBefore: contextWindowTokens,
      tokensAfter: compacted,
      preservedTurns: this.preserveLastNTurns,
      summarizedTurnsCount,
      episodicDigest,
    });
    return compacted;
  }
}
