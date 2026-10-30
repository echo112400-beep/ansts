/**
 * TASK-22: AgentHarnessRunner with Sub-Session Semaphore (Concurrent & Cumulative Total Caps)
 * and 80% Watermark Sliding Context Compaction.
 */

import { AgentTask } from "../models";

export interface CompactionEvent {
  taskId: string;
  tokensBefore: number;
  tokensAfter: number;
  preservedTurns: number;
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

  public checkAndCompactContext(
    task: AgentTask,
    contextWindowTokens: number,
    maxWindowTokens?: number,
  ): number {
    const windowCap = maxWindowTokens ?? task.maxContextWindow;
    if (contextWindowTokens >= Math.floor(this.compactionRatio * windowCap)) {
      const compacted = Math.floor(contextWindowTokens * 0.42);
      this.compactionHistory.push({
        taskId: task.taskId,
        tokensBefore: contextWindowTokens,
        tokensAfter: compacted,
        preservedTurns: this.preserveLastNTurns,
      });
      return compacted;
    }
    return contextWindowTokens;
  }
}
