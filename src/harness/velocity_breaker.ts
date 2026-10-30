/**
 * TASK-23 & Design Doc Section 7.3 Tier-1 Complete Suite:
 * 1. AST Identical Call Breaker (3x) AND A -> B -> A -> B Cyclic Oscillation Detector
 * 2. Consecutive Tool Error Breaker (>= 5 consecutive non-zero exits)
 * 3. Token Burn-Rate Spike (> 35% of maxTotalTokens in 1 turn) & Degenerate Repetition Breaker
 * 4. Sandbox Abuse & Command Deny Breaker (deny_commands, max_pids -> cgroup.freeze = 1)
 */

import * as crypto from "crypto";
import { AgentTask, TaskState } from "../models";

export class SyntacticAndVelocityBreaker {
  private callSignatures: Map<string, string[]> = new Map();
  private interventionsCount: Map<string, number> = new Map();
  private consecutiveToolErrors: Map<string, number> = new Map();

  constructor(
    public readonly astCallLimit: number = 3,
    public readonly maxConsecutiveErrors: number = 5,
    public readonly singleTurnBurnSpikeRatio: number = 0.35,
  ) {}

  public normalizeCallSignature(toolName: string, rawExpr: string): string {
    const canonical = rawExpr.replace(/\s+/g, "");
    return crypto.createHash("sha256").update(`${toolName}:${canonical}`, "utf8").digest("hex").slice(0, 16);
  }

  public recordAndCheckToolCall(
    taskId: string,
    toolName: string,
    expr: string,
  ): "OK" | "REFLEXION_INTERVENTION" | "CIRCUIT_BROKEN" {
    const sig = this.normalizeCallSignature(toolName, expr);
    const hist = this.callSignatures.get(taskId) ?? [];
    hist.push(sig);
    this.callSignatures.set(taskId, hist);

    // Check 1A: N consecutive identical normalized AST calls
    const isIdenticalStreak =
      hist.length >= this.astCallLimit &&
      new Set(hist.slice(-this.astCallLimit)).size === 1;

    // Check 1B: A -> B -> A -> B periodic 2-step oscillation
    const isCyclicOscillation =
      hist.length >= 4 &&
      hist[hist.length - 4] === hist[hist.length - 2] &&
      hist[hist.length - 3] === hist[hist.length - 1] &&
      hist[hist.length - 2] !== hist[hist.length - 1];

    if (isIdenticalStreak || isCyclicOscillation) {
      const count = (this.interventionsCount.get(taskId) ?? 0) + 1;
      this.interventionsCount.set(taskId, count);
      return count === 1 ? "REFLEXION_INTERVENTION" : "CIRCUIT_BROKEN";
    }
    return "OK";
  }

  public recordToolExitStatus(task: AgentTask, exitCode: number): "OK" | "CIRCUIT_BROKEN" {
    if (exitCode === 0) {
      this.consecutiveToolErrors.set(task.taskId, 0);
      return "OK";
    }
    const streak = (this.consecutiveToolErrors.get(task.taskId) ?? 0) + 1;
    this.consecutiveToolErrors.set(task.taskId, streak);
    if (streak >= this.maxConsecutiveErrors) {
      task.state = TaskState.CIRCUIT_BROKEN;
      return "CIRCUIT_BROKEN";
    }
    return "OK";
  }

  public evaluateBurnRateSpikeAndRepetition(
    task: AgentTask,
    turnTokensUsed: number,
    outputSample: string = "",
  ): { tripped: boolean; action: string; newModel?: string; newTemperature?: number } {
    const spikeRatio = turnTokensUsed / Math.max(1, task.maxTotalTokens);
    // Detect degenerate repetition (e.g., same 12-char substring repeated >= 6 times)
    const hasDegenerateRepeat = /(.{10,})\1{5,}/.test(outputSample);

    if (spikeRatio > this.singleTurnBurnSpikeRatio || hasDegenerateRepeat) {
      task.temperature = Math.max(0.0, Number((task.temperature * 0.5).toFixed(2)));
      const fallback = task.fallbackModels[0] ?? "Qwen-3.8";
      task.assignedModelPool = fallback;
      return {
        tripped: true,
        action: "ROLLBACK_TURN_AND_SWITCH_FALLBACK",
        newModel: fallback,
        newTemperature: task.temperature,
      };
    }
    return { tripped: false, action: "OK" };
  }

  public enforceSandboxAbuseAndCommandGuard(
    task: AgentTask,
    commandStr: string,
    activePids: number = 10,
  ): { allowed: boolean; cgroupFrozen: boolean; reason: string } {
    for (const denied of task.denyCommands) {
      if (commandStr.includes(denied)) {
        task.cgroupFrozen = true;
        task.state = TaskState.CIRCUIT_BROKEN;
        return {
          allowed: false,
          cgroupFrozen: true,
          reason: `Denied command '${denied}' triggered Cgroup freeze`,
        };
      }
    }
    if (activePids >= task.maxPids) {
      task.cgroupFrozen = true;
      task.state = TaskState.CIRCUIT_BROKEN;
      return {
        allowed: false,
        cgroupFrozen: true,
        reason: `PID count ${activePids} >= max_pids ${task.maxPids} triggered Cgroup freeze`,
      };
    }
    return { allowed: true, cgroupFrozen: false, reason: "OK" };
  }
}
