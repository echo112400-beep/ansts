/**
 * TASK-20: Linux PSI Memory Pressure Monitor and Proactive Low-Priority Hibernation Guard.
 */

import { AgentTask, SandboxPhase, TaskState } from "../models";

export class LinuxPsiMemoryGuard {
  public currentPsiSomeAvg10: number = 10.0;

  constructor(public readonly pressureThresholdPct: number = 40.0) {}

  public evaluateAndHibernate(psiPct: number, tasks: AgentTask[]): AgentTask[] {
    this.currentPsiSomeAvg10 = psiPct;
    const hibernated: AgentTask[] = [];
    if (psiPct <= this.pressureThresholdPct) {
      return hibernated;
    }
    const candidates = tasks.filter(
      (t) => t.state === TaskState.RUNNING && t.phase === SandboxPhase.COGNITIVE,
    );
    candidates.sort((a, b) => a.baseScore - b.baseScore);
    if (candidates.length > 0) {
      const victim = candidates[0];
      victim.state = TaskState.HIBERNATED;
      victim.phase = SandboxPhase.IDLE;
      victim.activeSessions = 0;
      hibernated.push(victim);
      this.currentPsiSomeAvg10 = Math.max(15.0, psiPct - 20.0);
    }
    return hibernated;
  }
}
