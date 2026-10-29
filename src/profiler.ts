/**
 * TASK-05: Online Conjugate Bayesian Task Profiler with Log-Normal/Beta Updates & JSON Persistence.
 */

import * as crypto from "crypto";
import * as fs from "fs";
import { AgentTask } from "./models";

export interface FingerprintStatsData {
  sampleCount: number;
  muLogTokens: number;
  sigmaLogTokens: number;
  alphaCog: number;
  betaExec: number;
}

export class FingerprintStats implements FingerprintStatsData {
  public sampleCount: number;
  public muLogTokens: number;
  public sigmaLogTokens: number;
  public alphaCog: number;
  public betaExec: number;

  constructor(init?: Partial<FingerprintStatsData>) {
    this.sampleCount = init?.sampleCount ?? 0;
    this.muLogTokens = init?.muLogTokens ?? Math.log(140000.0);
    this.sigmaLogTokens = init?.sigmaLogTokens ?? 0.45;
    this.alphaCog = init?.alphaCog ?? 15.0;
    this.betaExec = init?.betaExec ?? 5.0;
  }

  public get p50Tokens(): number {
    return Math.round(Math.exp(this.muLogTokens));
  }

  public get p85Tokens(): number {
    return Math.round(Math.exp(this.muLogTokens + 1.0364 * this.sigmaLogTokens));
  }

  public get rhoCog(): number {
    return Number((this.alphaCog / Math.max(1e-6, this.alphaCog + this.betaExec)).toFixed(4));
  }
}

export class BayesianTaskProfiler {
  private profiles: Map<string, FingerprintStats> = new Map();

  constructor(public readonly coldStartMinSamples: number = 5) {}

  public fingerprint(task: AgentTask): string {
    const raw = `${task.tenantId}:${task.primaryModel}:${task.isolationDriver}:${task.warmPoolId}`;
    return crypto.createHash("sha256").update(raw, "utf8").digest("hex").slice(0, 16);
  }

  public assignInitialP50Reservation(task: AgentTask): number {
    const fp = this.fingerprint(task);
    let stats = this.profiles.get(fp);
    if (!stats) {
      stats = new FingerprintStats();
      this.profiles.set(fp, stats);
    }
    const softTokens =
      stats.sampleCount < this.coldStartMinSamples
        ? Math.min(task.maxTotalTokens, Math.max(stats.p50Tokens, Math.floor(task.maxTotalTokens / 2)))
        : Math.min(task.maxTotalTokens, stats.p50Tokens);
    task.softReservedTokens = softTokens;
    return softTokens;
  }

  public checkAndRenewLease(task: AgentTask): boolean {
    if (task.usedTokens >= Math.floor(0.85 * task.softReservedTokens)) {
      if (task.softReservedTokens < task.maxTotalTokens) {
        const fp = this.fingerprint(task);
        const stats = this.profiles.get(fp) ?? new FingerprintStats();
        const increment = Math.max(50000, stats.p85Tokens - stats.p50Tokens);
        task.softReservedTokens = Math.min(task.maxTotalTokens, task.softReservedTokens + increment);
        return true;
      }
    }
    return false;
  }

  public recordCompletion(task: AgentTask, observedRhoCog: number = 0.75): FingerprintStats {
    const fp = this.fingerprint(task);
    let stats = this.profiles.get(fp);
    if (!stats) {
      stats = new FingerprintStats();
      this.profiles.set(fp, stats);
    }
    const obsTokens = Math.max(1000, task.usedTokens);
    const logX = Math.log(obsTokens);

    const priorWeight = Math.max(1, stats.sampleCount);
    const newMu = (stats.muLogTokens * priorWeight + logX) / (priorWeight + 1);
    const varPrev = stats.sigmaLogTokens ** 2;
    const newVar = (priorWeight * varPrev + (logX - newMu) * (logX - stats.muLogTokens)) / (priorWeight + 1);
    stats.muLogTokens = newMu;
    stats.sigmaLogTokens = Math.max(0.1, Math.sqrt(Math.max(0.01, newVar)));

    const clampedRho = Math.min(0.98, Math.max(0.02, observedRhoCog));
    stats.alphaCog += clampedRho * 4.0;
    stats.betaExec += (1.0 - clampedRho) * 4.0;
    stats.sampleCount += 1;
    return stats;
  }

  public saveToFile(filePath: string): void {
    const obj: Record<string, FingerprintStatsData> = {};
    for (const [k, v] of this.profiles.entries()) {
      obj[k] = {
        sampleCount: v.sampleCount,
        muLogTokens: v.muLogTokens,
        sigmaLogTokens: v.sigmaLogTokens,
        alphaCog: v.alphaCog,
        betaExec: v.betaExec,
      };
    }
    fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), "utf8");
  }

  public loadFromFile(filePath: string): void {
    const raw = JSON.parse(fs.readFileSync(filePath, "utf8")) as Record<string, FingerprintStatsData>;
    this.profiles.clear();
    for (const [k, v] of Object.entries(raw)) {
      this.profiles.set(k, new FingerprintStats(v));
    }
  }
}
