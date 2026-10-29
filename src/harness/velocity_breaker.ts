/**
 * TASK-23: Syntactic Normalized Tool Call Loop Breaker and Progress Velocity Guard.
 */

import * as crypto from "crypto";

export class SyntacticAndVelocityBreaker {
  private callSignatures: Map<string, string[]> = new Map();
  private interventionsCount: Map<string, number> = new Map();

  constructor(public readonly astCallLimit: number = 3) {}

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

    if (hist.length >= this.astCallLimit) {
      const tail = hist.slice(-this.astCallLimit);
      if (new Set(tail).size === 1) {
        const count = (this.interventionsCount.get(taskId) ?? 0) + 1;
        this.interventionsCount.set(taskId, count);
        return count === 1 ? "REFLEXION_INTERVENTION" : "CIRCUIT_BROKEN";
      }
    }
    return "OK";
  }
}
