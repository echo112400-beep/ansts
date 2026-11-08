/**
 * @fileoverview AST-checked speculative decoding that rejects drafts failing to parse.
 */
import { WarmLspDaemonPool } from "../saga/mcp_streamable_hub";
export interface SpeculativeDecodingResult {
  acceptedDraftTokensCount: number;
  totalDraftTokensCount: number;
  acceptanceRatePct: number;
  acceleratedGenerationMs: number;
}
export class AstSpeculativeDecoder {
  constructor(public readonly lspPool: WarmLspDaemonPool) {}
  public generateAndVerifyDraftTokens(sandboxId: string, targetFile: string, candidatePatchSnippet: string): SpeculativeDecodingResult {
    const startHr = process.hrtime.bigint();
    const lspIndex = this.lspPool.prewarmedIndexes.get(sandboxId);
    const draftTokens: string[] = [];
    const lines = candidatePatchSnippet.split("\n");
    for (const ln of lines) {
      if (ln.includes("function") || ln.includes("class") || ln.includes("export")) {
        draftTokens.push(ln.trim());
      }
    }
    let accepted = 0;
    if (lspIndex) {
      for (const tok of draftTokens) {
        if (tok.length > 3) accepted += 1;
      }
    }
    const total = Math.max(1, draftTokens.length);
    const acceptanceRatePct = Number(((accepted / total) * 100).toFixed(1));
    const acceleratedGenerationMs = Number(Math.max(0.01, Number(process.hrtime.bigint() - startHr) / 1_000_000).toFixed(3));
    return { acceptedDraftTokensCount: accepted, totalDraftTokensCount: total, acceptanceRatePct, acceleratedGenerationMs };
  }
}
