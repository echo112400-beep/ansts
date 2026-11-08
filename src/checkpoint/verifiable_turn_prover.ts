/**
 * @fileoverview Verifiable turn execution proofs bound to a fence and workspace hash.
 */
import * as crypto from "node:crypto";
import { TurnWalFrame } from "../models";
export interface VerifiableTurnProofStatement {
  proofId: string;
  taskId: string;
  turnSeq: number;
  inputPromptMerkleRoot: string;
  reasoningCotOutputHash: string;
  toolFootprintDigest: string;
  workspaceStateTransitionHash: string;
  proverHmacSignature: string;
  verifiedAtMs: number;
}
export class VerifiableTurnExecutionProver {
  private readonly proverKey: Buffer;
  constructor(secret = "ansts_v28_zk_turn_prover_hmac_secret") {
    this.proverKey = Buffer.from(secret, "utf8");
  }
  public generateTurnProof(frame: TurnWalFrame, inputPromptText: string): VerifiableTurnProofStatement {
    const inputPromptMerkleRoot = crypto.createHash("sha256").update(inputPromptText, "utf8").digest("hex");
    const reasoningCotOutputHash = crypto.createHash("sha256").update(frame.diffSnapshot ?? "", "utf8").digest("hex");
    const toolFootprintDigest = crypto.createHash("sha256").update(JSON.stringify(frame.contextMessages), "utf8").digest("hex");
    const workspaceStateTransitionHash = frame.workspaceMerkleHash;
    const canonicalBody = `${frame.taskId}|${frame.fence.turnSeq}|${inputPromptMerkleRoot}|${reasoningCotOutputHash}|${toolFootprintDigest}|${workspaceStateTransitionHash}`;
    const proverHmacSignature = crypto.createHmac("sha256", this.proverKey).update(canonicalBody, "utf8").digest("hex");
    return {
      proofId: `proof_${frame.taskId}_t${frame.fence.turnSeq}_${Date.now()}`,
      taskId: frame.taskId,
      turnSeq: frame.fence.turnSeq,
      inputPromptMerkleRoot,
      reasoningCotOutputHash,
      toolFootprintDigest,
      workspaceStateTransitionHash,
      proverHmacSignature,
      verifiedAtMs: Date.now(),
    };
  }
  public verifyTurnProof<T extends VerifiableTurnProofStatement>(proof: T): boolean {
    const canonicalBody = `${proof.taskId}|${proof.turnSeq}|${proof.inputPromptMerkleRoot}|${proof.reasoningCotOutputHash}|${proof.toolFootprintDigest}|${proof.workspaceStateTransitionHash}`;
    const expectedSig = crypto.createHmac("sha256", this.proverKey).update(canonicalBody, "utf8").digest("hex");
    return proof.proverHmacSignature === expectedSig;
  }
}
