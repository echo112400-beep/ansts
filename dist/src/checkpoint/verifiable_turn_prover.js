"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.VerifiableTurnExecutionProver = void 0;
const crypto = __importStar(require("node:crypto"));
class VerifiableTurnExecutionProver {
    proverKey;
    constructor(secret = "ansts_v28_zk_turn_prover_hmac_secret") {
        this.proverKey = Buffer.from(secret, "utf8");
    }
    generateTurnProof(frame, inputPromptText) {
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
    verifyTurnProof(proof) {
        const canonicalBody = `${proof.taskId}|${proof.turnSeq}|${proof.inputPromptMerkleRoot}|${proof.reasoningCotOutputHash}|${proof.toolFootprintDigest}|${proof.workspaceStateTransitionHash}`;
        const expectedSig = crypto.createHmac("sha256", this.proverKey).update(canonicalBody, "utf8").digest("hex");
        return proof.proverHmacSignature === expectedSig;
    }
}
exports.VerifiableTurnExecutionProver = VerifiableTurnExecutionProver;
