/**
 * @fileoverview M25 (TASK-117 & TASK-118): SLSA v1.0 Supply-Chain Provenance Attestor
 * (`SandboxProvenanceAttestor`) & Cryptographic Merge Gate Verifier.
 *
 * Generates tamper-evident in-toto / SLSA v1.0 provenance statements binding:
 * - Sandbox base checkpoint SHA-256 Merkle Root
 * - Monotonic TurnWAL hash chain digest
 * - `ZeroTrustSecurityGuard` cleanliness check (zero Cgroup freezes, zero unauthorized egress)
 * - HMAC-SHA256 / SPIFFE SVID cryptographic signature
 */

import * as crypto from "node:crypto";
import { ZeroTrustSecurityGuard } from "./zero_trust_security_guard";

export interface SlsaProvenanceAttestation {
  _type: "https://in-toto.io/Statement/v1";
  predicateType: "https://slsa.dev/provenance/v1";
  subject: Array<{
    name: string;
    digest: { sha256: string };
  }>;
  predicate: {
    builder: { id: string; spiffeId: string };
    sandboxId: string;
    branchId: string;
    baseCheckpointMerkleRoot: string;
    turnWalChainDigestSha256: string;
    zeroTrustClean: boolean;
    timestampMs: number;
  };
  signature: string;
}

export class SandboxProvenanceAttestor {
  private readonly signingKey: Buffer;

  constructor(
    public readonly builderSpiffeId: string = "spiffe://ansts.internal/ns/prod/sa/node-daemon-attestor",
    signingSecret: string = "ansts_slsa_v24_spiffe_svid_key",
  ) {
    this.signingKey = Buffer.from(signingSecret, "utf8");
  }

  public generateBranchAttestation(params: {
    sandboxId: string;
    branchId: string;
    artifactName: string;
    patchContent: string;
    baseCheckpointMerkleRoot: string;
    turnWalDiffSnapshots: string[];
    securityGuard: ZeroTrustSecurityGuard;
  }): SlsaProvenanceAttestation {
    const patchSha256 = crypto.createHash("sha256").update(params.patchContent, "utf8").digest("hex");
    const walChainSha256 = crypto
      .createHash("sha256")
      .update(params.turnWalDiffSnapshots.join("||"), "utf8")
      .digest("hex");

    const isFrozen = params.securityGuard.frozenSandboxes.has(params.sandboxId);
    const hasBlockedEgress = params.securityGuard.securityAuditLog.some(
      (entry) => entry.sandboxId === params.sandboxId && entry.event.startsWith("EGRESS_BLOCKED:"),
    );
    const zeroTrustClean = !isFrozen && !hasBlockedEgress;

    const unsignedStatement = {
      _type: "https://in-toto.io/Statement/v1" as const,
      predicateType: "https://slsa.dev/provenance/v1" as const,
      subject: [
        {
          name: params.artifactName,
          digest: { sha256: patchSha256 },
        },
      ],
      predicate: {
        builder: {
          id: "https://ansts.internal/builders/zero-trust-sandbox-v2.4",
          spiffeId: this.builderSpiffeId,
        },
        sandboxId: params.sandboxId,
        branchId: params.branchId,
        baseCheckpointMerkleRoot: params.baseCheckpointMerkleRoot,
        turnWalChainDigestSha256: walChainSha256,
        zeroTrustClean,
        timestampMs: Date.now(),
      },
    };

    const canonicalJson = JSON.stringify(unsignedStatement);
    const signature = crypto
      .createHmac("sha256", this.signingKey)
      .update(canonicalJson, "utf8")
      .digest("hex");

    return {
      ...unsignedStatement,
      signature,
    };
  }

  /**
   * TASK-118: Cryptographically verifies the SLSA v1.0 attestation signature, artifact SHA-256 digest,
   * and Zero-Trust cleanliness before allowing `GitConflictArbiterCell` to merge the patch.
   */
  public verifyAttestationForMergeGate(
    attestation: SlsaProvenanceAttestation,
    expectedPatchContent: string,
  ): { verified: boolean; reason?: string } {
    const { signature, ...unsignedPart } = attestation;
    const expectedSig = crypto
      .createHmac("sha256", this.signingKey)
      .update(JSON.stringify(unsignedPart), "utf8")
      .digest("hex");

    if (signature !== expectedSig) {
      return { verified: false, reason: "SLSA_SIGNATURE_TAMPERED" };
    }

    const actualPatchSha256 = crypto
      .createHash("sha256")
      .update(expectedPatchContent, "utf8")
      .digest("hex");
    if (attestation.subject[0]?.digest.sha256 !== actualPatchSha256) {
      return { verified: false, reason: "ARTIFACT_DIGEST_MISMATCH" };
    }

    if (!attestation.predicate.zeroTrustClean) {
      return {
        verified: false,
        reason: `SANDBOX_TAINTED_BY_SECURITY_VIOLATION:${attestation.predicate.sandboxId}`,
      };
    }

    return { verified: true };
  }
}
