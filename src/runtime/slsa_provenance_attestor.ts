/**
 * @fileoverview M25 SLSA v1.0 In-Toto Build & Sandbox Image Provenance Attestor.
 */
import * as crypto from "node:crypto";

export class SlsaProvenanceAttestor {
  private readonly signingKey: string;

  constructor(signingKey = "ansts-slsa-v1-signing-key") {
    this.signingKey = signingKey;
  }

  public createSignedProvenanceStatement(params: {
    imageReference: string;
    sha256Digest: string;
    builderId: string;
  }): {
    predicateType: "https://slsa.dev/provenance/v1";
    subjectDigest: string;
    signatureHex: string;
  } {
    const body = `${params.imageReference}:${params.sha256Digest}:${params.builderId}`;
    const signatureHex = crypto.createHmac("sha256", this.signingKey).update(body, "utf-8").digest("hex");
    return {
      predicateType: "https://slsa.dev/provenance/v1",
      subjectDigest: params.sha256Digest,
      signatureHex,
    };
  }
}
