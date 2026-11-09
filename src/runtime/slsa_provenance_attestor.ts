/**
 * @fileoverview M25 & M36 SLSA v1.0 In-Toto Build, Sandbox Image & LayerFS Commit Tree Provenance Attestor.
 */
import * as crypto from "node:crypto";
import { LayerFsWorkspaceDriver } from "./layerfs_workspace_driver.js";

export interface SlsaProvenanceStatement {
  _type: "https://in-toto.io/Statement/v1";
  predicateType: "https://slsa.dev/provenance/v1";
  subjectDigest: string;
  builderId: string;
  imageReference: string;
  commitId?: string;
  fileCount?: number;
  signatureHex: string;
}

export class SlsaProvenanceAttestor {
  private readonly signingKey: string;

  constructor(signingKey = "ansts-slsa-v1-signing-key") {
    this.signingKey = signingKey;
  }

  public createSignedProvenanceStatement(params: {
    imageReference: string;
    sha256Digest: string;
    builderId: string;
    commitId?: string;
    fileCount?: number;
  }): SlsaProvenanceStatement {
    const body = `${params.imageReference}:${params.sha256Digest}:${params.builderId}:${params.commitId ?? ""}`;
    const signatureHex = crypto.createHmac("sha256", this.signingKey).update(body, "utf-8").digest("hex");
    return {
      _type: "https://in-toto.io/Statement/v1",
      predicateType: "https://slsa.dev/provenance/v1",
      subjectDigest: params.sha256Digest,
      builderId: params.builderId,
      imageReference: params.imageReference,
      commitId: params.commitId,
      fileCount: params.fileCount,
      signatureHex,
    };
  }

  public verifyProvenanceStatement(stmt: SlsaProvenanceStatement): boolean {
    const body = `${stmt.imageReference}:${stmt.subjectDigest}:${stmt.builderId}:${stmt.commitId ?? ""}`;
    const expected = crypto.createHmac("sha256", this.signingKey).update(body, "utf-8").digest("hex");
    return expected === stmt.signatureHex;
  }

  public attestLayerFsCommit(
    driver: LayerFsWorkspaceDriver,
    branchOrCommitId: string,
    builderId = "ansts-layerfs-builder-v25"
  ): SlsaProvenanceStatement {
    let commit = driver.getCommit(branchOrCommitId);
    if (!commit) {
      commit = driver.getBranchHeadCommit(branchOrCommitId);
    }
    const sortedEntries = Array.from(commit.filesTree.entries()).sort((a, b) => a[0].localeCompare(b[0]));
    const merkleHasher = crypto.createHash("sha256");
    for (const [relPath, entry] of sortedEntries) {
      merkleHasher.update(`${relPath}:${entry.objectId}:${entry.mode}\n`, "utf-8");
    }
    const merkleDigest = merkleHasher.digest("hex");
    return this.createSignedProvenanceStatement({
      imageReference: `layerfs://${commit.branchName}@${commit.commitId}`,
      sha256Digest: merkleDigest,
      builderId,
      commitId: commit.commitId,
      fileCount: sortedEntries.length,
    });
  }
}
