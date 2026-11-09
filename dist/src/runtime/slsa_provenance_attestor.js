"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SlsaProvenanceAttestor = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M25 & M36 SLSA v1.0 In-Toto Build, Sandbox Image & LayerFS Commit Tree Provenance Attestor.
 */











class SlsaProvenanceAttestor {
  signingKey;

  constructor(signingKey = "ansts-slsa-v1-signing-key") {
    this.signingKey = signingKey;
  }

  createSignedProvenanceStatement(params)





  {
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
      signatureHex
    };
  }

  verifyProvenanceStatement(stmt) {
    const body = `${stmt.imageReference}:${stmt.subjectDigest}:${stmt.builderId}:${stmt.commitId ?? ""}`;
    const expected = crypto.createHmac("sha256", this.signingKey).update(body, "utf-8").digest("hex");
    return expected === stmt.signatureHex;
  }

  attestLayerFsCommit(
  driver,
  branchOrCommitId,
  builderId = "ansts-layerfs-builder-v25")
  {
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
      fileCount: sortedEntries.length
    });
  }
}exports.SlsaProvenanceAttestor = SlsaProvenanceAttestor;