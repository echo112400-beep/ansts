"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SlsaProvenanceAttestor = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M25, M36 & M58-3: SLSA v1.0 In-Toto Build, Sandbox Image & LayerFS Commit Tree Provenance Attestor.
 * Binds `fileCount` into the signed canonical statement body and verifies both HMAC-SHA256 and Ed25519 signatures.
 */












class SlsaProvenanceAttestor {
  signingKey;
  privateKey;
  publicKey;
  publicKeyDerHex;

  constructor(signingKey = "ansts-slsa-v1-signing-key") {
    this.signingKey = signingKey;
    const kp = crypto.generateKeyPairSync("ed25519");
    this.privateKey = kp.privateKey;
    this.publicKey = kp.publicKey;
    this.publicKeyDerHex = kp.publicKey.export({ type: "spki", format: "der" }).toString("hex");
  }

  static buildCanonicalBody(params)





  {
    return `${params.imageReference}:${params.subjectDigest}:${params.builderId}:${params.commitId ?? ""}:${params.fileCount !== undefined ? String(params.fileCount) : ""}`;
  }

  createSignedProvenanceStatement(params)





  {
    const body = SlsaProvenanceAttestor.buildCanonicalBody({
      imageReference: params.imageReference,
      subjectDigest: params.sha256Digest,
      builderId: params.builderId,
      commitId: params.commitId,
      fileCount: params.fileCount
    });
    const signatureHex = crypto.createHmac("sha256", this.signingKey).update(body, "utf-8").digest("hex");
    const ed25519SignatureHex = crypto.sign(null, Buffer.from(body, "utf-8"), this.privateKey).toString("hex");
    return {
      _type: "https://in-toto.io/Statement/v1",
      predicateType: "https://slsa.dev/provenance/v1",
      subjectDigest: params.sha256Digest,
      builderId: params.builderId,
      imageReference: params.imageReference,
      commitId: params.commitId,
      fileCount: params.fileCount,
      signatureHex,
      ed25519SignatureHex,
      publicKeyDerHex: this.publicKeyDerHex
    };
  }

  verifyProvenanceStatement(stmt) {
    const body = SlsaProvenanceAttestor.buildCanonicalBody({
      imageReference: stmt.imageReference,
      subjectDigest: stmt.subjectDigest,
      builderId: stmt.builderId,
      commitId: stmt.commitId,
      fileCount: stmt.fileCount
    });
    const expected = crypto.createHmac("sha256", this.signingKey).update(body, "utf-8").digest("hex");
    if (expected !== stmt.signatureHex) {
      return false;
    }
    if (stmt.ed25519SignatureHex !== undefined) {
      if (stmt.ed25519SignatureHex.trim().length === 0) {
        return false;
      }
      try {
        const ok = crypto.verify(
        null,
        Buffer.from(body, "utf-8"),
        this.publicKey,
        Buffer.from(stmt.ed25519SignatureHex, "hex"));

        if (!ok) return false;
      } catch {
        return false;
      }
    }
    return true;
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