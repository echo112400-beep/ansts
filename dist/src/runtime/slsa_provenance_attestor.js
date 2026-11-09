"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SlsaProvenanceAttestor = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M25 SLSA v1.0 In-Toto Build & Sandbox Image Provenance Attestor.
 */class SlsaProvenanceAttestor {
  signingKey;

  constructor(signingKey = "ansts-slsa-v1-signing-key") {
    this.signingKey = signingKey;
  }

  createSignedProvenanceStatement(params)







  {
    const body = `${params.imageReference}:${params.sha256Digest}:${params.builderId}`;
    const signatureHex = crypto.createHmac("sha256", this.signingKey).update(body, "utf-8").digest("hex");
    return {
      predicateType: "https://slsa.dev/provenance/v1",
      subjectDigest: params.sha256Digest,
      signatureHex
    };
  }
}exports.SlsaProvenanceAttestor = SlsaProvenanceAttestor;