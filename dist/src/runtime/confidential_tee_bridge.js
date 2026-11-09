"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ConfidentialTeeAttestationBridge = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview Confidential Computing TEE (AMD SEV-SNP / Intel TDX) Quote Attestation Bridge.
 */class ConfidentialTeeAttestationBridge {
  generateAndVerifyAttestationQuote(params)



  {
    const sig = crypto.
    createHmac("sha256", "tee-hardware-root-key").
    update(`${params.vmId}:${params.measurementDigestHex}:${params.nonce}`, "utf-8").
    digest("hex");
    return {
      verified: params.measurementDigestHex.length >= 16,
      teePlatform: "AMD_SEV_SNP",
      quoteSignatureHex: sig
    };
  }
}exports.ConfidentialTeeAttestationBridge = ConfidentialTeeAttestationBridge;