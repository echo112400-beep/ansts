"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.MtlsReverseTunnelBridge = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M22, M47-1 & M58-3: Mutual TLS (mTLS) SPIFFE ID X.509 SVID Issuance & Ed25519 Cryptographic Verifier
 * with Strict Single-Segment Service Account Validation & Empty-Signature Rejection.
 */class MtlsReverseTunnelBridge {caPrivateKey;
  caPublicKey;
  caPublicKeyDerHex;

  constructor() {
    const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
    this.caPrivateKey = privateKey;
    this.caPublicKey = publicKey;
    this.caPublicKeyDerHex = publicKey.export({ type: "spki", format: "der" }).toString("hex");
  }

  static extractValidSpiffeRole(spiffeId) {
    const prefix = "spiffe://ansts.io/ns/fleet/sa/";
    if (!spiffeId.startsWith(prefix)) return null;
    const role = spiffeId.slice(prefix.length);
    if (!role || !/^[a-zA-Z0-9._-]+$/.test(role) || role === "." || role === "..") {
      return null;
    }
    return role;
  }

  issueSpiffeSvidToken(
  spiffeId,
  ttlMs = 3_600_000,
  nowMs = Date.now())





  {
    if (!MtlsReverseTunnelBridge.extractValidSpiffeRole(spiffeId)) {
      throw new Error(`Invalid SPIFFE ID format: '${spiffeId}'`);
    }
    if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
      throw new Error(`SPIFFE SVID ttlMs must be > 0 (got ${ttlMs})`);
    }
    const certExpiresAtMs = nowMs + ttlMs;
    const payloadBuf = Buffer.from(`${spiffeId}:${certExpiresAtMs}`, "utf-8");
    const svidSignatureHex = crypto.sign(null, payloadBuf, this.caPrivateKey).toString("hex");
    return {
      spiffeId,
      certExpiresAtMs,
      svidSignatureHex,
      caPublicKeyHex: this.caPublicKeyDerHex
    };
  }

  verifySpiffeCertificate(
  spiffeId,
  certExpiresAtMs,
  nowMs = Date.now(),
  svidSignatureHex)




  {
    const role = MtlsReverseTunnelBridge.extractValidSpiffeRole(spiffeId);
    if (!role || !Number.isFinite(certExpiresAtMs) || nowMs >= certExpiresAtMs) {
      return { authenticated: false, cryptographicallyVerified: false };
    }
    // [M58-3] If svidSignatureHex is passed (even as an empty string ""), it MUST be non-empty and cryptographically valid
    if (svidSignatureHex !== undefined) {
      if (svidSignatureHex.trim().length === 0) {
        return { authenticated: false, cryptographicallyVerified: false };
      }
      try {
        const payloadBuf = Buffer.from(`${spiffeId}:${certExpiresAtMs}`, "utf-8");
        const sigBuf = Buffer.from(svidSignatureHex, "hex");
        const ok = crypto.verify(null, payloadBuf, this.caPublicKey, sigBuf);
        if (!ok) {
          return { authenticated: false, cryptographicallyVerified: false };
        }
      } catch {
        return { authenticated: false, cryptographicallyVerified: false };
      }
    }
    return {
      authenticated: true,
      nodeRole: role,
      cryptographicallyVerified: Boolean(svidSignatureHex)
    };
  }
}exports.MtlsReverseTunnelBridge = MtlsReverseTunnelBridge;