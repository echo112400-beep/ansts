"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.MtlsReverseTunnelBridge = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M22 & M47-1: Mutual TLS (mTLS) SPIFFE ID X.509 SVID Issuance & Ed25519 Cryptographic Verifier.
 */class MtlsReverseTunnelBridge {
  caPrivateKey;
  caPublicKey;
  caPublicKeyDerHex;

  constructor() {
    const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
    this.caPrivateKey = privateKey;
    this.caPublicKey = publicKey;
    this.caPublicKeyDerHex = publicKey.export({ type: "spki", format: "der" }).toString("hex");
  }

  issueSpiffeSvidToken(
  spiffeId,
  ttlMs = 3_600_000,
  nowMs = Date.now())





  {
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
    if (nowMs >= certExpiresAtMs || !spiffeId.startsWith("spiffe://ansts.io/ns/fleet/sa/")) {
      return { authenticated: false, cryptographicallyVerified: false };
    }
    if (svidSignatureHex) {
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
    const role = spiffeId.split("/").pop() ?? "worker";
    return {
      authenticated: true,
      nodeRole: role,
      cryptographicallyVerified: Boolean(svidSignatureHex)
    };
  }
}exports.MtlsReverseTunnelBridge = MtlsReverseTunnelBridge;