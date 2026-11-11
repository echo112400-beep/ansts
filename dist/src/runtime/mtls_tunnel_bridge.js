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
exports.MtlsReverseTunnelBridge = void 0;
/**
 * @fileoverview M22, M47-1 & M58-3: Mutual TLS (mTLS) SPIFFE ID X.509 SVID Issuance & Ed25519 Cryptographic Verifier
 * with Strict Single-Segment Service Account Validation & Empty-Signature Rejection.
 */
const crypto = __importStar(require("node:crypto"));
class MtlsReverseTunnelBridge {
    caPrivateKey;
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
        if (!spiffeId.startsWith(prefix))
            return null;
        const role = spiffeId.slice(prefix.length);
        if (!role || !/^[a-zA-Z0-9._-]+$/.test(role) || role === "." || role === "..") {
            return null;
        }
        return role;
    }
    issueSpiffeSvidToken(spiffeId, ttlMs = 3_600_000, nowMs = Date.now()) {
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
            caPublicKeyHex: this.caPublicKeyDerHex,
        };
    }
    verifySpiffeCertificate(spiffeId, certExpiresAtMs, nowMs = Date.now(), svidSignatureHex) {
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
            }
            catch {
                return { authenticated: false, cryptographicallyVerified: false };
            }
        }
        return {
            authenticated: true,
            nodeRole: role,
            cryptographicallyVerified: Boolean(svidSignatureHex),
        };
    }
}
exports.MtlsReverseTunnelBridge = MtlsReverseTunnelBridge;
