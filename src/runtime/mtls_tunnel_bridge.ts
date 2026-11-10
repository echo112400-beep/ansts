/**
 * @fileoverview M22, M47-1 & M58-3: Mutual TLS (mTLS) SPIFFE ID X.509 SVID Issuance & Ed25519 Cryptographic Verifier
 * with Strict Single-Segment Service Account Validation & Empty-Signature Rejection.
 */
import * as crypto from "node:crypto";

export class MtlsReverseTunnelBridge {
  private readonly caPrivateKey: crypto.KeyObject;
  private readonly caPublicKey: crypto.KeyObject;
  public readonly caPublicKeyDerHex: string;

  constructor() {
    const { publicKey, privateKey } = crypto.generateKeyPairSync("ed25519");
    this.caPrivateKey = privateKey;
    this.caPublicKey = publicKey;
    this.caPublicKeyDerHex = publicKey.export({ type: "spki", format: "der" }).toString("hex");
  }

  private static extractValidSpiffeRole(spiffeId: string): string | null {
    const prefix = "spiffe://ansts.io/ns/fleet/sa/";
    if (!spiffeId.startsWith(prefix)) return null;
    const role = spiffeId.slice(prefix.length);
    if (!role || !/^[a-zA-Z0-9._-]+$/.test(role) || role === "." || role === "..") {
      return null;
    }
    return role;
  }

  public issueSpiffeSvidToken(
    spiffeId: string,
    ttlMs = 3_600_000,
    nowMs = Date.now()
  ): {
    spiffeId: string;
    certExpiresAtMs: number;
    svidSignatureHex: string;
    caPublicKeyHex: string;
  } {
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

  public verifySpiffeCertificate(
    spiffeId: string,
    certExpiresAtMs: number,
    nowMs = Date.now(),
    svidSignatureHex?: string
  ): {
    authenticated: boolean;
    nodeRole?: string;
    cryptographicallyVerified?: boolean;
  } {
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
      cryptographicallyVerified: Boolean(svidSignatureHex),
    };
  }
}
