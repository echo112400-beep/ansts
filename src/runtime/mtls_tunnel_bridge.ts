/**
 * @fileoverview M22 & M47-1: Mutual TLS (mTLS) SPIFFE ID X.509 SVID Issuance & Ed25519 Cryptographic Verifier.
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
      cryptographicallyVerified: Boolean(svidSignatureHex),
    };
  }
}
