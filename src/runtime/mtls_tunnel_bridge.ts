/**
 * @fileoverview Mutual TLS (mTLS) SPIFFE ID Certificate Verifier for Edge-to-Gateway Tunnels.
 */
export class MtlsReverseTunnelBridge {
  public verifySpiffeCertificate(spiffeId: string, certExpiresAtMs: number, nowMs = Date.now()): {
    authenticated: boolean;
    nodeRole?: string;
  } {
    if (nowMs >= certExpiresAtMs || !spiffeId.startsWith("spiffe://ansts.io/ns/fleet/sa/")) {
      return { authenticated: false };
    }
    const role = spiffeId.split("/").pop() ?? "worker";
    return { authenticated: true, nodeRole: role };
  }
}
