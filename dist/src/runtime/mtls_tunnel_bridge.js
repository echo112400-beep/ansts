"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.MtlsReverseTunnelBridge = void 0; /**
 * @fileoverview Mutual TLS (mTLS) SPIFFE ID Certificate Verifier for Edge-to-Gateway Tunnels.
 */
class MtlsReverseTunnelBridge {
  verifySpiffeCertificate(spiffeId, certExpiresAtMs, nowMs = Date.now())


  {
    if (nowMs >= certExpiresAtMs || !spiffeId.startsWith("spiffe://ansts.io/ns/fleet/sa/")) {
      return { authenticated: false };
    }
    const role = spiffeId.split("/").pop() ?? "worker";
    return { authenticated: true, nodeRole: role };
  }
}exports.MtlsReverseTunnelBridge = MtlsReverseTunnelBridge;