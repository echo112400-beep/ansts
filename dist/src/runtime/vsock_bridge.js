"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ZeroTrustVsockBridge = exports.EgressPolicyViolationError = void 0; /**
 * @fileoverview M4 TASK-15: Zero-Trust virtio-vsock Host-Guest RPC Channel & Egress Policy Filter.
 */







class EgressPolicyViolationError extends Error {
  constructor(domain) {
    super(`Zero-trust egress policy blocked connection to unauthorized domain '${domain}'`);
    this.name = "EgressPolicyViolationError";
  }
}exports.EgressPolicyViolationError = EgressPolicyViolationError;

class ZeroTrustVsockBridge {
  allowedDomains;

  constructor(allowedDomains = ["api.openai.com", "api.anthropic.com", "registry.npmjs.org"]) {
    this.allowedDomains = new Set(allowedDomains);
  }

  sendRequest(msg) {
    if (msg.method === "HTTP_EGRESS") {
      const urlStr = String(msg.payload.url ?? "");
      const parsed = new URL(urlStr);
      if (!this.allowedDomains.has(parsed.hostname)) {
        throw new EgressPolicyViolationError(parsed.hostname);
      }
      return { status: 200, hostname: parsed.hostname, proxiedViaVsock: true };
    }
    if (msg.method === "EXEC_CMD") {
      return {
        exitCode: 0,
        stdout: `Executed '${String(msg.payload.command ?? "")}' inside ${msg.vmId}`,
        stderr: ""
      };
    }
    return { status: "OK", vmId: msg.vmId, method: msg.method };
  }
}exports.ZeroTrustVsockBridge = ZeroTrustVsockBridge;