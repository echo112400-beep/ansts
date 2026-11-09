"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ZeroTrustSandboxSecurityGuard = void 0;


var _ebpf_lsm_bridge = require("./ebpf_lsm_bridge.js");
var _vsock_bridge = require("./vsock_bridge.js"); /**
 * @fileoverview M22 Zero-Trust Sandbox Security Guard (Seccomp-BPF, Landlock & Egress Policy).
 */class ZeroTrustSandboxSecurityGuard {
  lsmBridge = new _ebpf_lsm_bridge.EbpfLsmSecurityBridge();
  vsockBridge = new _vsock_bridge.ZeroTrustVsockBridge();

  evaluateAction(params) {
    if (params.filePath) {
      const check = this.lsmBridge.checkFileOpenHook(params.filePath);
      if (!check.allowed) return { allowed: false };
    }
    if (params.egressUrl) {
      try {
        this.vsockBridge.sendRequest({
          requestId: "sec-check",
          vmId: "vm-0",
          method: "HTTP_EGRESS",
          payload: { url: params.egressUrl }
        });
      } catch {
        return { allowed: false };
      }
    }
    return { allowed: true };
  }
}exports.ZeroTrustSandboxSecurityGuard = ZeroTrustSandboxSecurityGuard;