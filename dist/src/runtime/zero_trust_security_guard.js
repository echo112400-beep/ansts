"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ZeroTrustSandboxSecurityGuard = void 0;


var _ebpf_lsm_bridge = require("./ebpf_lsm_bridge.js");
var _vsock_bridge = require("./vsock_bridge.js"); /**
 * @fileoverview M22 & M36 Zero-Trust Sandbox Security Guard (Seccomp-BPF, Landlock, Shell Command & Egress Policy).
 */class ZeroTrustSandboxSecurityGuard {
  lsmBridge = new _ebpf_lsm_bridge.EbpfLsmSecurityBridge();
  vsockBridge = new _vsock_bridge.ZeroTrustVsockBridge();

  blockedCommandPatterns = [
  { pattern: /rm\s+-rf\s+\/(?:\s|$)/i, reason: "ROOT_FS_DESTRUCTION_BLOCKED" },
  { pattern: /:\(\)\s*\{\s*:\|:&\s*\};:/, reason: "FORK_BOMB_BLOCKED" },
  { pattern: /\/etc\/shadow|\/proc\/kcore/, reason: "SENSITIVE_KERNEL_FILE_ACCESS_BLOCKED" },
  { pattern: /\/dev\/tcp\//, reason: "RAW_TCP_REVERSE_SHELL_BLOCKED" }];


  evaluateAction(params)



  {
    if (params.filePath) {
      const check = this.lsmBridge.checkFileOpenHook(params.filePath);
      if (!check.allowed) return { allowed: false, reason: `LSM_PATH_DENIED:${check.normalizedPath}` };
    }
    if (params.shellCommand) {
      const cmdCheck = this.evaluateShellCommand(params.shellCommand);
      if (!cmdCheck.allowed) return cmdCheck;
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
        return { allowed: false, reason: "VSOCK_EGRESS_ALLOWLIST_DENIED" };
      }
    }
    return { allowed: true };
  }

  evaluateShellCommand(command) {
    for (const rule of this.blockedCommandPatterns) {
      if (rule.pattern.test(command)) {
        return { allowed: false, reason: rule.reason };
      }
    }
    return { allowed: true };
  }
}exports.ZeroTrustSandboxSecurityGuard = ZeroTrustSandboxSecurityGuard;