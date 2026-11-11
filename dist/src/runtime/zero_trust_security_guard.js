"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ZeroTrustSandboxSecurityGuard = void 0;




var _ebpf_lsm_bridge = require("./ebpf_lsm_bridge.js");
var _vsock_bridge = require("./vsock_bridge.js"); /**
 * @fileoverview M22, M36, M57-6 & M63-5: Zero-Trust Sandbox Security Guard (Seccomp-BPF, Landlock, Shell Command & Egress Policy)
 * with Root Glob (`/*`), `--no-preserve-root`, Docker Socket, Block-Device Format/Wipe (`mkfs`, `dd of=/dev/`),
 * and IPv4/IPv6/ECS Cloud IMDS (`169.254.169.254`, `::ffff:169.254.169.254`, `169.254.170.2`, `fd00:ec2::254`) Blocking.
 */class ZeroTrustSandboxSecurityGuard {lsmBridge = new _ebpf_lsm_bridge.EbpfLsmSecurityBridge();vsockBridge = new _vsock_bridge.ZeroTrustVsockBridge();

  blockedCommandPatterns = [
  {
    pattern: /rm\s+(?:-[a-zA-Z]+\s+)*(?:--no-preserve-root\s+)?-?[rR][fF]?\s+\/(?:\*|\s|$)|rm\s+-rf\s+--no-preserve-root/i,
    reason: "ROOT_FS_DESTRUCTION_BLOCKED"
  },
  { pattern: /:\(\)\s*\{\s*:\|:&\s*\};:/, reason: "FORK_BOMB_BLOCKED" },
  {
    pattern: /\/etc\/shadow|\/proc\/kcore|\/proc\/self\/environ|\/var\/run\/docker\.sock|docker\.sock/,
    reason: "SENSITIVE_KERNEL_FILE_ACCESS_BLOCKED"
  },
  { pattern: /\/dev\/tcp\//, reason: "RAW_TCP_REVERSE_SHELL_BLOCKED" },
  {
    pattern: /169\.254\.169\.254|169\.254\.170\.2|fd00:ec2::254|::ffff:169\.254\.169\.254|metadata\.google\.internal/i,
    reason: "CLOUD_IMDS_METADATA_BLOCKED"
  },
  {
    pattern: /\bmkfs(?:\.[a-z0-9]+)?\b|\bdd\s+[^;|&\n]*\bof=\/dev\/(?:sd|nvme|vd|xvda|loop)/i,
    reason: "BLOCK_DEVICE_DESTRUCTION_BLOCKED"
  }];


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
    if (typeof command !== "string" || command.includes("\0")) {
      return { allowed: false, reason: "NULL_BYTE_COMMAND_INJECTION_BLOCKED" };
    }
    for (const rule of this.blockedCommandPatterns) {
      if (rule.pattern.test(command)) {
        return { allowed: false, reason: rule.reason };
      }
    }
    return { allowed: true };
  }
}exports.ZeroTrustSandboxSecurityGuard = ZeroTrustSandboxSecurityGuard;