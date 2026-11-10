/**
 * @fileoverview M22, M36 & M57-6: Zero-Trust Sandbox Security Guard (Seccomp-BPF, Landlock, Shell Command & Egress Policy)
 * with Root Glob (`/*`), `--no-preserve-root`, Docker Socket & Cloud IMDS (`169.254.169.254`) Blocking.
 */
import { EbpfLsmSecurityBridge } from "./ebpf_lsm_bridge.js";
import { ZeroTrustVsockBridge } from "./vsock_bridge.js";

export class ZeroTrustSandboxSecurityGuard {
  public readonly lsmBridge = new EbpfLsmSecurityBridge();
  public readonly vsockBridge = new ZeroTrustVsockBridge();

  private readonly blockedCommandPatterns: Array<{ pattern: RegExp; reason: string }> = [
    {
      pattern: /rm\s+(?:-[a-zA-Z]+\s+)*(?:--no-preserve-root\s+)?-?[rR][fF]?\s+\/(?:\*|\s|$)|rm\s+-rf\s+--no-preserve-root/i,
      reason: "ROOT_FS_DESTRUCTION_BLOCKED",
    },
    { pattern: /:\(\)\s*\{\s*:\|:&\s*\};:/, reason: "FORK_BOMB_BLOCKED" },
    {
      pattern: /\/etc\/shadow|\/proc\/kcore|\/var\/run\/docker\.sock|docker\.sock/,
      reason: "SENSITIVE_KERNEL_FILE_ACCESS_BLOCKED",
    },
    { pattern: /\/dev\/tcp\//, reason: "RAW_TCP_REVERSE_SHELL_BLOCKED" },
    { pattern: /169\.254\.169\.254|metadata\.google\.internal/i, reason: "CLOUD_IMDS_METADATA_BLOCKED" },
  ];

  public evaluateAction(params: {
    filePath?: string;
    egressUrl?: string;
    shellCommand?: string;
  }): { allowed: boolean; reason?: string } {
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
          payload: { url: params.egressUrl },
        });
      } catch {
        return { allowed: false, reason: "VSOCK_EGRESS_ALLOWLIST_DENIED" };
      }
    }
    return { allowed: true };
  }

  public evaluateShellCommand(command: string): { allowed: boolean; reason?: string } {
    for (const rule of this.blockedCommandPatterns) {
      if (rule.pattern.test(command)) {
        return { allowed: false, reason: rule.reason };
      }
    }
    return { allowed: true };
  }
}
