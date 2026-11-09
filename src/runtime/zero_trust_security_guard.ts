/**
 * @fileoverview M22 Zero-Trust Sandbox Security Guard (Seccomp-BPF, Landlock & Egress Policy).
 */
import { EbpfLsmSecurityBridge } from "./ebpf_lsm_bridge.js";
import { ZeroTrustVsockBridge } from "./vsock_bridge.js";

export class ZeroTrustSandboxSecurityGuard {
  public readonly lsmBridge = new EbpfLsmSecurityBridge();
  public readonly vsockBridge = new ZeroTrustVsockBridge();

  public evaluateAction(params: { filePath?: string; egressUrl?: string }): { allowed: boolean } {
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
          payload: { url: params.egressUrl },
        });
      } catch {
        return { allowed: false };
      }
    }
    return { allowed: true };
  }
}
