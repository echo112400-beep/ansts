"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.EbpfLsmSecurityBridge = void 0; /**
 * @fileoverview eBPF LSM Kernel Security Policy Bridge (Syscall & Path Access Enforcement).
 */
class EbpfLsmSecurityBridge {
  deniedPaths = ["/etc/shadow", "/proc/kcore", "/var/run/docker.sock"];

  checkFileOpenHook(filePath) {
    if (this.deniedPaths.some((p) => filePath.startsWith(p))) {
      return { allowed: false, errno: 13 };
    }
    return { allowed: true, errno: 0 };
  }
}exports.EbpfLsmSecurityBridge = EbpfLsmSecurityBridge;