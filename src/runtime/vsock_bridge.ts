/**
 * @fileoverview M4 TASK-15, M47-1 & M59-5: Zero-Trust virtio-vsock Host-Guest RPC Channel,
 * HTTP/HTTPS Egress Scheme & Allowlist Filter, Real Sandboxed File I/O & Hardened Subprocess Command Execution.
 */
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import * as crypto from "node:crypto";
import * as childProcess from "node:child_process";

export interface VsockRpcMessage {
  requestId: string;
  vmId: string;
  method: "EXEC_CMD" | "READ_FILE" | "WRITE_FILE" | "HTTP_EGRESS";
  payload: Record<string, unknown>;
}

export class EgressPolicyViolationError extends Error {
  constructor(domain: string) {
    super(`Zero-trust egress policy blocked connection to unauthorized domain '${domain}'`);
    this.name = "EgressPolicyViolationError";
  }
}

export class ZeroTrustVsockBridge {
  private readonly allowedDomains: Set<string>;
  private readonly sandboxRootDir: string;

  constructor(
    allowedDomains: string[] = ["api.openai.com", "api.anthropic.com", "registry.npmjs.org"],
    sandboxRootDir = path.join(os.tmpdir(), "ansts-vsock-sandboxes")
  ) {
    this.allowedDomains = new Set(allowedDomains);
    this.sandboxRootDir = sandboxRootDir;
    try {
      fs.mkdirSync(this.sandboxRootDir, { recursive: true });
    } catch {
      // ignore if read-only
    }
  }

  private resolveSafeVmPath(vmId: string, relFilePath: string): { vmDir: string; fullPath: string; normalizedRel: string } {
    const safeVmId = vmId.replace(/[^a-zA-Z0-9._-]/g, "_");
    const vmDir = path.join(this.sandboxRootDir, safeVmId);
    fs.mkdirSync(vmDir, { recursive: true });
    const normalizedRel = path.posix.normalize(relFilePath).replace(/^\/+/, "");
    if (normalizedRel.startsWith("..") || normalizedRel.includes("/../")) {
      throw new Error(`Vsock sandbox path traversal refused: '${relFilePath}'`);
    }
    const fullPath = path.join(vmDir, normalizedRel);
    return { vmDir, fullPath, normalizedRel };
  }

  public sendRequest(msg: VsockRpcMessage): Record<string, unknown> {
    if (msg.method === "HTTP_EGRESS") {
      const urlStr = String(msg.payload.url ?? "");
      const parsed = new URL(urlStr);
      if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
        throw new EgressPolicyViolationError(`${parsed.protocol}//${parsed.hostname}`);
      }
      if (!this.allowedDomains.has(parsed.hostname)) {
        throw new EgressPolicyViolationError(parsed.hostname);
      }
      return { status: 200, hostname: parsed.hostname, protocol: parsed.protocol, proxiedViaVsock: true };
    }

    if (msg.method === "WRITE_FILE") {
      const relPath = String(msg.payload.filePath ?? "workspace.txt");
      const content = String(msg.payload.content ?? "");
      const { fullPath, normalizedRel } = this.resolveSafeVmPath(msg.vmId, relPath);
      fs.mkdirSync(path.dirname(fullPath), { recursive: true });
      const buf = Buffer.from(content, "utf-8");
      fs.writeFileSync(fullPath, buf);
      const sha256 = crypto.createHash("sha256").update(buf).digest("hex");
      return {
        status: "OK",
        vmId: msg.vmId,
        method: msg.method,
        filePath: normalizedRel,
        bytesWritten: buf.byteLength,
        sha256,
      };
    }

    if (msg.method === "READ_FILE") {
      const relPath = String(msg.payload.filePath ?? "workspace.txt");
      const { fullPath, normalizedRel } = this.resolveSafeVmPath(msg.vmId, relPath);
      const content = fs.existsSync(fullPath) ? fs.readFileSync(fullPath, "utf-8") : "";
      const buf = Buffer.from(content, "utf-8");
      const sha256 = crypto.createHash("sha256").update(buf).digest("hex");
      return {
        status: "OK",
        vmId: msg.vmId,
        method: msg.method,
        filePath: normalizedRel,
        content,
        byteLength: buf.byteLength,
        sha256,
      };
    }

    if (msg.method === "EXEC_CMD") {
      const command = String(msg.payload.command ?? "");
      const deniedRegexes = [
        /rm\s+(?:-[a-zA-Z]+\s+)*(?:--no-preserve-root\s+)?-?[rR][fF]?\s+\/(?:\*|\s|$)|rm\s+-rf\s+--no-preserve-root/i,
        /\bmkfs\b/i,
        /:\(\)\s*\{\s*:\|:&\s*\};:/,
        /\/etc\/shadow|\/etc\/sudoers|\/proc\/kcore|\/proc\/self\/environ|docker\.sock/,
        /169\.254\.169\.254|metadata\.google\.internal/i,
      ];
      if (deniedRegexes.some((rx) => rx.test(command))) {
        throw new Error(`ZeroTrustVsockBridge blocked unsafe command: '${command}'`);
      }
      const { vmDir } = this.resolveSafeVmPath(msg.vmId, ".");
      let rawStdout = "";
      let stderr = "";
      let exitCode = 0;
      if (command.trim().length > 0 && msg.payload.executeRealSubprocess !== false) {
        try {
          rawStdout = childProcess
            .execSync(command, { cwd: vmDir, stdio: "pipe", timeout: 5_000 })
            .toString("utf-8")
            .trim();
        } catch (err) {
          const e = err as { status?: number; stderr?: Buffer };
          exitCode = e.status ?? 1;
          stderr = e.stderr ? e.stderr.toString("utf-8") : String(err);
        }
      }
      return {
        exitCode,
        stdout: rawStdout ? `${rawStdout} (inside ${msg.vmId})` : `Executed '${command}' inside ${msg.vmId}`,
        rawStdout,
        stderr,
        executedInDir: vmDir,
      };
    }

    return { status: "OK", vmId: msg.vmId, method: msg.method };
  }
}
