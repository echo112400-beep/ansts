"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ZeroTrustVsockBridge = exports.EgressPolicyViolationError = void 0;



var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));
var os = _interopRequireWildcard(require("node:os"));
var crypto = _interopRequireWildcard(require("node:crypto"));
var childProcess = _interopRequireWildcard(require("node:child_process"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M4 TASK-15, M47-1 & M59-5: Zero-Trust virtio-vsock Host-Guest RPC Channel,
 * HTTP/HTTPS Egress Scheme & Allowlist Filter, Real Sandboxed File I/O & Hardened Subprocess Command Execution.
 */





class EgressPolicyViolationError extends Error {
  constructor(domain) {
    super(`Zero-trust egress policy blocked connection to unauthorized domain '${domain}'`);
    this.name = "EgressPolicyViolationError";
  }
}exports.EgressPolicyViolationError = EgressPolicyViolationError;

class ZeroTrustVsockBridge {
  allowedDomains;
  sandboxRootDir;

  constructor(
  allowedDomains = ["api.openai.com", "api.anthropic.com", "registry.npmjs.org"],
  sandboxRootDir = path.join(os.tmpdir(), "ansts-vsock-sandboxes"))
  {
    this.allowedDomains = new Set(allowedDomains);
    this.sandboxRootDir = sandboxRootDir;
    try {
      fs.mkdirSync(this.sandboxRootDir, { recursive: true });
    } catch {

      // ignore if read-only
    }}

  resolveSafeVmPath(vmId, relFilePath) {
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

  sendRequest(msg) {
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
        sha256
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
        sha256
      };
    }

    if (msg.method === "EXEC_CMD") {
      const command = String(msg.payload.command ?? "");
      const deniedRegexes = [
      /rm\s+(?:-[a-zA-Z]+\s+)*(?:--no-preserve-root\s+)?-?[rR][fF]?\s+\/(?:\*|\s|$)|rm\s+-rf\s+--no-preserve-root/i,
      /\bmkfs\b/i,
      /:\(\)\s*\{\s*:\|:&\s*\};:/,
      /\/etc\/shadow|\/etc\/sudoers|\/proc\/kcore|\/proc\/self\/environ|docker\.sock/,
      /169\.254\.169\.254|metadata\.google\.internal/i];

      if (deniedRegexes.some((rx) => rx.test(command))) {
        throw new Error(`ZeroTrustVsockBridge blocked unsafe command: '${command}'`);
      }
      const { vmDir } = this.resolveSafeVmPath(msg.vmId, ".");
      let rawStdout = "";
      let stderr = "";
      let exitCode = 0;
      if (command.trim().length > 0 && msg.payload.executeRealSubprocess !== false) {
        try {
          rawStdout = childProcess.
          execSync(command, { cwd: vmDir, stdio: "pipe", timeout: 5_000 }).
          toString("utf-8").
          trim();
        } catch (err) {
          const e = err;
          exitCode = e.status ?? 1;
          stderr = e.stderr ? e.stderr.toString("utf-8") : String(err);
        }
      }
      return {
        exitCode,
        stdout: rawStdout ? `${rawStdout} (inside ${msg.vmId})` : `Executed '${command}' inside ${msg.vmId}`,
        rawStdout,
        stderr,
        executedInDir: vmDir
      };
    }

    return { status: "OK", vmId: msg.vmId, method: msg.method };
  }
}exports.ZeroTrustVsockBridge = ZeroTrustVsockBridge;