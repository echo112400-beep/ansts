/** Private Engine adapter. No task-controlled mounts, environment, networking or engine options. */
import * as http from "node:http";
import { randomUUID } from "node:crypto";

export interface SandboxCommand {
  argv: string[];
  timeoutMs: number;
  cpu: number;
  memoryMb: number;
}
export interface SandboxResult { exitCode: number; logs: string; logsTruncated: boolean; logsTailLimited: boolean; }
export class ContainerSandbox {
  constructor(private readonly image: string, private readonly socketPath = "/var/run/docker.sock") {
    if (!/^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$/.test(image)) throw new Error("SANDBOX_IMAGE_DIGEST_REQUIRED");
  }
  private request(method: string, route: string, payload?: unknown, signal = AbortSignal.timeout(10000), limit = 1048576): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const body = payload === undefined ? undefined : JSON.stringify(payload);
      const request = http.request({ socketPath: this.socketPath, path: `/v1.45${route}`, method, signal, headers: body ? { "content-type": "application/json", "content-length": Buffer.byteLength(body) } : {} }, response => {
        let size = 0; const chunks: Buffer[] = [];
        response.on("error", reject);
        response.on("data", chunk => { size += chunk.length; if (size > limit) { response.destroy(new Error("ENGINE_RESPONSE_LIMIT")); return; } chunks.push(chunk); });
        response.on("end", () => { if ((response.statusCode ?? 500) >= 300) reject(new Error(`ENGINE_STATUS_${response.statusCode}: ${Buffer.concat(chunks).toString("utf8").slice(0, 512)}`)); else resolve(Buffer.concat(chunks)); });
      });
      request.on("error", reject); request.end(body);
    });
  }
  async execute(command: SandboxCommand, signal: AbortSignal): Promise<SandboxResult> {
    if (!Array.isArray(command.argv) || !command.argv.length || command.argv.length > 64 || command.argv.some(value => typeof value !== "string" || value.includes("\0") || value.length > 8192) || !Number.isSafeInteger(command.timeoutMs) || command.timeoutMs < 1 || command.timeoutMs > 3600000 || !Number.isFinite(command.cpu) || command.cpu <= 0 || command.cpu > 32 || !Number.isSafeInteger(command.memoryMb) || command.memoryMb < 16 || command.memoryMb > 65536) throw new Error("INVALID_SANDBOX_COMMAND");
    signal.throwIfAborted();
    // Generated name permits cleanup even when create succeeds but its response is lost.
    const name = `ansts-sandbox-${randomUUID()}`;
    const combined = AbortSignal.any([signal, AbortSignal.timeout(command.timeoutMs)]);
    let result: SandboxResult | undefined;
    let failure: unknown;
    try {
      await this.request("POST", `/containers/create?name=${name}`, {
        Image: this.image, Cmd: command.argv, Entrypoint: [], User: "65534:65534", WorkingDir: "/workspace",
        Env: ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=/workspace", "TMPDIR=/tmp"],
        NetworkDisabled: true, AttachStdout: true, AttachStderr: true, Tty: false,
        Labels: { "ansts.managed": "sandbox" },
        HostConfig: { NetworkMode: "none", ReadonlyRootfs: true, CapDrop: ["ALL"], SecurityOpt: ["no-new-privileges:true"],
          Memory: command.memoryMb * 1048576, MemorySwap: command.memoryMb * 1048576, NanoCpus: Math.ceil(command.cpu * 1e9), PidsLimit: 64,
          Tmpfs: { "/workspace": "rw,noexec,nosuid,nodev,size=32m,mode=1777", "/tmp": "rw,noexec,nosuid,nodev,size=16m,mode=1777" },
          LogConfig: { Type: "local", Config: { "max-size": "1m", "max-file": "1", "compress": "false" } }, RestartPolicy: { Name: "no" } },
      });
      combined.throwIfAborted();
      await this.request("POST", `/containers/${name}/start`, undefined, combined);
      const waited = JSON.parse((await this.request("POST", `/containers/${name}/wait?condition=not-running`, undefined, combined)).toString());
      if (waited.Error?.Message || !Number.isSafeInteger(waited.StatusCode)) throw new Error("SANDBOX_WAIT_FAILED");
      const raw = await this.request("GET", `/containers/${name}/logs?stdout=true&stderr=true&tail=1000`);
      const chunks: Buffer[] = []; let offset = 0;
      while (offset + 8 <= raw.length) {
        const size = raw.readUInt32BE(offset + 4); offset += 8;
        if (offset + size > raw.length) throw new Error("INVALID_SANDBOX_LOG_FRAME");
        chunks.push(raw.subarray(offset, offset + size)); offset += size;
      }
      const logs = Buffer.concat(chunks);
      result = { exitCode: waited.StatusCode, logs: logs.subarray(0, 65536).toString("utf8"), logsTruncated: logs.length > 65536, logsTailLimited: true };
    } catch (error) { failure = error; }
    // Force-removal waits for Engine termination; inability to confirm removal is uncertainty.
    try { await this.request("DELETE", `/containers/${name}?force=true&v=true`); }
    catch (error) { if (String(error).includes("ENGINE_STATUS_404")) { /* creation failed before registration */ } else throw new Error("BODY_STOP_UNCONFIRMED"); }
    if (failure) throw failure;
    combined.throwIfAborted();
    if (!result) throw new Error("SANDBOX_RESULT_MISSING");
    return result;
  }
}
