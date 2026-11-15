/** Private Engine adapter. No task-controlled mounts, environment, networking or engine options. */
import * as http from "node:http";
import { randomUUID } from "node:crypto";
import { SandboxLogs, SandboxArtifact, SANDBOX_ARTIFACT_COLLECTOR } from "./sandbox_output.js";

export interface SandboxCommand {
  argv: string[];
  timeoutMs: number;
  cpu: number;
  memoryMb: number;
}
export interface SandboxResult { exitCode: number; logs: string; logsTruncated: boolean; logsTailLimited: boolean; executionOutcome?: "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "CANCELLED"; artifacts?: SandboxArtifact[]; artifactsTruncated?: boolean; }
export class ContainerSandbox {
  constructor(private readonly image: string, private readonly socketPath = "/var/run/docker.sock", private readonly owner = "standalone") {
    if (!/^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$/.test(image)) throw new Error("SANDBOX_IMAGE_DIGEST_REQUIRED");
  }
  private request(method: string, route: string, payload?: unknown, signal = AbortSignal.timeout(10000), limit = 1048576, consume?: (chunk: Buffer) => void): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const body = payload === undefined ? undefined : JSON.stringify(payload);
      const request = http.request({ socketPath: this.socketPath, path: `/v1.45${route}`, method, signal, headers: body ? { "content-type": "application/json", "content-length": Buffer.byteLength(body) } : {} }, response => {
        let size = 0; const chunks: Buffer[] = [];
        response.on("error", reject);
        response.on("data", chunk => {
          if (consume && (response.statusCode ?? 500) < 300) { try { consume(chunk); } catch (error) { response.destroy(error as Error); } return; }
          size += chunk.length; if (size > limit) { response.destroy(new Error("ENGINE_RESPONSE_LIMIT")); return; } chunks.push(chunk);
        });
        response.on("end", () => { if ((response.statusCode ?? 500) >= 300) reject(new Error(`ENGINE_STATUS_${response.statusCode}: ${Buffer.concat(chunks).toString("utf8").slice(0, 512)}`)); else resolve(Buffer.concat(chunks)); });
      });
      request.on("error", reject); request.end(body);
    });
  }
  /** Called before advertising readiness. Only this Daemon's labelled containers are eligible. */
  async recoverOrphans(): Promise<number> {
    const filters = encodeURIComponent(JSON.stringify({ label: ["ansts.managed=sandbox", `ansts.owner=${this.owner}`] }));
    const containers = JSON.parse((await this.request("GET", `/containers/json?all=true&filters=${filters}`)).toString()) as Array<{ Id: string; Labels?: Record<string, string> }>;
    let removed = 0;
    for (const container of containers) {
      if (!/^[a-f0-9]{64}$/.test(container.Id) || container.Labels?.["ansts.managed"] !== "sandbox" || container.Labels?.["ansts.owner"] !== this.owner) throw new Error("INVALID_ORPHAN_IDENTITY");
      await this.request("DELETE", `/containers/${container.Id}?force=true&v=true`); removed++;
    }
    return removed;
  }
  async execute(command: SandboxCommand, signal: AbortSignal, onOutput?: (logs: string) => void): Promise<SandboxResult> {
    if (!Array.isArray(command.argv) || !command.argv.length || command.argv.length > 64 || command.argv.some(value => typeof value !== "string" || value.includes("\0") || value.length > 8192) || !Number.isSafeInteger(command.timeoutMs) || command.timeoutMs < 1 || command.timeoutMs > 3600000 || !Number.isFinite(command.cpu) || command.cpu <= 0 || command.cpu > 32 || !Number.isSafeInteger(command.memoryMb) || command.memoryMb < 16 || command.memoryMb > 65536) throw new Error("INVALID_SANDBOX_COMMAND");
    signal.throwIfAborted();
    // Generated name permits cleanup even when create succeeds but its response is lost.
    const name = `ansts-sandbox-${randomUUID()}`;
    const combined = AbortSignal.any([signal, AbortSignal.timeout(command.timeoutMs)]);
    let result: SandboxResult | undefined;
    let failure: unknown;
    const output = new SandboxLogs();
    try {
      await this.request("POST", `/containers/create?name=${name}`, {
        Image: this.image, Cmd: ["node", "-e", "setInterval(()=>{},1000)"], Entrypoint: [], User: "65534:65534", WorkingDir: "/workspace",
        Env: ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=/workspace", "TMPDIR=/tmp"],
        NetworkDisabled: true, AttachStdout: true, AttachStderr: true, Tty: false,
        Labels: { "ansts.managed": "sandbox", "ansts.owner": this.owner },
        HostConfig: { NetworkMode: "none", ReadonlyRootfs: true, CapDrop: ["ALL"], SecurityOpt: ["no-new-privileges:true"],
          Memory: command.memoryMb * 1048576, MemorySwap: command.memoryMb * 1048576, NanoCpus: Math.ceil(command.cpu * 1e9), PidsLimit: 64,
          Tmpfs: { "/workspace": "rw,noexec,nosuid,nodev,size=32m,mode=1777", "/tmp": "rw,noexec,nosuid,nodev,size=16m,mode=1777" },
          LogConfig: { Type: "local", Config: { "max-size": "1m", "max-file": "1", "compress": "false" } }, RestartPolicy: { Name: "no" } },
      });
      combined.throwIfAborted();
      await this.request("POST", `/containers/${name}/start`, undefined, combined);
      const session = JSON.parse((await this.request("POST", `/containers/${name}/exec`, { AttachStdout: true, AttachStderr: true, Tty: false, Cmd: command.argv, User: "65534:65534", WorkingDir: "/workspace" }, combined)).toString());
      if (typeof session.Id !== "string" || !/^[a-f0-9]{64}$/.test(session.Id)) throw new Error("INVALID_EXEC_IDENTITY");
      await this.request("POST", `/exec/${session.Id}/start`, { Detach: false, Tty: false }, combined, 1048576, chunk => { output.push(chunk); onOutput?.(output.snapshot().logs); });
      output.finish();
      const exited = JSON.parse((await this.request("GET", `/exec/${session.Id}/json`, undefined, combined)).toString());
      if (exited.Running || !Number.isSafeInteger(exited.ExitCode)) throw new Error("SANDBOX_WAIT_FAILED");
      let files: { artifacts: SandboxArtifact[]; artifactsTruncated: boolean };
      try {
        const collector = JSON.parse((await this.request("POST", `/containers/${name}/exec`, { AttachStdout: true, AttachStderr: false, Tty: true, Cmd: ["node", "-e", SANDBOX_ARTIFACT_COLLECTOR], User: "65534:65534", WorkingDir: "/workspace" }, combined)).toString());
        if (!/^[a-f0-9]{64}$/.test(collector.Id)) throw new Error("INVALID_EXEC_IDENTITY");
        files = JSON.parse((await this.request("POST", `/exec/${collector.Id}/start`, { Detach: false, Tty: true }, combined, 24576)).toString());
        if (!Array.isArray(files.artifacts) || typeof files.artifactsTruncated !== "boolean") throw new Error("INVALID_ARTIFACT_RESULT");
      }
      catch { files = { artifacts: [], artifactsTruncated: true }; }
      result = { exitCode: exited.ExitCode, executionOutcome: exited.ExitCode === 0 ? "SUCCEEDED" : "FAILED", ...output.snapshot(), ...files };
    } catch (error) { failure = error; }
    // Force-removal waits for Engine termination; inability to confirm removal is uncertainty.
    try { await this.request("DELETE", `/containers/${name}?force=true&v=true`); }
    catch (error) { if (String(error).includes("ENGINE_STATUS_404")) { /* creation failed before registration */ } else throw new Error("BODY_STOP_UNCONFIRMED"); }
    if (failure && combined.aborted && (!signal.aborted || ["BODY_CANCELLED", "BODY_TIMED_OUT"].includes(String(signal.reason)))) return { exitCode: signal.reason === "BODY_CANCELLED" ? 137 : 124, executionOutcome: signal.reason === "BODY_CANCELLED" ? "CANCELLED" : "TIMED_OUT", ...output.snapshot(), artifacts: [], artifactsTruncated: true };
    if (failure) throw failure;
    combined.throwIfAborted();
    if (!result) throw new Error("SANDBOX_RESULT_MISSING");
    return result;
  }
}
