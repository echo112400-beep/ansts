/** @fileoverview Native Body execution service. No scheduler, Gateway, dashboard or control-cell store is embedded. */
import * as http from "node:http";
import * as fs from "node:fs";
import * as path from "node:path";
import { equalBodyToken } from "../cloudflare/body_auth.js";
import { executionDigest } from "../cloudflare/execution_attempt.js";
import { BodyExecutionJournal } from "./body_execution_journal.js";
import { LayerFsWorkspaceDriver } from "./layerfs_workspace_driver.js";
import { runBodyExecution } from "./body_execution_process.js";

export interface BodyDaemonOptions {
  nodeId: string;
  storageDir: string;
  bodyWorkerUrl: string;
  token: string;
  host?: string;
  port?: number;
  s3Endpoint?: string;
  executionDelayMs?: number;
}
interface Sandbox {
  taskId: string; attemptId: string; fencingToken: number; startedAt: number;
  controller: AbortController; done: Promise<void>;
  stopConfirmed?: boolean;
}
export class BodyDaemonServer {
  private server?: http.Server;
  private readonly journal: BodyExecutionJournal;
  private readonly active = new Map<string, Sandbox>();
  private readonly operations = new Set<Promise<void>>();
  constructor(private readonly options: BodyDaemonOptions) {
    if (!options.nodeId || !/^[a-f0-9]{64}$/.test(options.token) || !/^https?:$/.test(new URL(options.bodyWorkerUrl).protocol)) throw new Error("INVALID_BODY_DAEMON_CONFIGURATION");
    fs.mkdirSync(path.join(options.storageDir, "workspaces"), { recursive: true });
    this.journal = new BodyExecutionJournal(path.join(options.storageDir, "execution-journal"));
  }
  async start(): Promise<{ port: number }> {
    this.server = http.createServer((request, response) => {
      const operation = this.handle(request, response).catch(() => {
        if (!response.destroyed && !response.writableEnded) this.json(response, 503, { error: "BODY_EXECUTION_UNCERTAIN" });
      });
      this.operations.add(operation);
      void operation.finally(() => this.operations.delete(operation));
    });
    await new Promise<void>(resolve => this.server!.listen(this.options.port ?? 9501, this.options.host ?? "0.0.0.0", resolve));
    const address = this.server.address();
    return { port: typeof address === "object" && address ? address.port : 0 };
  }
  async stop(): Promise<void> {
    for (const sandbox of this.active.values()) sandbox.controller.abort();
    this.server?.closeAllConnections();
    if (this.server) await new Promise<void>(resolve => this.server!.close(() => resolve()));
    await Promise.allSettled([...this.operations]);
  }
  private json(response: http.ServerResponse, status: number, data: unknown): void {
    if (response.destroyed || response.writableEnded) return;
    response.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" });
    response.end(JSON.stringify(data));
  }
  private async rpc(route: string, body?: unknown, signal?: AbortSignal): Promise<Response> {
    return fetch(new URL(route, this.options.bodyWorkerUrl), {
      method: body === undefined ? "GET" : "POST", headers: { authorization: `Body ${this.options.token}`, "x-ansts-body-id": this.options.nodeId, "content-type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body), redirect: "error",
      signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(10000)]) : AbortSignal.timeout(10000),
    });
  }
  private async readBody(request: http.IncomingMessage): Promise<Record<string, unknown>> {
    const chunks: Buffer[] = []; let bytes = 0;
    for await (const chunk of request) {
      bytes += Buffer.byteLength(chunk);
      if (bytes > 65536) throw new Error("PAYLOAD_TOO_LARGE");
      chunks.push(Buffer.from(chunk));
    }
    const result = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    if (!result || typeof result !== "object" || Array.isArray(result)) throw new Error("INVALID_BODY");
    return result;
  }
  private async handle(request: http.IncomingMessage, response: http.ServerResponse): Promise<void> {
    const route = new URL(request.url ?? "/", "http://daemon").pathname;
    if (request.method === "GET" && ["/health", "/healthz", "/readyz"].includes(route)) {
      let ready = true;
      if (route === "/readyz") { try { ready = (await this.rpc("/readyz")).ok; } catch { ready = false; } }
      this.json(response, ready ? 200 : 503, { runtime: "native-body-daemon", nodeId: this.options.nodeId, controlPlane: "remote-celld-body", healthy: ready, activeSandboxes: this.active.size });
      return;
    }
    if (!equalBodyToken(String(request.headers.authorization ?? ""), `Body ${this.options.token}`) || request.headers["x-ansts-body-id"] !== this.options.nodeId) {
      this.json(response, 401, { error: "BODY_AUTH_REQUIRED" }); return;
    }
    if (request.method === "GET" && route === "/v1/sandboxes") {
      this.json(response, 200, { sandboxes: [...this.active.values()].map(({ taskId, attemptId, fencingToken, startedAt }) => ({ taskId, attemptId, fencingToken, startedAt, state: "RUNNING" })) }); return;
    }
    if (request.method !== "POST" || !["/v1/sandboxes/execute", "/internal/v1beta1/body/execute", "/v1/sandboxes/stop"].includes(route)) {
      this.json(response, 404, { error: "NOT_FOUND" }); return;
    }
    let body: Record<string, unknown>;
    try { body = await this.readBody(request); }
    catch (error) { this.json(response, String(error).includes("PAYLOAD_TOO_LARGE") ? 413 : 400, { error: "INVALID_REQUEST_BODY" }); return; }
    const { taskId, tenantId, attemptId, fencingToken } = body;
    if (![taskId, tenantId, attemptId].every(value => typeof value === "string" && value.length > 0 && value.length <= 2048) || !Number.isSafeInteger(fencingToken) || Number(fencingToken) < 1) {
      this.json(response, 400, { error: "EXECUTION_IDENTITY_REQUIRED" }); return;
    }
    const key = executionDigest([tenantId, taskId]);
    if (route === "/v1/sandboxes/stop") {
      const sandbox = this.active.get(key);
      if (!sandbox || sandbox.attemptId !== attemptId || sandbox.fencingToken !== fencingToken) { this.json(response, 409, { error: "SANDBOX_IDENTITY_CONFLICT" }); return; }
      sandbox.controller.abort(); await sandbox.done;
      this.json(response, sandbox.stopConfirmed === false ? 503 : 200, { stopped: sandbox.stopConfirmed !== false, taskId, attemptId }); return;
    }
    if (this.active.has(key)) { this.json(response, 409, { error: "SANDBOX_ALREADY_RUNNING" }); return; }
    const controller = new AbortController();
    const disconnect = () => { if (!response.writableEnded) controller.abort(); };
    response.once("close", disconnect);
    const timeoutSeconds = Number(body.timeoutSeconds ?? 1800);
    if (!Number.isFinite(timeoutSeconds) || timeoutSeconds <= 0 || timeoutSeconds > 86400) { this.json(response, 400, { error: "INVALID_TIMEOUT" }); response.removeListener("close", disconnect); return; }
    const timer = setTimeout(() => controller.abort(), timeoutSeconds * 1000);
    let done!: () => void;
    const sandbox: Sandbox = { taskId: String(taskId), attemptId: String(attemptId), fencingToken: Number(fencingToken), startedAt: Date.now(), controller, done: new Promise<void>(resolve => { done = resolve; }) };
    this.active.set(key, sandbox);
    let renewal: ReturnType<typeof setInterval> | undefined;
    try {
      const lease = { task_id: taskId, tenant_id: tenantId, attempt_id: attemptId, node_id: this.options.nodeId, fencing_token: fencingToken, input_digest: executionDigest({ tenant_id: tenantId, task_id: taskId, prompt: body.promptText }) };
      const granted = await this.rpc(`/v1/body/verify-execution-token?taskId=${encodeURIComponent(String(taskId))}`, { ...lease, execution_token: body.executionToken }, controller.signal);
      if (!granted.ok) { this.json(response, granted.status, { error: "EXECUTION_GRANT_REJECTED" }); return; }
      const leaseResponse = await this.rpc("/v1/scheduler/lease-renew", lease, controller.signal);
      if (!leaseResponse.ok) { this.json(response, 409, { error: "STALE_EXECUTION_LEASE" }); return; }
      const journalKey = JSON.stringify([tenantId, taskId, attemptId]);
      const admission = this.journal.admit(journalKey);
      if (!admission.admitted) { this.json(response, admission.result ? 200 : 409, admission.result ?? { error: "EXECUTION_OUTCOME_UNCERTAIN" }); return; }
      let renewing = false;
      renewal = setInterval(async () => {
        if (renewing) return; renewing = true;
        try { if (!(await this.rpc("/v1/scheduler/lease-renew", lease, controller.signal)).ok) controller.abort(); }
        catch { controller.abort(); } finally { renewing = false; }
      }, 3000);
      const branchName = `body-${key.slice(0, 32)}`;
      const snapshot = path.join(this.options.storageDir, "workspaces", `${key}.json`);
      const driver = new LayerFsWorkspaceDriver();
      if (fs.existsSync(snapshot)) driver.importBranchBundle(JSON.parse(fs.readFileSync(snapshot, "utf8")));
      else if (this.options.s3Endpoint) await driver.pullBranchFromRemoteS3(branchName, this.options.s3Endpoint, controller.signal);
      try { driver.getBranchHeadCommit(branchName); } catch { driver.forkBranch("main", branchName); }
      if (controller.signal.aborted) throw new Error("BODY_EXECUTION_STOPPED");
      const output = await runBodyExecution({ bundle: driver.exportBranchBundle(branchName), branchName, tenantId: String(tenantId), stepId: `exec-${taskId}`, prompt: String(body.promptText ?? ""), delayMs: this.options.executionDelayMs }, controller.signal);
      if (controller.signal.aborted) throw new Error("BODY_EXECUTION_STOPPED");
      const fd = fs.openSync(snapshot + ".tmp", "w", 0o600);
      try { fs.writeFileSync(fd, JSON.stringify(output.bundle)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
      fs.renameSync(snapshot + ".tmp", snapshot);
      driver.importBranchBundle(output.bundle);
      const s3Synced = this.options.s3Endpoint ? await driver.pushBranchToRemoteS3(branchName, this.options.s3Endpoint, controller.signal) : false;
      if (controller.signal.aborted) throw new Error("BODY_EXECUTION_STOPPED");
      const result = { executedByBodyId: this.options.nodeId, taskId, branchName, commitHash: output.result.commitId, commitId: output.result.commitId, committedNewSnapshot: output.result.committedNewSnapshot, mountLatencyMs: output.result.mountLatencyMs, s3Synced, status: "COMPLETED" };
      this.journal.complete(journalKey, result);
      this.json(response, 200, result);
    } catch (error) {
      sandbox.stopConfirmed = !String(error).includes("BODY_STOP_UNCONFIRMED");
      throw error;
    } finally {
      clearTimeout(timer); if (renewal) clearInterval(renewal);
      response.removeListener("close", disconnect); this.active.delete(key); done();
    }
  }
}
