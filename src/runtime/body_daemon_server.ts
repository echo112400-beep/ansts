import { BodyDrainState } from "./body_drain_state.js";
import { BodyResourceLedger, BodyResources } from "./body_resource_ledger.js";
import { AgentToolLoop } from "./agent_tool_loop.js";
import { AgentModel } from "./deepseek_model.js";
import { controlPlaneFetch } from "../cloudflare/control_plane_fetch.js";
import { BodyCleanupLedger } from "./body_cleanup_ledger.js";
import { ContainerSandbox } from "./container_sandbox.js";
import { normalizeExecutionSpec } from "../execution_spec.js";
/** @fileoverview Native Body execution service. No scheduler, Gateway, dashboard or control-cell store is embedded. */
import * as http from "node:http";
import { BodyCapacity } from "./body_capacity.js";
import * as fs from "node:fs";
import * as path from "node:path";
import { equalBodyToken } from "../cloudflare/body_auth.js";
import { executionDigest, taskInputDigest } from "../cloudflare/execution_attempt.js";
import { BodyExecutionJournal } from "./body_execution_journal.js";
import { LayerFsWorkspaceDriver } from "./layerfs_workspace_driver.js";
import { runBodyExecution } from "./body_execution_process.js";

export interface BodyDaemonOptions {
  nodeId: string;
  storageDir: string;
  bodyWorkerUrl: string;
  bodyWorkerUrls?: string;
  token: string;
  host?: string;
  port?: number;
  s3Endpoint?: string;
  executionDelayMs?: number;
  executionSlots?: number;
  sandboxImage?: string;
  resources?: BodyResources;
  agentModel?: AgentModel;
  containerRuntime?: Pick<ContainerSandbox, "execute" | "recoverOrphans">;
}
interface Sandbox {
  taskId: string; attemptId: string; fencingToken: number; startedAt: number;
  controller: AbortController; done: Promise<void>;
  stopConfirmed?: boolean;
  cancelRequested?: boolean;
  executionStarted?: boolean;
}
export class BodyDaemonServer {
  private server?: http.Server;
  private readonly containerRuntime?: Pick<ContainerSandbox, "execute" | "recoverOrphans">;
  private readonly capacity: BodyCapacity;
  private readonly cleanup: BodyCleanupLedger;
  private readonly drain: BodyDrainState;
  private readonly resources: BodyResourceLedger;
  private readonly journal: BodyExecutionJournal;
  private readonly active = new Map<string, Sandbox>();
  private readonly operations = new Set<Promise<void>>();
  constructor(private readonly options: BodyDaemonOptions) {
    if (!options.nodeId || !/^[a-f0-9]{64}$/.test(options.token) || !/^https?:$/.test(new URL(options.bodyWorkerUrl).protocol)) throw new Error("INVALID_BODY_DAEMON_CONFIGURATION");
    this.containerRuntime = options.containerRuntime ?? (options.sandboxImage ? new ContainerSandbox(options.sandboxImage, "/var/run/docker.sock", options.nodeId) : undefined);
    this.capacity = new BodyCapacity(options.executionSlots ?? 1);
    this.cleanup = new BodyCleanupLedger(path.join(options.storageDir, "cleanup-ledger"));
    this.resources = new BodyResourceLedger(path.join(options.storageDir, "resource-ledger"), options.resources ?? { cpu: 1, memoryMb: 1024 });
    this.drain = new BodyDrainState(path.join(options.storageDir, "drain-state.json"));
    fs.mkdirSync(path.join(options.storageDir, "workspaces"), { recursive: true });
    this.journal = new BodyExecutionJournal(path.join(options.storageDir, "execution-journal"));
  }
  async start(): Promise<{ port: number }> {
    if ((this.cleanup.pending().length || this.resources.keys().length) && !this.options.sandboxImage) throw new Error("BODY_RECOVERY_REQUIRES_ENGINE");
    if (this.options.sandboxImage) {
      await this.containerRuntime!.recoverOrphans();
      this.cleanup.confirmAllRemoved();
      for (const key of this.resources.keys()) this.resources.release(key);
    }
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
  private recoveryPending(): string[] { return [...new Set([...this.cleanup.pending(), ...this.resources.keys()])].filter(key => !this.active.has(key)); }
  private json(response: http.ServerResponse, status: number, data: unknown): void {
    if (response.destroyed || response.writableEnded) return;
    response.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" });
    response.end(JSON.stringify(data));
  }
  private async rpc(route: string, body?: unknown, signal?: AbortSignal): Promise<Response> {
    return controlPlaneFetch(this.options.bodyWorkerUrls || this.options.bodyWorkerUrl, route, {
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
      let ready = !this.drain.draining && this.recoveryPending().length === 0;
      if (route === "/readyz") { try { ready = ready && (await this.rpc("/readyz")).ok; } catch { ready = false; } }
      this.json(response, route === "/readyz" && !ready ? 503 : 200, { draining: this.drain.draining, drained: this.drain.draining && this.active.size === 0 && this.recoveryPending().length === 0, recoveryRequired: this.recoveryPending().length > 0, runtime: "native-body-daemon", nodeId: this.options.nodeId, controlPlane: "remote-celld-body", healthy: ready, activeSandboxes: this.active.size, capacity: this.capacity.snapshot(), resources: this.resources.snapshot() });
      return;
    }
    if (!equalBodyToken(String(request.headers.authorization ?? ""), `Body ${this.options.token}`) || request.headers["x-ansts-body-id"] !== this.options.nodeId) {
      this.json(response, 401, { error: "BODY_AUTH_REQUIRED" }); return;
    }
    if (request.method === "POST" && route === "/v1/daemon/drain") {
      let body;
      try { body = await this.readBody(request); } catch { this.json(response, 400, { error: "INVALID_DRAIN_REQUEST" }); return; }
      if (typeof body.draining !== "boolean" || Object.keys(body).length !== 1) { this.json(response, 400, { error: "INVALID_DRAIN_REQUEST" }); return; }
      this.drain.set(body.draining);
      this.json(response, 200, { draining: this.drain.draining, drained: this.drain.draining && this.active.size === 0 && this.cleanup.pending().length === 0, activeSandboxes: this.active.size }); return;
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
    let execution;
    try { execution = normalizeExecutionSpec(body.execution); } catch { this.json(response, 400, { error: "INVALID_EXECUTION_SPEC" }); return; }
    if (execution && execution.image !== this.options.sandboxImage) { this.json(response, 503, { error: "SANDBOX_IMAGE_NOT_ENABLED" }); return; }
    if (execution?.kind === "agent" && !this.options.agentModel) { this.json(response, 503, { error: "AGENT_MODEL_NOT_CONFIGURED" }); return; }
    const key = executionDigest([tenantId, taskId]);
    if (route === "/v1/sandboxes/stop") {
      const sandbox = this.active.get(key);
      if (!sandbox || sandbox.attemptId !== attemptId || sandbox.fencingToken !== fencingToken) { this.json(response, 409, { error: "SANDBOX_IDENTITY_CONFLICT" }); return; }
      sandbox.cancelRequested = true;
      if (sandbox.executionStarted) sandbox.controller.abort("BODY_CANCELLED");
      await sandbox.done;
      this.json(response, sandbox.stopConfirmed === false ? 503 : 200, { stopped: sandbox.stopConfirmed !== false, taskId, attemptId }); return;
    }
    if (this.drain.draining) { this.json(response, 503, { error: "BODY_DRAINING" }); return; }
    if (this.active.has(key)) { this.json(response, 409, { error: "SANDBOX_ALREADY_RUNNING" }); return; }
    if (this.recoveryPending().length) { this.json(response, 503, { error: "BODY_RECOVERY_REQUIRED" }); return; }
    const controller = new AbortController();
    const disconnect = () => { if (!response.writableEnded) controller.abort(); };
    response.once("close", disconnect);
    const timeoutSeconds = Number(body.timeoutSeconds ?? 1800);
    if (!Number.isFinite(timeoutSeconds) || timeoutSeconds <= 0 || timeoutSeconds > 86400) { this.json(response, 400, { error: "INVALID_TIMEOUT" }); response.removeListener("close", disconnect); return; }
    if (!this.capacity.reserve(key)) { response.removeListener("close", disconnect); this.json(response, 429, { error: "BODY_CAPACITY_EXHAUSTED" }); return; }
    if (execution && !this.resources.reserve(key, execution)) { this.capacity.release(key); response.removeListener("close", disconnect); this.json(response, 429, { error: "BODY_RESOURCE_EXHAUSTED" }); return; }
    const timer = setTimeout(() => controller.abort("BODY_TIMED_OUT"), Math.min(timeoutSeconds * 1000, execution?.timeoutMs ?? Infinity));
    let done!: () => void;
    const sandbox: Sandbox = { taskId: String(taskId), attemptId: String(attemptId), fencingToken: Number(fencingToken), startedAt: Date.now(), controller, done: new Promise<void>(resolve => { done = resolve; }) };
    this.active.set(key, sandbox);
    let renewal: ReturnType<typeof setInterval> | undefined;
    try {
      const lease = { task_id: taskId, tenant_id: tenantId, attempt_id: attemptId, node_id: this.options.nodeId, fencing_token: fencingToken, input_digest: taskInputDigest(tenantId, taskId, body.promptText, execution) };
      const granted = await this.rpc(`/v1/body/verify-execution-token?taskId=${encodeURIComponent(String(taskId))}`, { ...lease, execution_token: body.executionToken }, controller.signal);
      if (!granted.ok) { this.json(response, granted.status, { error: "EXECUTION_GRANT_REJECTED" }); return; }
      const leaseResponse = await this.rpc("/v1/scheduler/lease-renew", lease, controller.signal);
      if (!leaseResponse.ok) { this.json(response, 409, { error: "STALE_EXECUTION_LEASE" }); return; }
      const leaseState = await leaseResponse.json() as { cancel_requested?: unknown; lease_expires_at?: number; resume_node_id?: string };
      let leaseDeadline = (leaseState.lease_expires_at ?? Date.now() / 1000 + 12) * 1000;
      const journalKey = JSON.stringify([tenantId, taskId, attemptId]);
      const admission = this.journal.admit(journalKey);
      if (!admission.admitted) { this.json(response, admission.result ? 200 : 409, admission.result ?? { error: "EXECUTION_OUTCOME_UNCERTAIN" }); return; }
      if (leaseState.cancel_requested || sandbox.cancelRequested) {
        const result = { executedByBodyId: this.options.nodeId, taskId, status: "COMPLETED", executionOutcome: "CANCELLED", exitCode: 137, logs: "", neverStarted: true };
        this.journal.complete(journalKey, result); this.json(response, 200, result); return;
      }
      sandbox.executionStarted = true;
      let renewing = false;
      let runtimeLogs = "", logSequence = 0;
      renewal = setInterval(async () => {
        if (renewing) return; renewing = true;
        try {
          const renewed = await this.rpc("/v1/scheduler/lease-renew", lease, AbortSignal.any([controller.signal, AbortSignal.timeout(2500)]));
          if (!renewed.ok) controller.abort();
          else {
            const state = await renewed.json() as { cancel_requested?: unknown; lease_expires_at?: number };
            leaseDeadline = (state.lease_expires_at ?? Date.now() / 1000 + 12) * 1000;
            if (state.cancel_requested) controller.abort("BODY_CANCELLED");
          }
          if (runtimeLogs && !controller.signal.aborted) await this.rpc(`/v1/body/logs?taskId=${encodeURIComponent(String(taskId))}`, { ...lease, sequence: ++logSequence, logs: runtimeLogs }, controller.signal);
        }
        catch { if (Date.now() >= leaseDeadline - 3000) controller.abort(); } finally { renewing = false; }
      }, 3000);
      if (execution) {
        this.cleanup.reserve(key);
        let outcome;
        try {
          const output = (logs: string) => { runtimeLogs = logs; };
          outcome = execution.kind === "agent"
            ? await new AgentToolLoop(path.join(this.options.storageDir, "agent-turns"), this.options.agentModel!).run(JSON.stringify([tenantId, taskId, lease.input_digest]), Number(fencingToken), String(body.promptText ?? ""), execution, this.containerRuntime!, controller.signal, output, !!leaseState.resume_node_id)
            : await this.containerRuntime!.execute(execution, controller.signal, output);
        } catch (error) {
          if (!String(error).includes("BODY_STOP_UNCONFIRMED")) this.cleanup.confirmRemoved(key);
          throw error;
        }
        this.cleanup.confirmRemoved(key);
        const result = { executedByBodyId: this.options.nodeId, taskId, status: "COMPLETED", executionKind: execution.kind, ...outcome };
        this.journal.complete(journalKey, result);
        this.json(response, 200, result); return;
      }
      const branchName = `body-${key.slice(0, 32)}`;
      const snapshot = path.join(this.options.storageDir, "workspaces", `${key}.json`);
      const driver = new LayerFsWorkspaceDriver();
      if (fs.existsSync(snapshot)) driver.importBranchBundle(JSON.parse(fs.readFileSync(snapshot, "utf8")));
      else if (this.options.s3Endpoint) await driver.pullBranchFromRemoteS3(branchName, this.options.s3Endpoint, controller.signal);
      try { driver.getBranchHeadCommit(branchName); } catch { driver.forkBranch("main", branchName); }
      if (controller.signal.aborted) throw new Error("BODY_EXECUTION_STOPPED");
      let output;
      try { output = await runBodyExecution({ bundle: driver.exportBranchBundle(branchName), branchName, tenantId: String(tenantId), stepId: `exec-${taskId}`, prompt: String(body.promptText ?? ""), delayMs: this.options.executionDelayMs }, controller.signal); }
      catch (error) {
        if (!String(error).includes("BODY_STOP_UNCONFIRMED") && ["BODY_CANCELLED", "BODY_TIMED_OUT"].includes(String(controller.signal.reason))) {
          const result = { executedByBodyId: this.options.nodeId, taskId, status: "COMPLETED", executionOutcome: controller.signal.reason === "BODY_CANCELLED" ? "CANCELLED" : "TIMED_OUT", exitCode: controller.signal.reason === "BODY_CANCELLED" ? 137 : 124 };
          this.journal.complete(journalKey, result); this.json(response, 200, result); return;
        }
        throw error;
      }
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
      response.removeListener("close", disconnect); this.active.delete(key);
      if (!this.cleanup.pending().includes(key)) { this.resources.release(key); this.capacity.release(key); }
      done();
    }
  }
}
