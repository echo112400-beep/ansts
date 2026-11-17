import {CapacityReporter} from './capacity_reporter.js';
import {BodyUsageJournal} from './body_usage_journal.js';
import {signUsageEvent,usageEventDigest,type UsageIdentity} from '../control/usage_event.js';
import {monitoringAuthorized,gaugeMetrics,AdmissionMetrics,admissionRejection} from "../control/monitoring.js";
import {BodyExecutionEvidenceLedger} from "./body_execution_evidence_ledger.js";
import {bodyEvidenceDigest,canonicalBodyEvidence,signBodyEvidence,type BodyExecutionEvidence} from "../control/body_execution_evidence.js";
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
  usageMetering?:boolean;
  usageTenantIds?:readonly string[];
  physicalHostId?: string;
  nodeId: string;
  storageDir: string;
  bodyWorkerUrl: string;
  bodyWorkerUrls?: string;
  token: string;
  monitoringToken?: string;
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
  private readonly admissionMetrics=new AdmissionMetrics();
  private readonly admissionResponses=new WeakSet<http.ServerResponse>();
  private server?: http.Server;
  private readonly containerRuntime?: Pick<ContainerSandbox, "execute" | "recoverOrphans">;
  private readonly capacity: BodyCapacity;
  private readonly cleanup: BodyCleanupLedger;
  private readonly drain: BodyDrainState;
  private readonly resources: BodyResourceLedger;
  private readonly journal: BodyExecutionJournal;
  private readonly evidence:BodyExecutionEvidenceLedger;
  private readonly capacityReporter?:CapacityReporter;
  private readonly usage?:BodyUsageJournal;
  private usageTimer?:ReturnType<typeof setInterval>;
  private usageUnavailable=false;
  private readonly active = new Map<string, Sandbox>();
  private readonly operations = new Set<Promise<void>>();
  constructor(private readonly options: BodyDaemonOptions) {
    if (!options.nodeId || !/^[a-f0-9]{64}$/.test(options.token) || !/^https?:$/.test(new URL(options.bodyWorkerUrl).protocol)) throw new Error("INVALID_BODY_DAEMON_CONFIGURATION");
    this.containerRuntime = options.containerRuntime ?? (options.sandboxImage ? new ContainerSandbox(options.sandboxImage, "/var/run/docker.sock", options.nodeId) : undefined);
    this.capacity = new BodyCapacity(options.executionSlots ?? 1);
    if(options.usageMetering)this.usage=new BodyUsageJournal(path.join(options.storageDir,'usage-outbox'));
    this.cleanup = new BodyCleanupLedger(path.join(options.storageDir, "cleanup-ledger"));
    this.resources = new BodyResourceLedger(path.join(options.storageDir, "resource-ledger"), options.resources ?? { cpu: 1, memoryMb: 1024 });
    this.drain = new BodyDrainState(path.join(options.storageDir, "drain-state.json"));
    fs.mkdirSync(path.join(options.storageDir, "workspaces"), { recursive: true });
    if(options.physicalHostId)this.capacityReporter=new CapacityReporter(path.join(options.storageDir,"capacity-reporter.json"),options.physicalHostId,options.nodeId,options.token);
    this.evidence=new BodyExecutionEvidenceLedger(path.join(options.storageDir,"execution-evidence"));
    this.journal = new BodyExecutionJournal(path.join(options.storageDir, "execution-journal"));
  }
  async start(): Promise<{ port: number }> {
    if ((this.cleanup.pending().length || this.resources.keys().length) && !this.options.sandboxImage) throw new Error("BODY_RECOVERY_REQUIRES_ENGINE");
    if (this.options.sandboxImage) {
      await this.containerRuntime!.recoverOrphans();
      this.usage?.recoverReleased();
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
    if(this.usage){this.usageTimer=setInterval(()=>{try{this.usage!.checkpointActive();this.usageUnavailable=false;}catch{this.usageUnavailable=true;}},60000);this.usageTimer.unref();}
    return { port: typeof address === "object" && address ? address.port : 0 };
  }
  async stop(): Promise<void> {
    if(this.usageTimer)clearInterval(this.usageTimer);
    for (const sandbox of this.active.values()) sandbox.controller.abort();
    this.server?.closeAllConnections();
    if (this.server) await new Promise<void>(resolve => this.server!.close(() => resolve()));
    await Promise.allSettled([...this.operations]);
  }
  private recoveryPending(): string[] { return [...new Set([...this.cleanup.pending(), ...this.resources.keys(),...(this.usage&&this.usage.stats().openStreams>this.active.size?['usage-uncertain']:[])])].filter(key => !this.active.has(key)); }
  private json(response: http.ServerResponse, status: number, data: unknown): void {
    if (response.destroyed || response.writableEnded) return;
    if(this.admissionResponses.has(response)) {
      this.admissionResponses.delete(response);
      this.admissionMetrics.record(admissionRejection(status,(data as {error?:unknown}|null)?.error));
    }
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
    if(request.method==='GET'&&route==='/metrics') {
      if(!monitoringAuthorized(String(request.headers.authorization??''),this.options.monitoringToken)){this.json(response,401,{error:'MONITORING_AUTH_REQUIRED'});return;}
      const slots=this.capacity.snapshot(),resources=this.resources.snapshot();
      const reservationDirectory=path.join(this.options.storageDir,'resource-ledger');
      const reservationFiles=fs.readdirSync(reservationDirectory).filter(name=>/^[a-f0-9]{64}\.json$/.test(name));
      const oldestReservation=reservationFiles.length?Math.max(0,...reservationFiles.map(name=>(Date.now()-fs.statSync(path.join(reservationDirectory,name)).mtimeMs)/1000)):0;
      const outboxDirectory=path.join(this.options.storageDir,'result-outbox');
      const pendingResults=fs.existsSync(outboxDirectory)?fs.readdirSync(outboxDirectory).filter(name=>/^[a-f0-9]{64}\.json$/.test(name)).length:0;
      const usage=this.usage?.stats()??{incompleteResourceStreams:0,pendingEvents:0,openStreams:0,quarantinedStreams:0,unresolvedProviderCalls:0,oldestPendingSeconds:0};
      const metrics=gaugeMetrics({ansts_body_usage_metering_enabled:Number(!!this.usage),ansts_body_usage_incomplete_resource_streams:usage.incompleteResourceStreams,ansts_body_usage_outbox_pending:usage.pendingEvents,ansts_body_usage_open_streams:usage.openStreams,ansts_body_usage_quarantined_streams:usage.quarantinedStreams,ansts_body_usage_unknown_provider_calls:usage.unresolvedProviderCalls,ansts_body_usage_oldest_pending_seconds:usage.oldestPendingSeconds,ansts_body_result_outbox_pending:pendingResults,ansts_body_oldest_resource_reservation_seconds:oldestReservation,ansts_body_active_sandboxes:this.active.size,ansts_body_slots_total:slots.slots,ansts_body_slots_reserved:slots.reserved,ansts_body_slots_available:slots.available,ansts_body_cpu_cores_total:resources.total.cpu,ansts_body_cpu_cores_reserved:resources.reserved.cpu,ansts_body_memory_bytes_total:resources.total.memoryMb*1048576,ansts_body_memory_bytes_reserved:resources.reserved.memoryMb*1048576,ansts_body_draining:Number(this.drain.draining),ansts_body_recovery_required:Number(this.recoveryPending().length>0)});
      response.writeHead(200,{'content-type':'text/plain; version=0.0.4; charset=utf-8','cache-control':'no-store'});response.end(metrics+this.admissionMetrics.render());return;
    }
    if (request.method === "GET" && ["/health", "/healthz", "/readyz"].includes(route)) {
      let ready = !this.drain.draining && this.recoveryPending().length === 0&&!this.usageUnavailable;
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
    if(request.method==='POST'&&route.startsWith('/v1/daemon/usage/')){
      if(!this.usage){this.json(response,503,{error:'USAGE_METERING_NOT_CONNECTED'});return;}
      let body:Record<string,unknown>;try{body=await this.readBody(request);}catch{this.json(response,400,{error:'INVALID_USAGE_REQUEST'});return;}
      if(route==='/v1/daemon/usage/status'){this.json(response,200,{enabled:true,...this.usage.stats()});return;}
      if(route==='/v1/daemon/usage/pending'){
        const limit=body.limit??4;if(!Number.isSafeInteger(limit)||Number(limit)<1||Number(limit)>100){this.json(response,400,{error:'INVALID_USAGE_LIMIT'});return;}
        this.json(response,200,{events:this.usage.pending(Number(limit)).map(event=>({proof:signUsageEvent(event,this.options.token),digest:usageEventDigest(event)}))});return;
      }
      if(!['/v1/daemon/usage/ack','/v1/daemon/usage/defer'].includes(route)){this.json(response,404,{error:'NOT_FOUND'});return;}
      const {producerId,tenantId,taskId,attemptId,fencing,executionInputDigest,sequence,digest}=body;
      if(producerId!==this.options.nodeId||typeof digest!=='string'||!/^[a-f0-9]{64}$/.test(digest)||!Number.isSafeInteger(sequence)||Number(sequence)<1){this.json(response,400,{error:'INVALID_USAGE_RECEIPT'});return;}
      try{
        const identity={producerId,tenantId,taskId,attemptId,fencing,executionInputDigest} as UsageIdentity;
        const acknowledged=route.endsWith('/ack')?this.usage.acknowledge(identity,Number(sequence),digest):this.usage.defer(identity,Number(sequence),digest,body.quarantine===true);
        this.json(response,acknowledged?200:409,route.endsWith('/ack')?{acknowledged}:{deferred:acknowledged});
      }catch{this.json(response,409,{error:'USAGE_RECEIPT_CONFLICT'});}return;
    }
    if(request.method==='POST'&&route==='/v1/daemon/capacity-report'){
      let body;try{body=await this.readBody(request);}catch{this.json(response,400,{error:'INVALID_CAPACITY_REQUEST'});return;}
      if(Object.keys(body).length){this.json(response,400,{error:'INVALID_CAPACITY_REQUEST'});return;}
      if(!this.capacityReporter){this.json(response,503,{error:'PHYSICAL_HOST_IDENTITY_UNCONFIGURED'});return;}
      const resources=this.resources.snapshot(),slots=this.capacity.snapshot();
      const {proof}=this.capacityReporter.capture({total:{cpuMilliCores:Math.floor(resources.total.cpu*1000),memoryMiB:resources.total.memoryMb,slots:slots.slots},reserved:{cpuMilliCores:Math.ceil(resources.reserved.cpu*1000),memoryMiB:resources.reserved.memoryMb,slots:Math.max(slots.reserved,this.usage?.stats().unknownRetainedSlots??0)},draining:this.drain.draining,recoveryRequired:this.recoveryPending().length>0||this.usageUnavailable});
      this.json(response,200,{proof});return;
    }
    if(request.method==='POST'&&route==='/v1/daemon/execution-evidence/pending'){
      let body:Record<string,unknown>;try{body=await this.readBody(request);}catch{this.json(response,400,{error:'INVALID_EVIDENCE_REQUEST'});return;}
      if(Object.keys(body).some(key=>key!=='limit')||body.limit!==undefined&&(!Number.isSafeInteger(body.limit)||Number(body.limit)<1||Number(body.limit)>100)){this.json(response,400,{error:'INVALID_EVIDENCE_REQUEST'});return;}
      const facts=this.evidence.pending(body.limit===undefined?25:Number(body.limit));
      this.json(response,200,{observations:facts.map(fact=>({proof:signBodyEvidence(fact,this.options.token),digest:bodyEvidenceDigest(fact)}))});return;
    }
    if(request.method==='POST'&&['/v1/daemon/execution-evidence','/v1/daemon/execution-evidence/ack','/v1/daemon/execution-evidence/defer'].includes(route)){
      let identity:Record<string,unknown>;try{identity=await this.readBody(request);}catch{this.json(response,400,{error:'INVALID_EVIDENCE_REQUEST'});return;}
      if(![identity.tenantId,identity.taskId,identity.attemptId].every((value,index)=>typeof value==='string'&&value.trim().length>0&&value.length<=(index===0?128:512))){this.json(response,400,{error:'INVALID_EVIDENCE_REQUEST'});return;}
      const evidence=this.evidence.read(String(identity.tenantId),String(identity.taskId),String(identity.attemptId));
      if(!evidence){this.json(response,404,{error:'EXECUTION_EVIDENCE_UNKNOWN'});return;}
      if(route.endsWith('/ack')||route.endsWith('/defer')){
        if(typeof identity.digest!=='string'||!/^[a-f0-9]{64}$/.test(identity.digest)){this.json(response,400,{error:'INVALID_EVIDENCE_ACK'});return;}
        if(route.endsWith('/defer')){this.json(response,200,{deferred:this.evidence.defer(evidence,identity.digest)});return;}
        const acknowledged=this.evidence.acknowledge(evidence,identity.digest);
        this.json(response,acknowledged?200:409,{acknowledged,...(!acknowledged?{error:'STALE_EVIDENCE_ACK'}:{})});return;
      }
      this.json(response,200,{proof:signBodyEvidence(evidence,this.options.token)});return;
    }
    if (request.method === "GET" && route === "/v1/sandboxes") {
      this.json(response, 200, { sandboxes: [...this.active.values()].map(({ taskId, attemptId, fencingToken, startedAt }) => ({ taskId, attemptId, fencingToken, startedAt, state: "RUNNING" })) }); return;
    }
    if (request.method !== "POST" || !["/v1/sandboxes/execute", "/internal/v1beta1/body/execute", "/v1/sandboxes/stop"].includes(route)) {
      this.json(response, 404, { error: "NOT_FOUND" }); return;
    }
    if(route!=="/v1/sandboxes/stop")this.admissionResponses.add(response);
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
    let observation:BodyExecutionEvidence|undefined;
    if(route!=='/v1/sandboxes/stop'&&body.tenantAdmissionObservation!==undefined){
      try{
        if(!execution||!body.tenantAdmissionObservation||typeof body.tenantAdmissionObservation!=='object'||Array.isArray(body.tenantAdmissionObservation))throw new Error('EXECUTION_OBSERVATION_REQUIRED');
        observation=canonicalBodyEvidence({...body.tenantAdmissionObservation,schemaVersion:1,purpose:'ansts-body-execution-observation-v1',hostId:this.options.nodeId,tenantId:String(tenantId),taskId:String(taskId),attemptId:String(attemptId),fencing:Number(fencingToken),resources:{concurrentAttempts:1,cpuMilliCores:Math.ceil(execution.cpu*1000),memoryMiB:execution.memoryMb,queuedTasks:0},executionObserved:false,sequence:1,phase:'UNCERTAIN',observedAt:Date.now()} as BodyExecutionEvidence);
        if(observation.executionInputDigest!==taskInputDigest(tenantId,taskId,body.promptText,execution))throw new Error('OBSERVATION_INPUT_MISMATCH');
      }catch{this.json(response,400,{error:'INVALID_ADMISSION_OBSERVATION'});return;}
    }
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
    if(this.usageUnavailable){this.json(response,503,{error:'USAGE_SOURCE_UNAVAILABLE'});return;}
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
    let usageIdentity:UsageIdentity|undefined;
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
      if (!admission.admitted&&observation){const existing=this.evidence.read(String(tenantId),String(taskId),String(attemptId));if(!existing||existing.reservationId!==observation.reservationId||existing.requestDigest!==observation.requestDigest||existing.generation!==observation.generation||existing.executionInputDigest!==observation.executionInputDigest){this.json(response,409,{error:'EXECUTION_OBSERVATION_UNAVAILABLE'});return;}}
      if (!admission.admitted) { this.json(response, admission.result ? 200 : 409, admission.result ?? { error: "EXECUTION_OUTCOME_UNCERTAIN" }); return; }
      const resourceSnapshot=execution?{cpuNanoCores:Math.ceil(execution.cpu*1000000000),cpuMilliCores:Math.ceil(Math.ceil(execution.cpu*1000000000)/1000000),memoryMiB:execution.memoryMb,slots:1}:{cpuNanoCores:null,cpuMilliCores:null,memoryMiB:null,slots:1};
      if(this.usage&&(!this.options.usageTenantIds||this.options.usageTenantIds.includes(String(tenantId)))){usageIdentity={producerId:this.options.nodeId,tenantId:String(tenantId),taskId:String(taskId),attemptId:String(attemptId),fencing:Number(fencingToken),executionInputDigest:lease.input_digest};this.usage.begin(usageIdentity,resourceSnapshot,sandbox.startedAt);}
      if(observation)this.evidence.begin(observation);
      if(usageIdentity){
        const pinned=await this.rpc('/internal/v1/body-usage-binding',{identity:usageIdentity,resources:resourceSnapshot},controller.signal);
        if(!pinned.ok||(await pinned.json() as {bound?:boolean}).bound!==true)throw new Error('USAGE_BINDING_UNAVAILABLE');
      }

      this.admissionResponses.delete(response);this.admissionMetrics.record("accepted");
      if (leaseState.cancel_requested || sandbox.cancelRequested) {
        if(observation)this.evidence.advance(observation,"NOT_STARTED");
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
            ? await new AgentToolLoop(path.join(this.options.storageDir, "agent-turns"), this.options.agentModel!).run(JSON.stringify([tenantId, taskId, lease.input_digest]), Number(fencingToken), String(body.promptText ?? ""), execution, this.containerRuntime!, controller.signal, output, !!leaseState.resume_node_id,observation?()=>this.evidence.advance(observation!,"RUNNING"):undefined,usageIdentity?{started:(call,source)=>this.usage!.providerStarted(usageIdentity!,call,source),response:(call,usage)=>this.usage!.providerFinished(usageIdentity!,call,usage),uncertain:call=>this.usage!.providerUncertain(usageIdentity!,call)}:undefined)
            : await this.containerRuntime!.execute(execution, controller.signal, output,observation?{started:()=>this.evidence.advance(observation!,"RUNNING"),stopped:()=>this.evidence.advance(observation!,"TERMINATED")}:undefined);
          if(execution.kind==='agent'&&observation)this.evidence.advance(observation,'TERMINATED');
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
      if(observation){const saved=this.evidence.read(observation.tenantId,observation.taskId,observation.attemptId);if(saved&&!['NOT_STARTED','TERMINATED'].includes(saved.phase))this.evidence.advance(saved,'UNCERTAIN');}
      throw error;
    } finally {
      clearTimeout(timer); if (renewal) clearInterval(renewal);
      response.removeListener("close", disconnect); this.active.delete(key);
      try{if (!this.cleanup.pending().includes(key)&&sandbox.stopConfirmed!==false) { if(usageIdentity)this.usage!.finish(usageIdentity);this.resources.release(key); this.capacity.release(key); }}
      finally{done();}
    }
  }
}
