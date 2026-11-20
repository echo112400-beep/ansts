import {rejectUnavailableRegionPlacement} from '../control/region_placement_gate.js';
import {createHash} from "node:crypto";
import type {AdmissionReservation} from '../control/admission_reservations.js';
/**
 * @fileoverview M7 TASK-26, M52-1, M60-1, M64-1/M64-2 & M69-2: AnstsSchedulerCell (Stateful Durable Object Control Plane with SQLite Backing,
 * Monotonic Fencing Watermark Rehydration, Tenant-Scoped Idempotency Index Restoration, Active-Only Content-Fingerprint Rehydration,
 * Interrupted-Lease Node Capacity Reclamation, 100% TaskSpec Field Fidelity, Alarm Renewal & Worker Prefix-Aware Route Matching).
 */
import { SchedulerConfigTarget } from "./scheduler_config_target.js";
import type { CloudflareCelldEnv, DurableObjectStateLike } from "./env.js";
import { authenticateGatewayService, equalBodyToken, type BodyAuthConfiguration } from "./body_auth.js";
import { CoreScheduler5D } from "../scheduler.js";
import { NodeHeartbeat, LeaseRecord, ResourceProfile, TaskPriority, TaskSpec, TaskState } from "../models.js";
import {ExecutionInventory, type InventoryObservation} from '../control/execution_inventory.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';
import {taskInputDigest} from './execution_attempt.js';

const ACTIVE_FINGERPRINT_STATES = new Set<TaskState>([
  TaskState.SUBMITTED,
  TaskState.QUEUED,
  TaskState.LEASED,
  TaskState.RUNNING_THINK,
  TaskState.RUNNING_ACT,
  TaskState.WAITING_SUB_AGENT,
  TaskState.SUSPENDED_APPROVAL,
]);

export class AnstsSchedulerCell {
  private readonly ctx: DurableObjectStateLike;
  public readonly scheduler: CoreScheduler5D;
  private readonly configTarget:SchedulerConfigTarget|null;
  private readonly inventory:ExecutionInventory|null;

  constructor(ctx: DurableObjectStateLike, private readonly env:Pick<CloudflareCelldEnv,'ANSTS_CONFIG_TARGET_TOKEN'|'ANSTS_DIRECTORY_SERVICE_TOKEN'|'ANSTS_CAPACITY_INVENTORY_CELL'|'ANSTS_TENANT_CONTROL_CELL'|'ANSTS_ADMISSION_SERVICE_TOKEN'|'ANSTS_ADMISSION_PILOT_POOL'|'ANSTS_ADMISSION_MODE'|'ANSTS_ADMISSION_PILOT_TENANTS'>&BodyAuthConfiguration={}) {
    this.ctx = ctx;
    this.scheduler = new CoreScheduler5D(15.0);
    this.initSchema();
    const storage=ctx.storage as typeof ctx.storage & Partial<DirectoryStorage>;
    this.configTarget=typeof storage.transactionSync==='function'?new SchedulerConfigTarget(storage as DirectoryStorage):null;
    this.inventory=typeof storage.transactionSync==='function'?new ExecutionInventory(storage as DirectoryStorage,'SCHEDULER',ctx.id.toString(),()=>this.ctx.storage.sql.exec<{task_id:string;tenant_id:string;state:string;fencing_token:number;assigned_node_id:string;payload_json:string}>('SELECT * FROM ansts_tasks').toArray().map(row=>{
      const payload=JSON.parse(row.payload_json);
      return {tenantId:row.tenant_id,taskId:row.task_id,state:row.state,fencing:row.fencing_token??0,nodeId:row.assigned_node_id||null,attemptId:null,version:null,sourceAt:typeof payload.created_at==='number'?Math.round(payload.created_at*1000):null,inputDigest:taskInputDigest(row.tenant_id,row.task_id,payload.prompt,payload.execution)};
    })):null;
    this.rehydrateFromSql();
  }

  private initSchema(): void {
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS ansts_pilot_submission_accepted (reservation_id TEXT PRIMARY KEY)"
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS ansts_pilot_submissions (reservation_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, input_digest TEXT NOT NULL, expires_at INTEGER NOT NULL)"
    );

    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS ansts_tasks (task_id TEXT PRIMARY KEY, tenant_id TEXT, priority INTEGER, state TEXT, fencing_token INTEGER, assigned_node_id TEXT, payload_json TEXT)"
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS ansts_nodes (node_id TEXT PRIMARY KEY, heartbeat_json TEXT, updated_at INTEGER)"
    );
    this.ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS ansts_claims (task_id TEXT PRIMARY KEY, holder_id TEXT, claimed_at INTEGER)"
    );
  }

  public rehydrateSchedulerFromSql(): number {
    return this.rehydrateFromSql();
  }

  public async submitTaskRpc(task: TaskSpec, signal?: AbortSignal): Promise<TaskSpec> {
    if (signal?.aborted) {
      throw new Error("submitTaskRpc aborted by caller AbortSignal");
    }
    return this.submitTask(task);
  }

  public async scheduleTickRpc(signal?: AbortSignal): Promise<{ placements: Array<[string, string, number]>; recoveredIds: string[] }> {
    if (signal?.aborted) {
      throw new Error("scheduleTickRpc aborted by caller AbortSignal");
    }
    return this.alarm();
  }

  /**
   * [M52-1, M60-1, M64-2 & M69-2] Rehydrates registered nodes, monotonic fencing token watermarks, tenant-scoped `idempotencyIndex`,
   * active-only `contentFingerprintIndex`, full `TaskSpec` fields (`resource_profile`, `parent_task_id`, `expected_SubAgent_count`, `prefix_Cache_hash`),
   * `current_turn` breakpoint progress, and interrupted LEASED tasks (refunding pre-crash node CPU/memory/`running_tasks` so rehydrated nodes
   * never suffer permanent phantom capacity loss).
   */
  public rehydrateFromSql(): number {
    const nodeRows = this.ctx.storage.sql
      .exec<Record<string, unknown>>("SELECT * FROM ansts_nodes")
      .toArray();
    for (const nr of nodeRows) {
      if (!nr.heartbeat_json) continue;
      try {
        const parsedHb = JSON.parse(String(nr.heartbeat_json)) as Record<string, unknown>;
        this.scheduler.registerOrUpdateNode(
          new NodeHeartbeat({
            node_id: String(nr.node_id),
            total_cpu_cores: Number(parsedHb.total_cpu_cores ?? 32),
            free_cpu_cores: Number(parsedHb.free_cpu_cores ?? 32),
            total_memory_mb: Number(parsedHb.total_memory_mb ?? 65536),
            free_memory_mb: Number(parsedHb.free_memory_mb ?? 65536),
            warm_pool_ready: Number(parsedHb.warm_pool_ready ?? 8),
            psi_memory_some_avg10: Number(parsedHb.psi_memory_some_avg10 ?? 1.5),
            execution_slots: parsedHb.execution_slots === undefined ? undefined : Number(parsedHb.execution_slots),
              running_tasks: Number(parsedHb.running_tasks ?? 0),
            healthy: Boolean(parsedHb.healthy ?? true),
            timestamp: typeof parsedHb.timestamp === "number" && Number.isFinite(parsedHb.timestamp)
              ? parsedHb.timestamp
              : (Number(nr.updated_at) || 0) / 1000,
          })
        );
      } catch {
        // ignore malformed node row
      }
    }

    const rows = this.ctx.storage.sql
      .exec<Record<string, unknown>>("SELECT * FROM ansts_tasks")
      .toArray();
    let count = 0;
    for (const r of rows) {
      if (!r.payload_json) continue;
      const parsed = JSON.parse(String(r.payload_json)) as Record<string, unknown>;
      const rawProfile =
        parsed.resource_profile && typeof parsed.resource_profile === "object"
          ? (parsed.resource_profile as Partial<ResourceProfile>)
          : undefined;
      const spec = new TaskSpec({
        task_id: String(r.task_id),
        tenant_id: String(r.tenant_id),
        priority: Number(r.priority ?? TaskPriority.P2_STANDARD) as TaskPriority,
        execution: parsed.execution as TaskSpec["execution"],
        prompt: String(parsed.prompt ?? ""),
        max_turns: Number(parsed.max_turns ?? 30),
        timeout_seconds: Number(parsed.timeout_seconds ?? 1800),
        resource_profile: rawProfile,
        dependencies: Array.isArray(parsed.dependencies) ? parsed.dependencies.map(String) : [],
        idempotency_key: parsed.idempotency_key ? String(parsed.idempotency_key) : undefined,
        parent_task_id: parsed.parent_task_id ? String(parsed.parent_task_id) : undefined,
        expected_SubAgent_count:
          parsed.expected_SubAgent_count !== undefined ? Number(parsed.expected_SubAgent_count) : 0,
        prefix_Cache_hash: parsed.prefix_Cache_hash ? String(parsed.prefix_Cache_hash) : "",
        content_fingerprint: parsed.content_fingerprint ? String(parsed.content_fingerprint) : undefined,
        merged_duplicate_count:
          parsed.merged_duplicate_count !== undefined ? Number(parsed.merged_duplicate_count) : 0,
        merged_request_ids: Array.isArray(parsed.merged_request_ids)
          ? parsed.merged_request_ids.map(String)
          : [],
        merged_idempotency_keys: Array.isArray(parsed.merged_idempotency_keys)
          ? parsed.merged_idempotency_keys.map(String)
          : undefined,
      });
      if (parsed.current_turn !== undefined) {
        spec.current_turn = Number(parsed.current_turn);
      }
      if (parsed.last_preempted_at !== undefined) {
        spec.last_preempted_at = Number(parsed.last_preempted_at);
      }
      if (parsed.created_at !== undefined) {
        spec.created_at = Number(parsed.created_at);
      }
      spec.state = (r.state as TaskState) ?? TaskState.QUEUED;
      spec.cancel_requested = parsed.cancel_requested as TaskSpec["cancel_requested"];
      spec.execution_outcome = parsed.execution_outcome as TaskSpec["execution_outcome"];
      spec.resume_node_id = typeof parsed.resume_node_id === "string" ? parsed.resume_node_id : undefined;
      spec.capacity_target_node_id = typeof parsed.capacity_target_node_id === "string" ? parsed.capacity_target_node_id : undefined;
      spec.capacity_fencing = typeof parsed.capacity_fencing === "number" ? parsed.capacity_fencing : undefined;
      spec.capacity_expires_at = typeof parsed.capacity_expires_at === "number" ? parsed.capacity_expires_at : undefined;
      spec.capacity_attempt_id = typeof parsed.capacity_attempt_id === "string" ? parsed.capacity_attempt_id : undefined;
      spec.capacity_reservation_id = typeof parsed.capacity_reservation_id === "string" ? parsed.capacity_reservation_id : undefined;
      spec.capacity_receipt = typeof parsed.capacity_receipt === "string" ? parsed.capacity_receipt : undefined;
      spec.capacity_generation = typeof parsed.capacity_generation === "number" ? parsed.capacity_generation : undefined;
      spec.capacity_start_checked = parsed.capacity_start_checked === true;
      spec.lease_fencing_token = Number(r.fencing_token ?? 0);
      spec.retry_count = Number(parsed.retry_count ?? 0);
      spec.preempted_count = Number(parsed.preempted_count ?? 0);
      spec.dead_letter_reason = parsed.dead_letter_reason ? String(parsed.dead_letter_reason) : undefined;
      spec.assigned_node_id = r.assigned_node_id ? String(r.assigned_node_id) : undefined;
      if (!spec.content_fingerprint) {
        spec.content_fingerprint = CoreScheduler5D.computeTaskContentFingerprint(spec);
      }

      if (spec.lease_fencing_token > 0) {
        this.scheduler.leaseCoordinator.seedHighestFencingToken(spec.task_id, spec.lease_fencing_token);
      }

      const savedLease = parsed.execution_lease as LeaseRecord | undefined;
      if ([TaskState.LEASED, TaskState.RUNNING_THINK, TaskState.RUNNING_ACT].includes(spec.state) && savedLease && savedLease.task_id === spec.task_id && savedLease.node_id === spec.assigned_node_id && savedLease.fencing_token === spec.lease_fencing_token) {
        this.scheduler.leaseCoordinator.restoreLease(savedLease);
      }
      if (
        spec.state === TaskState.LEASED ||
        spec.state === TaskState.RUNNING_THINK ||
        spec.state === TaskState.RUNNING_ACT
      ) {
        if (!this.scheduler.leaseCoordinator.getLease(spec.task_id)) {
          if (spec.assigned_node_id) {
            const oldNode = this.scheduler.nodes.get(spec.assigned_node_id);
            if (oldNode) {
              oldNode.free_cpu_cores = Math.min(
                oldNode.total_cpu_cores,
                oldNode.free_cpu_cores + spec.resource_profile.think_cpu_cores
              );
              oldNode.free_memory_mb = Math.min(
                oldNode.total_memory_mb,
                oldNode.free_memory_mb + Math.floor(spec.resource_profile.act_memory_mb * 0.25)
              );
              oldNode.running_tasks = Math.max(0, oldNode.running_tasks - 1);
            }
          }
          spec.state = TaskState.QUEUED;
          spec.assigned_node_id = undefined;
        }
      }

      this.scheduler.allTasks.set(spec.task_id, spec);
      if (spec.idempotency_key) {
        this.scheduler.idempotencyIndex.set(
          CoreScheduler5D.scopeIdempotencyKey(spec.tenant_id, spec.idempotency_key),
          spec.task_id
        );
      }
      for (const mKey of spec.merged_idempotency_keys) {
        this.scheduler.idempotencyIndex.set(
          CoreScheduler5D.scopeIdempotencyKey(spec.tenant_id, mKey),
          spec.task_id
        );
      }
      if (ACTIVE_FINGERPRINT_STATES.has(spec.state)) {
        this.scheduler.contentFingerprintIndex.set(spec.content_fingerprint, spec.task_id);
      }

      if (spec.state === TaskState.QUEUED) {
        this.scheduler.queueManager.enqueue(spec);
      } else if (spec.state === TaskState.COMPLETED) {
        this.scheduler.completedIds.add(spec.task_id);
      } else if (spec.state === TaskState.DEAD_LETTER) {
        this.scheduler.deadLetterIds.add(spec.task_id);
      }
      count += 1;
    }
    return count;
  }

  private persistTask(task: TaskSpec): void {
    const write=()=>{
      if(task.capacity_reservation_id){
        // Persist acceptance in the same inventory transaction as the task row.
        this.ctx.storage.sql.exec(
          "INSERT OR IGNORE INTO ansts_pilot_submission_accepted VALUES (?)",task.capacity_reservation_id,
        );
      }
      this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO ansts_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
      task.task_id,
      task.tenant_id,
      task.priority,
      task.state,
      task.lease_fencing_token,
      task.assigned_node_id ?? "",
      JSON.stringify({
        execution_lease: this.scheduler.leaseCoordinator.getLease(task.task_id),
        capacity_target_node_id: task.capacity_target_node_id,
        capacity_attempt_id: task.capacity_attempt_id,
        capacity_reservation_id: task.capacity_reservation_id,
        capacity_receipt: task.capacity_receipt,
        capacity_generation: task.capacity_generation,
        capacity_fencing: task.capacity_fencing,
        capacity_expires_at: task.capacity_expires_at,
        capacity_start_checked: task.capacity_start_checked,
        execution: task.execution,
        cancel_requested: task.cancel_requested,
        execution_outcome: task.execution_outcome,
        resume_node_id: task.resume_node_id,
        prompt: task.prompt,
        max_turns: task.max_turns,
        current_turn: task.current_turn,
        timeout_seconds: task.timeout_seconds,
        resource_profile: task.resource_profile,
        dependencies: task.dependencies,
        idempotency_key: task.idempotency_key,
        parent_task_id: task.parent_task_id,
        expected_SubAgent_count: task.expected_SubAgent_count,
        prefix_Cache_hash: task.prefix_Cache_hash,
        content_fingerprint: task.content_fingerprint,
        merged_duplicate_count: task.merged_duplicate_count,
        merged_request_ids: task.merged_request_ids,
        merged_idempotency_keys: task.merged_idempotency_keys,
        retry_count: task.retry_count,
        preempted_count: task.preempted_count,
        last_preempted_at: task.last_preempted_at,
        dead_letter_reason: task.dead_letter_reason,
        created_at: task.created_at,
      })
    );
    };
    const observation:InventoryObservation={tenantId:task.tenant_id,taskId:task.task_id,inputDigest:taskInputDigest(task.tenant_id,task.task_id,task.prompt,task.execution),state:task.state,fencing:task.lease_fencing_token,nodeId:task.assigned_node_id??null,attemptId:null,version:null,sourceAt:Math.round(task.created_at*1000)};
    if(this.inventory)this.inventory.record(observation,write);else write();
  }

  public async submitTask(task: TaskSpec, options?: { mergeDuplicateInFlight?: boolean }): Promise<TaskSpec> {
    const failure = this.checkAdmissionPilot(task.tenant_id, false);
    if (failure) throw Object.assign(new Error(failure.code), failure);
    return this.persistSubmission(task, options);
  }

  private hasLegacyOccupancy(tenantId: string): boolean {
    return [...this.scheduler.allTasks.values()].some(task => task.tenant_id === tenantId &&
      !task.capacity_reservation_id && ![TaskState.COMPLETED, TaskState.FAILED, TaskState.DEAD_LETTER].includes(task.state));
  }

  private checkAdmissionPilot(tenantId: string, hasReservation: boolean): {code: string; status: number} | null {
    const mode = this.env.ANSTS_ADMISSION_MODE ?? "observe";
    const pilots = (this.env.ANSTS_ADMISSION_PILOT_TENANTS ?? "").split(",").map(id => id.trim()).filter(Boolean);
    if (!["observe", "shadow", "enforce"].includes(mode) || (mode === "enforce" && pilots.length === 0)) {
      return {code: "ADMISSION_PILOT_MISCONFIGURED", status: 503};
    }
    if (!pilots.includes(tenantId) || hasReservation) return null;
    if (mode === "enforce") return {code: "ADMISSION_RESERVATION_REQUIRED", status: 428};
    return null;
  }

  private async observeShadowQuota(spec:TaskSpec):Promise<void> {
    const tenantId=spec.tenant_id,execution=spec.execution,ns=this.env.ANSTS_TENANT_CONTROL_CELL;
    const token=this.env.ANSTS_ADMISSION_SERVICE_TOKEN;
    let code='ADMISSION_SHADOW_UNAVAILABLE';
    if(execution&&ns&&token&&token.length>=32&&token!==this.env.ANSTS_DIRECTORY_SERVICE_TOKEN){
      try {
        const response=await ns.get(ns.idFromName('tenant-control-v1:'+tenantId)).fetch(new Request(
          'https://tenant.internal/admission-shadow-preview',{
            method:'POST',signal:AbortSignal.timeout(1000),headers:{authorization:'Bearer '+token,'content-type':'application/json'},
            body:JSON.stringify({tenantId,actor:{sub:'system:scheduler',role:'OPERATOR',scopeTenant:tenantId,scopeRegion:'all'},
              input:{resources:{concurrentAttempts:1,queuedTasks:0,cpuMilliCores:Math.ceil(execution.cpu*1000),memoryMiB:execution.memoryMb}}}),
          },
        ));
        const result=await response.json() as {code?:string};
        code=result.code??code;
      } catch { /* A preview failure must not block shadow submissions. */ }
    }
    console.info('admission_shadow_quota',{tenantId,code});
  }

  private async preparePilotSubmission(spec: TaskSpec): Promise<string | Response> {
    const tenantId=spec.tenant_id, execution=spec.execution;
    const capacity=this.env.ANSTS_CAPACITY_INVENTORY_CELL, tenants=this.env.ANSTS_TENANT_CONTROL_CELL;
    const poolId=this.env.ANSTS_ADMISSION_PILOT_POOL, directoryKey=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
    const admissionKey=this.env.ANSTS_ADMISSION_SERVICE_TOKEN;
    const unavailable=()=>Response.json({code:'CAPACITY_UNAVAILABLE'},{status:503});
    if(!capacity||!tenants||!poolId||!directoryKey||directoryKey.length<32||
      !admissionKey||admissionKey.length<32||admissionKey===directoryKey)return unavailable();
    if(!execution)return Response.json({code:'ADMISSION_EXECUTION_REQUIRED'},{status:422});
    const actor={sub:'system:scheduler',role:'OPERATOR',scopeTenant:tenantId,scopeRegion:'all'};
    const key=createHash('sha256').update(JSON.stringify([tenantId,spec.task_id])).digest('hex');
    const digest=taskInputDigest(tenantId,spec.task_id,spec.prompt,execution);
    const resources={cpuMilliCores:Math.ceil(execution.cpu*1000),memoryMiB:execution.memoryMb};
    const call=(ns:typeof capacity,name:string,token:string,path:string,body:object)=>ns.get(ns.idFromName(name)).fetch(
      new Request('https://control.internal/'+path,{method:'POST',signal:AbortSignal.timeout(3500),
        headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify({actor,...body})}));
    const cap=(path:string,requestInput:object)=>call(capacity,'capacity-inventory-v1',directoryKey,path,{requestInput});
    const admit=(path:string,body:object)=>call(tenants,'tenant-control-v1:'+tenantId,admissionKey,path,{tenantId,...body});
    try {
      const requested=await cap('request-submit',{tenantId,poolId,key,resources:{...resources,slots:1}});
      if(!requested.ok)return requested;
      const {id,state}=await requested.json() as {id:string;state:string};
      if(state==='QUEUED')return Response.json({code:'CAPACITY_PENDING'},{status:409,headers:{'retry-after':'1'}});
      if(state!=='GRANTED')return Response.json({code:'CAPACITY_REQUEST_INACTIVE'},{status:409});
      const granted=await cap('grant-read',{tenantId,poolId,id});
      if(!granted.ok)return granted;
      const {grant}=await granted.json() as {grant:{target:string;capacityReceipt:string}|null};
      if(!grant)return Response.json({code:'CAPACITY_TARGET_UNAVAILABLE'},{status:409});
      const prepared=await admit('admission-prepare-estimated',{idempotencyKey:'prepare-'+id,input:{
        reservationId:id,attemptId:id,taskId:spec.task_id,fencing:1,executionInputDigest:digest,schedulerObjectId:this.ctx.id.toString(),
        resources:{...resources,concurrentAttempts:1,queuedTasks:0},
      }});
      if(!prepared.ok)return prepared;
      let {reservation}=await prepared.json() as {reservation:AdmissionReservation};
      const transition=(to:string,fields:object={})=>admit('admission-transition',{idempotencyKey:to+'-'+id,
        input:{reservationId:id,expectedVersion:reservation.version,generation:reservation.generation,
          fencing:reservation.fencing,to,reason:'pilot task submission',...fields}});
      if(reservation.phase==='PREPARING'){
        const reserved=await transition('RESERVED',{target:grant.target,capacityReceipt:grant.capacityReceipt});
        if(!reserved.ok)return reserved;
        ({reservation}=await reserved.json() as {reservation:AdmissionReservation});
      }
      if(reservation.phase==='RESERVED'){
        // Journal the submission before asking the tenant to authorize a start.
        this.ctx.storage.sql.exec(
          "INSERT OR IGNORE INTO ansts_pilot_submissions (reservation_id, task_id, input_digest, expires_at) VALUES (?, ?, ?, ?)",
          id,spec.task_id,digest,reservation.expiresAt,
        );

        const authorized=await transition('START_AUTHORIZED');
        if(!authorized.ok)return authorized;
        ({reservation}=await authorized.json() as {reservation:AdmissionReservation});
      }
      if(reservation.phase!=='START_AUTHORIZED')return Response.json({code:'ADMISSION_RESERVATION_INACTIVE'},{status:409});
      return id;
    } catch {
      return unavailable();
    }
  }

  private async persistSubmission(task: TaskSpec, options?: { mergeDuplicateInFlight?: boolean }): Promise<TaskSpec> {
    const submitted = this.scheduler.submitTask(task, options);
    this.persistTask(submitted);
    const currentAlarm = await this.ctx.storage.getAlarm();
    if (!currentAlarm) {
      await this.ctx.storage.setAlarm(Date.now() + 1000);
    }
    return submitted;
  }

  public registerNode(hb: NodeHeartbeat): void {
    this.scheduler.registerOrUpdateNode(hb);
    this.ctx.storage.sql.exec(
      "INSERT OR REPLACE INTO ansts_nodes (node_id, heartbeat_json, updated_at) VALUES (?, ?, ?)",
      hb.node_id,
      JSON.stringify(hb),
      Date.now()
    );
  }

  /**
   * [M52-1] Persists all reconciled/dead-lettered tasks as well as newly placed tasks,
   * and renews the DO alarm whenever queued tasks OR active leases exist.
   */
  public async alarm(): Promise<{ placements: Array<[string, string, number]>; recoveredIds: string[] }> {
    const recoveredIds = this.scheduler.reconcileOrphanedLeases();
    const placements = this.scheduler.scheduleTick();

    for (const t of this.scheduler.allTasks.values()) {
      this.persistTask(t);
    }

    if (
      this.scheduler.queueManager.totalQueued() > 0 ||
      this.scheduler.leaseCoordinator.activeLeaseCount() > 0
    ) {
      await this.ctx.storage.setAlarm(Date.now() + 1000);
    }
    return { placements, recoveredIds };
  }

  /**
   * [M64-1] Supports both direct Cell paths (`/submit`, `/schedule-tick`, `/register-node`, `/tasks`)
   * and Worker-forwarded prefixed paths (`/v1/scheduler/submit`, `/v1/scheduler/schedule-tick`, etc.).
   */
  public async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const normalizedPath = url.pathname.replace(/^\/v1\/scheduler(?=\/|$)/, "") || "/";
    if(normalizedPath==='/config-readiness'&&request.method==='GET'){
      const threshold=this.configTarget?.unhealthyNodeThresholdBps;
      if(threshold===undefined)return new Response(null,{status:204});
      const nodes=[...this.scheduler.nodes.values()],now=Date.now()/1000;
      const unhealthy=nodes.filter(node=>!node.healthy||now-node.timestamp>this.scheduler.nodeHeartbeatTtlSeconds).length;
      const healthy=nodes.length>0&&unhealthy*10000<=threshold*nodes.length;
      return Response.json({healthy,nodeCount:nodes.length,unhealthyNodeCount:unhealthy,unhealthyNodeThresholdBps:threshold},
        {status:healthy?200:503});
    }
    if(normalizedPath.startsWith('/config-target/')){
      return this.configTarget?.fetch(request,normalizedPath,this.env.ANSTS_CONFIG_TARGET_TOKEN)??
        Response.json({code:'CONFIG_TARGET_UNAVAILABLE'},{status:503});
    }
    if(normalizedPath==='/capacity-grant'){
      const token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN,ns=this.env.ANSTS_CAPACITY_INVENTORY_CELL;
      if(request.method!=='POST')return Response.json({code:'METHOD_NOT_ALLOWED'},{status:405});
      if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token)){
        return Response.json({code:'SERVICE_AUTH_REQUIRED'},{status:401});
      }
      if(!ns)return Response.json({code:'CAPACITY_UNAVAILABLE'},{status:503});
      try{
        const raw=await request.text();
        if(raw.length>4096)return Response.json({code:'PAYLOAD_TOO_LARGE'},{status:413});
        const {poolId,tenantId,requestId,binding}=JSON.parse(raw) as {
          poolId:string;tenantId:string;requestId:string;
          binding?:{reservationId:string;attemptId:string;generation:number;fencing:number};
        };
        if(typeof poolId!=='string'||typeof tenantId!=='string'||typeof requestId!=='string'){
          return Response.json({code:'INVALID_CAPACITY_INPUT'},{status:422});
        }
        const actor={sub:'system:scheduler',role:'OPERATOR',scopeTenant:tenantId,scopeRegion:'all'};
        return await ns.get(ns.idFromName('capacity-inventory-v1')).fetch(new Request(
          'https://capacity.internal/grant-read',{
            method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},
            body:JSON.stringify({actor,requestInput:{poolId,tenantId,id:requestId,binding}}),
          },
        ));
      }catch(error){
        return Response.json({code:error instanceof SyntaxError?'INVALID_JSON':'CAPACITY_UNAVAILABLE'},
          {status:error instanceof SyntaxError?400:503});
      }
    }
    if(normalizedPath==='/execution-inventory') {
      if(request.method!=='GET'||request.headers.get('x-ansts-verified-inventory')!=='true')return Response.json({code:'INTERNAL_ROUTE'},{status:403});
      if(!this.inventory)return Response.json({code:'EXECUTION_INVENTORY_UNAVAILABLE'},{status:503});
      const tenantId=url.searchParams.get('tenantId')??'',after=url.searchParams.get('after');
      if(after!==null&&!/^\d+$/.test(after))return Response.json({code:'INVALID_INVENTORY_SCOPE'},{status:422});
      try {
        const result=after===null?this.inventory.snapshot(tenantId):this.inventory.page(tenantId,Number(after));
        return Response.json(result,{headers:{'cache-control':'no-store'}});
      } catch(error) {
        const failure=error as {code?:string;status?:number};
        return Response.json({code:failure.code??'EXECUTION_INVENTORY_UNAVAILABLE'},{status:failure.status??503});
      }
    }

    if (request.method === "POST" && normalizedPath === "/pilot-expiry-status") {
      const token=this.env.ANSTS_ADMISSION_SERVICE_TOKEN;
      if(!token||token.length<32||!equalBodyToken(request.headers.get("authorization")??"","Bearer "+token))
        return Response.json({code:"SERVICE_AUTH_REQUIRED"},{status:401});
      const {reservationId,taskId,inputDigest}=await request.json() as Record<string,unknown>;
      if(typeof reservationId!=="string"||typeof taskId!=="string"||typeof inputDigest!=="string")
        return Response.json({code:"INVALID_ADMISSION_INPUT"},{status:422});
      const intent=this.ctx.storage.sql.exec<{expires_at:number}>(
        "SELECT expires_at FROM ansts_pilot_submissions WHERE reservation_id=? AND task_id=? AND input_digest=?",
        reservationId,taskId,inputDigest,
      ).toArray()[0];
      const accepted=this.ctx.storage.sql.exec(
        "SELECT reservation_id FROM ansts_pilot_submission_accepted WHERE reservation_id=?",reservationId,
      ).toArray().length>0;
      return Response.json({expiredBeforeAcceptance:!!intent&&intent.expires_at<=Date.now()&&
        !accepted&&!this.scheduler.allTasks.has(taskId)});
    }

    if (request.method === "GET" && normalizedPath === "/pilot-occupancy") {
      const token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
      if(!token||token.length<32||!equalBodyToken(request.headers.get("authorization")??"","Bearer "+token))
        return Response.json({code:"SERVICE_AUTH_REQUIRED"},{status:401});
      const tenantId=url.searchParams.get("tenantId");
      if(!tenantId)return Response.json({code:"INVALID_TENANT"},{status:422});
      return Response.json({occupied:this.hasLegacyOccupancy(tenantId)});
    }

    if (request.method === "POST" && normalizedPath === "/submit") {
      const body = (await request.json()) as Record<string, unknown>;
      const regionalDenial=rejectUnavailableRegionPlacement(body);
      if(regionalDenial)return regionalDenial;
      const defaultTimeout=this.configTarget?.defaultTaskTimeoutSeconds;
      const execution=body.execution as TaskSpec["execution"];
      if(body.timeout_seconds===undefined&&defaultTimeout!==undefined&&execution&&execution.timeoutMs>defaultTimeout*1000){
        return Response.json({code:'EXECUTION_EXCEEDS_CONFIGURED_TIMEOUT'},{status:422});
      }
      const spec = new TaskSpec({
        task_id: String(body.task_id ?? `task-${Date.now()}`),
        tenant_id: String(body.tenant_id ?? "default"),
        priority: Number(body.priority ?? TaskPriority.P2_STANDARD) as TaskPriority,
        execution: body.execution as TaskSpec["execution"],
        prompt: String(body.prompt ?? ""),
        max_turns: body.max_turns !== undefined ? Number(body.max_turns) : undefined,
        timeout_seconds: body.timeout_seconds !== undefined ? Number(body.timeout_seconds) : defaultTimeout,
        resource_profile:
          body.resource_profile && typeof body.resource_profile === "object"
            ? (body.resource_profile as Partial<ResourceProfile>)
            : undefined,
        dependencies: Array.isArray(body.dependencies) ? body.dependencies.map(String) : undefined,
        idempotency_key: body.idempotency_key ? String(body.idempotency_key) : undefined,
        parent_task_id: body.parent_task_id ? String(body.parent_task_id) : undefined,
        expected_SubAgent_count:
          body.expected_SubAgent_count !== undefined ? Number(body.expected_SubAgent_count) : undefined,
        prefix_Cache_hash: body.prefix_Cache_hash ? String(body.prefix_Cache_hash) : undefined,
      });
      const pilotFailure = this.checkAdmissionPilot(spec.tenant_id, body.capacity_reservation_id !== undefined);
      if (this.env.ANSTS_ADMISSION_MODE === "enforce" &&
          (this.env.ANSTS_ADMISSION_PILOT_TENANTS ?? "").split(",").some(id => id.trim() === spec.tenant_id) &&
          this.hasLegacyOccupancy(spec.tenant_id)) {
        return Response.json({code:"ADMISSION_LEGACY_OCCUPANCY_PRESENT"}, {status:409});
      }

      if (pilotFailure?.status === 428 && this.env.ANSTS_ADMISSION_PILOT_POOL && authenticateGatewayService(request,this.env)) {
        const previous = this.scheduler.allTasks.get(spec.task_id);
        if (previous && (!previous.capacity_reservation_id || previous.tenant_id !== spec.tenant_id ||
           taskInputDigest(previous.tenant_id,previous.task_id,previous.prompt,previous.execution) !==
             taskInputDigest(spec.tenant_id,spec.task_id,spec.prompt,spec.execution))) {
          return Response.json({code:'CAPACITY_TASK_BINDING_CONFLICT'},{status:409});
        }
        if (previous) return Response.json({task_id:previous.task_id,tenant_id:previous.tenant_id,
          state:previous.state,lease_fencing_token:previous.lease_fencing_token});
        const prepared = await this.preparePilotSubmission(spec);
        if (typeof prepared !== 'string') return prepared;
        body.capacity_reservation_id = prepared;
      } else if (pilotFailure) {
        return Response.json({code: pilotFailure.code}, {status: pilotFailure.status});
      }
      if(this.env.ANSTS_ADMISSION_MODE==='shadow'&&body.capacity_reservation_id===undefined&&
         (this.env.ANSTS_ADMISSION_PILOT_TENANTS??'').split(',').some(id=>id.trim()===spec.tenant_id)&&
         authenticateGatewayService(request,this.env))await this.observeShadowQuota(spec);
      if(body.capacity_reservation_id!==undefined){
        const directoryToken=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
        const token=this.env.ANSTS_ADMISSION_SERVICE_TOKEN,ns=this.env.ANSTS_TENANT_CONTROL_CELL;
        if(!directoryToken||directoryToken.length<32||
          (!equalBodyToken(request.headers.get('authorization')??'','Bearer '+directoryToken)&&!authenticateGatewayService(request,this.env))){
          return Response.json({code:'SERVICE_AUTH_REQUIRED'},{status:401});
        }
        if(!token||token===directoryToken||!ns)return Response.json({code:'CAPACITY_UNAVAILABLE'},{status:503});
        if(typeof body.capacity_reservation_id!=='string'||!body.capacity_reservation_id)
          return Response.json({code:'INVALID_CAPACITY_INPUT'},{status:422});
        const tenantId=spec.tenant_id,actor={sub:'system:scheduler',role:'OPERATOR',scopeTenant:tenantId,scopeRegion:'all'};
        const response=await ns.get(ns.idFromName('tenant-control-v1:'+tenantId)).fetch(new Request(
          'https://tenant.internal/admission-read',{
            method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},
            body:JSON.stringify({actor,tenantId,reservationId:body.capacity_reservation_id}),
          },
        )).catch(() => null);
        if (!response) return Response.json({code:"CAPACITY_UNAVAILABLE"}, {status:503});
        if(!response.ok)return Response.json({code:'CAPACITY_RESERVATION_UNAVAILABLE'},{status:response.status});
        const {reservation}=await response.json() as {reservation:AdmissionReservation};
        if(!reservation||reservation.tenantId!==tenantId||reservation.taskId!==spec.task_id||
          (reservation.schedulerObjectId!==undefined&&reservation.schedulerObjectId!==this.ctx.id.toString())||
          reservation.phase!=='START_AUTHORIZED'||!reservation.startAuthorized||!reservation.target||
          !/^capacity-grant:[a-f0-9]{64}$/.test(reservation.capacityReceipt??'')||reservation.expiresAt<=Date.now()||
          !spec.execution||Math.ceil(spec.execution.cpu*1000)!==reservation.resources.cpuMilliCores||
          spec.execution.memoryMb!==reservation.resources.memoryMiB||
          reservation.executionInputDigest!==taskInputDigest(tenantId,spec.task_id,spec.prompt,spec.execution)){
          return Response.json({code:'CAPACITY_RESERVATION_MISMATCH'},{status:409});
        }
        const capacity=this.env.ANSTS_CAPACITY_INVENTORY_CELL;
        if(!capacity)return Response.json({code:'CAPACITY_UNAVAILABLE'},{status:503});
        const checked=await capacity.get(capacity.idFromName('capacity-inventory-v1')).fetch(new Request(
          'https://capacity.internal/grant-read',{
            method:'POST',headers:{authorization:'Bearer '+directoryToken,'content-type':'application/json'},
            body:JSON.stringify({actor,requestInput:{
              tenantId,id:reservation.capacityReceipt!.slice('capacity-grant:'.length),
              binding:{reservationId:reservation.id,attemptId:reservation.attemptId,
                generation:reservation.generation,fencing:reservation.fencing},
            }}),
          },
        )).catch(() => null);
        if (!checked) return Response.json({code:"CAPACITY_UNAVAILABLE"}, {status:503});
        if(!checked.ok){
          await checked.body?.cancel();
          return Response.json({code:'CAPACITY_UNAVAILABLE'},{status:503});
        }
        const {grant}=await checked.json() as {grant:{target:string;capacityReceipt:string}|null};
        if(!grant||grant.target!==reservation.target||grant.capacityReceipt!==reservation.capacityReceipt){
          return Response.json({code:'CAPACITY_TARGET_UNAVAILABLE'},{status:409});
        }
        if(reservation.expiresAt<=Date.now())return Response.json({code:'ADMISSION_EXPIRED'},{status:409});
        // Capacity tasks use the same CPU and memory at placement and execution.
        spec.resource_profile = {...spec.resource_profile,
          think_cpu_cores:spec.execution.cpu,act_cpu_cores:spec.execution.cpu,act_memory_mb:spec.execution.memoryMb};
        spec.capacity_target_node_id=reservation.target;
        spec.capacity_attempt_id=reservation.attemptId;
        spec.capacity_reservation_id=reservation.id;
        spec.capacity_receipt=reservation.capacityReceipt!;
        spec.capacity_generation=reservation.generation;
        spec.capacity_fencing=reservation.fencing;
        spec.capacity_expires_at=reservation.expiresAt;
      }
      const existing=this.scheduler.allTasks.get(spec.task_id);
      const capacitySubmission=Boolean(spec.capacity_target_node_id);
      if (capacitySubmission && spec.idempotency_key) {
        return Response.json({code:'CAPACITY_TASK_ID_REQUIRED'},{status:422});
      }
      if (existing && (existing.capacity_target_node_id || capacitySubmission)) {
        const sameBinding=capacitySubmission&&existing.tenant_id===spec.tenant_id&&
          existing.capacity_reservation_id===spec.capacity_reservation_id&&existing.capacity_attempt_id===spec.capacity_attempt_id&&
          existing.capacity_fencing===spec.capacity_fencing&&existing.capacity_generation===spec.capacity_generation&&
          existing.capacity_receipt===spec.capacity_receipt&&existing.capacity_target_node_id===spec.capacity_target_node_id&&
          taskInputDigest(existing.tenant_id,existing.task_id,existing.prompt,existing.execution)===
            taskInputDigest(spec.tenant_id,spec.task_id,spec.prompt,spec.execution);
        if (!sameBinding) return Response.json({code:'CAPACITY_TASK_BINDING_CONFLICT'},{status:409});
      }
      const submitted = existing && capacitySubmission ? existing : await this.persistSubmission(spec, {
        mergeDuplicateInFlight: !capacitySubmission && Boolean(body.merge_duplicate ?? body.deduplicate_by_fingerprint ?? false),
      });
      return new Response(
        JSON.stringify({
          task_id: submitted.task_id,
          tenant_id: submitted.tenant_id,
          state: submitted.state,
          priority: submitted.priority,
          merged_duplicate_count: submitted.merged_duplicate_count,
        }),
        {
          status: 202,
          headers: { "content-type": "application/json" },
        }
      );
    }

    if (request.method === "POST" && normalizedPath === "/register-node") {
      const body = (await request.json()) as Record<string, unknown>;
      const hb = new NodeHeartbeat({
        node_id: String(body.node_id ?? "body-node-01"),
        total_cpu_cores: Number(body.total_cpu_cores ?? 32),
        free_cpu_cores: Number(body.free_cpu_cores ?? 32),
        total_memory_mb: Number(body.total_memory_mb ?? 65536),
        free_memory_mb: Number(body.free_memory_mb ?? 65536),
        warm_pool_ready: Number(body.warm_pool_ready ?? 8),
        psi_memory_some_avg10: Number(body.psi_memory_some_avg10 ?? 1.0),
        execution_slots: body.execution_slots === undefined ? undefined : Number(body.execution_slots),
          running_tasks: Number(body.running_tasks ?? 0),
        healthy: body.healthy !== undefined ? Boolean(body.healthy) : true,
      });
      this.registerNode(hb);
      return new Response(JSON.stringify({ registered: true, node_id: hb.node_id }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }

    if (request.method === "POST" && normalizedPath === "/claim") {
      const body = (await request.json()) as Record<string, unknown>;
      const taskId = String(body.task_id ?? "");
      const holderId = String(body.holder_id ?? "");
      const task = this.scheduler.allTasks.get(taskId);
      if (!task) {
        return new Response(JSON.stringify({ claimed: false, reason: "TASK_NOT_FOUND" }), {
          status: 404,
          headers: { "content-type": "application/json" },
        });
      }
      // The cell is a single-threaded Durable Object, so this check-then-insert is the atomic arbiter that
      // makes exactly one gateway execute a placed task even when several gateways poll the same cell.
      const existingRow = this.ctx.storage.sql
        .exec<Record<string, unknown>>("SELECT holder_id FROM ansts_claims WHERE task_id = ?", taskId)
        .toArray()[0];
      const existingHolder = existingRow ? String(existingRow.holder_id) : null;
      if (existingHolder && existingHolder !== holderId) {
        return new Response(JSON.stringify({ claimed: false, holder_id: existingHolder, state: task.state }), {
          status: 409,
          headers: { "content-type": "application/json" },
        });
      }
      if (!existingHolder) {
        this.ctx.storage.sql.exec(
          "INSERT OR REPLACE INTO ansts_claims (task_id, holder_id, claimed_at) VALUES (?, ?, ?)",
          taskId,
          holderId,
          Date.now()
        );
      }
      return new Response(JSON.stringify({ claimed: true, holder_id: holderId, state: task.state }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }

    if (request.method === "POST" && normalizedPath === "/lease-renew") {
      const body = (await request.json()) as Record<string, unknown>;
      const taskId = String(body.task_id ?? "");
      const nodeId = String(body.node_id ?? "");
      const fencingToken = Number(body.fencing_token ?? 0);
      const task = this.scheduler.allTasks.get(taskId);
      if (!task) {
        return new Response(JSON.stringify({ renewed: false, reason: "TASK_NOT_FOUND" }), {
          status: 404,
          headers: { "content-type": "application/json" },
        });
      }
      if (task.capacity_target_node_id &&
          (nodeId !== task.capacity_target_node_id || fencingToken !== task.capacity_fencing ||
           !task.capacity_attempt_id || body.attempt_id !== task.capacity_attempt_id ||
           body.tenant_id !== task.tenant_id ||
           body.input_digest !== taskInputDigest(task.tenant_id,task.task_id,task.prompt,task.execution))) {
        return Response.json({renewed:false,reason:"CAPACITY_LEASE_BINDING_MISMATCH"},{status:409});
      }
      if (task.capacity_target_node_id && (body.start_check === true || !task.capacity_start_checked)) {
        const token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN,capacity=this.env.ANSTS_CAPACITY_INVENTORY_CELL;
        if(!token||token.length<32||!capacity||!task.capacity_receipt||!task.capacity_reservation_id||
           !task.capacity_generation||!task.capacity_expires_at||task.capacity_expires_at<=Date.now()){
          return Response.json({renewed:false,reason:"CAPACITY_START_UNAVAILABLE"},{status:409});
        }
        const actor={sub:'system:scheduler',role:'OPERATOR',scopeTenant:task.tenant_id,scopeRegion:'all'};
        const tenants=this.env.ANSTS_TENANT_CONTROL_CELL,admissionKey=this.env.ANSTS_ADMISSION_SERVICE_TOKEN;
        const unavailable=(status:number)=>Response.json({renewed:false,reason:"CAPACITY_START_UNAVAILABLE"},{status});
        if(!tenants||!admissionKey||admissionKey===token)return unavailable(503);
        const authority=await tenants.get(tenants.idFromName('tenant-control-v1:'+task.tenant_id)).fetch(new Request(
          'https://tenant.internal/admission-read',{
            method:'POST',signal:AbortSignal.timeout(3500),
            headers:{authorization:'Bearer '+admissionKey,'content-type':'application/json'},
            body:JSON.stringify({actor,tenantId:task.tenant_id,reservationId:task.capacity_reservation_id}),
          },
        )).catch(()=>null);
        if(!authority?.ok){await authority?.body?.cancel();return unavailable(503);}
        const {reservation}=await authority.json() as {reservation:AdmissionReservation|null};
        if(!reservation||reservation.phase!=='START_AUTHORIZED'||!reservation.startAuthorized||
           reservation.id!==task.capacity_reservation_id||reservation.taskId!==taskId||
           reservation.tenantId!==task.tenant_id||reservation.attemptId!==task.capacity_attempt_id||
           reservation.target!==nodeId||reservation.generation!==task.capacity_generation||
           reservation.fencing!==fencingToken||reservation.capacityReceipt!==task.capacity_receipt||
           reservation.executionInputDigest!==body.input_digest||reservation.expiresAt!==task.capacity_expires_at)
          return unavailable(409);
        const result=await capacity.get(capacity.idFromName('capacity-inventory-v1')).fetch(new Request(
          'https://capacity.internal/grant-read',{
            method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},
            body:JSON.stringify({actor,requestInput:{
              tenantId:task.tenant_id,id:task.capacity_receipt.slice('capacity-grant:'.length),
              binding:{reservationId:task.capacity_reservation_id,attemptId:task.capacity_attempt_id,
                generation:task.capacity_generation,fencing:fencingToken},
            }}),
          },
        ));
        if(!result.ok)return Response.json({renewed:false,reason:"CAPACITY_START_UNAVAILABLE"},{status:503});
        const {grant}=await result.json() as {grant:{target:string;capacityReceipt:string}|null};
        if(grant?.target!==nodeId||grant?.capacityReceipt!==task.capacity_receipt||
           task.capacity_expires_at<=Date.now()||task.cancel_requested){
          return Response.json({renewed:false,reason:"CAPACITY_START_UNAVAILABLE"},{status:409});
        }
      }
      const renewed = this.scheduler.leaseCoordinator.renewLease(taskId, nodeId, fencingToken);
      if (renewed) {
        if(task.capacity_target_node_id)task.capacity_start_checked=true;
        this.persistTask(task);
      }
      return new Response(JSON.stringify({ renewed, task_id: taskId, state: task.state, capacity_observation_required: Boolean(task.capacity_target_node_id), lease_expires_at: this.scheduler.leaseCoordinator.getLease(taskId)?.expires_at, cancel_requested: task.cancel_requested, resume_node_id: task.resume_node_id }), {
        status: renewed ? 200 : 409,
        headers: { "content-type": "application/json" },
      });
    }

    if (request.method === "POST" && normalizedPath === "/cancel") {
      const body = await request.json() as Record<string, unknown>;
      const task = this.scheduler.allTasks.get(String(body.task_id));
      if (!task || task.tenant_id !== body.tenant_id) return Response.json({ error: "TASK_NOT_FOUND" }, { status: 404 });
      if (typeof body.actor !== "string" || !body.actor.trim() || body.actor.length > 256 || typeof body.reason !== "string" || body.reason.length > 1024) return Response.json({ error: "INVALID_CANCEL_REQUEST" }, { status: 400 });
      const cancelled = this.scheduler.requestCancellation(task.task_id, task.tenant_id, body.actor, body.reason);
      if (cancelled) for (const changed of this.scheduler.allTasks.values()) this.persistTask(changed);
      return Response.json({ cancelled, state: task.state, cancel_requested: task.cancel_requested, execution_outcome: task.execution_outcome }, { status: cancelled ? 200 : 409 });
    }
    if (request.method === "POST" && normalizedPath === "/resolve-uncertain") {
      const body = await request.json() as Record<string, unknown>;
      const resolved = this.scheduler.resolveReconciliation(String(body.task_id), String(body.tenant_id), Number(body.fencing_token), body.retry === true);
      const task = this.scheduler.allTasks.get(String(body.task_id));
      if (resolved && task) { if (typeof body.resume_node_id === "string") task.resume_node_id = body.resume_node_id; this.persistTask(task); }
      return Response.json({ resolved }, { status: resolved ? 200 : 409 });
    }
    if (request.method === "POST" && normalizedPath === "/suspend-uncertain") {
      const body = await request.json() as Record<string, unknown>;
      const taskId = String(body.task_id ?? "");
      const suspended = this.scheduler.suspendForReconciliation(taskId, String(body.node_id ?? ""), Number(body.fencing_token));
      const task = this.scheduler.allTasks.get(taskId);
      if (suspended && task) this.persistTask(task);
      return Response.json({ suspended, task_id: taskId }, { status: suspended ? 200 : 409 });
    }
    if (request.method === "POST" && normalizedPath === "/complete") {
      const body = (await request.json()) as Record<string, unknown>;
      const taskId = String(body.task_id ?? "");
      const existing = this.scheduler.allTasks.get(taskId);
      const outcome = body.execution_outcome;
      if (outcome !== undefined && !["SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"].includes(String(outcome))) return Response.json({ error: "INVALID_EXECUTION_OUTCOME" }, { status: 400 });
      if (existing && existing.assigned_node_id === body.node_id && existing.lease_fencing_token === Number(body.fencing_token) && [TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED].includes(existing.state) && existing.execution_outcome === outcome) return Response.json({ completed: true, task_id: taskId });
      const completed = this.scheduler.completeTaskExecution(
        taskId,
        String(body.node_id ?? ""),
        Number(body.fencing_token ?? 0),
        Number(body.observed_peak_cpu ?? 1.0),
        Number(body.observed_peak_memory_mb ?? 1024)
      );
      if (completed) {
        const task = this.scheduler.allTasks.get(taskId);
        if (task) {
          task.execution_outcome = outcome as TaskSpec["execution_outcome"];
          if (outcome === "FAILED" || outcome === "TIMED_OUT" || outcome === "CANCELLED") {
            task.state = outcome === "CANCELLED" ? TaskState.CANCELLED : TaskState.FAILED;
            this.scheduler.completedIds.delete(taskId);
            task.dead_letter_reason = String(outcome);
            this.scheduler.cascadeDeadLetterToDependents(taskId);
          }
          for (const changed of this.scheduler.allTasks.values()) this.persistTask(changed);
        }
        this.ctx.storage.sql.exec("DELETE FROM ansts_claims WHERE task_id = ?", taskId);
      }
      return new Response(JSON.stringify({ completed, task_id: taskId }), {
        status: completed ? 200 : 409,
        headers: { "content-type": "application/json" },
      });
    }

    if (request.method === "POST" && normalizedPath === "/schedule-tick") {
      const res = await this.alarm();
      return new Response(JSON.stringify(res), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }

    if (request.method === "POST" && normalizedPath === "/purge") {
      const body = (await request.json()) as Record<string, unknown>;
      const states = Array.isArray(body.states)
        ? body.states.map((state) => String(state) as TaskState)
        : undefined;
      const olderThanMs =
        body.older_than_ms === undefined ? undefined : Number(body.older_than_ms);
      const dryRun = body.dry_run === true;
      let result: { purged: string[]; remaining: number };
      try {
        result = this.scheduler.purgeTerminalTasks({ states, olderThanMs, dryRun });
      } catch (err) {
        return new Response(
          JSON.stringify({ error: "PURGE_REJECTED", message: (err as Error).message }),
          { status: 400, headers: { "content-type": "application/json" } }
        );
      }
      if (!dryRun) {
        for (const taskId of result.purged) {
          this.ctx.storage.sql.exec("DELETE FROM ansts_tasks WHERE task_id = ?", taskId);
          this.ctx.storage.sql.exec("DELETE FROM ansts_claims WHERE task_id = ?", taskId);
        }
      }
      return new Response(
        JSON.stringify({
          purged: result.purged.length,
          task_ids: result.purged,
          remaining: result.remaining,
          dry_run: dryRun,
        }),
        { status: 200, headers: { "content-type": "application/json" } }
      );
    }

    if (request.method === "GET" && normalizedPath === "/tasks") {
      const tasks = Array.from(this.scheduler.allTasks.values());
      return new Response(JSON.stringify({ cellId: this.ctx.id.toString(), totalTasks: tasks.length, tasks }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }

    return new Response(JSON.stringify({ cellId: this.ctx.id.toString(), totalTasks: this.scheduler.allTasks.size }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }
}











