/** Single global Host ownership authority. Pool grant authorities must consume this directory. */
import {createHash} from 'node:crypto';
import {assertControlAccess,assertExpectedVersion,ControlError,type ControlActor} from '../control/contracts.js';
import {capacityId,capacityResources,canonicalCapacityReport,capacityReportDigest,CAPACITY_REPORT_TTL_MS,type CapacityReport,type CapacityResources} from '../control/capacity_report.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';
export interface CapacityPool {id:string;name:string;region:string;status:'ACTIVE'|'DISABLED';limit:CapacityResources;version:number;}
export interface PhysicalCapacityHost {id:string;poolId:string;daemonIds:string[];status:'ACTIVE'|'DISABLED';limit:CapacityResources;version:number;}
interface Audit {operationId:string;actor:string;kind:string;id:string;reason:string;requestId:string;at:number;previousVersion:number;version:number;before:unknown;after:unknown;}
interface StoredReport {report:CapacityReport;digest:string;receivedAt:number;quarantined:boolean;}
interface State {schemaVersion:1;version:number;pools:CapacityPool[];hosts:PhysicalCapacityHost[];reports:Record<string,StoredReport>;receipts:Record<string,{digest:string;result:unknown;operationId:string}>;audit:Audit[];conflicts:{daemonId:string;epoch:number;sequence:number;digest:string;at:number}[];}
interface Mutation {expectedVersion:number;reason:string;requestId:string;idempotencyKey:string;}
export type PoolWrite=Omit<CapacityPool,'version'>&Mutation;
export type HostWrite=Omit<PhysicalCapacityHost,'version'>&Mutation;
const dimensions=['cpuMilliCores','memoryMiB','slots'] as const;
const zero=():CapacityResources=>({cpuMilliCores:0,memoryMiB:0,slots:0});
const hash=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
export function assertCapacityAdmin(actor:ControlActor):void {
 if(!actor||actor.role!=='SRE_ADMIN'||actor.scopeTenant!=='all'||actor.scopeRegion!=='all'||typeof actor.sub!=='string'||!actor.sub.trim())throw new ControlError('CONTROL_FORBIDDEN',403);
}
function clock(now:number){if(!Number.isSafeInteger(now)||now<0)throw new ControlError('INVALID_CAPACITY_CLOCK',422);}
function status(value:string){if(!['ACTIVE','DISABLED'].includes(value))throw new ControlError('INVALID_CAPACITY_STATUS',422);}
function text(value:unknown,max=128):asserts value is string {if(typeof value!=='string'||!value.trim()||value.length>max||/[\u0000-\u001f]/.test(value))throw new ControlError('INVALID_CAPACITY_INPUT',422);}
export class CapacityInventory {
 constructor(private readonly storage:DirectoryStorage){if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS capacity_inventory (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)'));}
 private load():State {
  const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM capacity_inventory WHERE id = ?','inventory').toArray()[0];
  if(!row)return {schemaVersion:1,version:0,pools:[],hosts:[],reports:{},receipts:{},audit:[],conflicts:[]};
  let state:State;try{state=JSON.parse(row.payload_json);}catch{throw new ControlError('CAPACITY_STATE_CORRUPT',503);}
  if(state.schemaVersion!==1||!Array.isArray(state.pools)||!Array.isArray(state.hosts)||!state.reports||!state.receipts||!Array.isArray(state.audit)||!Array.isArray(state.conflicts))throw new ControlError('CAPACITY_STATE_CORRUPT',503);return state;
 }
 private save(state:State){const json=JSON.stringify(state);if(state.pools.length>100||state.hosts.length>1000||state.audit.length>10000||state.conflicts.length>1000||Buffer.byteLength(json)>8*1024*1024)throw new ControlError('CAPACITY_DIRECTORY_FULL',503);this.storage.sql.exec('INSERT OR REPLACE INTO capacity_inventory (id, payload_json) VALUES (?, ?)', 'inventory',json);}
 private mutate<T extends {id:string;version:number}>(actor:ControlActor,kind:string,input:Mutation&{id:string},command:unknown,change:(state:State)=>{before:T|null;after:T},now:number):{record:T;operationId:string} {
  assertCapacityAdmin(actor);clock(now);text(input.reason,1000);text(input.idempotencyKey);text(input.requestId);const receiptKey=hash([actor.sub,input.idempotencyKey]),digest=hash({kind,command,reason:input.reason});
  return this.storage.transactionSync(()=>{
   const state=this.load(),old=state.receipts[receiptKey];if(old){if(old.digest!==digest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);return {record:structuredClone(old.result) as T,operationId:old.operationId};}
   const {before,after}=change(state),operationId='capacity:'+hash([actor.sub,input.idempotencyKey]);
   state.audit.push({operationId,actor:actor.sub,kind,id:input.id,reason:input.reason,requestId:input.requestId,at:now,previousVersion:before?.version??0,version:after.version,before,after:structuredClone(after)});state.version++;
   state.receipts[receiptKey]={digest,result:structuredClone(after),operationId};this.save(state);return {record:structuredClone(after),operationId};
  });
 }
 writePool(actor:ControlActor,input:PoolWrite,now:number){
  capacityId(input.id);capacityId(input.region);text(input.name);status(input.status);const command={id:input.id,name:input.name,region:input.region,status:input.status,limit:capacityResources(input.limit,true),expectedVersion:input.expectedVersion};
  return this.mutate<CapacityPool>(actor,'pool',input,command,state=>{const index=state.pools.findIndex(p=>p.id===input.id),old=state.pools[index];assertExpectedVersion(input.expectedVersion,old?.version??0);if(old&&old.region!==input.region)throw new ControlError('POOL_REGION_IMMUTABLE',409);const result={...command,version:(old?.version??0)+1};const after:CapacityPool={id:result.id,name:result.name,region:result.region,status:result.status,limit:result.limit,version:result.version};if(old)state.pools[index]=after;else state.pools.push(after);return {before:old??null,after};},now);
 }
 writeHost(actor:ControlActor,input:HostWrite,now:number){
  capacityId(input.id);capacityId(input.poolId);status(input.status);
  if(!Array.isArray(input.daemonIds)||input.daemonIds.length<1||input.daemonIds.length>16||new Set(input.daemonIds).size!==input.daemonIds.length)throw new ControlError('INVALID_DAEMON_BINDINGS',422);input.daemonIds.forEach(capacityId);
  const command={id:input.id,poolId:input.poolId,status:input.status,daemonIds:[...input.daemonIds].sort(),limit:capacityResources(input.limit,true),expectedVersion:input.expectedVersion};
  return this.mutate<PhysicalCapacityHost>(actor,'host',input,command,state=>{
   if(!state.pools.some(p=>p.id===input.poolId))throw new ControlError('CAPACITY_POOL_NOT_FOUND',404);
   const index=state.hosts.findIndex(h=>h.id===input.id),old=state.hosts[index];assertExpectedVersion(input.expectedVersion,old?.version??0);
   if(old&&(old.poolId!==command.poolId||JSON.stringify(old.daemonIds)!==JSON.stringify(command.daemonIds)))throw new ControlError('HOST_BINDING_IMMUTABLE',409);
   if(state.hosts.some(h=>h.id!==input.id&&h.daemonIds.some(id=>command.daemonIds.includes(id))))throw new ControlError('DAEMON_ALREADY_REGISTERED',409);
   const after:PhysicalCapacityHost={id:command.id,poolId:command.poolId,status:command.status,daemonIds:command.daemonIds,limit:command.limit,version:(old?.version??0)+1};if(old)state.hosts[index]=after;else state.hosts.push(after);return {before:old??null,after};
  },now);
 }
 report(authenticatedDaemon:string,input:CapacityReport,now:number):{outcome:'RECORDED'|'REPLAY'|'CONFLICT';digest:string} {
  clock(now);input=canonicalCapacityReport(input);if(input.daemonId!==authenticatedDaemon)throw new ControlError('CAPACITY_REPORT_SCOPE_FORBIDDEN',403);const digest=capacityReportDigest(input);
  return this.storage.transactionSync(()=>{
   const state=this.load(),host=state.hosts.find(host=>host.id===input.hostId&&host.daemonIds.includes(authenticatedDaemon));if(!host)throw new ControlError('CAPACITY_HOST_NOT_REGISTERED',403);
   const old=Object.hasOwn(state.reports,authenticatedDaemon)?state.reports[authenticatedDaemon]:undefined;
   if(old&&input.epoch===old.report.epoch&&input.sequence===old.report.sequence){
    if(digest===old.digest)return {outcome:'REPLAY',digest};
    old.quarantined=true;if(!state.conflicts.some(c=>c.daemonId===input.daemonId&&c.epoch===input.epoch&&c.sequence===input.sequence&&c.digest===digest))state.conflicts.push({daemonId:input.daemonId,epoch:input.epoch,sequence:input.sequence,digest,at:now});state.version++;this.save(state);return {outcome:'CONFLICT',digest};
   }
   if(old&&(input.epoch<old.report.epoch||input.epoch===old.report.epoch&&input.sequence<old.report.sequence||input.observedAt<old.report.observedAt||now<old.receivedAt))throw new ControlError('STALE_CAPACITY_REPORT',409);
   if(input.observedAt>now+2000||now-input.observedAt>=CAPACITY_REPORT_TTL_MS)throw new ControlError('CAPACITY_REPORT_NOT_FRESH',409);
   state.reports[authenticatedDaemon]={report:input,digest,receivedAt:now,quarantined:old?.quarantined??false};state.version++;this.save(state);return {outcome:'RECORDED',digest};
  });
 }
 snapshot(actor:ControlActor,now:number){
  assertCapacityAdmin(actor);return this.buildSnapshot(now);
 }
 listPools(actor:ControlActor,tenantId:string,now:number){
  capacityId(tenantId);
  assertControlAccess(actor,'read',tenantId,actor.scopeRegion);
  const {asOf,pools}=this.buildSnapshot(now);
  return {asOf,pools:pools.filter(pool=>actor.scopeRegion==='all'||pool.region===actor.scopeRegion)};
 }
 private buildSnapshot(now:number){
  clock(now);const state=this.load();
  const hosts=state.hosts.map(host=>{
   const stored=host.daemonIds.map(id=>Object.hasOwn(state.reports,id)?state.reports[id]:undefined),reports=stored.filter((r):r is StoredReport=>!!r),reserved=zero(),capacity=reports.length?{...host.limit}:zero();
   for(const dimension of dimensions){for(const item of reports)reserved[dimension]+=item.report.reserved[dimension];for(const item of reports)capacity[dimension]=Math.min(capacity[dimension],item.report.total[dimension]);}
   const missing=stored.some(r=>!r),quarantined=reports.some(r=>r.quarantined||r.report.recoveryRequired),fresh=stored.every(r=>!!r&&now>=r.receivedAt&&now>=r.report.observedAt-2000&&now-r.receivedAt<CAPACITY_REPORT_TTL_MS&&now-r.report.observedAt<CAPACITY_REPORT_TTL_MS);
   const disabled=host.status!=='ACTIVE'||state.pools.find(p=>p.id===host.poolId)?.status!=='ACTIVE',draining=reports.some(r=>r.report.draining),overcommitted=dimensions.some(d=>reserved[d]>capacity[d]);
   const health=disabled?'DISABLED':quarantined?'QUARANTINED':missing?'UNVERIFIED':!fresh?'STALE':draining?'DRAINING':overcommitted?'OVERCOMMITTED':'HEALTHY';
   const available=zero();if(health==='HEALTHY')for(const dimension of dimensions)available[dimension]=Math.max(0,capacity[dimension]-reserved[dimension]);
   return {...host,health,lastKnownCapacity:capacity,retainedReserved:reserved,available,reports:host.daemonIds.map((daemonId,index)=>({daemonId,epoch:stored[index]?.report.epoch??null,sequence:stored[index]?.report.sequence??null,observedAt:stored[index]?.report.observedAt??null,receivedAt:stored[index]?.receivedAt??null,quarantined:stored[index]?.quarantined??false}))};
  });
  const pools=state.pools.map(pool=>{const members=hosts.filter(h=>h.poolId===pool.id),capacity=zero(),reserved=zero(),available=zero();for(const d of dimensions){for(const h of members){capacity[d]+=h.lastKnownCapacity[d];reserved[d]+=h.retainedReserved[d];available[d]+=h.available[d];}capacity[d]=Math.min(pool.limit[d],capacity[d]);available[d]=pool.status==='ACTIVE'?Math.min(available[d],Math.max(0,capacity[d]-reserved[d])):0;}return {...pool,capacity,retainedReserved:reserved,available,hostCount:members.length,isolatedHostCount:members.filter(h=>['STALE','QUARANTINED','UNVERIFIED','OVERCOMMITTED'].includes(h.health)).length,complete:members.every(h=>h.health==='HEALTHY')};});
  return {schemaVersion:1,version:state.version,asOf:now,reportTtlMs:CAPACITY_REPORT_TTL_MS,pools,hosts,conflictCount:state.conflicts.length,allocation:'NOT_CONNECTED' as const};
 }
 getPool(id:string):CapacityPool {
  capacityId(id);
  const pool=this.load().pools.find(candidate=>candidate.id===id);
  if(!pool)throw new ControlError('CAPACITY_POOL_NOT_FOUND',404);
  return pool;
 }
 audit(actor:ControlActor){assertCapacityAdmin(actor);return this.load().audit;}
}



