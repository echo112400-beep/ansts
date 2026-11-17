/** Durable tenant-side protocol foundation. Not a runtime authorization or billing ledger. */
import {createHash,randomUUID} from 'node:crypto';
import {bodyEvidenceDigest,canonicalBodyEvidence,canonicalBodyObservationLookup,type BodyExecutionEvidence,type BodyObservationLookup,type BodyObservationBinding} from './body_execution_evidence.js';
import {AdmissionOutbox,type AdmissionMessageReceipt,type AdmissionDelivery} from './admission_messages.js';
import {assertControlAccess,assertExpectedVersion,ControlError,parseCreditUnits,validateQuota,type ControlActor,type ResourceQuota} from './contracts.js';
import type {DirectoryStorage,TenantRecord} from './tenant_directory.js';
import {internalBudgetPeriod} from '../finops/tenant_policy.js';
import {TenantPolicyStore,type QuotaPolicyVersion} from '../finops/tenant_policy_store.js';
export type ReservationPhase='PREPARING'|'RESERVED'|'START_AUTHORIZED'|'RUNNING'|'UNCERTAIN'|'SETTLING'|'SETTLED'|'CANCELLED';
export interface AdmissionReservation {
 id:string;tenantId:string;taskId:string;attemptId:string;requestDigest:string;executionInputDigest?:string;evidenceSequence?:number;fencing:number;generation:number;version:number;
 phase:ReservationPhase;policy:QuotaPolicyVersion;resources:ResourceQuota;estimatedCreditUnits:string;
 period:string;expiresAt:number;createdAt:number;updatedAt:number;startAuthorized:boolean;executionObserved:boolean;
 target:string|null;capacityReceipt:string|null;terminalEvidence:string|null;
 actualCreditUnits:string|null;rateSnapshot:null;
}
export interface PrepareAdmission {
 reservationId:string;taskId:string;attemptId:string;fencing:number;expectedPolicyVersion:number;
 resources:ResourceQuota;estimatedCreditUnits:string;expiresAt:number;executionInputDigest?:string;
}
export interface AdmissionTransition {
 reservationId:string;expectedVersion:number;generation:number;fencing:number;
 to:Exclude<ReservationPhase,'PREPARING'>;reason:string;
 target?:string;capacityReceipt?:string;
 /** Supplied only by a trusted execution-evidence adapter, never accepted directly from a browser. */
 evidence?:{kind:'NOT_STARTED'|'TERMINATED';id:string;target:string;attemptId:string;fencing:number;generation:number};
 actualCreditUnits?:string;
}
interface State {
 schemaVersion:1;tenantId:string;reservations:AdmissionReservation[];evidenceReceipts?:Record<string,{digest:string;evidence:BodyExecutionEvidence;result:AdmissionReservation}>;
 receipts:Record<string,{digest:string;result:AdmissionReservation}>;
 audit:{reservationId:string;actor:string;reason:string;version:number;phase:ReservationPhase;at:number}[];
}
function required(value:unknown,maxLength=128):asserts value is string {
 if(typeof value!=='string'||!value.trim()||value.length>maxLength)throw new ControlError('INVALID_ADMISSION_IDENTIFIER',422);
}
function integer(value:number){if(!Number.isSafeInteger(value)||value<1)throw new ControlError('INVALID_ADMISSION_GENERATION',422);}
function hash(value:unknown){return createHash('sha256').update(JSON.stringify(value)).digest('hex');}
const held=(r:AdmissionReservation)=>!['CANCELLED','SETTLED'].includes(r.phase);
/** All policy reads, quota checks, receipts and reservation changes share one native transaction. */
export class AdmissionReservationStore {
 private readonly policies:TenantPolicyStore;
 private readonly messages:AdmissionOutbox;
 constructor(private readonly storage:DirectoryStorage){
  if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);
  this.policies=new TenantPolicyStore(storage);this.messages=new AdmissionOutbox(storage);
  storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_admission_state (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)'));
 }
 private load(tenantId:string):State {
  const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_admission_state WHERE id = ?','admission').toArray()[0];
  if(!row)return {schemaVersion:1,tenantId,reservations:[],receipts:{},audit:[]};
  let state:State;try{state=JSON.parse(row.payload_json);}catch{throw new ControlError('ADMISSION_STATE_CORRUPT',503);}
  if(state.schemaVersion!==1||state.tenantId!==tenantId||!Array.isArray(state.reservations)||!state.receipts||!Array.isArray(state.audit))throw new ControlError('ADMISSION_IDENTITY_CONFLICT',503);
  return state;
 }
 private commit(state:State){
  const payload=JSON.stringify(state);
  if(state.reservations.length>10000||state.audit.length>50000||Buffer.byteLength(payload)>8*1024*1024)throw new ControlError('ADMISSION_CAPACITY_REACHED',503);
  this.storage.sql.exec('INSERT OR REPLACE INTO tenant_admission_state (id, payload_json) VALUES (?, ?)', 'admission',payload);
 }
 private access(actor:ControlActor,tenant:TenantRecord){assertControlAccess(actor,'request_capacity',tenant.id,tenant.region);}
 private clock(now:number){internalBudgetPeriod(now,'UTC_DAY');}
 private mutate(actor:ControlActor,tenant:TenantRecord,key:string,command:unknown,change:(state:State)=>AdmissionReservation){
  this.access(actor,tenant);required(key);const receiptKey=hash(key),digest=hash({actor:actor.sub,command});
  return this.storage.transactionSync(()=>{
   const state=this.load(tenant.id),receipt=state.receipts[receiptKey];
   if(receipt){if(receipt.digest!==digest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);assertControlAccess(actor,'read',tenant.id,receipt.result.policy.region);return structuredClone(receipt.result);}
   const result=change(state);state.receipts[receiptKey]={digest,result:structuredClone(result)};this.commit(state);this.messages.enqueueWithinTransaction(result);return structuredClone(result);
  });
 }
 prepare(actor:ControlActor,tenant:TenantRecord,input:PrepareAdmission,key:string,now:number):AdmissionReservation {
  this.clock(now);required(input.reservationId);required(input.taskId,512);required(input.attemptId,512);integer(input.fencing);
  validateQuota(input.resources);parseCreditUnits(input.estimatedCreditUnits);if(input.executionInputDigest!==undefined&&(typeof input.executionInputDigest!=='string'||!/^[a-f0-9]{64}$/.test(input.executionInputDigest)))throw new ControlError('INVALID_EXECUTION_INPUT_DIGEST',422);
  if(input.resources.concurrentAttempts!==1||input.resources.queuedTasks!==0||!Number.isSafeInteger(input.expiresAt))throw new ControlError('INVALID_ADMISSION_PREPARATION',422);
  // Canonical allowlist: unrecognized request fields never enter durable state or receipts.
  const command:PrepareAdmission={reservationId:input.reservationId,taskId:input.taskId,attemptId:input.attemptId,fencing:input.fencing,expectedPolicyVersion:input.expectedPolicyVersion,resources:{concurrentAttempts:1,cpuMilliCores:input.resources.cpuMilliCores,memoryMiB:input.resources.memoryMiB,queuedTasks:0},estimatedCreditUnits:input.estimatedCreditUnits,expiresAt:input.expiresAt,...(input.executionInputDigest?{executionInputDigest:input.executionInputDigest}:{})};
  return this.mutate(actor,tenant,key,{prepare:command},state=>{
   if(tenant.status!=='ACTIVE')throw new ControlError('TENANT_DISABLED',422);
   if(command.expiresAt<=now||command.expiresAt-now>300000)throw new ControlError('INVALID_ADMISSION_PREPARATION',422);
   const policy=this.policies.read(actor,tenant).current;if(!policy)throw new ControlError('QUOTA_POLICY_REQUIRED',409);
   assertExpectedVersion(input.expectedPolicyVersion,policy.version);
   if(policy.tenantVersion!==tenant.version||policy.region!==tenant.region||policy.costCenter!==tenant.costCenter)throw new ControlError('POLICY_METADATA_NOT_ACTIVATED',409);
   if(state.reservations.some(r=>r.id===input.reservationId||r.attemptId===input.attemptId))throw new ControlError('ATTEMPT_ALREADY_RESERVED',409);
   const active=state.reservations.filter(r=>held(r)&&r.phase!=='SETTLING');
   for(const dimension of ['concurrentAttempts','cpuMilliCores','memoryMiB'] as const){
    const total=active.reduce((sum,r)=>sum+BigInt(r.resources[dimension]),BigInt(command.resources[dimension]));
    if(total>BigInt(policy.resources[dimension]))throw new ControlError('RESOURCE_QUOTA_EXCEEDED',409);
   }
   const period=internalBudgetPeriod(now,policy.budget.period);
   // Pending reservations from every period remain occupied after UTC boundaries.
   const occupied=state.reservations.reduce((sum,r)=>sum+(held(r)?parseCreditUnits(r.estimatedCreditUnits):r.phase==='SETTLED'&&internalBudgetPeriod(r.createdAt,policy.budget.period)===period?parseCreditUnits(r.actualCreditUnits!):0n),0n);
   if(occupied+parseCreditUnits(command.estimatedCreditUnits)>parseCreditUnits(policy.budget.limitCreditUnits))throw new ControlError('BUDGET_QUOTA_EXCEEDED',409);
   const result:AdmissionReservation={id:command.reservationId,tenantId:tenant.id,taskId:command.taskId,attemptId:command.attemptId,requestDigest:hash({tenantId:tenant.id,prepare:command,...(command.executionInputDigest?{nonce:randomUUID()}: {})}),...(command.executionInputDigest?{executionInputDigest:command.executionInputDigest}:{}),fencing:command.fencing,generation:1,version:1,phase:'PREPARING',policy:structuredClone(policy),resources:command.resources,estimatedCreditUnits:command.estimatedCreditUnits,period,expiresAt:command.expiresAt,createdAt:now,updatedAt:now,startAuthorized:false,executionObserved:false,target:null,capacityReceipt:null,terminalEvidence:null,actualCreditUnits:null,rateSnapshot:null};
   state.reservations.push(result);state.audit.push({reservationId:result.id,actor:actor.sub,reason:'prepare',version:1,phase:result.phase,at:now});return result;
  });
 }
 /** Internal coordinator primitive: execution evidence must already be authenticated by its adapter. */
 transition(actor:ControlActor,tenant:TenantRecord,input:AdmissionTransition,key:string,now:number):AdmissionReservation {
  this.clock(now);required(input.reservationId);integer(input.generation);integer(input.fencing);
  if(typeof input.reason!=='string'||!input.reason.trim()||input.reason.length>1000)throw new ControlError('ADMISSION_REASON_REQUIRED',422);
  const evidence=input.evidence?{kind:input.evidence.kind,id:input.evidence.id,target:input.evidence.target,attemptId:input.evidence.attemptId,fencing:input.evidence.fencing,generation:input.evidence.generation}:undefined;
  const command:AdmissionTransition={reservationId:input.reservationId,expectedVersion:input.expectedVersion,generation:input.generation,fencing:input.fencing,to:input.to,reason:input.reason,target:input.target,capacityReceipt:input.capacityReceipt,evidence,actualCreditUnits:input.actualCreditUnits};
  return this.mutate(actor,tenant,key,{transition:command},state=>{
   const r=state.reservations.find(r=>r.id===input.reservationId);if(!r)throw new ControlError('RESERVATION_NOT_FOUND',404);
   assertControlAccess(actor,'request_capacity',tenant.id,r.policy.region);
   assertExpectedVersion(input.expectedVersion,r.version);
   if(now<r.updatedAt)throw new ControlError('ADMISSION_CLOCK_REGRESSION',409);
   if(r.generation!==input.generation||r.fencing!==input.fencing)throw new ControlError('STALE_ADMISSION_GENERATION',409);
   if(['SETTLED','CANCELLED'].includes(r.phase))throw new ControlError('RESERVATION_TERMINAL',409);
   const allowed:Record<string,string[]>={PREPARING:['RESERVED','UNCERTAIN','CANCELLED'],RESERVED:['START_AUTHORIZED','UNCERTAIN','CANCELLED'],START_AUTHORIZED:['RUNNING','UNCERTAIN','SETTLING','CANCELLED'],RUNNING:['UNCERTAIN','SETTLING'],UNCERTAIN:['SETTLING','CANCELLED'],SETTLING:['SETTLED']};
   if(!allowed[r.phase]?.includes(input.to))throw new ControlError('INVALID_ADMISSION_TRANSITION',409);
   if(input.to==='RESERVED'){if(now>=r.expiresAt)throw new ControlError('ADMISSION_EXPIRED',409);required(input.target);required(input.capacityReceipt);r.target=input.target;r.capacityReceipt=input.capacityReceipt;}
   if(input.to==='START_AUTHORIZED'){
    if(tenant.status!=='ACTIVE')throw new ControlError('TENANT_DISABLED',422);
    if(r.policy.tenantVersion!==tenant.version||r.policy.region!==tenant.region||r.policy.costCenter!==tenant.costCenter)throw new ControlError('POLICY_METADATA_NOT_ACTIVATED',409);
    if(now>=r.expiresAt)throw new ControlError('ADMISSION_EXPIRED',409);
    r.startAuthorized=true;
   }
   if(input.to==='RUNNING'){if(now>=r.expiresAt)throw new ControlError('ADMISSION_EXPIRED',409);r.executionObserved=true;}
   if(input.to==='SETTLING'||(input.to==='CANCELLED'&&r.startAuthorized)){
    if(!evidence||!['NOT_STARTED','TERMINATED'].includes(evidence.kind)||evidence.attemptId!==r.attemptId||evidence.target!==r.target||evidence.fencing!==r.fencing||evidence.generation!==r.generation)throw new ControlError('EXECUTION_EVIDENCE_REQUIRED',409);
    required(evidence.id);
    if(r.executionObserved&&evidence.kind!=='TERMINATED')throw new ControlError('EXECUTION_EVIDENCE_REQUIRED',409);
    if(input.to==='CANCELLED'&&evidence.kind!=='NOT_STARTED')throw new ControlError('SETTLEMENT_REQUIRED',409);
    r.terminalEvidence=evidence.id;
   }
   if(input.to==='SETTLED'){if(input.actualCreditUnits===undefined)throw new ControlError('ACTUAL_COST_REQUIRED',422);parseCreditUnits(input.actualCreditUnits);r.actualCreditUnits=input.actualCreditUnits;}
   if(input.to==='CANCELLED'){integer(r.generation+1);r.generation++;}
   r.phase=input.to;r.version++;r.updatedAt=now;
   state.audit.push({reservationId:r.id,actor:actor.sub,reason:input.reason,version:r.version,phase:r.phase,at:now});return r;
  });
 }
 /** Lookup only: this neither creates a reservation nor authorizes execution. Expired running
  * attempts may still retrieve their binding so terminal observations can be delivered. */
 observationBinding(actor:ControlActor,tenant:TenantRecord,host:string,input:BodyObservationLookup):BodyObservationBinding|null {
  this.access(actor,tenant);input=canonicalBodyObservationLookup(input);required(host);
  if(input.tenantId!==tenant.id)throw new ControlError('CONTROL_FORBIDDEN',403);
  const r=this.load(tenant.id).reservations.find(r=>r.taskId===input.taskId&&r.attemptId===input.attemptId&&r.fencing===input.fencing&&r.target===host&&r.executionInputDigest===input.executionInputDigest);
  if(!r||!r.startAuthorized||['CANCELLED','SETTLED'].includes(r.phase))return null;
  assertControlAccess(actor,'request_capacity',tenant.id,r.policy.region);
  if((['concurrentAttempts','cpuMilliCores','memoryMiB','queuedTasks'] as const).some(key=>r.resources[key]!==input.resources[key]))return null;
  return {reservationId:r.id,generation:r.generation,policyVersion:r.policy.version,tenantVersion:r.policy.tenantVersion,requestDigest:r.requestDigest,executionInputDigest:r.executionInputDigest!};
 }
 usageContext(actor:ControlActor,tenant:TenantRecord,identity:import('./usage_event.js').UsageIdentity){
  this.access(actor,tenant);const r=this.load(tenant.id).reservations.find(r=>r.taskId===identity.taskId&&r.attemptId===identity.attemptId&&r.fencing===identity.fencing&&r.target===identity.producerId&&r.executionInputDigest===identity.executionInputDigest);
  return {tenantVersion:r?.policy.tenantVersion??tenant.version,region:r?.policy.region??tenant.region,costCenter:r?.policy.costCenter??tenant.costCenter,rateCardVersion:null};
 }
 /** Evidence must first be authenticated against this reservation's pinned Body host. */
 applyBodyEvidence(actor:ControlActor,tenant:TenantRecord,evidence:BodyExecutionEvidence):AdmissionReservation {
  this.access(actor,tenant);evidence=canonicalBodyEvidence(evidence);const digest=bodyEvidenceDigest(evidence),key=hash([evidence.hostId,evidence.tenantId,evidence.taskId,evidence.attemptId,evidence.sequence]);
  return this.storage.transactionSync(()=>{
   const state=this.load(tenant.id),old=state.evidenceReceipts?.[key];if(old){if(old.digest!==digest)throw new ControlError('EXECUTION_EVIDENCE_CONFLICT',409);assertControlAccess(actor,'read',tenant.id,old.result.policy.region);return structuredClone(old.result);}
   const r=state.reservations.find(r=>r.id===evidence.reservationId);if(!r)throw new ControlError('RESERVATION_NOT_FOUND',404);
   assertControlAccess(actor,'request_capacity',tenant.id,r.policy.region);
   if(!r.executionInputDigest)throw new ControlError('EXECUTION_INPUT_NOT_PINNED',409);
   if(evidence.tenantId!==tenant.id||evidence.taskId!==r.taskId||evidence.attemptId!==r.attemptId||evidence.hostId!==r.target||evidence.fencing!==r.fencing||evidence.generation!==r.generation||evidence.requestDigest!==r.requestDigest||evidence.executionInputDigest!==r.executionInputDigest||evidence.policyVersion!==r.policy.version||evidence.tenantVersion!==r.policy.tenantVersion||(['concurrentAttempts','cpuMilliCores','memoryMiB','queuedTasks'] as const).some(key=>evidence.resources[key]!==r.resources[key]))throw new ControlError('EXECUTION_EVIDENCE_BINDING_CONFLICT',409);
   if(evidence.sequence<=(r.evidenceSequence??0))throw new ControlError('STALE_EXECUTION_EVIDENCE',409);
   if(['CANCELLED','SETTLED'].includes(r.phase))throw new ControlError('RESERVATION_TERMINAL',409);
   r.executionObserved=r.executionObserved||evidence.executionObserved;
   if(evidence.phase==='NOT_STARTED'){
    if(r.executionObserved||r.phase==='SETTLING')throw new ControlError('EXECUTION_EVIDENCE_CONFLICT',409);
    r.phase='CANCELLED';r.generation++;r.terminalEvidence=key;
   }else if(evidence.phase==='TERMINATED'){r.phase='SETTLING';r.terminalEvidence=key;}
   else if(evidence.phase==='RUNNING'){
    if(r.phase==='SETTLING')throw new ControlError('EXECUTION_EVIDENCE_CONFLICT',409);
    r.executionObserved=true;r.phase=r.phase==='START_AUTHORIZED'&&Date.now()<r.expiresAt?'RUNNING':'UNCERTAIN';
   }else if(r.phase!=='SETTLING')r.phase='UNCERTAIN';
   r.evidenceSequence=evidence.sequence;r.version++;r.updatedAt=Date.now();state.audit.push({reservationId:r.id,actor:actor.sub,reason:'Authenticated Body observation: '+evidence.phase,version:r.version,phase:r.phase,at:r.updatedAt});
   state.evidenceReceipts??={};state.evidenceReceipts[key]={digest,evidence:structuredClone(evidence),result:structuredClone(r)};this.commit(state);this.messages.enqueueWithinTransaction(r);return structuredClone(r);
  });
 }
 /** Trusted alarm hook; the persisted delivery controller supplies the bound tenant identity. */
 recoveryWake(tenantId:string,now:number):number|null {
  this.clock(now);return this.storage.transactionSync(()=>{this.messages.recoverWithinTransaction(tenantId,this.load(tenantId).reservations);return this.messages.nextWake(tenantId,now);});
 }
 listMessages(actor:ControlActor,tenant:TenantRecord,limit=25):AdmissionDelivery[] {
  assertControlAccess(actor,'read',tenant.id,tenant.region);this.load(tenant.id);
  return this.messages.list(tenant.id,limit,actor.scopeRegion);
 }
 claimMessage(actor:ControlActor,tenant:TenantRecord,now:number):AdmissionDelivery|null {
  this.access(actor,tenant);this.clock(now);
  return this.storage.transactionSync(()=>{
   // Recover legacy live states by stable IDs; never invent acknowledgement or release evidence.
   this.messages.recoverWithinTransaction(tenant.id,this.load(tenant.id).reservations.filter(reservation=>actor.scopeRegion==='all'||actor.scopeRegion===reservation.policy.region));
   return this.messages.claimWithinTransaction(tenant.id,now,actor.scopeRegion,tenant.status==='ACTIVE');
  });
 }
 finishMessage(actor:ControlActor,tenant:TenantRecord,eventId:string,attempt:number,receipt:AdmissionMessageReceipt|null,now:number):AdmissionDelivery {
  this.access(actor,tenant);this.clock(now);
  return this.storage.transactionSync(()=>this.messages.finishWithinTransaction(tenant.id,eventId,attempt,receipt,now,actor.scopeRegion));
 }
 read(actor:ControlActor,tenant:TenantRecord,reservationId:string):AdmissionReservation {
  assertControlAccess(actor,'read',tenant.id,tenant.region);required(reservationId);
  const result=this.load(tenant.id).reservations.find(r=>r.id===reservationId);if(!result)throw new ControlError('RESERVATION_NOT_FOUND',404);
  assertControlAccess(actor,'read',tenant.id,result.policy.region);return structuredClone(result);
 }
}
