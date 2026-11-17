/** Transactional shadow admission messages. Receipts acknowledge recording, never execution. */
import {createHash} from 'node:crypto';
import {ControlError,validateQuota,type ResourceQuota} from './contracts.js';
import type {DirectoryStorage} from './tenant_directory.js';
import type {AdmissionReservation} from './admission_reservations.js';
export type AdmissionMessageKind='RESERVE_CAPACITY'|'START_EXECUTION'|'QUERY_CAPACITY'|'QUERY_EXECUTION'|'RELEASE_CAPACITY';
export interface AdmissionMessage {
 schemaVersion:1;protocolMode:'SHADOW';eventId:string;kind:AdmissionMessageKind;receiver:string;
 tenantId:string;reservationId:string;reservationVersion:number;taskId:string;attemptId:string;
 fencing:number;generation:number;requestDigest:string;policyVersion:number;tenantVersion:number;
 region:string;resources:ResourceQuota;target:string|null;capacityReceipt:string|null;expiresAt:number;createdAt:number;
}
export interface AdmissionMessageReceipt {
 eventId:string;digest:string;receiver:string;protocolMode:'SHADOW';outcome:'RECORDED'|'STALE'|'CONFLICT';
}
export interface AdmissionDelivery {
 message:AdmissionMessage;digest:string;status:'PENDING'|'IN_FLIGHT'|'ACKNOWLEDGED'|'SUPERSEDED'|'QUARANTINED';
 attempts:number;leaseUntil:number;nextAttemptAt:number;lastError:null|'TRANSPORT_ERROR'|'INVALID_RECEIPT'|'RECEIVER_CONFLICT'|'EXPIRED_INTENT'|'TENANT_DISABLED';receipt:AdmissionMessageReceipt|null;
}
function digest(value:unknown){return createHash('sha256').update(JSON.stringify(value)).digest('hex');}
function text(value:unknown,maxLength=256):asserts value is string {if(typeof value!=='string'||!value.trim()||value.length>maxLength)throw new ControlError('INVALID_ADMISSION_MESSAGE',422);}
function positive(value:number){if(!Number.isSafeInteger(value)||value<1)throw new ControlError('INVALID_ADMISSION_MESSAGE',422);}
function instant(value:number){if(!Number.isSafeInteger(value)||value<0)throw new ControlError('INVALID_ADMISSION_MESSAGE',422);}
function eventId(tenantId:string,reservationId:string,version:number,kind:string){return 'admission:'+digest([tenantId,reservationId,version,kind]);}
export function messageForReservation(r:AdmissionReservation):AdmissionMessage|null {
 const kind:AdmissionMessageKind|null=r.phase==='PREPARING'?'RESERVE_CAPACITY':r.phase==='START_AUTHORIZED'?'START_EXECUTION':r.phase==='UNCERTAIN'?(r.startAuthorized||r.executionObserved?'QUERY_EXECUTION':'QUERY_CAPACITY'):['CANCELLED','SETTLING','SETTLED'].includes(r.phase)?'RELEASE_CAPACITY':null;
 if(!kind)return null;
 return canonicalMessage({schemaVersion:1,protocolMode:'SHADOW',eventId:eventId(r.tenantId,r.id,r.version,kind),kind,receiver:kind==='START_EXECUTION'||kind==='QUERY_EXECUTION'?'execution:'+r.target:'capacity',tenantId:r.tenantId,reservationId:r.id,reservationVersion:r.version,taskId:r.taskId,attemptId:r.attemptId,fencing:r.fencing,generation:r.generation,requestDigest:r.requestDigest,policyVersion:r.policy.version,tenantVersion:r.policy.tenantVersion,region:r.policy.region,resources:r.resources,target:r.target,capacityReceipt:r.capacityReceipt,expiresAt:r.expiresAt,createdAt:r.updatedAt});
}
/** Allowlist and stable field order; no credentials or caller-selected URLs are persisted. */
export function canonicalMessage(input:AdmissionMessage):AdmissionMessage {
 if(!input||input.schemaVersion!==1||input.protocolMode!=='SHADOW'||!['RESERVE_CAPACITY','START_EXECUTION','QUERY_CAPACITY','QUERY_EXECUTION','RELEASE_CAPACITY'].includes(input.kind))throw new ControlError('INVALID_ADMISSION_MESSAGE',422);
 for(const value of [input.tenantId,input.reservationId,input.requestDigest,input.region])text(value);
 text(input.taskId,512);text(input.attemptId,512);
 for(const value of [input.reservationVersion,input.fencing,input.generation,input.policyVersion,input.tenantVersion])positive(value);
 instant(input.expiresAt);instant(input.createdAt);validateQuota(input.resources);
 if(input.target!==null)text(input.target);if(input.capacityReceipt!==null)text(input.capacityReceipt);
 const execution=input.kind==='START_EXECUTION'||input.kind==='QUERY_EXECUTION';
 if(execution&&(!input.target||!input.capacityReceipt))throw new ControlError('INVALID_ADMISSION_MESSAGE',422);
 if(input.receiver!==(execution?'execution:'+input.target:'capacity')||input.eventId!==eventId(input.tenantId,input.reservationId,input.reservationVersion,input.kind))throw new ControlError('INVALID_ADMISSION_MESSAGE',422);
 return {schemaVersion:1,protocolMode:'SHADOW',eventId:input.eventId,kind:input.kind,receiver:input.receiver,tenantId:input.tenantId,reservationId:input.reservationId,reservationVersion:input.reservationVersion,taskId:input.taskId,attemptId:input.attemptId,fencing:input.fencing,generation:input.generation,requestDigest:input.requestDigest,policyVersion:input.policyVersion,tenantVersion:input.tenantVersion,region:input.region,resources:{concurrentAttempts:input.resources.concurrentAttempts,cpuMilliCores:input.resources.cpuMilliCores,memoryMiB:input.resources.memoryMiB,queuedTasks:input.resources.queuedTasks},target:input.target,capacityReceipt:input.capacityReceipt,expiresAt:input.expiresAt,createdAt:input.createdAt};
}
interface OutboxState {schemaVersion:1;tenantId:string;deliveries:AdmissionDelivery[]}
/** WithinTransaction methods must be called from the reservation authority's native transaction. */
export class AdmissionOutbox {
 constructor(private readonly storage:DirectoryStorage){
  if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);
  storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS admission_outbox (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)'));
 }
 private load(tenantId:string):OutboxState {
  const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM admission_outbox WHERE id = ?','outbox').toArray()[0];
  if(!row)return {schemaVersion:1,tenantId,deliveries:[]};
  let state:OutboxState;try{state=JSON.parse(row.payload_json);}catch{throw new ControlError('ADMISSION_OUTBOX_CORRUPT',503);}
  if(state.schemaVersion!==1||state.tenantId!==tenantId||!Array.isArray(state.deliveries))throw new ControlError('ADMISSION_OUTBOX_IDENTITY_CONFLICT',503);
  return state;
 }
 private save(state:OutboxState){const json=JSON.stringify(state);if(state.deliveries.length>10000||Buffer.byteLength(json)>8*1024*1024)throw new ControlError('ADMISSION_OUTBOX_CAPACITY_REACHED',503);this.storage.sql.exec('INSERT OR REPLACE INTO admission_outbox (id, payload_json) VALUES (?, ?)', 'outbox',json);}
 enqueueWithinTransaction(r:AdmissionReservation):void {this.recoverWithinTransaction(r.tenantId,[r]);}
 recoverWithinTransaction(tenantId:string,reservations:AdmissionReservation[]):void {
  const state=this.load(tenantId),current=new Map(reservations.map(r=>[r.id,r])),existing=new Map(state.deliveries.map(item=>[item.message.eventId,item]));
  for(const item of state.deliveries){const r=current.get(item.message.reservationId);if(r&&item.message.reservationVersion<r.version&&['PENDING','IN_FLIGHT'].includes(item.status))item.status='SUPERSEDED';}
  for(const r of reservations){
   if(r.tenantId!==tenantId)throw new ControlError('ADMISSION_OUTBOX_IDENTITY_CONFLICT',503);
   const message=messageForReservation(r);if(!message)continue;
   const old=existing.get(message.eventId);if(old&&old.digest!==digest(message))throw new ControlError('ADMISSION_EVENT_CONFLICT',409);
   if(!old){const item:AdmissionDelivery={message,digest:digest(message),status:'PENDING',attempts:0,leaseUntil:0,nextAttemptAt:r.updatedAt,lastError:null,receipt:null};state.deliveries.push(item);existing.set(message.eventId,item);}
  }
  this.save(state);
 }
 list(tenantId:string,limit=25,allowedRegion='all'):AdmissionDelivery[]{if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new ControlError('INVALID_PAGE_LIMIT',422);return structuredClone(this.load(tenantId).deliveries.filter(item=>!['ACKNOWLEDGED','SUPERSEDED'].includes(item.status)&&(allowedRegion==='all'||item.message.region===allowedRegion)).slice(0,limit));}
 nextWake(tenantId:string,now:number):number|null {
  instant(now);let next:number|null=null;
  for(const item of this.load(tenantId).deliveries){if(!['PENDING','IN_FLIGHT'].includes(item.status))continue;const due=item.status==='IN_FLIGHT'?Math.max(item.leaseUntil,item.nextAttemptAt):item.nextAttemptAt;next=next===null?due:Math.min(next,due);}
  return next===null?null:Math.max(now,next);
 }
 claimWithinTransaction(tenantId:string,now:number,allowedRegion:string,allowAcquisition=true):AdmissionDelivery|null {
  instant(now);const state=this.load(tenantId);
  let result:AdmissionDelivery|null=null;
  for(const item of [...state.deliveries].sort((a,b)=>a.nextAttemptAt-b.nextAttemptAt||a.message.createdAt-b.message.createdAt)){
   if(allowedRegion!=='all'&&item.message.region!==allowedRegion)continue;
   if(!['PENDING','IN_FLIGHT'].includes(item.status)||item.nextAttemptAt>now||(item.status==='IN_FLIGHT'&&item.leaseUntil>now))continue;
   if(['RESERVE_CAPACITY','START_EXECUTION'].includes(item.message.kind)&&!allowAcquisition){item.status='QUARANTINED';item.lastError='TENANT_DISABLED';continue;}
   if(['RESERVE_CAPACITY','START_EXECUTION'].includes(item.message.kind)&&item.message.expiresAt<=now){item.status='QUARANTINED';item.lastError='EXPIRED_INTENT';continue;}
   positive(item.attempts+1);item.status='IN_FLIGHT';item.attempts++;item.leaseUntil=now+10000;result=structuredClone(item);break;
  }
  this.save(state);return result;
 }
 finishWithinTransaction(tenantId:string,event:string,attempt:number,receipt:AdmissionMessageReceipt|null,now:number,allowedRegion:string):AdmissionDelivery {
  instant(now);text(event);positive(attempt);const state=this.load(tenantId),item=state.deliveries.find(item=>item.message.eventId===event);
  if(!item)throw new ControlError('ADMISSION_MESSAGE_NOT_FOUND',404);
  if(allowedRegion!=='all'&&item.message.region!==allowedRegion)throw new ControlError('CONTROL_FORBIDDEN',403);
  if(item.status!=='IN_FLIGHT'||item.attempts!==attempt)throw new ControlError('STALE_DELIVERY_LEASE',409);
  item.leaseUntil=0;
  if(receipt===null){item.status='PENDING';item.lastError='TRANSPORT_ERROR';item.nextAttemptAt=now+Math.min(60000,1000*2**Math.min(6,item.attempts-1));}
  else if(!receipt||typeof receipt!=='object'||receipt.eventId!==event||receipt.digest!==item.digest||receipt.receiver!==item.message.receiver||receipt.protocolMode!=='SHADOW'||!['RECORDED','STALE','CONFLICT'].includes(receipt.outcome)){item.status='QUARANTINED';item.lastError='INVALID_RECEIPT';}
  else {item.receipt={eventId:receipt.eventId,digest:receipt.digest,receiver:receipt.receiver,protocolMode:'SHADOW',outcome:receipt.outcome};item.status=receipt.outcome==='CONFLICT'?'QUARANTINED':'ACKNOWLEDGED';item.lastError=receipt.outcome==='CONFLICT'?'RECEIVER_CONFLICT':null;}
  this.save(state);return structuredClone(item);
 }
}
interface InboxState {
 schemaVersion:1;receiver:string;tenantId:string;
 receipts:{eventId:string;digest:string;receipt:AdmissionMessageReceipt}[];
 streams:{reservationId:string;taskId:string;attemptId:string;fencing:number;generation:number;version:number;revoked:boolean}[];
 conflicts:{eventId:string;originalDigest:string;incomingDigest:string}[];
}
/** Persists shadow observations and revocation tombstones only. It never grants physical capacity or runs Engine. */
export class AdmissionInbox {
 constructor(private readonly storage:DirectoryStorage){if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS admission_inbox (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)'));}
 receive(input:AdmissionMessage,tenantId:string,receiver:string):AdmissionMessageReceipt {
  const message=canonicalMessage(input),requestDigest=digest(message);
  if(message.tenantId!==tenantId||message.receiver!==receiver)throw new ControlError('ADMISSION_RECEIVER_MISMATCH',403);
  return this.storage.transactionSync(()=>{
   const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM admission_inbox WHERE id = ?','inbox').toArray()[0];
   let state:InboxState={schemaVersion:1,tenantId,receiver,receipts:[],streams:[],conflicts:[]};
   if(row){try{state=JSON.parse(row.payload_json);}catch{throw new ControlError('ADMISSION_INBOX_CORRUPT',503);}}
   if(state.schemaVersion!==1||state.tenantId!==tenantId||state.receiver!==receiver||!Array.isArray(state.receipts)||!Array.isArray(state.streams)||!Array.isArray(state.conflicts))throw new ControlError('ADMISSION_INBOX_IDENTITY_CONFLICT',503);
   const old=state.receipts.find(item=>item.eventId===message.eventId);
   if(old?.digest===requestDigest)return structuredClone(old.receipt);
   const receipt:AdmissionMessageReceipt={eventId:message.eventId,digest:requestDigest,receiver,protocolMode:'SHADOW',outcome:'RECORDED'};
   if(old){receipt.outcome='CONFLICT';if(!state.conflicts.some(item=>item.eventId===message.eventId&&item.incomingDigest===requestDigest))state.conflicts.push({eventId:message.eventId,originalDigest:old.digest,incomingDigest:requestDigest});}
   else{
    let stream=state.streams.find(item=>item.reservationId===message.reservationId);
    if(stream&&(stream.taskId!==message.taskId||stream.attemptId!==message.attemptId||message.fencing<stream.fencing||message.generation<stream.generation||message.reservationVersion<stream.version||stream.revoked&&message.kind!=='RELEASE_CAPACITY'))receipt.outcome='STALE';
    else {if(!stream){stream={reservationId:message.reservationId,taskId:message.taskId,attemptId:message.attemptId,fencing:message.fencing,generation:message.generation,version:message.reservationVersion,revoked:false};state.streams.push(stream);}stream.fencing=message.fencing;stream.generation=message.generation;stream.version=message.reservationVersion;if(message.kind==='RELEASE_CAPACITY')stream.revoked=true;}
    state.receipts.push({eventId:message.eventId,digest:requestDigest,receipt});
   }
   const json=JSON.stringify(state);if(state.receipts.length>10000||state.conflicts.length>1000||Buffer.byteLength(json)>8*1024*1024)throw new ControlError('ADMISSION_INBOX_CAPACITY_REACHED',503);
   this.storage.sql.exec('INSERT OR REPLACE INTO admission_inbox (id, payload_json) VALUES (?, ?)', 'inbox',json);return structuredClone(receipt);
  });
 }
}
