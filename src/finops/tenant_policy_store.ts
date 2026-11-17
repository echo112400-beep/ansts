import {createHash} from 'node:crypto';
import type {DirectoryStorage,TenantRecord} from '../control/tenant_directory.js';
import {assertControlAccess,assertExpectedVersion,ControlError,type ControlActor,type ResourceQuota} from '../control/contracts.js';
import {validateTenantQuotaPolicy,type InternalBudget,type TenantQuotaPolicy} from './tenant_policy.js';
export interface QuotaPolicyVersion extends TenantQuotaPolicy {tenantVersion:number;region:string;costCenter:string}
export interface QuotaPolicyWrite {expectedVersion:number;resources:ResourceQuota;budget:InternalBudget;reason:string;idempotencyKey:string;requestId:string}
interface PolicyState {schemaVersion:1;tenantId:string;history:QuotaPolicyVersion[];audit:{actor:string;requestId:string;reason:string;version:number;timestamp:string}[];receipts:Record<string,{digest:string;version:number}>}
/** One native transactional authority per tenant. No admission enforcement is implied by storing policy. */
export class TenantPolicyStore {
 constructor(private readonly storage:DirectoryStorage) {
  if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);
  storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_policy_state (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)'));
 }
 private load(tenantId:string):PolicyState {
  const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_policy_state WHERE id = ?','policy').toArray()[0];
  if(!row)return {schemaVersion:1,tenantId,history:[],audit:[],receipts:{}};
  let state:PolicyState;try{state=JSON.parse(row.payload_json);}catch{throw new ControlError('TENANT_POLICY_STATE_CORRUPT',503);}
  if(state.schemaVersion!==1||state.tenantId!==tenantId||!Array.isArray(state.history)||!Array.isArray(state.audit)||!state.receipts)throw new ControlError('TENANT_CONTROL_IDENTITY_CONFLICT',503);
  return state;
 }
 read(actor:ControlActor,tenant:TenantRecord,options:{limit?:number;cursor?:string}={}) {
  assertControlAccess(actor,'read',tenant.id,tenant.region);const state=this.load(tenant.id);
  const current=state.history.at(-1)??null;
  if(current)assertControlAccess(actor,'read',tenant.id,current.region);
  const visible=state.history.filter(policy=>actor.scopeRegion==='all'||actor.scopeRegion===policy.region);
  const limit=options.limit??25;if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new ControlError('INVALID_PAGE_LIMIT',422);
  const snapshot=createHash('sha256').update(JSON.stringify([actor.sub,actor.role,actor.scopeTenant,actor.scopeRegion,tenant.id,tenant.version,state.history.length])).digest('hex');
  let offset=0;
  if(options.cursor){try{if(options.cursor.length>1024)throw new Error();const cursor=JSON.parse(Buffer.from(options.cursor,'base64url').toString('utf8'));if(cursor.snapshot!==snapshot)throw new ControlError('CURSOR_STALE',409);if(!Number.isSafeInteger(cursor.offset)||cursor.offset<0||cursor.offset>visible.length)throw new Error();offset=cursor.offset;}catch(error){if(error instanceof ControlError)throw error;throw new ControlError('INVALID_CURSOR',422);}}
  const history=visible.slice(offset,offset+limit),next=offset+history.length;
  const nextCursor=next<visible.length?Buffer.from(JSON.stringify({snapshot,offset:next})).toString('base64url'):null;
  return {current,history,nextCursor,historyTotal:visible.length,audit:actor.role==='SRE_ADMIN'?state.audit.filter(entry=>history.some(policy=>policy.version===entry.version)):undefined,enforcement:'NOT_CONNECTED' as const,tenantVersion:tenant.version,tenantStatus:tenant.status};
 }
 write(actor:ControlActor,tenant:TenantRecord,input:QuotaPolicyWrite):QuotaPolicyVersion {
  assertControlAccess(actor,'administer',tenant.id,tenant.region);
  if(!input||typeof input.reason!=='string'||!input.reason.trim()||input.reason.length>1000||typeof input.idempotencyKey!=='string'||!input.idempotencyKey.trim()||input.idempotencyKey.length>128||typeof input.requestId!=='string'||!input.requestId.trim())throw new ControlError('INVALID_QUOTA_POLICY_WRITE',422);
  const resources:ResourceQuota={concurrentAttempts:input.resources?.concurrentAttempts,cpuMilliCores:input.resources?.cpuMilliCores,memoryMiB:input.resources?.memoryMiB,queuedTasks:input.resources?.queuedTasks};
  const budget:InternalBudget={period:input.budget?.period,limitCreditUnits:input.budget?.limitCreditUnits,softThresholdBps:input.budget?.softThresholdBps};
  const digest=createHash('sha256').update(JSON.stringify({actor:actor.sub,resources,budget,reason:input.reason,expectedVersion:input.expectedVersion})).digest('hex');
  const key=createHash('sha256').update(input.idempotencyKey).digest('hex');
  return this.storage.transactionSync(()=>{
   const state=this.load(tenant.id);const receipt=state.receipts[key];
   if(receipt){if(receipt.digest!==digest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);const result=state.history.find(policy=>policy.version===receipt.version);if(!result)throw new ControlError('TENANT_POLICY_STATE_CORRUPT',503);assertControlAccess(actor,'read',tenant.id,result.region);return result;}
   if(tenant.status!=='ACTIVE')throw new ControlError('TENANT_DISABLED',422);
   const current=state.history.at(-1);assertExpectedVersion(input.expectedVersion,current?.version??0);
   if(current)assertControlAccess(actor,'administer',tenant.id,current.region);
   const policy:QuotaPolicyVersion={schemaVersion:1,tenantId:tenant.id,tenantVersion:tenant.version,region:tenant.region,costCenter:tenant.costCenter,version:input.expectedVersion+1,resources,budget,actor:actor.sub,reason:input.reason,createdAt:new Date().toISOString()};
   validateTenantQuotaPolicy(policy);
   state.history.push(policy);state.audit.push({actor:actor.sub,requestId:input.requestId,reason:input.reason,version:policy.version,timestamp:policy.createdAt});state.receipts[key]={digest,version:policy.version};
   const payload=JSON.stringify(state);if(state.history.length>10000||Buffer.byteLength(payload)>8*1024*1024)throw new ControlError('TENANT_POLICY_CAPACITY_REACHED',503);
   this.storage.sql.exec('INSERT OR REPLACE INTO tenant_policy_state (id, payload_json) VALUES (?, ?)', 'policy',payload);
   return policy;
  });
 }
}
