/** Persistent automatic shadow-delivery configuration and bounded operational evidence. */
import {createHash} from 'node:crypto';
import {assertControlAccess,assertExpectedVersion,ControlError,type ControlActor} from './contracts.js';
import type {DirectoryStorage,TenantRecord} from './tenant_directory.js';
export interface DeliveryConfiguration {tenantId:string;version:number;mode:'UNINITIALIZED'|'RUNNING'|'PAUSED';actor:string;reason:string;updatedAt:number}
export type DeliveryRunError='DIRECTORY_UNAVAILABLE'|'MAILBOX_UNAVAILABLE'|'DELIVERY_FAILED'|'CONTROL_UNAVAILABLE'|null;
export interface DeliveryRuntime {lastRunAt:number|null;lastError:DeliveryRunError;acknowledgedMessages:number;retriedMessages:number;quarantinedReceipts:number}
interface State {schemaVersion:1;configuration:DeliveryConfiguration;runtime:DeliveryRuntime;history:DeliveryConfiguration[];receipts:Record<string,{digest:string;result:DeliveryConfiguration}>}
const hash=(value:unknown)=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
export class AdmissionDeliveryControl {
 constructor(private readonly storage:DirectoryStorage){if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS admission_delivery_control (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)'));}
 private load():State|null {
  const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM admission_delivery_control WHERE id = ?','control').toArray()[0];if(!row)return null;
  let state:State;try{state=JSON.parse(row.payload_json);}catch{throw new ControlError('DELIVERY_CONTROL_CORRUPT',503);}
  if(state.schemaVersion!==1||!state.configuration||!['UNINITIALIZED','RUNNING','PAUSED'].includes(state.configuration.mode)||!state.runtime||!Array.isArray(state.history)||!state.receipts)throw new ControlError('DELIVERY_CONTROL_CORRUPT',503);
  return state;
 }
 private save(state:State){const json=JSON.stringify(state);if(state.history.length>10000||Buffer.byteLength(json)>8*1024*1024)throw new ControlError('DELIVERY_CONTROL_CAPACITY_REACHED',503);this.storage.sql.exec('INSERT OR REPLACE INTO admission_delivery_control (id, payload_json) VALUES (?, ?)', 'control',json);}
 known(){const state=this.load();return state?structuredClone({configuration:state.configuration,runtime:state.runtime}):null;}
 ensure(tenantId:string,now:number):DeliveryConfiguration {
  if(typeof tenantId!=='string'||!tenantId.trim()||tenantId.length>128)throw new ControlError('INVALID_TENANT_ID',422);
  return this.storage.transactionSync(()=>{let state=this.load();if(state&&state.configuration.tenantId!==tenantId)throw new ControlError('DELIVERY_CONTROL_IDENTITY_CONFLICT',503);if(!state){state={schemaVersion:1,configuration:{tenantId,version:0,mode:'UNINITIALIZED',actor:'system:admission-delivery',reason:'Awaiting initial alarm registration',updatedAt:now},runtime:{lastRunAt:null,lastError:null,acknowledgedMessages:0,retriedMessages:0,quarantinedReceipts:0},history:[],receipts:{}};this.save(state);}return structuredClone(state.configuration);});
 }
 initializeAfterAlarm(tenantId:string,now:number):void {
  this.storage.transactionSync(()=>{const state=this.load();if(!state||state.configuration.tenantId!==tenantId)throw new ControlError('DELIVERY_CONTROL_IDENTITY_CONFLICT',503);if(state.configuration.mode!=='UNINITIALIZED')return;state.configuration={tenantId,version:1,mode:'RUNNING',actor:'system:admission-delivery',reason:'Automatic shadow delivery initialized after alarm registration',updatedAt:now};state.history.push(state.configuration);this.save(state);});
 }
 change(actor:ControlActor,tenant:TenantRecord,input:{expectedVersion:number;enabled:boolean;reason:string;idempotencyKey:string},now:number):DeliveryConfiguration {
  assertControlAccess(actor,'administer',tenant.id,tenant.region);
  if(!input||typeof input.enabled!=='boolean'||typeof input.reason!=='string'||!input.reason.trim()||input.reason.length>1000||typeof input.idempotencyKey!=='string'||!input.idempotencyKey.trim()||input.idempotencyKey.length>128)throw new ControlError('INVALID_DELIVERY_CONTROL',422);
  const key=hash(input.idempotencyKey),digest=hash({actor:actor.sub,enabled:input.enabled,reason:input.reason,expectedVersion:input.expectedVersion});
  return this.storage.transactionSync(()=>{
   const state=this.load();if(!state||state.configuration.tenantId!==tenant.id)throw new ControlError('DELIVERY_CONTROL_IDENTITY_CONFLICT',503);
   const old=state.receipts[key];if(old){if(old.digest!==digest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);return structuredClone(old.result);}
   assertExpectedVersion(input.expectedVersion,state.configuration.version);
   const result:DeliveryConfiguration={tenantId:tenant.id,version:input.expectedVersion+1,mode:input.enabled?'RUNNING':'PAUSED',actor:actor.sub,reason:input.reason,updatedAt:now};
   state.configuration=result;state.history.push(result);state.receipts[key]={digest,result};this.save(state);return structuredClone(result);
  });
 }
 noteRun(tenantId:string,now:number,error:DeliveryRunError,status?:string):void {
  this.storage.transactionSync(()=>{const state=this.load();if(!state||state.configuration.tenantId!==tenantId)throw new ControlError('DELIVERY_CONTROL_IDENTITY_CONFLICT',503);state.runtime.lastRunAt=now;state.runtime.lastError=error;
   if(status==='ACKNOWLEDGED')state.runtime.acknowledgedMessages++;
   if(status==='PENDING')state.runtime.retriedMessages++;
   if(status==='QUARANTINED')state.runtime.quarantinedReceipts++;
   if([state.runtime.acknowledgedMessages,state.runtime.retriedMessages,state.runtime.quarantinedReceipts].some(value=>!Number.isSafeInteger(value)))throw new ControlError('DELIVERY_CONTROL_CAPACITY_REACHED',503);
   this.save(state);
  });
 }
}
