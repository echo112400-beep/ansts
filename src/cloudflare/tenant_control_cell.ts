import {UsageInbox} from '../finops/usage_inbox.js';
import {canonicalUsageIdentity,canonicalUsageResources,verifyUsageEvent,type UsageIdentity,type UsageResources} from '../control/usage_event.js';
import type {DurableObjectNamespaceLike,DurableObjectStateLike} from './env.js';
import type {DirectoryStorage,TenantRecord} from '../control/tenant_directory.js';
import {assertControlAccess,ControlError,type ControlActor} from '../control/contracts.js';
import {TenantPolicyStore,type QuotaPolicyWrite} from '../finops/tenant_policy_store.js';
import {verifyBodyEvidence} from '../control/body_execution_evidence.js';
import {AdmissionDeliveryControl} from '../control/admission_delivery_control.js';
import {dispatchAdmissionMessage} from '../control/admission_dispatcher.js';
import type {AdmissionMessageReceipt} from '../control/admission_messages.js';
import {AdmissionReservationStore,type PrepareAdmission,type AdmissionTransition} from '../control/admission_reservations.js';
import {issueAdmissionCredential,verifyAdmissionCredential} from '../control/admission_credentials.js';
import {equalBodyToken,serviceKeys,bodyServiceToken,type BodyAuthConfiguration} from './body_auth.js';
export interface TenantControlEnv extends BodyAuthConfiguration {ANSTS_DIRECTORY_SERVICE_TOKEN?:string;ANSTS_ADMISSION_SERVICE_TOKEN?:string;ANSTS_ADMISSION_SIGNING_KEY?:string;ANSTS_TENANT_DIRECTORY_CELL?:DurableObjectNamespaceLike;ANSTS_ADMISSION_MAILBOX?:DurableObjectNamespaceLike}
export class TenantControlCell {
 private readonly store:TenantPolicyStore|null;
 private readonly usage:UsageInbox|null;
 private readonly admissions:AdmissionReservationStore|null;
 private readonly delivery:AdmissionDeliveryControl|null;
 private alarmRegistration:Promise<void>=Promise.resolve();
 constructor(private readonly ctx:DurableObjectStateLike&{storage:{transactionSync?:DirectoryStorage['transactionSync']}},private readonly env:TenantControlEnv={}) {
  const storage=typeof ctx.storage.transactionSync==='function'?{sql:ctx.storage.sql,transactionSync:ctx.storage.transactionSync.bind(ctx.storage)}:null;
  this.usage=storage?new UsageInbox(storage):null;
  this.store=storage?new TenantPolicyStore(storage):null;this.admissions=storage?new AdmissionReservationStore(storage):null;this.delivery=storage?new AdmissionDeliveryControl(storage):null;
 }
 private async arm(at:number,replace=false):Promise<void> {
  if(typeof this.ctx.storage.getAlarm!=='function'||typeof this.ctx.storage.setAlarm!=='function')throw new ControlError('ADMISSION_ALARM_UNAVAILABLE',503);
  this.alarmRegistration=this.alarmRegistration.catch(()=>{}).then(async()=>{const current=await this.ctx.storage.getAlarm();if(replace||current==null||current>at)await this.ctx.storage.setAlarm(at);});
  await this.alarmRegistration;
 }
 private async beforeMutation(actor:ControlActor,tenant:TenantRecord):Promise<void> {
  assertControlAccess(actor,'request_capacity',tenant.id,tenant.region);
  const configuration=this.delivery!.ensure(tenant.id,Date.now());if(configuration.mode==='PAUSED')return;
  // Pre-arm: a crash after commit cannot strand a newly committed outbox event.
  await this.arm(Date.now()+1000,true);this.delivery!.initializeAfterAlarm(tenant.id,Date.now());
 }
 private async deliverOne(actor:ControlActor,tenant:TenantRecord){
  const ns=this.env.ANSTS_ADMISSION_MAILBOX,token=this.env.ANSTS_ADMISSION_SERVICE_TOKEN;if(!ns||!token)throw new ControlError('ADMISSION_MAILBOX_UNAVAILABLE',503);
  return dispatchAdmissionMessage(this.admissions!,actor,tenant,{async deliver(message,signal){
   const response=await ns.get(ns.idFromName('admission-shadow-v1:'+tenant.id+':'+message.receiver)).fetch(new Request('https://shadow-mailbox.internal/record',{method:'POST',signal,headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify({actor,tenantId:tenant.id,message})}));
   if(!response.ok){await response.body?.cancel();throw new Error('SHADOW_DELIVERY_FAILED');}
   const result=await response.json() as {receipt:AdmissionMessageReceipt;protocolMode:string;enforcement:string};
   if(result.protocolMode!=='SHADOW'||result.enforcement!=='NOT_CONNECTED')throw new Error('SHADOW_MODE_REQUIRED');return result.receipt;
  }});
 }
 private async alarmTenant(actor:ControlActor,tenantId:string):Promise<TenantRecord> {
  const ns=this.env.ANSTS_TENANT_DIRECTORY_CELL!,controller=new AbortController();let timer:ReturnType<typeof setTimeout>|undefined;
  try {
   const response=await Promise.race([ns.get(ns.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/tenant',{method:'POST',signal:controller.signal,headers:{authorization:'Bearer '+this.env.ANSTS_DIRECTORY_SERVICE_TOKEN,'content-type':'application/json'},body:JSON.stringify({actor,tenantId})})),new Promise<never>((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new ControlError('DIRECTORY_UNAVAILABLE',503));},3000);})]);
   if(!response.ok){await response.body?.cancel();throw new ControlError('DIRECTORY_UNAVAILABLE',503);}const tenant=await response.json() as TenantRecord;if(tenant.id!==tenantId)throw new ControlError('DIRECTORY_UNAVAILABLE',503);return tenant;
  }finally{if(timer)clearTimeout(timer);}
 }
 async alarm():Promise<void> {
  const configuration=this.delivery?.known()?.configuration;if(!configuration||configuration.mode!=='RUNNING'||!this.admissions)return;
  const tenantId=configuration.tenantId,now=Date.now();
  // RUNNING keeps a durable heartbeat even while idle, closing the empty-queue/new-commit wakeup race.
  await this.arm(now+60000,true);let due:number|null;
  try{due=this.admissions.recoveryWake(tenantId,now);}catch{this.delivery!.noteRun(tenantId,now,'CONTROL_UNAVAILABLE');return;}if(due===null)return;
  if(due>now){await this.arm(due);return;}
  // Persistent watchdog is registered before network I/O; no reliance on an in-memory timer.
  let failure=false;
  try {
   if(!this.env.ANSTS_TENANT_DIRECTORY_CELL||!this.env.ANSTS_DIRECTORY_SERVICE_TOKEN||!this.env.ANSTS_ADMISSION_SERVICE_TOKEN||this.env.ANSTS_ADMISSION_SERVICE_TOKEN===this.env.ANSTS_DIRECTORY_SERVICE_TOKEN)throw new ControlError('CONTROL_UNAVAILABLE',503);
   if(!this.env.ANSTS_ADMISSION_MAILBOX)throw new ControlError('ADMISSION_MAILBOX_UNAVAILABLE',503);
   const actor:ControlActor={sub:'system:admission-delivery',role:'SRE_ADMIN',scopeTenant:tenantId,scopeRegion:'all'},tenant=await this.alarmTenant(actor,tenantId);
   for(let count=0;count<4&&this.delivery?.known()?.configuration.mode==='RUNNING';count++){
    const result=await this.deliverOne(actor,tenant);if(!result)break;this.delivery!.noteRun(tenantId,Date.now(),result.status==='PENDING'?'DELIVERY_FAILED':null,result.status);
   }
  }catch(error){failure=true;this.delivery!.noteRun(tenantId,Date.now(),error instanceof ControlError&&error.code==='DIRECTORY_UNAVAILABLE'?'DIRECTORY_UNAVAILABLE':error instanceof ControlError&&error.code==='ADMISSION_MAILBOX_UNAVAILABLE'?'MAILBOX_UNAVAILABLE':error instanceof ControlError&&error.code==='CONTROL_UNAVAILABLE'?'CONTROL_UNAVAILABLE':'DELIVERY_FAILED');}
  finally {if(this.delivery?.known()?.configuration.mode==='RUNNING'){const next=this.admissions.recoveryWake(tenantId,Date.now());if(next!==null)await this.arm(Math.max(Date.now()+(failure?30000:250),next));}}
 }
 async fetch(request:Request):Promise<Response> {
  const respond=(body:unknown,status=200)=>Response.json(body,{status,headers:{'cache-control':'no-store'}});
  const path=new URL(request.url).pathname;
  const admission=path.startsWith('/admission-');
  const token=admission?this.env.ANSTS_ADMISSION_SERVICE_TOKEN:this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
  if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token))return respond({code:'SERVICE_AUTH_REQUIRED'},401);
  if(admission&&token===this.env.ANSTS_DIRECTORY_SERVICE_TOKEN)return respond({code:'ADMISSION_IDENTITIES_NOT_SEPARATED'},503);
  if(!this.store||!this.env.ANSTS_TENANT_DIRECTORY_CELL)return respond({code:'TENANT_CONTROL_UNAVAILABLE'},503);
  if(request.method!=='POST'||!['/policy-read','/policy-write','/admission-read','/admission-prepare','/admission-transition','/admission-credential','/admission-verify','/admission-outbox','/admission-dispatch','/admission-delivery-status','/admission-delivery-control','/admission-observe-execution','/admission-observation-binding','/admission-usage-binding','/admission-usage-bind','/admission-usage-ingest','/usage-read'].includes(path))return respond({code:'NOT_FOUND'},404);
  try {
   const raw=await request.text();if(raw.length>65536)return respond({code:'PAYLOAD_TOO_LARGE'},413);
   const body=JSON.parse(raw) as {actor:ControlActor;tenantId:string;input:QuotaPolicyWrite;page?:{limit?:number;cursor?:string};idempotencyKey:string;reservationId:string;credential:string;target:string;proof?:string;identity?:UsageIdentity;resources?:UsageResources;limit?:number;control?:{expectedVersion:number;enabled:boolean;reason:string;idempotencyKey:string}};
   if(!body?.actor||!['VIEWER','OPERATOR','SRE_ADMIN'].includes(body.actor.role)||typeof body.actor.sub!=='string'||!body.actor.sub.trim()||typeof body.tenantId!=='string')return respond({code:'INVALID_ACTOR'},403);
   const directoryToken=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;if(!directoryToken||directoryToken.length<32)return respond({code:'TENANT_CONTROL_UNAVAILABLE'},503);
   const ns=this.env.ANSTS_TENANT_DIRECTORY_CELL;
   const resolved=await ns.get(ns.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/tenant',{method:'POST',headers:{authorization:'Bearer '+directoryToken,'content-type':'application/json'},body:JSON.stringify({actor:body.actor,tenantId:body.tenantId})}));
   if(!resolved.ok)return new Response(resolved.body,{status:resolved.status,headers:{'content-type':'application/json','cache-control':'no-store'}});
   const tenant=await resolved.json() as TenantRecord;
   if(tenant.id!==body.tenantId)return respond({code:'TENANT_CONTROL_IDENTITY_CONFLICT'},503);
   if(path==='/usage-read')return respond(this.usage!.page(body.actor,tenant,body.page??{},this.env.ANSTS_ADMISSION_SIGNING_KEY??''));
   if(path.startsWith('/admission-usage-')){
    assertControlAccess(body.actor,'request_capacity',tenant.id,tenant.region);const identity=canonicalUsageIdentity(body.identity!);if(identity.tenantId!==tenant.id)throw new ControlError('USAGE_BINDING_FORBIDDEN',403);
    if(path==='/admission-usage-binding')return respond({binding:this.usage!.binding(identity)});
    if(path==='/admission-usage-bind'){
     const resources=canonicalUsageResources(body.resources!);this.usage!.bind(identity,resources,this.admissions!.usageContext(body.actor,tenant,identity));return respond({bound:true});
    }
    const binding=this.usage!.binding(identity);if(!binding)throw new ControlError('USAGE_EXECUTION_BINDING_REQUIRED',409);
    let event;for(const key of serviceKeys(this.env))try{event=verifyUsageEvent(body.proof??'',bodyServiceToken(key,identity.producerId));break;}catch(error){if(!(error instanceof ControlError)||error.code!=='USAGE_AUTH_REQUIRED')throw error;}
    if(!event)throw new ControlError('USAGE_AUTH_REQUIRED',401);const receipt=this.usage!.receive(event,identity);return respond({receipt,sourceVerified:true},receipt.outcome==='RECORDED'?200:409);
   }
   if(admission){
    const store=this.admissions!;const now=Date.now();const envelope=(reservation:unknown)=>({reservation,protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
    if(path==='/admission-observation-binding')return respond({binding:store.observationBinding(body.actor,tenant,body.target,body.input as unknown as import('../control/body_execution_evidence.js').BodyObservationLookup),protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
    if(path==='/admission-outbox')return respond({messages:store.listMessages(body.actor,tenant,body.limit),protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
    if(path==='/admission-observe-execution'){
     const reservation=store.read(body.actor,tenant,body.reservationId);if(!reservation.target||!reservation.executionInputDigest)throw new ControlError('EXECUTION_INPUT_NOT_PINNED',409);
     const keys=serviceKeys(this.env);if(!keys.length)throw new ControlError('BODY_EVIDENCE_VERIFIER_UNAVAILABLE',503);
     let fact;for(const key of keys){try{fact=verifyBodyEvidence(body.proof??'',bodyServiceToken(key,reservation.target));break;}catch(error){if(!(error instanceof ControlError)||error.code!=='BODY_EVIDENCE_AUTH_REQUIRED')throw error;}}
     if(!fact)throw new ControlError('BODY_EVIDENCE_AUTH_REQUIRED',401);
     if(fact.reservationId!==body.reservationId)throw new ControlError('EXECUTION_EVIDENCE_BINDING_CONFLICT',409);
     await this.beforeMutation(body.actor,tenant);return respond({...envelope(store.applyBodyEvidence(body.actor,tenant,fact)),sourceVerified:true});
    }
    if(path==='/admission-delivery-status'){
     assertControlAccess(body.actor,'read',tenant.id,tenant.region);const state=this.delivery!.known();if(state&&state.configuration.tenantId!==tenant.id)throw new ControlError('DELIVERY_CONTROL_IDENTITY_CONFLICT',503);
     return respond({delivery:state,alarmAt:typeof this.ctx.storage.getAlarm==='function'?await this.ctx.storage.getAlarm():null,protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
    }
    if(path==='/admission-delivery-control'){
     assertControlAccess(body.actor,'administer',tenant.id,tenant.region);
     if(!body.control||typeof body.control.enabled!=='boolean')throw new ControlError('INVALID_DELIVERY_CONTROL',422);
     this.delivery!.ensure(tenant.id,now);if(body.control.enabled)await this.arm(now+1000,true);
     const configuration=this.delivery!.change(body.actor,tenant,body.control,now);
     return respond({configuration,current:this.delivery!.known(),protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
    }
    if(path==='/admission-dispatch'){const delivery=await this.deliverOne(body.actor,tenant);return respond({delivery,protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});}
    if(path==='/admission-read')return respond(envelope(store.read(body.actor,tenant,body.reservationId)));
    if(path==='/admission-credential'||path==='/admission-verify'){
     if(!['OPERATOR','SRE_ADMIN'].includes(body.actor.role))return respond({code:'CONTROL_FORBIDDEN'},403);
     const current=store.read(body.actor,tenant,body.reservationId);
     if(tenant.status!=='ACTIVE'||current.policy.tenantVersion!==tenant.version)return respond({code:'POLICY_METADATA_NOT_ACTIVATED'},409);
     const key=this.env.ANSTS_ADMISSION_SIGNING_KEY??'';
     if(path==='/admission-credential')return respond({credential:issueAdmissionCredential(current,key,now),protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
     return respond({claims:verifyAdmissionCredential(body.credential,key,current,now,body.target),protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
    }
    if(!body.input||typeof body.input!=='object')return respond({code:'INVALID_ADMISSION_INPUT'},422);
    if(path==='/admission-prepare'){await this.beforeMutation(body.actor,tenant);return respond(envelope(store.prepare(body.actor,tenant,body.input as unknown as PrepareAdmission,body.idempotencyKey,now)));}
    const input=body.input as unknown as AdmissionTransition;
    // Runtime acknowledgement and cost/terminal evidence need authenticated adapters before exposure.
    if(['RUNNING','SETTLING','SETTLED'].includes(input.to)||input.evidence)return respond({code:'EXECUTION_ADAPTER_REQUIRED'},409);
    await this.beforeMutation(body.actor,tenant);return respond(envelope(store.transition(body.actor,tenant,input,body.idempotencyKey,now)));
   }
   if(path==='/policy-read')return respond(this.store.read(body.actor,tenant,body.page));
   return respond({policy:this.store.write(body.actor,tenant,body.input),enforcement:'NOT_CONNECTED'});
  }catch(error){if(error instanceof ControlError)return respond({code:error.code},error.status);return respond({code:error instanceof SyntaxError?'INVALID_JSON':'TENANT_CONTROL_UNAVAILABLE'},error instanceof SyntaxError?400:503);}
 }
}
