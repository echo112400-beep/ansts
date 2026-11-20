import {CostExportStore} from '../finops/cost_export_store.js';
import {AuthorityArchiveStore,type ArchiveWrite} from '../finops/authority_archive.js';
import {joinedTransactions} from '../finops/finops_page.js';
import {TenantRateStore} from '../finops/tenant_rate_store.js';
import {UsageCostLedger} from '../finops/usage_cost_ledger.js';
import {verifyRateCard} from '../finops/rate_cards.js';
import {PeriodSettlement,settlementMonth,type ClosePeriodWrite,type ManualAdjustmentWrite} from '../finops/period_settlement.js';
import {UsageInbox} from '../finops/usage_inbox.js';
import {collectTrustedExecutionCoverage} from '../control/execution_inventory_ingress.js';
import {canonicalUsageIdentity,canonicalUsageResources,verifyUsageEvent,type UsageIdentity,type UsageResources} from '../control/usage_event.js';
import type {CloudflareCelldEnv,DurableObjectNamespaceLike,DurableObjectStateLike} from './env.js';
import type {DirectoryStorage,TenantRecord} from '../control/tenant_directory.js';
import {assertControlAccess,ControlError,parseCreditUnits,type ControlActor} from '../control/contracts.js';
import {TenantPolicyStore,type QuotaPolicyWrite} from '../finops/tenant_policy_store.js';
import {verifyBodyEvidence} from '../control/body_execution_evidence.js';
import {AdmissionDeliveryControl} from '../control/admission_delivery_control.js';
import {dispatchAdmissionMessage} from '../control/admission_dispatcher.js';
import type {AdmissionMessageReceipt} from '../control/admission_messages.js';
import {AdmissionReservationStore,type AdmissionReservation,type PrepareAdmission,type AdmissionTransition,type SettleAdmissionCost} from '../control/admission_reservations.js';
import {issueAdmissionCredential,verifyAdmissionCredential} from '../control/admission_credentials.js';
import {equalBodyToken,serviceKeys,bodyServiceToken,type BodyAuthConfiguration} from './body_auth.js';
export interface TenantControlEnv extends BodyAuthConfiguration {ANSTS_CAPACITY_INVENTORY_CELL?:DurableObjectNamespaceLike;ANSTS_DIRECTORY_SERVICE_TOKEN?:string;ANSTS_ADMISSION_SERVICE_TOKEN?:string;ANSTS_ADMISSION_SIGNING_KEY?:string;ANSTS_ADMISSION_ESTIMATED_CREDIT_UNITS?:string;ANSTS_TENANT_DIRECTORY_CELL?:DurableObjectNamespaceLike;ANSTS_ADMISSION_MAILBOX?:DurableObjectNamespaceLike;ANSTS_SCHEDULER_CELL?:CloudflareCelldEnv['ANSTS_SCHEDULER_CELL'];ANSTS_BODY_CELL?:CloudflareCelldEnv['ANSTS_BODY_CELL'];ANSTS_BODY_WORKER_URL?:string;ANSTS_BODY_NAMESPACE_PREFIX?:string}
export class TenantControlCell {
 private readonly archives:AuthorityArchiveStore|null;
 private readonly store:TenantPolicyStore|null;
 private readonly storage:DirectoryStorage|null;
 private readonly rates:TenantRateStore|null;
 private readonly ledger:UsageCostLedger|null;
 private readonly settlements:PeriodSettlement|null;
 private readonly usage:UsageInbox|null;
 private readonly admissions:AdmissionReservationStore|null;
 private readonly delivery:AdmissionDeliveryControl|null;
 private alarmRegistration:Promise<void>=Promise.resolve();
 constructor(private readonly ctx:DurableObjectStateLike&{storage:{transactionSync?:DirectoryStorage['transactionSync']}},private readonly env:TenantControlEnv={}) {
  const storage=typeof ctx.storage.transactionSync==='function'?joinedTransactions({sql:ctx.storage.sql,transactionSync:ctx.storage.transactionSync.bind(ctx.storage)}):null;
  storage?.sql.exec('CREATE TABLE IF NOT EXISTS capacity_binding_intents (reservation_id TEXT PRIMARY KEY, receipt TEXT NOT NULL)');
  storage?.sql.exec('CREATE TABLE IF NOT EXISTS capacity_binding_targets (reservation_id TEXT PRIMARY KEY, target TEXT NOT NULL)');
  storage?.sql.exec('CREATE TABLE IF NOT EXISTS capacity_release_outbox (receipt TEXT PRIMARY KEY, reservation_json TEXT NOT NULL)');
  storage?.sql.exec('CREATE TABLE IF NOT EXISTS estimated_admission_expiry (reservation_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, expires_at INTEGER NOT NULL)');
  this.archives=storage?new AuthorityArchiveStore(storage,'TENANT'):null;
  this.storage=storage;this.rates=storage?new TenantRateStore(storage):null;this.ledger=storage?new UsageCostLedger(storage):null;this.settlements=storage?new PeriodSettlement(storage):null;
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
 private async deliverCapacityRelease(reservation:AdmissionReservation):Promise<void> {
  const ns=this.env.ANSTS_CAPACITY_INVENTORY_CELL,token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
  if(!ns||!token||token.length<32)throw new ControlError('CAPACITY_RELEASE_UNAVAILABLE',503);
  const response=await ns.get(ns.idFromName('capacity-inventory-v1')).fetch(new Request('https://capacity.internal/grant-release',{
   method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},
   body:JSON.stringify({reservation}),
  }));
  await response.body?.cancel();
  if(!response.ok)throw new ControlError('CAPACITY_RELEASE_UNAVAILABLE',503);
  this.storage!.sql.exec('DELETE FROM capacity_release_outbox WHERE receipt = ?',reservation.capacityReceipt);
 }
 async alarm():Promise<void> {
  try {
   await this.expireEstimatedAdmissions();
   await this.runAdmissionAlarm();
  } finally {
   const next=this.storage?.sql.exec<{expires_at:number}>(
    'SELECT expires_at FROM estimated_admission_expiry ORDER BY expires_at LIMIT 1',
   ).toArray()[0];
   if(next)await this.arm(Math.max(Date.now()+1000,next.expires_at));
  }
 }
 private async confirmPilotExpiry(candidate:AdmissionReservation):Promise<boolean> {
  const ns=this.env.ANSTS_SCHEDULER_CELL, token=this.env.ANSTS_ADMISSION_SERVICE_TOKEN;
  if(!ns||!token||token.length<32)return false;
  const id=ns.idFromName('global-shard-0');
  if(id.toString()!==candidate.schedulerObjectId)return false;
  const response=await ns.get(id).fetch(new Request('https://scheduler.internal/pilot-expiry-status',{
   method:'POST',signal:AbortSignal.timeout(2000),
   headers:{authorization:'Bearer '+token,'content-type':'application/json'},
   body:JSON.stringify({reservationId:candidate.id,taskId:candidate.taskId,inputDigest:candidate.executionInputDigest}),
  }));
  if(!response.ok){await response.body?.cancel();throw new ControlError('SCHEDULER_EXPIRY_UNAVAILABLE',503);}
  return (await response.json() as {expiredBeforeAcceptance?:boolean}).expiredBeforeAcceptance===true;
 }
 private async expireEstimatedAdmissions():Promise<void> {
  if(!this.storage||!this.admissions)return;
  const now=Date.now();
  const expired=this.storage.sql.exec<{tenant_id:string;reservation_id:string}>(
   'SELECT tenant_id,reservation_id FROM estimated_admission_expiry WHERE expires_at<=? ORDER BY expires_at LIMIT 16',now,
  ).toArray();
  if(expired.length)await this.arm(now+5000);
  for(const row of expired){
   const actor:ControlActor={sub:'system:admission-expiry',role:'SRE_ADMIN',scopeTenant:row.tenant_id,scopeRegion:'all'};
   const tenant=await this.alarmTenant(actor,row.tenant_id);
   const candidate=this.admissions.read(actor,tenant,row.reservation_id);
   if(candidate.phase==='START_AUTHORIZED'&&candidate.schedulerObjectId&&!candidate.executionObserved){
    let confirmed:boolean;
    try { confirmed=await this.confirmPilotExpiry(candidate); }
    catch {
     // Delay this probe without changing the reservation deadline or starving later rows.
     this.storage.sql.exec('UPDATE estimated_admission_expiry SET expires_at=? WHERE reservation_id=?',
      Date.now()+5000,row.reservation_id);
     continue;
    }
    if(confirmed){
     this.storage.transactionSync(()=>{
      const current=this.admissions!.read(actor,tenant,row.reservation_id);
      if(current.version!==candidate.version)return;
      const result=this.admissions!.expireUnacceptedPilot(actor,tenant,candidate,Date.now());
      this.storage!.sql.exec('INSERT OR IGNORE INTO capacity_release_outbox VALUES (?,?)',
       result.capacityReceipt,JSON.stringify(result));
      this.storage!.sql.exec('DELETE FROM estimated_admission_expiry WHERE reservation_id=?',row.reservation_id);
     });
     continue;
    }
   }
   this.storage.transactionSync(()=>{
    const current=this.admissions!.read(actor,tenant,row.reservation_id);
    if(!current.startAuthorized&&!current.executionObserved&&['PREPARING','RESERVED'].includes(current.phase)){
     const intent=this.storage!.sql.exec<{receipt:string}>(
      'SELECT receipt FROM capacity_binding_intents WHERE reservation_id=?',current.id,
     ).toArray()[0];
     const receipt=current.capacityReceipt??intent?.receipt;
     const result=this.admissions!.transition(actor,tenant,{reservationId:current.id,expectedVersion:current.version,
      generation:current.generation,fencing:current.fencing,to:'CANCELLED',reason:'Estimated admission expired before start authorization'},
      'expire-'+current.id,now);
     if(receipt?.startsWith('capacity-grant:'))this.storage!.sql.exec(
      'INSERT OR IGNORE INTO capacity_release_outbox VALUES (?,?)',receipt,JSON.stringify({...result,capacityReceipt:receipt}));
    }
    this.storage!.sql.exec('DELETE FROM estimated_admission_expiry WHERE reservation_id=?',row.reservation_id);
   });
  }
 }
 private async runAdmissionAlarm():Promise<void> {
  const pending=this.storage?.sql.exec<{reservation_json:string}>(
   'SELECT reservation_json FROM capacity_release_outbox ORDER BY rowid LIMIT 4',
  ).toArray()??[];
  if(pending.length){
   await this.arm(Date.now()+30000);
   for(const row of pending){
    try{await this.deliverCapacityRelease(JSON.parse(row.reservation_json) as AdmissionReservation);}
    catch{ /* The pending row remains for the next alarm. */ }
   }
  }
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
  if(request.method!=='POST'||!['/policy-read','/policy-write','/admission-read','/admission-capacity-headroom','/admission-cost-preview','/admission-settle-cost','/admission-prepare','/admission-prepare-estimated','/admission-shadow-preview','/admission-transition','/admission-credential','/admission-verify','/admission-outbox','/admission-dispatch','/admission-delivery-status','/admission-delivery-control','/admission-observe-execution','/admission-observation-binding','/admission-usage-binding','/admission-usage-bind','/admission-usage-ingest','/usage-read','/reservation-summary','/rate-read','/rate-activate','/cost-read','/cost-summary','/cost-export-create','/cost-export-run','/cost-export-status','/cost-export-download','/rate-activation-audit','/finops-archive-status','/finops-archive-create','/finops-archive-read','/period-preview','/period-close','/period-adjustment'].includes(path))return respond({code:'NOT_FOUND'},404);
  try {
   const raw=await request.text();if(raw.length>65536)return respond({code:'PAYLOAD_TOO_LARGE'},413);
   const body=JSON.parse(raw) as {actor:ControlActor;tenantId:string;input:QuotaPolicyWrite;page?:{limit?:number;cursor?:string};idempotencyKey:string;reservationId:string;credential:string;target:string;proof?:string;identity?:UsageIdentity;resources?:UsageResources;priceEligible?:boolean;archiveId?:string;exportId?:string;periodId?:string;limit?:number;control?:{expectedVersion:number;enabled:boolean;reason:string;idempotencyKey:string}};
   if(!body?.actor||!['VIEWER','OPERATOR','SRE_ADMIN'].includes(body.actor.role)||typeof body.actor.sub!=='string'||!body.actor.sub.trim()||typeof body.tenantId!=='string')return respond({code:'INVALID_ACTOR'},403);
   const directoryToken=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;if(!directoryToken||directoryToken.length<32)return respond({code:'TENANT_CONTROL_UNAVAILABLE'},503);
   const ns=this.env.ANSTS_TENANT_DIRECTORY_CELL;
   const resolved=await ns.get(ns.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/tenant',{method:'POST',headers:{authorization:'Bearer '+directoryToken,'content-type':'application/json'},body:JSON.stringify({actor:body.actor,tenantId:body.tenantId})}));
   if(!resolved.ok)return new Response(resolved.body,{status:resolved.status,headers:{'content-type':'application/json','cache-control':'no-store'}});
   const tenant=await resolved.json() as TenantRecord;
   if(tenant.id!==body.tenantId)return respond({code:'TENANT_CONTROL_IDENTITY_CONFLICT'},503);
   if(path==='/finops-archive-status')return respond(this.archives!.status(body.actor,tenant));
   if(path==='/finops-archive-create')return respond(this.archives!.create(body.actor,tenant,body.input as unknown as ArchiveWrite,directoryToken));
   if(path==='/finops-archive-read')return respond(this.archives!.read(body.actor,tenant,body.archiveId??''));
   if(path==='/rate-activation-audit')return respond(this.rates!.audit(body.actor,tenant,body.page??{},directoryToken));
   if(path==='/rate-read')return respond(this.rates!.read(body.actor,tenant,body.page??{},directoryToken));
   if(path==='/rate-activate'){
    assertControlAccess(body.actor,'administer',tenant.id,tenant.region);
    const input=body.input as unknown as Parameters<TenantRateStore['activate']>[2];
    if(!input||!Number.isSafeInteger(input.rateCardVersion)||input.rateCardVersion<1)throw new ControlError('INVALID_RATE_ACTIVATION',422);
    // Caller identity retains tenant scope; directory template resolution uses a private service-only actor.
    const resolver:ControlActor={sub:'system:tenant-rate-resolver',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'};
    const response=await ns.get(ns.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/rate-resolve',{method:'POST',headers:{authorization:'Bearer '+directoryToken,'content-type':'application/json'},body:JSON.stringify({actor:resolver,rateCardVersion:input.rateCardVersion})}));
    if(!response.ok)return new Response(response.body,{status:response.status,headers:{'content-type':'application/json','cache-control':'no-store'}});
    const value=await response.json() as {rateCard:Parameters<TenantRateStore['activate']>[3]};
    return respond({activation:this.rates!.activate(body.actor,tenant,input,verifyRateCard(value.rateCard)),enforcement:'NOT_CONNECTED'});
   }
   if(path.startsWith('/cost-export-')){
    const exports=new CostExportStore(this.storage!);
    if(path==='/cost-export-create')return respond(exports.create(body.actor,tenant,body.idempotencyKey),202);
    const id=body.exportId??'';
    if(path==='/cost-export-status')return respond(exports.status(body.actor,tenant,id));
    if(path==='/cost-export-download')return new Response(exports.download(body.actor,tenant,id),{headers:{
      'content-type':'text/csv; charset=utf-8','content-disposition':'attachment; filename="cost-export.csv"',
      'cache-control':'no-store','x-content-type-options':'nosniff',
    }});
    const status=exports.status(body.actor,tenant,id);
    if(status.status==='READY')return respond(status);
    this.ledger!.projectPending(100);
    const rows:string[][]=[['event_id','watermark','utc_day','cost_center','cost_center_version','rate_card_version','activation_version','unit','dimension','exact_numerator','exact_denominator','pending']];
    let cursor:string|undefined;
    do{
     const page=this.ledger!.page(body.actor,tenant,{limit:100,cursor},directoryToken);
     for(const item of page.items){
      const entry=item.entry,context=entry?.context,activation=context?.activation;
      const prefix=[item.eventId,String(page.watermark),entry?.periodId??'',context?.costCenter??'',
       String(context?.costCenterVersion??''),String(activation?.rateCard.version??''),String(activation?.version??''),'credit-subunit'];
      if(!entry?.lines.length)rows.push([...prefix,'','','',item.pending??entry?.pending.join(';')??'']);
      for(const line of entry?.lines??[])rows.push([...prefix,line.dimension,line.exactCredit?.numerator??'',line.exactCredit?.denominator??'',
       [...new Set([...entry!.pending,...line.pending])].join(';')]);
      if(rows.length>10000)throw new ControlError('COST_EXPORT_TOO_LARGE',413);
     }
     cursor=page.nextCursor??undefined;
    }while(cursor);
    return respond(exports.complete(body.actor,tenant,id,rows));
   }
   if(path==='/cost-read'||path==='/cost-summary'){
    assertControlAccess(body.actor,'read',tenant.id,tenant.region);
    // Bounded recovery from committed source rows, including pre-upgrade history with null prices.
    this.ledger!.projectPending(100);
    const summary=this.ledger!.summary(body.actor,tenant);
    return respond(path==='/cost-summary'?summary:{...this.ledger!.page(body.actor,tenant,body.page??{},directoryToken),summary});
   }
   if(path==='/period-adjustment'){
    if(typeof body.periodId!=='string')throw new ControlError('INVALID_SETTLEMENT_PERIOD',422);
    return respond(this.settlements!.addManualAdjustment(body.actor,tenant,body.periodId,body.input as unknown as ManualAdjustmentWrite));
   }
   if(path==='/period-close'){
    assertControlAccess(body.actor,'administer',tenant.id,tenant.region);
    if(body.actor.scopeRegion!=='all')throw new ControlError('COMPLETE_TENANT_SCOPE_REQUIRED',403);
    if(typeof body.periodId!=='string')throw new ControlError('INVALID_SETTLEMENT_PERIOD',422);
    const input=body.input as unknown as ClosePeriodWrite;
    const settled=this.settlements!;
    const replay=settled.replayClose(body.actor,tenant,body.periodId,input);
    const existing=replay??settled.readClosed(body.actor,tenant,body.periodId);
    const coverage=existing?null:(await collectTrustedExecutionCoverage(body.actor,tenant.id,body.periodId,this.env as unknown as CloudflareCelldEnv)).coverage;
    const statement=replay??settled.close(body.actor,tenant,body.periodId,input,coverage);
    return respond({tenantId:statement.tenantId,periodId:statement.periodId,calendar:statement.calendar,version:statement.version,
     revision:statement.revision,digest:statement.digest,closedAt:statement.closedAt,watermarks:statement.watermarks,
     unit:statement.unit,unitVersion:statement.unitVersion,totalCreditUnits:statement.totalCreditUnits,
     buckets:statement.buckets,adjustmentBuckets:statement.adjustmentBuckets,manualAdjustmentCreditUnits:statement.manualAdjustmentCreditUnits,final:true});
   }
   if(path==='/period-preview'){
    assertControlAccess(body.actor,'read',tenant.id,tenant.region);
    if(body.actor.scopeRegion!=='all')throw new ControlError('COMPLETE_TENANT_SCOPE_REQUIRED',403);
    if(typeof body.periodId!=='string')throw new ControlError('INVALID_SETTLEMENT_PERIOD',422);
    settlementMonth(body.periodId);
    const coverage=await collectTrustedExecutionCoverage(body.actor,tenant.id,body.periodId,this.env as unknown as CloudflareCelldEnv);
    const preview=this.settlements!.preview(body.actor,tenant,body.periodId,coverage.coverage);
    return respond({tenantId:preview.tenantId,periodId:preview.periodId,tenantVersion:preview.tenantVersion,calendar:preview.calendar,unit:preview.unit,unitVersion:preview.unitVersion,watermarks:preview.watermarks,buckets:preview.buckets,adjustmentBuckets:preview.adjustmentBuckets,manualAdjustmentCreditUnits:preview.manualAdjustmentCreditUnits,knownSubtotalCreditUnits:preview.knownSubtotalCreditUnits,totalCreditUnits:preview.totalCreditUnits,reasons:preview.reasons,complete:preview.complete,revision:preview.revision,final:false,coverage:{complete:coverage.coverage.complete,observedAt:coverage.coverage.observedAt,inventoryWatermark:coverage.coverage.inventoryWatermark,attemptCount:coverage.coverage.attempts.length},coverageDiagnostics:coverage.reasons});
   }
   if(path==='/reservation-summary'){
    const {used,remaining,policyVersion}=this.admissions!.capacityHeadroom(body.actor,tenant);
    return respond({used,remaining,policyVersion});
   }
   if(path==='/usage-read')return respond(this.usage!.page(body.actor,tenant,body.page??{},this.env.ANSTS_ADMISSION_SIGNING_KEY??''));
   if(path.startsWith('/admission-usage-')){
    assertControlAccess(body.actor,'request_capacity',tenant.id,tenant.region);const identity=canonicalUsageIdentity(body.identity!);if(identity.tenantId!==tenant.id)throw new ControlError('USAGE_BINDING_FORBIDDEN',403);
    if(path==='/admission-usage-binding')return respond({binding:this.usage!.binding(identity)});
    if(path==='/admission-usage-bind'){
     const resources=canonicalUsageResources(body.resources!);const existing=this.usage!.binding(identity);
     let centerVersion:number|null=null;
     const eligible=!existing&&body.priceEligible===true&&this.admissions!.usageContext(body.actor,tenant,identity,resources).priceEligible;
     if(eligible){
      const response=await ns.get(ns.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/accounting-context',{method:'POST',headers:{authorization:'Bearer '+directoryToken,'content-type':'application/json'},body:JSON.stringify({actor:body.actor,tenantId:tenant.id})}));
      if(!response.ok)return new Response(response.body,{status:response.status,headers:{'content-type':'application/json','cache-control':'no-store'}});
      const metadata=await response.json() as {tenant:TenantRecord;costCenterVersion:number|null};
      if(!metadata.tenant||metadata.tenant.id!==tenant.id||metadata.tenant.version!==tenant.version||metadata.tenant.region!==tenant.region||metadata.tenant.costCenter!==tenant.costCenter)throw new ControlError('ACCOUNTING_METADATA_CHANGED',409);
      centerVersion=metadata.costCenterVersion;
     }
     this.storage!.transactionSync(()=>{
      const current=this.usage!.binding(identity);
      const context=this.admissions!.usageContext(body.actor,tenant,identity,resources);
      // Preserve a binding created while directory metadata was being resolved.
      const price=!current&&eligible&&context.priceEligible?this.rates!.pin(body.actor,tenant,identity,{costCenterVersion:centerVersion,poolId:null}):null;
      this.usage!.bind(identity,resources,{tenantVersion:context.tenantVersion,region:context.region,costCenter:context.costCenter,rateCardVersion:price?.activation?.rateCard.version??null});
     });return respond({bound:true});
    }
    const binding=this.usage!.binding(identity);if(!binding)throw new ControlError('USAGE_EXECUTION_BINDING_REQUIRED',409);
    let event;for(const key of serviceKeys(this.env))try{event=verifyUsageEvent(body.proof??'',bodyServiceToken(key,identity.producerId));break;}catch(error){if(!(error instanceof ControlError)||error.code!=='USAGE_AUTH_REQUIRED')throw error;}
    if(!event)throw new ControlError('USAGE_AUTH_REQUIRED',401);
    const usageEvent=event,receivedAt=Date.now();
    const receipt=this.storage!.transactionSync(()=>{
     const accepted=this.usage!.receive(usageEvent,identity,receivedAt);
     const entry=accepted.outcome==='RECORDED'?this.ledger!.project(usageEvent.eventId):null;
     this.settlements!.recordUsageAdjustment(tenant.id,usageEvent,accepted,entry,receivedAt);
     return accepted;
    });
    return respond({receipt,sourceVerified:true},receipt.outcome==='RECORDED'?200:409);
   }
   if(admission){
    const store=this.admissions!;const now=Date.now();const envelope=(reservation:unknown)=>({reservation,protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
    if(path==='/admission-capacity-headroom')return respond(store.capacityHeadroom(body.actor,tenant));
    if(path==='/admission-observation-binding')return respond({binding:store.observationBinding(body.actor,tenant,body.target,body.input as unknown as import('../control/body_execution_evidence.js').BodyObservationLookup),protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
    if(path==='/admission-outbox')return respond({messages:store.listMessages(body.actor,tenant,body.limit),protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
    if(path==='/admission-observe-execution'){
     const reservation=store.read(body.actor,tenant,body.reservationId);if(!reservation.target||!reservation.executionInputDigest)throw new ControlError('EXECUTION_INPUT_NOT_PINNED',409);
     const keys=serviceKeys(this.env);if(!keys.length)throw new ControlError('BODY_EVIDENCE_VERIFIER_UNAVAILABLE',503);
     let fact;for(const key of keys){try{fact=verifyBodyEvidence(body.proof??'',bodyServiceToken(key,reservation.target));break;}catch(error){if(!(error instanceof ControlError)||error.code!=='BODY_EVIDENCE_AUTH_REQUIRED')throw error;}}
     if(!fact)throw new ControlError('BODY_EVIDENCE_AUTH_REQUIRED',401);
     if(fact.reservationId!==body.reservationId)throw new ControlError('EXECUTION_EVIDENCE_BINDING_CONFLICT',409);
     await this.beforeMutation(body.actor,tenant);
     const release=['TERMINATED','NOT_STARTED'].includes(fact.phase)&&reservation.capacityReceipt?.startsWith('capacity-grant:');
     if(release)await this.arm(Date.now()+1000);
     const observed=this.storage!.transactionSync(()=>{
      const result=store.applyBodyEvidence(body.actor,tenant,fact);
      if(release)this.storage!.sql.exec('INSERT OR IGNORE INTO capacity_release_outbox VALUES (?, ?)',
       result.capacityReceipt,JSON.stringify(result));
      return result;
     });
     if(release)await this.deliverCapacityRelease(observed);
     return respond({...envelope(observed),sourceVerified:true});
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
    if(path==='/admission-settle-cost'){
     assertControlAccess(body.actor,'administer',tenant.id,tenant.region);
     if(body.actor.scopeRegion!=='all')throw new ControlError('COMPLETE_TENANT_SCOPE_REQUIRED',403);
     const input=body.input as unknown as SettleAdmissionCost;
     await this.beforeMutation(body.actor,tenant);
     const currentTime=Date.now();
     return respond(envelope(store.settleUsageCost(body.actor,tenant,input,body.idempotencyKey,currentTime,
      ()=>this.settlements!.previewReservationCost(body.actor,tenant,store,input.reservationId,currentTime))));
    }
    if(path==='/admission-shadow-preview'){
     const estimate=this.env.ANSTS_ADMISSION_ESTIMATED_CREDIT_UNITS;
     try {if(!estimate||parseCreditUnits(estimate)<=0n)throw new Error();}
     catch{return respond({code:'ADMISSION_ESTIMATE_UNAVAILABLE'},503);}
     const {resources}=body.input as unknown as Pick<PrepareAdmission,'resources'> ?? {};
     store.previewEstimatedAdmission(body.actor,tenant,resources,estimate,now);
     return respond({code:'ADMISSION_QUOTA_AVAILABLE'});
    }
    if(path==='/admission-cost-preview')return respond(this.settlements!.previewReservationCost(body.actor,tenant,store,body.reservationId,now));
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
    if(path==='/admission-prepare-estimated'){
     const input=body.input as unknown as Pick<PrepareAdmission,'reservationId'|'taskId'|'attemptId'|'fencing'|'resources'|'executionInputDigest'|'schedulerObjectId'>;
     const fields=['reservationId','taskId','attemptId','fencing','resources','executionInputDigest','schedulerObjectId'];
     if(Object.keys(input).some(field=>!fields.includes(field))||!input.executionInputDigest)
      return respond({code:'INVALID_ESTIMATED_ADMISSION_INPUT'},422);
     assertControlAccess(body.actor,'request_capacity',tenant.id,tenant.region);
     await this.beforeMutation(body.actor,tenant);
     await this.arm(Date.now()+60000);
     const currentTime=Date.now();
     try {
      const previous=store.read(body.actor,tenant,input.reservationId);
      if(previous.taskId!==input.taskId||previous.attemptId!==input.attemptId||
       previous.fencing!==input.fencing||previous.executionInputDigest!==input.executionInputDigest||
       previous.schedulerObjectId!==input.schedulerObjectId||
       previous.resources.cpuMilliCores!==input.resources?.cpuMilliCores||
       previous.resources.memoryMiB!==input.resources?.memoryMiB||
       input.resources?.concurrentAttempts!==1||input.resources?.queuedTasks!==0)
       throw new ControlError('ADMISSION_RESERVATION_MISMATCH',409);
      if(previous.expiresAt<=currentTime||['CANCELLED','SETTLED'].includes(previous.phase))
       throw new ControlError('ADMISSION_RESERVATION_INACTIVE',409);
      return respond(envelope(previous));
     } catch(error) {
      if(!(error instanceof ControlError)||error.code!=='RESERVATION_NOT_FOUND')throw error;
     }
     const estimate=this.env.ANSTS_ADMISSION_ESTIMATED_CREDIT_UNITS;
     try {
      if(!estimate||parseCreditUnits(estimate)<=0n)throw new Error();
     } catch {
      return respond({code:'ADMISSION_ESTIMATE_UNAVAILABLE'},503);
     }
     const policy=this.store!.read(body.actor,tenant).current;
     if(!policy)throw new ControlError('QUOTA_POLICY_REQUIRED',409);
     return respond(envelope(this.storage!.transactionSync(()=>{
      const reservation=store.prepare(body.actor,tenant,{...input,estimatedCreditUnits:estimate,
       expectedPolicyVersion:policy.version,expiresAt:currentTime+60000},body.idempotencyKey,currentTime);
      this.storage!.sql.exec('INSERT OR IGNORE INTO estimated_admission_expiry VALUES (?,?,?)',
       reservation.id,tenant.id,reservation.expiresAt);
      return reservation;
     })));
    }
    if(path==='/admission-prepare'){await this.beforeMutation(body.actor,tenant);return respond(envelope(store.prepare(body.actor,tenant,body.input as unknown as PrepareAdmission,body.idempotencyKey,now)));}
    const input=body.input as unknown as AdmissionTransition;
    if(input.to==='RESERVED'){
     const pending=this.storage!.sql.exec<{receipt:string;target:string|null}>(
      `SELECT i.receipt,t.target FROM capacity_binding_intents i
       LEFT JOIN capacity_binding_targets t ON t.reservation_id=i.reservation_id
       WHERE i.reservation_id=?`,input.reservationId,
     ).toArray()[0];
     if(pending&&(pending.receipt!==input.capacityReceipt||(pending.target!==null&&pending.target!==input.target))){
      throw new ControlError('CAPACITY_GRANT_BINDING_CONFLICT',409);
     }
    }
    const grant=/^capacity-grant:([a-f0-9]{64})$/.exec(input.capacityReceipt??'');
    if(input.to==='RESERVED'&&input.capacityReceipt?.startsWith('capacity-grant:')){
     if(!grant)throw new ControlError('INVALID_CAPACITY_INPUT',422);
     assertControlAccess(body.actor,'request_capacity',tenant.id,tenant.region);
     const reservation=store.read(body.actor,tenant,input.reservationId);
     assertControlAccess(body.actor,'request_capacity',tenant.id,reservation.policy.region);
     if(reservation.generation!==input.generation||reservation.fencing!==input.fencing)
      throw new ControlError('CAPACITY_GRANT_BINDING_CONFLICT',409);
     if(reservation.capacityReceipt!==input.capacityReceipt||reservation.target!==input.target){
      if(reservation.phase!=='PREPARING'||reservation.version!==input.expectedVersion)
       throw new ControlError('CAPACITY_RESERVATION_MISMATCH',409);
      const ns=this.env.ANSTS_CAPACITY_INVENTORY_CELL,token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
      if(!ns||!token||token.length<32)throw new ControlError('CAPACITY_BIND_UNAVAILABLE',503);
      this.storage!.transactionSync(()=>{
       const intent=this.storage!.sql.exec<{receipt:string}>(
        'SELECT receipt FROM capacity_binding_intents WHERE reservation_id = ?',reservation.id,
       ).toArray()[0];
       if(intent&&intent.receipt!==input.capacityReceipt)throw new ControlError('CAPACITY_GRANT_BINDING_CONFLICT',409);
       if(!intent)this.storage!.sql.exec('INSERT INTO capacity_binding_intents VALUES (?, ?)',reservation.id,input.capacityReceipt);
       if(typeof input.target!=='string'||!input.target)throw new ControlError('INVALID_CAPACITY_INPUT',422);
       this.storage!.sql.exec('INSERT OR IGNORE INTO capacity_binding_targets VALUES (?, ?)',reservation.id,input.target);
      });
      const response=await ns.get(ns.idFromName('capacity-inventory-v1')).fetch(new Request('https://capacity.internal/grant-bind',{
       method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},
       body:JSON.stringify({requestId:grant[1],reservation,target:input.target}),
      }));
      if(!response.ok)throw new ControlError('CAPACITY_BIND_REJECTED',response.status);
      const result=await response.json() as {capacityReceipt?:string};
      if(result.capacityReceipt!==input.capacityReceipt)throw new ControlError('CAPACITY_BIND_UNAVAILABLE',503);
     }
    }
    if(input.to==='START_AUTHORIZED'){
     const reservation=store.read(body.actor,tenant,input.reservationId);
     if(reservation.capacityReceipt?.startsWith('capacity-grant:')&&!reservation.startAuthorized){
      assertControlAccess(body.actor,'request_capacity',tenant.id,reservation.policy.region);
      const ns=this.env.ANSTS_CAPACITY_INVENTORY_CELL,token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
      if(!ns||!token||token.length<32)throw new ControlError('CAPACITY_BIND_UNAVAILABLE',503);
      const {id:reservationId,attemptId,generation,fencing}=reservation;
      const response=await ns.get(ns.idFromName('capacity-inventory-v1')).fetch(new Request('https://capacity.internal/grant-read',{
       method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},
       body:JSON.stringify({actor:body.actor,requestInput:{
        id:reservation.capacityReceipt.slice('capacity-grant:'.length),tenantId:tenant.id,
        binding:{reservationId,attemptId,generation,fencing},
       }}),
      }));
      if(!response.ok)throw new ControlError('CAPACITY_BIND_UNAVAILABLE',503);
      const {grant}=await response.json() as {grant:{target:string;capacityReceipt:string}|null};
      if(grant?.target!==reservation.target||grant?.capacityReceipt!==reservation.capacityReceipt)
       throw new ControlError('CAPACITY_TARGET_UNAVAILABLE',409);
     }
    }
    // Runtime acknowledgement and cost/terminal evidence need authenticated adapters before exposure.
    if(['RUNNING','SETTLING','SETTLED'].includes(input.to)||input.evidence)return respond({code:'EXECUTION_ADAPTER_REQUIRED'},409);
    await this.beforeMutation(body.actor,tenant);
    if(input.to!=='CANCELLED'){
     const result=this.storage!.transactionSync(()=>{
      if(input.to==='RESERVED'&&!grant&&this.storage!.sql.exec(
       'SELECT reservation_id FROM capacity_binding_intents WHERE reservation_id=?',input.reservationId,
      ).toArray().length)throw new ControlError('CAPACITY_GRANT_BINDING_CONFLICT',409);
      return store.transition(body.actor,tenant,input,body.idempotencyKey,Date.now());
     });
     return respond(envelope(result));
    }
    const intent=this.storage!.sql.exec<{receipt:string}>(
     'SELECT receipt FROM capacity_binding_intents WHERE reservation_id = ?',input.reservationId,
    ).toArray()[0];
    const current=store.read(body.actor,tenant,input.reservationId);
    const receipt=current.capacityReceipt??intent?.receipt;
    if(!receipt?.startsWith('capacity-grant:')){
     return respond(envelope(store.transition(body.actor,tenant,input,body.idempotencyKey,now)));
    }
    await this.arm(Date.now()+1000);
    const cancelled=this.storage!.transactionSync(()=>{
     const result=store.transition(body.actor,tenant,input,body.idempotencyKey,now);
     this.storage!.sql.exec('INSERT OR IGNORE INTO capacity_release_outbox VALUES (?, ?)',
      receipt,JSON.stringify({...result,capacityReceipt:receipt}));
     return result;
    });
    return respond(envelope(cancelled));
   }
   if(path==='/policy-read')return respond(this.store.read(body.actor,tenant,body.page));
   return respond({policy:this.store.write(body.actor,tenant,body.input),enforcement:'NOT_CONNECTED'});
  }catch(error){if(error instanceof ControlError)return respond({code:error.code},error.status);return respond({code:error instanceof SyntaxError?'INVALID_JSON':'TENANT_CONTROL_UNAVAILABLE'},error instanceof SyntaxError?400:503);}
 }
}









