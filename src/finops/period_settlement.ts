/** UTC-month close/late-event kernel. No runtime adapter, budget release or browser coverage claims. */
import {assertControlAccess,ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage,TenantRecord} from '../control/tenant_directory.js';
import type {AdmissionReservationStore,ReservationPhase} from '../control/admission_reservations.js';
import {canonicalUsageEvent,canonicalUsageIdentity,canonicalUsageResources,usageEventDigest,usageEventId,usageStreamId,type UsageEvent,type UsageIdentity,type ProviderUsage} from '../control/usage_event.js';
import {joinedTransactions,pageWindow,type FinopsPage} from './finops_page.js';
import {UsageCostLedger,type UsageCostEntry} from './usage_cost_ledger.js';
import {MAX_USAGE_STREAM_EVENTS,usageConflictId,type UsageReceipt} from './usage_inbox.js';
import {exactAdd,roundCredit,rateHash,MAX_CREDIT,validText,type ExactAmount,type Rounding} from './rate_cards.js';
const LIMIT=10000,MAX_DOCUMENT=8*1024*1024;
/** The future adapter must authenticate and fully read the execution inventory. Never browser input. */
export interface SettlementCoverage {
 schemaVersion:1;purpose:'ansts-settlement-coverage-v1';tenantId:string;periodId:string;
 inventoryWatermark:string;observedAt:number;complete:boolean;attempts:UsageIdentity[];
}
interface BucketDescriptor {periodId:string;costCenter:string;costCenterVersion:number;rateCardVersion:number;activationVersion:number;dimension:string;rounding:Rounding}
export interface SettlementBucket {key:string;descriptor:BucketDescriptor;exactCredit:ExactAmount;creditUnits:string}
export interface ReservationCostPreview {
 reservationId:string;version:number;phase:ReservationPhase;terminalDigest:string|null;
 complete:boolean;buckets:Omit<SettlementBucket,'creditUnits'>[];revision:string|null;reasons:string[];
}
export interface AdjustmentBucket {key:string;originPeriodId:string;originCloseDigest:string;descriptor:BucketDescriptor;previousExact:ExactAmount;addedExact:ExactAmount;nextExact:ExactAmount;creditUnits:string}
export interface ManualPeriodAdjustment {creditUnits:string;reason:string;actor:string;requestId:string;requestDigest:string}
export interface ManualAdjustmentWrite {creditUnits:string;reason:string;expectedCloseDigest:string;requestId:string;idempotencyKey:string}
export interface PeriodAdjustment {schemaVersion:1;id:string;ordinal:number;tenantId:string;originPeriodId:string;originCloseDigest:string;targetPeriodId:string;eventId:string;kind:'LATE_USAGE'|'SOURCE_CONFLICT'|'MANUAL';entry:UsageCostEntry|null;pending:string[];observedAt:number;digest:string;manual?:ManualPeriodAdjustment}
export interface PeriodStatement {
 schemaVersion:1;calendar:'UTC_MONTH';tenantId:string;periodId:string;version:1;tenantVersion:number;closedAt:number;actor:string;reason:string;requestId:string;
 revision:string;basis:SettlementBasis;watermarks:{usage:number;adjustments:number};coverage:SettlementCoverage;entryDigests:{eventId:string;digest:string}[];streamIds:string[];
 buckets:SettlementBucket[];adjustmentBuckets:AdjustmentBucket[];adjustmentIds:string[];totalCreditUnits:string;unit:'credit-subunit';unitVersion:1;digest:string;manualAdjustmentCreditUnits?:string;
}
export interface ClosePeriodWrite {expectedRevision:string;reason:string;requestId:string;idempotencyKey:string}
interface SourceRow {id:string;event_json:string;digest:string;ordinal:number|null}
interface Source {event:UsageEvent;ordinal:number}
interface Stream {schemaVersion:1;identity:UsageIdentity;resources:UsageEvent['resources'];sequence:number;cursor:number;lastAt:number;receivedAt:number;closed:boolean;quarantined:boolean;calls:Record<string,{request:ProviderUsage;response:ProviderUsage|null}>}
type SettlementBasisCoverage=Omit<SettlementCoverage,'observedAt'>&{observedThroughPeriodEnd:boolean};
export interface SettlementBasis {
 tenantId:string;tenantVersion:number;periodId:string;watermarks:{usage:number;adjustments:number};coverage:SettlementBasisCoverage|SettlementCoverage|null;
 entryDigests:{eventId:string;digest:string}[];streamEvidence:{id:string;digest:string;state:Stream}[];reservations:unknown[];
 adjustments:string[];normal:SettlementBucket[];late:AdjustmentBucket[];reasons:string[];
}
export function settlementMonth(id:string){
 if(typeof id!=='string'||!/^\d{4}-(0[1-9]|1[0-2])$/.test(id))throw new ControlError('INVALID_SETTLEMENT_PERIOD',422);
 const year=Number(id.slice(0,4)),month=Number(id.slice(5));if(year<1970||year>9998)throw new ControlError('INVALID_SETTLEMENT_PERIOD',422);
 return {id,startAt:Date.UTC(year,month-1,1),endAt:Date.UTC(year,month,1)};
}
function monthAt(at:number){if(!Number.isSafeInteger(at)||at<0||at>=Date.UTC(9999,0,1))throw new ControlError('INVALID_SETTLEMENT_CLOCK',422);return new Date(at).toISOString().slice(0,7);}
function eventMonth(event:UsageEvent){return monthAt(event.allocation?.startAt??event.at);}
function billable(event:UsageEvent){return event.kind==='RESOURCE_SEGMENT'||event.kind==='PROVIDER_USAGE';}
function bounded<T>(rows:T[]):T[]{if(rows.length>LIMIT)throw new ControlError('SETTLEMENT_SCAN_LIMIT',503);return rows;}
function sourceEvent(row:SourceRow,tenantId:string):Source {
 const value=JSON.parse(row.event_json),event=canonicalUsageEvent(value.event??value);
 if(event.tenantId!==tenantId||event.eventId!==row.id||usageEventDigest(event)!==row.digest||row.ordinal===null||!Number.isSafeInteger(row.ordinal)||row.ordinal<1){
  throw new ControlError('SETTLEMENT_SOURCE_CORRUPT',503);
 }
 return {event,ordinal:row.ordinal};
}
function zero():ExactAmount{return {numerator:'0',denominator:'1'};}
function total(values:Iterable<string>):string{let sum=0n;for(const value of values)sum+=BigInt(value);if(sum < -MAX_CREDIT||sum>MAX_CREDIT)throw new ControlError('CREDIT_OVERFLOW',422);return sum.toString();}
function validAdjustmentCredit(value:unknown):value is string {
 return typeof value==='string'&&/^-?[1-9][0-9]{0,38}$/.test(value)&&BigInt(value)>=-MAX_CREDIT&&BigInt(value)<=MAX_CREDIT;
}
function coverageValue(value:SettlementCoverage|null|undefined,tenantId:string,periodId:string,now:number):SettlementCoverage|null {
 if(!value)return null;
 if(value.schemaVersion!==1||value.purpose!=='ansts-settlement-coverage-v1'||value.tenantId!==tenantId||value.periodId!==periodId||typeof value.complete!=='boolean'||!validText(value.inventoryWatermark,256)||!Number.isSafeInteger(value.observedAt)||value.observedAt<0||value.observedAt>now||!Array.isArray(value.attempts)||value.attempts.length>LIMIT)throw new ControlError('INVALID_SETTLEMENT_COVERAGE',422);
 const attempts=value.attempts.map(canonicalUsageIdentity).sort((a,b)=>usageStreamId(a)<usageStreamId(b)?-1:1);
 if(attempts.some(a=>a.tenantId!==tenantId)||new Set(attempts.map(usageStreamId)).size!==attempts.length)throw new ControlError('INVALID_SETTLEMENT_COVERAGE',422);
 return {schemaVersion:1,purpose:'ansts-settlement-coverage-v1',tenantId,periodId,inventoryWatermark:value.inventoryWatermark,observedAt:value.observedAt,complete:value.complete,attempts};
}
function basisCoverage(coverage:SettlementCoverage|null,periodEnd:number):SettlementBasisCoverage|null {
 if(!coverage)return null;
 const {observedAt,...evidence}=coverage;
 return {...evidence,observedThroughPeriodEnd:observedAt>=periodEnd};
}
function entryBuckets(entry:UsageCostEntry):{buckets:SettlementBucket[];pending:string[]}{
 const pending=new Set([...entry.pending,...entry.lines.flatMap(line=>line.pending)]),context=entry.context;
 if(!context?.activation)pending.add('PRICE_SNAPSHOT_MISSING');if(context?.costCenterVersion==null)pending.add('COST_CENTER_VERSION_MISSING');
 if(pending.size||!context?.activation||context.costCenterVersion===null)return {buckets:[],pending:[...pending].sort()};
 return {buckets:entry.lines.map(line=>{
  if(!line.exactCredit)throw new ControlError('COST_ENTRY_CORRUPT',503);
  const descriptor:BucketDescriptor={periodId:entry.periodId,costCenter:context.costCenter,costCenterVersion:context.costCenterVersion!,rateCardVersion:context.activation!.rateCard.version,activationVersion:context.activation!.version,dimension:line.dimension,rounding:context.activation!.rateCard.rounding};
  return {key:rateHash(descriptor),descriptor,exactCredit:line.exactCredit,creditUnits:'0'};
 }),pending:[]};
}
function merge(buckets:Map<string,SettlementBucket>,entry:UsageCostEntry):string[]{
 const priced=entryBuckets(entry);for(const bucket of priced.buckets){const old=buckets.get(bucket.key);buckets.set(bucket.key,{...bucket,exactCredit:exactAdd(old?.exactCredit??zero(),bucket.exactCredit)});}return priced.pending;
}
/** All mutations are synchronous native transactions. Source rows are never edited. */
export class PeriodSettlement {
 private readonly storage:DirectoryStorage;
 private readonly ledger:UsageCostLedger;
 constructor(storage:DirectoryStorage){
  if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);
  this.storage=joinedTransactions(storage);this.ledger=new UsageCostLedger(this.storage);
  this.storage.transactionSync(()=>{
   this.storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_settlement_identity (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL)');
   this.storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_period_closes (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, digest TEXT NOT NULL)');
   this.storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_period_close_requests (id TEXT PRIMARY KEY, digest TEXT NOT NULL, period_id TEXT NOT NULL)');
   this.storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_period_adjustments (id TEXT PRIMARY KEY, ordinal INTEGER NOT NULL UNIQUE, origin_period_id TEXT NOT NULL, target_period_id TEXT NOT NULL, payload_json TEXT NOT NULL)');
  });
 }
 private access(actor:ControlActor,tenant:TenantRecord,write=false){assertControlAccess(actor,write?'administer':'read',tenant.id,tenant.region);if(actor.scopeRegion!=='all')throw new ControlError('COMPLETE_TENANT_SCOPE_REQUIRED',403);const owner=this.storage.sql.exec<{tenant_id:string}>('SELECT tenant_id FROM tenant_settlement_identity WHERE id = ?','tenant').toArray()[0];if(owner&&owner.tenant_id!==tenant.id)throw new ControlError('SETTLEMENT_TENANT_CONFLICT',503);}
 private anchor(tenantId:string){const owner=this.storage.sql.exec<{tenant_id:string}>('SELECT tenant_id FROM tenant_settlement_identity WHERE id = ?','tenant').toArray()[0];if(owner&&owner.tenant_id!==tenantId)throw new ControlError('SETTLEMENT_TENANT_CONFLICT',503);if(!owner)this.storage.sql.exec('INSERT INTO tenant_settlement_identity (id, tenant_id) VALUES (?, ?)', 'tenant',tenantId);}
 private guardText(table:string,column:string){const row=this.storage.sql.exec<{size:number}>(`SELECT COALESCE(SUM(length(${column})),0) AS size FROM ${table}`).toArray()[0];if(row.size>MAX_DOCUMENT)throw new ControlError('SETTLEMENT_HISTORY_LIMIT',503);}
 private source(tenantId:string){
  this.guardText('tenant_usage_events','event_json');this.guardText('tenant_usage_streams','payload_json');this.guardText('tenant_usage_cost_entries','payload_json');
  const rows=bounded(this.storage.sql.exec<SourceRow>('SELECT e.id, e.event_json, e.digest, o.ordinal FROM tenant_usage_events e LEFT JOIN tenant_usage_event_order o ON e.id = o.id ORDER BY e.rowid LIMIT ?',LIMIT+1).toArray());
  const events=rows.map(row=>sourceEvent(row,tenantId));
  const order=this.storage.sql.exec<{n:number;maximum:number}>('SELECT COUNT(*) AS n, COALESCE(MAX(ordinal),0) AS maximum FROM tenant_usage_event_order').toArray()[0];if(order.n>LIMIT)throw new ControlError('SETTLEMENT_SCAN_LIMIT',503);if(order.n!==events.length||order.maximum!==events.length)throw new ControlError('SETTLEMENT_SOURCE_CORRUPT',503);
  const streamRows=bounded(this.storage.sql.exec<{id:string;payload_json:string}>('SELECT id, payload_json FROM tenant_usage_streams ORDER BY id LIMIT ?',LIMIT+1).toArray());
  const streams=new Map<string,Stream>();for(const row of streamRows){const stream=JSON.parse(row.payload_json) as Stream;if(stream.schemaVersion!==1||stream.identity.tenantId!==tenantId||usageStreamId(stream.identity)!==row.id)throw new ControlError('SETTLEMENT_SOURCE_CORRUPT',503);streams.set(row.id,stream);}
  for(const table of ['tenant_usage_metadata','tenant_rate_identity'])for(const row of this.storage.sql.exec<{tenant_id:string}>(`SELECT tenant_id FROM ${table}`).toArray())if(row.tenant_id!==tenantId)throw new ControlError('SETTLEMENT_TENANT_CONFLICT',503);
  for(const source of events)if(!streams.has(usageStreamId(source.event)))throw new ControlError('SETTLEMENT_SOURCE_CORRUPT',503);
  return {events,streams,watermark:events.reduce((max,row)=>Math.max(max,row.ordinal),0)};
 }
 private entry(eventId:string):UsageCostEntry|null {const old=this.storage.sql.exec('SELECT id FROM tenant_usage_cost_entries WHERE id = ?',eventId).toArray()[0];return old?this.ledger.project(eventId):null;}
 private statement(periodId:string):PeriodStatement|null {
  const row=this.storage.sql.exec<{payload_json:string;digest:string}>('SELECT payload_json, digest FROM tenant_period_closes WHERE id = ?',periodId).toArray()[0];if(!row)return null;
  const value=JSON.parse(row.payload_json) as PeriodStatement,{digest,...content}=value;if(value.schemaVersion!==1||value.calendar!=='UTC_MONTH'||value.periodId!==periodId||row.digest!==digest||rateHash(content)!==digest||!value.basis||rateHash(value.basis)!==value.revision||value.basis.tenantId!==value.tenantId||value.basis.periodId!==periodId)throw new ControlError('PERIOD_STATEMENT_CORRUPT',503);return value;
 }
 readClosed(actor:ControlActor,tenant:TenantRecord,periodId:string){this.access(actor,tenant);settlementMonth(periodId);const record=this.statement(periodId);if(record&&record.tenantId!==tenant.id)throw new ControlError('SETTLEMENT_TENANT_CONFLICT',503);return record;}
 private allAdjustments(tenantId:string):PeriodAdjustment[]{
  this.guardText('tenant_period_adjustments','payload_json');this.guardText('tenant_period_closes','payload_json');const origins=new Map<string,PeriodStatement|null>();
  return bounded(this.storage.sql.exec<{payload_json:string;id:string;ordinal:number}>('SELECT id, ordinal, payload_json FROM tenant_period_adjustments ORDER BY ordinal LIMIT ?',LIMIT+1).toArray()).map(row=>{const value=JSON.parse(row.payload_json) as PeriodAdjustment,{digest,...content}=value;if(value.schemaVersion!==1||value.id!==row.id||value.ordinal!==row.ordinal||value.tenantId!==tenantId||rateHash(content)!==digest)throw new ControlError('PERIOD_ADJUSTMENT_CORRUPT',503);if(!origins.has(value.originPeriodId))origins.set(value.originPeriodId,this.statement(value.originPeriodId));const origin=origins.get(value.originPeriodId);if(!origin||origin.digest!==value.originCloseDigest)throw new ControlError('PERIOD_ADJUSTMENT_CORRUPT',503);if(value.kind==='MANUAL'){const m=value.manual;if(!m||value.eventId!==value.id||value.entry!==null||value.pending.length||!validAdjustmentCredit(m.creditUnits)||!validText(m.reason,1000)||!validText(m.actor,256)||!validText(m.requestId,256)||m.requestDigest!==rateHash([tenantId,value.originPeriodId,value.originCloseDigest,m.creditUnits,m.reason]))throw new ControlError('PERIOD_ADJUSTMENT_CORRUPT',503);}else if(value.manual||!['LATE_USAGE','SOURCE_CONFLICT'].includes(value.kind))throw new ControlError('PERIOD_ADJUSTMENT_CORRUPT',503);if(value.entry){const actual=this.entry(value.eventId);if(!actual||actual.digest!==value.entry.digest)throw new ControlError('PERIOD_ADJUSTMENT_CORRUPT',503);}return value;});
 }
 private nextOpen(origin:string,now:number){let id=monthAt(Math.max(settlementMonth(origin).endAt,now));for(let attempt=0;attempt<=1200;attempt++){const closed=this.statement(id);if(!closed)return id;if(closed.closedAt>now)throw new ControlError('SETTLEMENT_CLOCK_REGRESSION',409);id=monthAt(settlementMonth(id).endAt);}throw new ControlError('PERIOD_CAPACITY_REACHED',503);}
 private appendAdjustment(tenantId:string,origin:PeriodStatement,eventId:string,kind:PeriodAdjustment['kind'],entry:UsageCostEntry|null,pending:string[],id:string,now:number,manual?:ManualPeriodAdjustment):boolean {
  if(this.storage.sql.exec('SELECT id FROM tenant_period_adjustments WHERE id = ?',id).toArray().length)return false;
  if(origin.tenantId!==tenantId)throw new ControlError('SETTLEMENT_TENANT_CONFLICT',503);
  if(now<origin.closedAt)throw new ControlError('SETTLEMENT_CLOCK_REGRESSION',409);
  const {ordinal,size}=this.storage.sql.exec<{ordinal:number;size:number}>(
   'SELECT COALESCE(MAX(ordinal),0)+1 AS ordinal, COALESCE(SUM(length(payload_json)),0) AS size FROM tenant_period_adjustments').toArray()[0];
  if(ordinal>LIMIT)throw new ControlError('PERIOD_ADJUSTMENT_LIMIT',503);
  const content={schemaVersion:1 as const,id,ordinal,tenantId,originPeriodId:origin.periodId,originCloseDigest:origin.digest,
   targetPeriodId:this.nextOpen(origin.periodId,now),eventId,kind,entry,pending,observedAt:now,...(manual?{manual}:{})};
  const payload=JSON.stringify({...content,digest:rateHash(content)});
  if(Buffer.byteLength(payload)>MAX_DOCUMENT)throw new ControlError('PERIOD_DOCUMENT_LIMIT',503);
  if(size+payload.length>MAX_DOCUMENT)throw new ControlError('SETTLEMENT_HISTORY_LIMIT',503);
  this.storage.sql.exec('INSERT INTO tenant_period_adjustments (id, ordinal, origin_period_id, target_period_id, payload_json) VALUES (?, ?, ?, ?, ?)',
   id,ordinal,content.originPeriodId,content.targetPeriodId,payload);
  return true;
 }
 addManualAdjustment(actor:ControlActor,tenant:TenantRecord,periodId:string,input:ManualAdjustmentWrite,now=Date.now()):PeriodAdjustment {
  this.access(actor,tenant,true);settlementMonth(periodId);monthAt(now);
  if(!input||!validAdjustmentCredit(input.creditUnits)||!validText(input.reason,1000)||!validText(input.idempotencyKey)||
     !validText(input.requestId,256)||typeof input.expectedCloseDigest!=='string'||!/^[a-f0-9]{64}$/.test(input.expectedCloseDigest)){
   throw new ControlError('INVALID_MANUAL_ADJUSTMENT',422);
  }
  const id='manual:'+rateHash([actor.sub,input.idempotencyKey]);
  const requestDigest=rateHash([tenant.id,periodId,input.expectedCloseDigest,input.creditUnits,input.reason]);
  return this.storage.transactionSync(()=>{
   const existing=this.allAdjustments(tenant.id).find(adjustment=>adjustment.id===id);
   if(existing){
    if(existing.manual?.requestDigest!==requestDigest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);
    return existing;
   }
   const origin=this.statement(periodId);
   if(!origin)throw new ControlError('PERIOD_NOT_CLOSED',409);
   if(origin.digest!==input.expectedCloseDigest)throw new ControlError('VERSION_CONFLICT',409);
   this.appendAdjustment(tenant.id,origin,id,'MANUAL',null,[],id,now,
    {creditUnits:input.creditUnits,reason:input.reason,actor:actor.sub,requestId:input.requestId,requestDigest});
   return this.allAdjustments(tenant.id).find(adjustment=>adjustment.id===id)!;
  });
 }
 /** Called only by the authenticated inbox adapter inside its write transaction. */
 recordUsageAdjustment(tenantId:string,event:UsageEvent,receipt:UsageReceipt,entry:UsageCostEntry|null,now:number):void {
  if(receipt.outcome==='QUARANTINED')return;
  if(event.tenantId!==tenantId||receipt.eventId!==event.eventId)throw new ControlError('SETTLEMENT_TENANT_CONFLICT',503);
  if(receipt.outcome==='RECORDED'){
   if(!billable(event))return;
   const origin=this.statement(eventMonth(event));if(!origin)return;
   const row=this.storage.sql.exec<{ordinal:number}>('SELECT ordinal FROM tenant_usage_event_order WHERE id = ?',event.eventId).toArray()[0];
   if(!row||!Number.isSafeInteger(row.ordinal)||row.ordinal<1||!entry||entry.sourceDigest!==receipt.digest)throw new ControlError('SETTLEMENT_SOURCE_CORRUPT',503);
   if(row.ordinal>origin.watermarks.usage)this.appendAdjustment(tenantId,origin,event.eventId,'LATE_USAGE',entry,entryBuckets(entry).pending,'late:'+event.eventId,now);
   return;
  }
  this.guardText('tenant_period_closes','payload_json');
  const closes=this.storage.sql.exec<{id:string}>('SELECT id FROM tenant_period_closes LIMIT 1201').toArray();
  if(closes.length>1200)throw new ControlError('PERIOD_CAPACITY_REACHED',503);
  const sourceId=usageStreamId(event),conflictId=usageConflictId(event.eventId,receipt.digest);
  for(const closed of closes){
   const origin=this.statement(closed.id)!;
   if(origin.streamIds.includes(sourceId)){
    this.appendAdjustment(tenantId,origin,event.eventId,'SOURCE_CONFLICT',null,['SOURCE_CONFLICT_AFTER_CLOSE'],'conflict:'+rateHash([origin.digest,conflictId]),now);
   }
  }
 }
 /** Rebuild adjustments for already committed historical events. */
 reconcileLate(actor:ControlActor,tenant:TenantRecord,now=Date.now()){
  this.access(actor,tenant,true);monthAt(now);return this.storage.transactionSync(()=>{
   this.anchor(tenant.id);const source=this.source(tenant.id);this.allAdjustments(tenant.id);let added=0;
   const save=(id:string,origin:PeriodStatement,eventId:string,kind:PeriodAdjustment['kind'],entry:UsageCostEntry|null,pending:string[])=>{
    if(this.appendAdjustment(tenant.id,origin,eventId,kind,entry,pending,id,now))added++;
   };
   for(const {event,ordinal:sourceOrdinal} of source.events){if(!billable(event))continue;const origin=this.statement(eventMonth(event));if(!origin||sourceOrdinal<=origin.watermarks.usage)continue;const entry=this.entry(event.eventId);if(!entry)throw new ControlError('UNPROJECTED_USAGE',409);save('late:'+event.eventId,origin,event.eventId,'LATE_USAGE',entry,entryBuckets(entry).pending);}
   this.guardText('tenant_usage_conflicts','payload_json');const conflicts=bounded(this.storage.sql.exec<{id:string;payload_json:string}>('SELECT id, payload_json FROM tenant_usage_conflicts ORDER BY id LIMIT ?',LIMIT+1).toArray());
   const closes=bounded(this.storage.sql.exec<{id:string}>('SELECT id FROM tenant_period_closes LIMIT ?',1201).toArray());if(closes.length>1200)throw new ControlError('PERIOD_CAPACITY_REACHED',503);
   for(const row of conflicts){const conflict=JSON.parse(row.payload_json);for(const closed of closes){const origin=this.statement(closed.id)!;if(origin.streamIds.includes(conflict.streamId))save('conflict:'+rateHash([origin.digest,row.id]),origin,conflict.eventId,'SOURCE_CONFLICT',null,['SOURCE_CONFLICT_AFTER_CLOSE']);}}
   return added;
  });
 }
 private streamIssues(stream:Stream,events:UsageEvent[]):string[]{
  const issues=new Set<string>();if(stream.quarantined)issues.add('USAGE_QUARANTINED');if(!stream.closed)issues.add('OPEN_USAGE_STREAM');if(stream.resources.cpuNanoCores===null||stream.resources.memoryMiB===null)issues.add('RESOURCE_QUANTITY_COVERAGE_MISSING');
  if(!events.length){issues.add('STREAM_WITHOUT_EVENTS');return [...issues];}events.sort((a,b)=>a.sequence-b.sequence);
  let cursor=events[0].at,lastAt=events[0].at,closed=false;const calls:Stream['calls']={};
  for(const [i,event] of events.entries()){
   if(event.sequence!==i+1||event.at<lastAt||JSON.stringify(canonicalUsageIdentity(event))!==JSON.stringify(canonicalUsageIdentity(stream.identity))||JSON.stringify(event.resources)!==JSON.stringify(canonicalUsageResources(stream.resources)))issues.add('SOURCE_SEQUENCE_INCOMPLETE');
   if(event.kind==='RESOURCE_OPEN'){if(i!==0)issues.add('SOURCE_WINDOW_INCOMPLETE');cursor=event.at;}
   else if(i===0)issues.add('SOURCE_WINDOW_INCOMPLETE');
   if(event.kind==='RESOURCE_SEGMENT'){if(closed||event.allocation!.startAt!==cursor)issues.add('SOURCE_WINDOW_INCOMPLETE');cursor=event.allocation!.endAt;}
   if(event.kind==='RESOURCE_CLOSE'){if(closed||event.at!==cursor)issues.add('SOURCE_WINDOW_INCOMPLETE');closed=true;}
   if(event.kind==='PROVIDER_REQUEST'){if(closed||calls[String(event.provider!.call)])issues.add('PROVIDER_USAGE_INCOMPLETE');calls[String(event.provider!.call)]={request:event.provider!,response:null};}
   if(event.kind==='PROVIDER_USAGE'){const call=calls[String(event.provider!.call)];if(!call||call.response||call.request.provider!==event.provider!.provider||call.request.requestedModel!==event.provider!.requestedModel)issues.add('PROVIDER_USAGE_INCOMPLETE');else call.response=event.provider!;}
   lastAt=event.at;
  }
  if(stream.sequence!==events.length||stream.cursor!==cursor||stream.lastAt!==lastAt||stream.closed!==closed||JSON.stringify(stream.calls)!==JSON.stringify(calls))issues.add('SOURCE_SEQUENCE_INCOMPLETE');
  if(Object.values(calls).some(call=>!call.response||call.response.completeness!=='COMPLETE'))issues.add('PROVIDER_USAGE_INCOMPLETE');return [...issues];
 }
 /** Read a single reservation's exact contribution to the tenant's daily rounding buckets. */
 previewReservationCost(actor:ControlActor,tenant:TenantRecord,admissions:AdmissionReservationStore,reservationId:string,now=Date.now()):ReservationCostPreview {
  return this.storage.transactionSync(()=>{
   const {reservation,terminalDigest}=admissions.readSettlementEvidence(actor,tenant,reservationId);
   const result={reservationId,version:reservation.version,phase:reservation.phase,terminalDigest};
   const pending=(...reasons:string[])=>({...result,complete:false,buckets:[],revision:null,reasons:[...new Set(reasons)].sort()});
   if(!terminalDigest||!reservation.target||!reservation.executionInputDigest)return pending('VERIFIED_TERMINATION_REQUIRED');
   const identity=canonicalUsageIdentity({tenantId:tenant.id,taskId:reservation.taskId,attemptId:reservation.attemptId,
    producerId:reservation.target,fencing:reservation.fencing,executionInputDigest:reservation.executionInputDigest});
   const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_usage_streams WHERE id = ?',usageStreamId(identity)).toArray()[0];
   if(!row)return pending('EXPECTED_ATTEMPT_MISSING');
   const stream=JSON.parse(row.payload_json) as Stream;
   if(stream.schemaVersion!==1||JSON.stringify(canonicalUsageIdentity(stream.identity))!==JSON.stringify(identity)){
    throw new ControlError('ATTEMPT_COVERAGE_CONFLICT',503);
   }
   if(!Number.isSafeInteger(stream.sequence)||stream.sequence<0||stream.sequence>MAX_USAGE_STREAM_EVENTS){
    throw new ControlError('SETTLEMENT_SOURCE_CORRUPT',503);
   }
   const events:UsageEvent[]=[];
   // Indexed event IDs keep the scan bounded to this execution attempt.
   for(let offset=0;offset<stream.sequence;offset+=100){
    const ids=Array.from({length:Math.min(100,stream.sequence-offset)},(_,i)=>usageEventId(identity,offset+i+1));
    const rows=this.storage.sql.exec<SourceRow>(
     'SELECT e.id, e.event_json, e.digest, o.ordinal FROM tenant_usage_events e LEFT JOIN tenant_usage_event_order o ON e.id = o.id WHERE e.id IN ('+ids.map(()=>'?').join(',')+')',...ids).toArray();
    for(const item of rows)events.push(sourceEvent(item,tenant.id).event);
   }
   const issues=this.streamIssues(stream,events);
   if(stream.lastAt>now)issues.push('SOURCE_TIME_IN_FUTURE');
   const {resources}=stream,{policy}=reservation;
   if((resources.cpuMilliCores??0)>reservation.resources.cpuMilliCores||
      (resources.memoryMiB??0)>reservation.resources.memoryMiB||resources.slots!==reservation.resources.concurrentAttempts){
    issues.push('RESERVATION_USAGE_RESOURCES_CONFLICT');
   }
   if(issues.length)return pending(...issues);
   const buckets=new Map<string,SettlementBucket>(),entryDigests:string[]=[];
   let priceDigest:string|undefined;
   for(const event of events){
    const entry=this.entry(event.eventId);
    if(!entry){issues.push('UNPROJECTED_USAGE');continue;}
    const context=entry.context;
    if(!context?.activation||context.costCenterVersion===null){issues.push('PRICE_SNAPSHOT_MISSING');continue;}
    const digest=rateHash(context);
    if(context.tenantId!==tenant.id||context.tenantVersion!==policy.tenantVersion||
       context.region!==policy.region||context.costCenter!==policy.costCenter||
       priceDigest!==undefined&&priceDigest!==digest)throw new ControlError('USAGE_PRICE_CONTEXT_CONFLICT',503);
    priceDigest=digest;
    entryDigests.push(entry.digest);
    if(billable(event))for(const reason of merge(buckets,entry))issues.push(reason);
   }
   if(issues.length)return pending(...issues);
   const exactBuckets=[...buckets.values()].sort((a,b)=>a.key.localeCompare(b.key))
    .map(({key,descriptor,exactCredit})=>({key,descriptor,exactCredit}));
   return {...result,complete:true,buckets:exactBuckets,revision:rateHash({result,stream,entryDigests,buckets:exactBuckets}),reasons:[]};
  });
 }
 private previewWithin(actor:ControlActor,tenant:TenantRecord,periodId:string,coverageInput:SettlementCoverage|null|undefined,now:number){
  this.access(actor,tenant);const period=settlementMonth(periodId);monthAt(now);const coverage=coverageValue(coverageInput,tenant.id,periodId,now),source=this.source(tenant.id),adjustments=this.allAdjustments(tenant.id),reasons=new Set<string>();
  const coverageBasis=basisCoverage(coverage,period.endAt);
  if(now<period.endAt)reasons.add('PERIOD_NOT_ENDED');if(!coverageBasis?.complete||!coverageBasis.observedThroughPeriodEnd)reasons.add('EXECUTION_COVERAGE_UNVERIFIED');
  const expected=new Map((coverage?.attempts??[]).map(identity=>[usageStreamId(identity),identity])),related=new Set<string>();
  const byStream=new Map<string,UsageEvent[]>();for(const {event} of source.events){const id=usageStreamId(event);const list=byStream.get(id)??[];list.push(event);byStream.set(id,list);}
  for(const [id,stream] of source.streams){const events=byStream.get(id)??[],first=events.find(e=>e.kind==='RESOURCE_OPEN'),start=first?.at??stream.receivedAt;const end=stream.closed?stream.cursor:Number.POSITIVE_INFINITY;if(start<period.endAt&&(end>period.startAt||start===period.startAt&&end===period.startAt))related.add(id);}
  const entries:UsageCostEntry[]=[],buckets=new Map<string,SettlementBucket>();
  for(const {event,ordinal} of source.events){
   if(billable(event)&&eventMonth(event)===periodId){related.add(usageStreamId(event));const entry=this.entry(event.eventId);if(!entry){reasons.add('UNPROJECTED_USAGE');continue;}entries.push(entry);for(const reason of merge(buckets,entry))reasons.add(reason);}
   if(billable(event)){const original=this.statement(eventMonth(event));if(original&&ordinal>original.watermarks.usage&&!adjustments.some(a=>a.id==='late:'+event.eventId))reasons.add('LATE_EVENT_NOT_ROUTED');}
  }
  const selected=adjustments.filter(a=>a.targetPeriodId===periodId),adjustmentBuckets=new Map<string,AdjustmentBucket>();
  for(const adjustment of selected){for(const reason of adjustment.pending)reasons.add(reason);if(adjustment.entry)related.add(usageStreamId(adjustment.entry.source));}
  for(const id of expected.keys())related.add(id);
  const streamEvidence:SettlementBasis['streamEvidence']=[];
  for(const id of related){const stream=source.streams.get(id);if(!stream){reasons.add('EXPECTED_ATTEMPT_MISSING');continue;}if(!expected.has(id))reasons.add('ATTEMPT_NOT_IN_COVERAGE');else if(JSON.stringify(canonicalUsageIdentity(stream.identity))!==JSON.stringify(expected.get(id)))reasons.add('ATTEMPT_COVERAGE_CONFLICT');
   const price=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_usage_price_bindings WHERE id = ?',id).toArray()[0];const priceContext=price?JSON.parse(price.payload_json).context:null;if(!priceContext?.activation)reasons.add('PRICE_SNAPSHOT_MISSING');if(priceContext?.costCenterVersion==null)reasons.add('COST_CENTER_VERSION_MISSING');
   for(const reason of this.streamIssues(stream,byStream.get(id)??[]))reasons.add(reason);if(stream.lastAt>now)reasons.add('SOURCE_TIME_IN_FUTURE');streamEvidence.push({id,digest:rateHash(stream),state:stream});
  }
  for(const adjustment of selected){if(!adjustment.entry||adjustment.pending.length)continue;const origin=this.statement(adjustment.originPeriodId)!;
   for(const line of entryBuckets(adjustment.entry).buckets){const key=rateHash([origin.digest,line.key]);let bucket=adjustmentBuckets.get(key);
    if(!bucket){let previousExact=origin.buckets.find(b=>b.key===line.key)?.exactCredit??zero();
     for(const earlier of adjustments.filter(a=>a.originPeriodId===origin.periodId&&a.targetPeriodId<periodId)){
      if(!this.statement(earlier.targetPeriodId)){reasons.add('PRIOR_ADJUSTMENT_PERIOD_OPEN');continue;}
      if(earlier.entry)for(const prior of entryBuckets(earlier.entry).buckets)if(prior.key===line.key)previousExact=exactAdd(previousExact,prior.exactCredit);
     }
     bucket={key:line.key,originPeriodId:origin.periodId,originCloseDigest:origin.digest,descriptor:line.descriptor,previousExact,addedExact:zero(),nextExact:previousExact,creditUnits:'0'};
    }
    bucket.addedExact=exactAdd(bucket.addedExact,line.exactCredit);bucket.nextExact=exactAdd(bucket.previousExact,bucket.addedExact);bucket.creditUnits=(BigInt(roundCredit(bucket.nextExact,bucket.descriptor.rounding))-BigInt(roundCredit(bucket.previousExact,bucket.descriptor.rounding))).toString();adjustmentBuckets.set(key,bucket);
   }
  }
  this.guardText('tenant_usage_conflicts','payload_json');
  const conflicts=bounded(this.storage.sql.exec<{id:string;payload_json:string}>('SELECT id, payload_json FROM tenant_usage_conflicts LIMIT ?',LIMIT+1).toArray());
  if(conflicts.length){const closes=this.storage.sql.exec<{id:string}>('SELECT id FROM tenant_period_closes LIMIT 1201').toArray();if(closes.length>1200)throw new ControlError('PERIOD_CAPACITY_REACHED',503);for(const closed of closes){const original=this.statement(closed.id)!;for(const row of conflicts){const conflict=JSON.parse(row.payload_json);if(original.streamIds.includes(conflict.streamId)&&!adjustments.some(a=>a.id==='conflict:'+rateHash([original.digest,row.id])))reasons.add('SOURCE_CONFLICT_NOT_ROUTED');}}}
  const admissionTable=this.storage.sql.exec("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tenant_admission_state'").toArray()[0];let reservations:unknown[]=[];
  if(admissionTable){const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_admission_state WHERE id = ?','admission').toArray()[0];if(row){const state=JSON.parse(row.payload_json);if(state.tenantId!==tenant.id||!Array.isArray(state.reservations))throw new ControlError('ADMISSION_IDENTITY_CONFLICT',503);reservations=state.reservations.filter((r:{createdAt:number;phase:string})=>r.createdAt<period.endAt&&!['SETTLED','CANCELLED'].includes(r.phase));if(reservations.length)reasons.add('UNSETTLED_ADMISSION_RESERVATIONS');}}
  for(const bucket of buckets.values())bucket.creditUnits=roundCredit(bucket.exactCredit,bucket.descriptor.rounding);
  const normal=[...buckets.values()].sort((a,b)=>a.key<b.key?-1:1),late=[...adjustmentBuckets.values()].sort((a,b)=>rateHash(a)<rateHash(b)?-1:1);
  const manual=selected.filter(adjustment=>adjustment.kind==='MANUAL');
  const manualAdjustmentCreditUnits=manual.length?total(manual.map(adjustment=>adjustment.manual!.creditUnits)):undefined;
  const knownSubtotalCreditUnits=total([...normal.map(b=>b.creditUnits),...late.map(b=>b.creditUnits),manualAdjustmentCreditUnits??'0']),watermarks={usage:source.watermark,adjustments:adjustments.at(-1)?.ordinal??0};
  const entryDigests=entries.map(entry=>({eventId:entry.eventId,digest:entry.digest})).sort((a,b)=>a.eventId<b.eventId?-1:1),adjustmentIds=selected.map(a=>a.id),sortedReasons=[...reasons].sort();
  const basis:SettlementBasis={tenantId:tenant.id,tenantVersion:tenant.version,periodId,watermarks,coverage:coverageBasis,entryDigests,streamEvidence:streamEvidence.sort((a,b)=>a.id<b.id?-1:1),reservations,adjustments:adjustments.map(a=>a.digest),normal,late,reasons:sortedReasons};const revision=rateHash(basis);
  return {tenantId:tenant.id,periodId,tenantVersion:tenant.version,calendar:'UTC_MONTH' as const,unit:'credit-subunit' as const,unitVersion:1,watermarks,coverage,entryDigests,streamIds:[...related].sort(),buckets:normal,adjustmentBuckets:late,adjustmentIds,manualAdjustmentCreditUnits,knownSubtotalCreditUnits,totalCreditUnits:reasons.size?null:knownSubtotalCreditUnits,reasons:sortedReasons,complete:reasons.size===0,revision,basis,final:false as const};
 }
 preview(actor:ControlActor,tenant:TenantRecord,periodId:string,coverage?:SettlementCoverage|null,now=Date.now()){return this.storage.transactionSync(()=>{this.access(actor,tenant);settlementMonth(periodId);if(this.statement(periodId))throw new ControlError('PERIOD_ALREADY_CLOSED',409);return this.previewWithin(actor,tenant,periodId,coverage,now);});}
 private closeRequest(actor:ControlActor,tenant:TenantRecord,periodId:string,input:ClosePeriodWrite){
  this.access(actor,tenant,true);settlementMonth(periodId);
  if(!input||!validText(input.reason,1000)||!validText(input.requestId,256)||!validText(input.idempotencyKey)||
     typeof input.expectedRevision!=='string'||!/^[a-f0-9]{64}$/.test(input.expectedRevision))throw new ControlError('INVALID_PERIOD_CLOSE',422);
  return {key:rateHash([actor.sub,input.idempotencyKey]),requestDigest:rateHash([tenant.id,periodId,input.expectedRevision,input.reason])};
 }
 private closeReceipt(tenantId:string,key:string,requestDigest:string):PeriodStatement|null {
  const receipt=this.storage.sql.exec<{digest:string;period_id:string}>('SELECT digest, period_id FROM tenant_period_close_requests WHERE id = ?',key).toArray()[0];
  if(!receipt)return null;
  if(receipt.digest!==requestDigest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);
  const statement=this.statement(receipt.period_id);
  if(!statement||statement.tenantId!==tenantId)throw new ControlError('PERIOD_STATEMENT_CORRUPT',503);
  return statement;
 }
 replayClose(actor:ControlActor,tenant:TenantRecord,periodId:string,input:ClosePeriodWrite):PeriodStatement|null {
  const {key,requestDigest}=this.closeRequest(actor,tenant,periodId,input);
  return this.closeReceipt(tenant.id,key,requestDigest);
 }
 close(actor:ControlActor,tenant:TenantRecord,periodId:string,input:ClosePeriodWrite,coverage?:SettlementCoverage|null,now=Date.now()):PeriodStatement {
  const {key,requestDigest}=this.closeRequest(actor,tenant,periodId,input);monthAt(now);
  return this.storage.transactionSync(()=>{
   this.anchor(tenant.id);const receipt=this.closeReceipt(tenant.id,key,requestDigest);
   if(receipt)return receipt;
   const old=this.statement(periodId);if(old&&old.tenantId!==tenant.id)throw new ControlError('SETTLEMENT_TENANT_CONFLICT',503);
   let result=old;
   if(old){if(old.revision!==input.expectedRevision)throw new ControlError('PERIOD_ALREADY_CLOSED',409);}
   else{
    if(this.storage.sql.exec('SELECT id FROM tenant_period_closes LIMIT 1200').toArray().length>=1200)throw new ControlError('PERIOD_CAPACITY_REACHED',503);
    const preview=this.previewWithin(actor,tenant,periodId,coverage,now);if(preview.revision!==input.expectedRevision)throw new ControlError('VERSION_CONFLICT',409);if(!preview.complete||!preview.coverage||preview.totalCreditUnits===null)throw new ControlError('PERIOD_INCOMPLETE',409);
    const content={schemaVersion:1 as const,calendar:'UTC_MONTH' as const,tenantId:tenant.id,periodId,version:1 as const,tenantVersion:tenant.version,closedAt:now,actor:actor.sub,reason:input.reason,requestId:input.requestId,revision:preview.revision,basis:preview.basis,watermarks:preview.watermarks,coverage:preview.coverage,entryDigests:preview.entryDigests,streamIds:preview.streamIds,buckets:preview.buckets,adjustmentBuckets:preview.adjustmentBuckets,adjustmentIds:preview.adjustmentIds,...(preview.manualAdjustmentCreditUnits!==undefined?{manualAdjustmentCreditUnits:preview.manualAdjustmentCreditUnits}:{}),totalCreditUnits:preview.totalCreditUnits,unit:'credit-subunit' as const,unitVersion:1 as const};result={...content,digest:rateHash(content)};
    const payload=JSON.stringify(result);if(Buffer.byteLength(payload)>MAX_DOCUMENT)throw new ControlError('PERIOD_DOCUMENT_LIMIT',503);this.storage.sql.exec('INSERT INTO tenant_period_closes (id, payload_json, digest) VALUES (?, ?, ?)',periodId,payload,result.digest);
   }
   this.storage.sql.exec('INSERT INTO tenant_period_close_requests (id, digest, period_id) VALUES (?, ?, ?)',key,requestDigest,periodId);return result!;
  });
 }
 adjustments(actor:ControlActor,tenant:TenantRecord,options:FinopsPage,secret:string){
  this.access(actor,tenant);const rows=this.allAdjustments(tenant.id),page=pageWindow(actor,'settlement-adjustments:'+tenant.id,tenant.version,rows.at(-1)?.ordinal??0,options,secret),visible=rows.filter(r=>r.ordinal>page.after&&r.ordinal<=page.upper).slice(0,page.limit+1),items=visible.slice(0,page.limit);
  return {items,watermark:page.upper,nextCursor:page.next(items.at(-1)?.ordinal??page.after,visible.length>page.limit),final:false};
 }
}
