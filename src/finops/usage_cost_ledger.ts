import {pageWindow,type FinopsPage} from './finops_page.js';
/** Rebuildable immutable projection of accepted source events; this is not a settlement ledger. */
import {assertControlAccess,ControlError,type ControlActor} from '../control/contracts.js';
import {canonicalUsageEvent,usageEventDigest,usageStreamId,type UsageEvent} from '../control/usage_event.js';
import type {DirectoryStorage,TenantRecord} from '../control/tenant_directory.js';
import {exactAdd,exactPrice,roundCredit,rateHash,verifyRateCard,MAX_CREDIT,type ExactAmount,type Rounding} from './rate_cards.js';
import {TenantRateStore,type UsagePriceContext} from './tenant_rate_store.js';
export interface CostLine {dimension:string;quantity:string|null;rate:ExactAmount|null;exactCredit:ExactAmount|null;pending:string[]}
export interface UsageCostEntry {
 schemaVersion:1;entryId:string;eventId:string;sourceDigest:string;source:UsageEvent;receivedAt:number|null;
 context:UsagePriceContext|null;periodId:string;lines:CostLine[];pending:string[];digest:string;
}
function day(at:number):string {if(at>8640000000000000)throw new ControlError('INVALID_COST_PERIOD',422);return new Date(at).toISOString().slice(0,10);}
function pricingLines(event:UsageEvent,context:UsagePriceContext|null):{lines:CostLine[];pending:string[]} {
 const lines:CostLine[]=[],pending:string[]=[],card=context?.activation?.rateCard;
 const billable=event.kind==='RESOURCE_SEGMENT'||event.kind==='PROVIDER_USAGE';
 if(!billable)return {lines,pending};
 if(!context)pending.push('PRICE_SNAPSHOT_MISSING');
 else{if(context.costCenterVersion===null)pending.push('COST_CENTER_VERSION_MISSING');if(!card)pending.push('RATE_NOT_ACTIVATED');}
 let scope=true;
 if(card){verifyRateCard(card);if(card.scope.region!==null&&card.scope.region!==context!.region||card.scope.poolId!==null&&card.scope.poolId!==context!.poolId){scope=false;pending.push('RATE_SCOPE_UNKNOWN_OR_MISMATCH');}}
 const line=(dimension:string,quantity:string|null,rate:ExactAmount|null)=>{
  const reasons:string[]=[];if(quantity===null)reasons.push('QUANTITY_MISSING');if(rate===null)reasons.push('RATE_MISSING');
  lines.push({dimension,quantity,rate,exactCredit:quantity!==null&&rate!==null&&scope?exactPrice(quantity,rate):null,pending:reasons});
 };
 if(event.allocation){
  for(const dimension of ['cpuNanoCoreMilliseconds','memoryMiBMilliseconds','slotMilliseconds'] as const)line(dimension,event.allocation[dimension],card?.resources[dimension]??null);
 }
 if(event.kind==='PROVIDER_USAGE'){
  const usage=event.provider!;
  // Exact provider + requested + actually reported model match; no wildcard fallback.
  const model=card?.models.find(m=>m.provider===usage.provider&&m.requestedModel===usage.requestedModel&&m.reportedModel===usage.reportedModel);
  if(usage.completeness!=='COMPLETE'||usage.source!=='PROVIDER_RESPONSE')pending.push('PROVIDER_USAGE_INCOMPLETE');
  if(!model)pending.push('MODEL_RATE_MISSING');
  if(model?.input.mode==='CACHE_SPLIT'){
   line('cacheHitTokens',usage.cacheHitTokens,model.input.hit);line('cacheMissTokens',usage.cacheMissTokens,model.input.miss);
  }else line('inputTokens',usage.inputTokens,model?.input.mode==='ALL'?model.input.rate:null);
  line('outputTokens',usage.outputTokens,model?.output??null);
 }
 return {lines,pending};
}
/** Only accepted UsageInbox rows may be projected. Historical absence of a snapshot stays pending. */
export class UsageCostLedger {
 private readonly rates:TenantRateStore;
 constructor(private readonly storage:DirectoryStorage){
  if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);
  this.rates=new TenantRateStore(storage);
  storage.transactionSync(()=>storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_usage_cost_entries (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, digest TEXT NOT NULL)'));
 }
 project(eventId:string):UsageCostEntry {
  return this.storage.transactionSync(()=>{
   const source=this.storage.sql.exec<{event_json:string;digest:string}>('SELECT event_json, digest FROM tenant_usage_events WHERE id = ?',eventId).toArray()[0];
   if(!source)throw new ControlError('ACCEPTED_USAGE_REQUIRED',409);
   const envelope=JSON.parse(source.event_json),event=canonicalUsageEvent(envelope.event??envelope),digest=usageEventDigest(event);
   if(event.eventId!==eventId||source.digest!==digest)throw new ControlError('USAGE_SOURCE_CORRUPT',503);
   const existing=this.storage.sql.exec<{payload_json:string;digest:string}>('SELECT payload_json, digest FROM tenant_usage_cost_entries WHERE id = ?',eventId).toArray()[0];
   if(existing){const result=this.verify(JSON.parse(existing.payload_json));if(result.sourceDigest!==digest||result.digest!==existing.digest)throw new ControlError('COST_ENTRY_CONFLICT',409);return result;}
   const context=this.rates.binding(event);
   // The separate price pin must agree with the authenticated usage binding, not current directory metadata.
   const streamRow=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_usage_streams WHERE id = ?',usageStreamId(event)).toArray()[0];
   const stream=streamRow?JSON.parse(streamRow.payload_json):null;
   if(context&&(!stream?.context||stream.context.tenantVersion!==context.tenantVersion||stream.context.region!==context.region||stream.context.costCenter!==context.costCenter||stream.context.rateCardVersion!==(context.activation?.rateCard.version??null)))throw new ControlError('USAGE_PRICE_CONTEXT_CONFLICT',409);
   const priced=pricingLines(event,context),content={schemaVersion:1 as const,entryId:'cost:'+eventId,eventId,sourceDigest:digest,source:event,receivedAt:envelope.event&&Number.isSafeInteger(envelope.receivedAt)?envelope.receivedAt:null,context,periodId:day(event.allocation?.startAt??event.at),...priced};
   const result:UsageCostEntry={...content,digest:rateHash(content)};
   this.storage.sql.exec('INSERT INTO tenant_usage_cost_entries (id, payload_json, digest) VALUES (?, ?, ?)',eventId,JSON.stringify(result),result.digest);return result;
  });
 }
 projectPending(limit=100){
  if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new ControlError('INVALID_PAGE_LIMIT',422);
  const rows=this.storage.sql.exec<{id:string}>('SELECT e.id FROM tenant_usage_events e LEFT JOIN tenant_usage_cost_entries c ON c.id = e.id WHERE c.id IS NULL ORDER BY e.rowid LIMIT ?',limit).toArray();
  for(const row of rows)this.project(row.id);return rows.length;
 }
 page(actor:ControlActor,tenant:TenantRecord,options:FinopsPage,secret:string){
  assertControlAccess(actor,'read',tenant.id,tenant.region);
  const maximum=Number(this.storage.sql.exec<{n:number}>('SELECT COALESCE(MAX(ordinal),0) AS n FROM tenant_usage_event_order').toArray()[0].n);
  const page=pageWindow(actor,'cost-entries:'+tenant.id,tenant.version,maximum,options,secret);
  const rows=this.storage.sql.exec<{id:string;ordinal:number;payload_json:string|null}>('SELECT o.id, o.ordinal, c.payload_json FROM tenant_usage_event_order o LEFT JOIN tenant_usage_cost_entries c ON c.id = o.id WHERE o.tenant_id = ? AND (? = ? OR o.region = ?) AND o.ordinal > ? AND o.ordinal <= ? ORDER BY o.ordinal LIMIT ?',tenant.id,actor.scopeRegion,'all',actor.scopeRegion,page.after,page.upper,page.limit+1).toArray();
  const visible=rows.slice(0,page.limit);return {items:visible.map(row=>({eventId:row.id,entry:row.payload_json?this.verify(JSON.parse(row.payload_json)):null,pending:row.payload_json?undefined:'UNPROJECTED_USAGE'})),watermark:page.upper,nextCursor:page.next(visible.at(-1)?.ordinal??page.after,rows.length>page.limit),final:false};
 }
 private verify(value:UsageCostEntry):UsageCostEntry {
  const {digest,...content}=value;if(rateHash(content)!==digest||usageEventDigest(value.source)!==value.sourceDigest||value.eventId!==value.source.eventId||value.entryId!=='cost:'+value.eventId)throw new ControlError('COST_ENTRY_CORRUPT',503);
  const recomputed=pricingLines(value.source,value.context);
  if(JSON.stringify(recomputed.lines)!==JSON.stringify(value.lines)||JSON.stringify(recomputed.pending)!==JSON.stringify(value.pending)||day(value.source.allocation?.startAt??value.source.at)!==value.periodId)throw new ControlError('COST_ENTRY_CORRUPT',503);
  return value;
 }
 /** Bound work; never turn a truncated or unreachable projection into a complete zero total. */
 summary(actor:ControlActor,tenant:TenantRecord){
  assertControlAccess(actor,'read',tenant.id,tenant.region);
  const rows=this.storage.sql.exec<{event_json:string;ordinal:number;payload_json:string|null}>('SELECT e.event_json, o.ordinal, c.payload_json FROM tenant_usage_events e JOIN tenant_usage_event_order o ON e.id = o.id LEFT JOIN tenant_usage_cost_entries c ON c.id = e.id WHERE o.tenant_id = ? AND (? = ? OR o.region = ?) ORDER BY o.ordinal LIMIT 10001',tenant.id,actor.scopeRegion,'all',actor.scopeRegion).toArray();
  const reasons=new Set<string>();if(rows.length>10000)reasons.add('PROJECTION_LIMIT');
  const buckets=new Map<string,{periodId:string;costCenter:string;costCenterVersion:number;rateCardVersion:number;activationVersion:number;dimension:string;rounding:Rounding;exactCredit:ExactAmount;creditUnits?:string}>();
  let pendingEntries=0,watermark=0;
  for(const row of rows.slice(0,10000)){
   watermark=row.ordinal;const envelope=JSON.parse(row.event_json),event=canonicalUsageEvent(envelope.event??envelope);
   if(!row.payload_json){pendingEntries++;reasons.add('UNPROJECTED_USAGE');continue;}
   const entry=this.verify(JSON.parse(row.payload_json));if(entry.sourceDigest!==usageEventDigest(event))throw new ControlError('COST_ENTRY_CONFLICT',503);
   if(entry.context&&actor.scopeRegion!=='all'&&entry.context.region!==actor.scopeRegion)throw new ControlError('COST_ENTRY_SCOPE_CONFLICT',503);
   if(entry.pending.length||entry.lines.some(l=>l.pending.length)){pendingEntries++;for(const reason of [...entry.pending,...entry.lines.flatMap(l=>l.pending)])reasons.add(reason);}
   // Unknown ownership/rate/scope is never assigned an invented bucket, including explicit zero quantities.
   if(entry.pending.length||!entry.context?.activation||entry.context.costCenterVersion===null)continue;
   for(const line of entry.lines){if(!line.exactCredit)continue;const context=entry.context,activation=context.activation!;
    const descriptor={periodId:entry.periodId,costCenter:context.costCenter,costCenterVersion:context.costCenterVersion!,rateCardVersion:activation.rateCard.version,activationVersion:activation.version,dimension:line.dimension,rounding:activation.rateCard.rounding};
    const key=JSON.stringify(descriptor),old=buckets.get(key);buckets.set(key,{...descriptor,exactCredit:exactAdd(old?.exactCredit??{numerator:'0',denominator:'1'},line.exactCredit)});
   }
  }
  // A committed binding can precede its first event. It must not disappear from completeness checks.
  const allStreams=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_usage_streams ORDER BY id LIMIT 10001').toArray();
  if(allStreams.length>10000)reasons.add('STREAM_PROJECTION_LIMIT');
  for(const row of allStreams.slice(0,10000)){
   const stream=JSON.parse(row.payload_json);
   if(stream.identity?.tenantId!==tenant.id)throw new ControlError('USAGE_SOURCE_CORRUPT',503);
   if(actor.scopeRegion!=='all'&&stream.context?.region!==actor.scopeRegion)continue;
   if(stream.resources?.cpuNanoCores===null||stream.resources?.memoryMiB===null)reasons.add('RESOURCE_QUANTITY_COVERAGE_MISSING');
   if(stream.quarantined)reasons.add('USAGE_QUARANTINED');if(!stream.closed)reasons.add('OPEN_USAGE_STREAM');
   if(Object.values(stream.calls??{}).some((c:unknown)=>{const call=c as {response?:{completeness?:string}};return !call.response||call.response.completeness!=='COMPLETE';}))reasons.add('PROVIDER_USAGE_INCOMPLETE');
  }
  const admissionTable=this.storage.sql.exec("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tenant_admission_state'").toArray()[0];
  if(admissionTable){const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_admission_state WHERE id = ?','admission').toArray()[0];if(row){const state=JSON.parse(row.payload_json);if(state.tenantId!==tenant.id||!Array.isArray(state.reservations))throw new ControlError('ADMISSION_IDENTITY_CONFLICT',503);if(state.reservations.some((r:{phase:string;policy?:{region?:string}})=>!['CANCELLED','SETTLED'].includes(r.phase)&&(actor.scopeRegion==='all'||r.policy?.region===actor.scopeRegion)))reasons.add('UNSETTLED_ADMISSION_RESERVATIONS');}}
  let subtotal=0n;
  for(const bucket of buckets.values()){bucket.creditUnits=roundCredit(bucket.exactCredit,bucket.rounding);subtotal+=BigInt(bucket.creditUnits);if(subtotal>MAX_CREDIT)throw new ControlError('CREDIT_OVERFLOW',422);}
  const complete=reasons.size===0;
  return {tenantId:tenant.id,scopeRegion:actor.scopeRegion,unit:'credit-subunit' as const,unitVersion:1,roundingScope:'UTC_DAY_COST_CENTER_VERSION_RATE_ACTIVATION_DIMENSION' as const,buckets:[...buckets.values()],knownSubtotalCreditUnits:subtotal.toString(),totalCreditUnits:complete?subtotal.toString():null,pendingEntries,reasons:[...reasons].sort(),complete,watermark,final:false as const};
 }
}
export function aggregateTenantCosts(expectedTenantIds:readonly string[],results:ReadonlyMap<string,{knownSubtotalCreditUnits:string;totalCreditUnits:string|null;complete:boolean}>){
 if(new Set(expectedTenantIds).size!==expectedTenantIds.length||expectedTenantIds.length>10000)throw new ControlError('INVALID_AGGREGATE_SCOPE',422);
 let known=0n;const missing:string[]=[],pending:string[]=[];
 for(const id of expectedTenantIds){const result=results.get(id);if(!result){missing.push(id);continue;}if(!result.complete||result.totalCreditUnits===null)pending.push(id);
  // Parse through exact arithmetic to reject malformed, negative or overflowing transported amounts.
  known+=BigInt(roundCredit({numerator:result.knownSubtotalCreditUnits,denominator:'1'},'FLOOR'));if(known>MAX_CREDIT)throw new ControlError('CREDIT_OVERFLOW',422);
 }
 const complete=missing.length===0&&pending.length===0;
 return {knownSubtotalCreditUnits:known.toString(),totalCreditUnits:complete?known.toString():null,complete,missingTenantIds:missing,pendingTenantIds:pending,final:false as const};
}
