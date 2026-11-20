/** Durable quantity inbox. Caller must authenticate the producer and pin the execution identity first. */
import {createHash,createHmac,timingSafeEqual} from 'node:crypto';
import {canonicalUsageIdentity,canonicalUsageResources,canonicalUsageEvent,usageStreamId,usageEventDigest,type UsageIdentity,type UsageEvent,type ProviderUsage} from '../control/usage_event.js';
import {ControlError,assertControlAccess,type ControlActor} from '../control/contracts.js';
import type {TenantRecord} from '../control/tenant_directory.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';
export interface UsageContext {tenantVersion:number;region:string;costCenter:string;rateCardVersion:number|null;}
export const MAX_USAGE_STREAM_EVENTS=5000;
interface Stream {schemaVersion:1;context?:UsageContext|null;identity:UsageIdentity;resources:UsageEvent['resources'];sequence:number;cursor:number;lastAt:number;receivedAt:number;closed:boolean;quarantined:boolean;calls:Record<string,{request:ProviderUsage;response:ProviderUsage|null}>;}
export interface UsageReceipt {eventId:string;digest:string;outcome:'RECORDED'|'CONFLICT'|'QUARANTINED';}
export function usageConflictId(eventId:string,incomingDigest:string):string {
 return createHash('sha256').update(eventId+':'+incomingDigest).digest('hex');
}
export class UsageInbox {
 constructor(private readonly storage:DirectoryStorage){
  if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);
  storage.transactionSync(()=>{
   storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_usage_metadata (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_usage_streams (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_usage_events (id TEXT PRIMARY KEY, event_json TEXT NOT NULL, digest TEXT NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_usage_conflicts (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_usage_event_order (id TEXT PRIMARY KEY, ordinal INTEGER NOT NULL UNIQUE, tenant_id TEXT NOT NULL, region TEXT)');
   // Add stable paging to an existing inbox without rewriting immutable event bytes.
   const missing=storage.sql.exec<{id:string;event_json:string}>('SELECT e.id, e.event_json FROM tenant_usage_events e LEFT JOIN tenant_usage_event_order o ON o.id = e.id WHERE o.id IS NULL ORDER BY e.rowid').toArray();
   let ordinal=Number(storage.sql.exec<{n:number}>('SELECT COALESCE(MAX(ordinal),0) AS n FROM tenant_usage_event_order').toArray()[0].n);
   for(const row of missing){const stored=JSON.parse(row.event_json),event=stored.event??stored;storage.sql.exec('INSERT INTO tenant_usage_event_order (id, ordinal, tenant_id, region) VALUES (?, ?, ?, ?)',row.id,++ordinal,event.tenantId,null);}

  });
 }
 private tenant(tenantId:string,create=false){
  const old=this.storage.sql.exec<{tenant_id:string}>('SELECT tenant_id FROM tenant_usage_metadata WHERE id = ?', 'tenant').toArray()[0];
  if(old&&old.tenant_id!==tenantId)throw new ControlError('USAGE_TENANT_IDENTITY_CONFLICT',503);
  if(!old){const owners=this.storage.sql.exec<{tenant_id:string}>('SELECT DISTINCT tenant_id FROM tenant_usage_event_order LIMIT 2').toArray();if(owners.some(row=>row.tenant_id!==tenantId))throw new ControlError('USAGE_TENANT_IDENTITY_CONFLICT',503);if(create)this.storage.sql.exec('INSERT INTO tenant_usage_metadata (id, tenant_id) VALUES (?, ?)', 'tenant',tenantId);}
 }
 private stream(id:string):Stream|null {const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_usage_streams WHERE id = ?',id).toArray()[0];if(!row)return null;const s=JSON.parse(row.payload_json) as Stream;if(s.schemaVersion!==1||usageStreamId(s.identity)!==id)throw new ControlError('USAGE_INBOX_CORRUPT',503);return s;}
 private save(id:string,stream:Stream){const json=JSON.stringify(stream);if(Buffer.byteLength(json)>1024*1024||stream.sequence>MAX_USAGE_STREAM_EVENTS)throw new ControlError('USAGE_INBOX_FULL',503);this.storage.sql.exec('INSERT OR REPLACE INTO tenant_usage_streams (id, payload_json) VALUES (?, ?)',id,json);}
 bind(identity:UsageIdentity,resources:UsageEvent['resources'],context:UsageContext,now=Date.now()):void {
  identity=canonicalUsageIdentity(identity);resources=canonicalUsageResources(resources);const id=usageStreamId(identity);
  if(!Number.isSafeInteger(context.tenantVersion)||context.tenantVersion<1||!context.region||!context.costCenter||context.rateCardVersion!==null&&(!Number.isSafeInteger(context.rateCardVersion)||context.rateCardVersion<1))throw new ControlError('INVALID_USAGE_CONTEXT',422);
  this.storage.transactionSync(()=>{this.tenant(identity.tenantId,true);const old=this.stream(id);if(old){if(JSON.stringify(old.identity)!==JSON.stringify(identity)||JSON.stringify(old.resources)!==JSON.stringify(resources))throw new ControlError('USAGE_BINDING_CONFLICT',409);return;}
   this.save(id,{schemaVersion:1,identity,resources,context,sequence:0,cursor:0,lastAt:0,receivedAt:now,closed:false,quarantined:false,calls:{}});
  });
 }
 binding(identity:UsageIdentity){this.tenant(identity.tenantId);const stream=this.stream(usageStreamId(identity));if(!stream)return null;if(JSON.stringify(stream.identity)!==JSON.stringify(canonicalUsageIdentity(identity)))throw new ControlError('USAGE_BINDING_FORBIDDEN',403);return {identity:stream.identity,resources:stream.resources,context:stream.context??null};}
 receive(input:UsageEvent,trustedIdentity:UsageIdentity,now=Date.now()):UsageReceipt {
  if(!Number.isSafeInteger(now)||now<0)throw new ControlError('INVALID_USAGE_CLOCK',422);
  const event=canonicalUsageEvent(input),identity=canonicalUsageIdentity(trustedIdentity),streamId=usageStreamId(event),digest=usageEventDigest(event);
  if(JSON.stringify(canonicalUsageIdentity(event))!==JSON.stringify(identity))throw new ControlError('USAGE_BINDING_FORBIDDEN',403);
  return this.storage.transactionSync(()=>{
   this.tenant(identity.tenantId,true);let stream=this.stream(streamId);const previous=this.storage.sql.exec<{digest:string}>('SELECT digest FROM tenant_usage_events WHERE id = ?',event.eventId).toArray()[0];
   if(previous){
    if(previous.digest===digest)return {eventId:event.eventId,digest,outcome:'RECORDED'};
    if(!stream)throw new ControlError('USAGE_INBOX_CORRUPT',503);stream.quarantined=true;this.save(streamId,stream);
    const id=usageConflictId(event.eventId,digest);
    this.storage.sql.exec('INSERT OR REPLACE INTO tenant_usage_conflicts (id, payload_json) VALUES (?, ?)',id,JSON.stringify({streamId,eventId:event.eventId,originalDigest:previous.digest,incomingDigest:digest,code:'USAGE_EVENT_CONFLICT',requiresAttention:true,receivedAt:now}));
    return {eventId:event.eventId,digest,outcome:'CONFLICT'};
   }
   if(stream?.quarantined)return {eventId:event.eventId,digest,outcome:'QUARANTINED'};
   if(event.sequence!==(stream?.sequence??0)+1)throw new ControlError('USAGE_SEQUENCE_GAP',409);
   if(!stream){if(event.kind!=='RESOURCE_OPEN')throw new ControlError('USAGE_OPEN_REQUIRED',409);stream={schemaVersion:1,identity,resources:event.resources,sequence:0,cursor:event.at,lastAt:event.at,receivedAt:now,closed:false,quarantined:false,calls:{}};}
   else if(JSON.stringify(stream.identity)!==JSON.stringify(identity)||JSON.stringify(stream.resources)!==JSON.stringify(event.resources))throw new ControlError('USAGE_STREAM_BINDING_CONFLICT',409);
   if(event.at<stream.lastAt)throw new ControlError('USAGE_TIME_REGRESSION',409);
   if(event.kind==='RESOURCE_OPEN'){if(stream.sequence!==0)throw new ControlError('USAGE_ALREADY_OPEN',409);stream.cursor=event.at;}
   if(event.kind==='RESOURCE_SEGMENT'){if(stream.closed||event.allocation!.startAt!==stream.cursor)throw new ControlError('USAGE_WINDOW_CONFLICT',409);stream.cursor=event.allocation!.endAt;}
   if(event.kind==='RESOURCE_CLOSE'){if(stream.closed||event.at!==stream.cursor)throw new ControlError('USAGE_WINDOW_CONFLICT',409);stream.closed=true;}
   if(event.kind==='PROVIDER_REQUEST'){if(stream.closed||stream.calls[String(event.provider!.call)])throw new ControlError('USAGE_CALL_CONFLICT',409);stream.calls[String(event.provider!.call)]={request:event.provider!,response:null};}
   if(event.kind==='PROVIDER_USAGE'){
    const call=stream.calls[String(event.provider!.call)];if(!call||call.response||call.request.provider!==event.provider!.provider||call.request.requestedModel!==event.provider!.requestedModel)throw new ControlError('USAGE_CALL_CONFLICT',409);call.response=event.provider!;
   }
   stream.sequence=event.sequence;stream.lastAt=event.at;stream.receivedAt=now;
   this.storage.sql.exec('INSERT INTO tenant_usage_events (id, event_json, digest) VALUES (?, ?, ?)',event.eventId,JSON.stringify({event,receivedAt:now}),digest);
   const ordinal=Number(this.storage.sql.exec<{n:number}>('SELECT COALESCE(MAX(ordinal),0) AS n FROM tenant_usage_event_order').toArray()[0].n)+1;
   this.storage.sql.exec('INSERT INTO tenant_usage_event_order (id, ordinal, tenant_id, region) VALUES (?, ?, ?, ?)',event.eventId,ordinal,event.tenantId,stream.context?.region??null);this.save(streamId,stream);
   return {eventId:event.eventId,digest,outcome:'RECORDED'};
  });
 }
 inspect(identity:UsageIdentity){const stream=this.stream(usageStreamId(identity));if(!stream)return null;if(JSON.stringify(stream.identity)!==JSON.stringify(canonicalUsageIdentity(identity)))throw new ControlError('USAGE_BINDING_FORBIDDEN',403);return {...stream,pending:stream.resources.cpuNanoCores===null||stream.resources.memoryMiB===null||!stream.closed||stream.quarantined||Object.values(stream.calls).some(c=>!c.response||c.response.completeness!=='COMPLETE')};}
 page(actor:ControlActor,tenant:TenantRecord,options:{limit?:number;cursor?:string},secret:string){
  assertControlAccess(actor,'read',tenant.id,tenant.region);this.tenant(tenant.id);if(!/^[a-f0-9]{64}$/.test(secret))throw new ControlError('USAGE_CURSOR_UNAVAILABLE',503);
  const limit=options.limit??25;if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new ControlError('INVALID_PAGE_LIMIT',422);
  const scope=createHash('sha256').update(JSON.stringify([actor.sub,actor.role,actor.scopeTenant,actor.scopeRegion,tenant.id,tenant.version])).digest('hex');
  let after=0,upper=Number(this.storage.sql.exec<{n:number}>('SELECT COALESCE(MAX(ordinal),0) AS n FROM tenant_usage_event_order').toArray()[0].n);
  if(options.cursor){try{if(options.cursor.length>2048||!(/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$/).test(options.cursor))throw new Error();const [raw,signature]=options.cursor.split('.'),actual=Buffer.from(signature,'base64url'),expected=createHmac('sha256',secret).update('usage-page-v1.'+raw).digest();if(actual.toString('base64url')!==signature||actual.length!==expected.length||!timingSafeEqual(actual,expected))throw new Error();const cursor=JSON.parse(Buffer.from(raw,'base64url').toString());if(cursor.scope!==scope)throw new ControlError('CURSOR_STALE',409);if(!Number.isSafeInteger(cursor.after)||!Number.isSafeInteger(cursor.upper)||cursor.after<0||cursor.after>cursor.upper||cursor.upper>upper)throw new Error();after=cursor.after;upper=cursor.upper;}catch(error){if(error instanceof ControlError)throw error;throw new ControlError('INVALID_CURSOR',422);}}
  const rows=this.storage.sql.exec<{event_json:string;ordinal:number}>('SELECT e.event_json, o.ordinal FROM tenant_usage_events e JOIN tenant_usage_event_order o ON e.id = o.id WHERE o.tenant_id = ? AND (? = ? OR o.region = ?) AND o.ordinal > ? AND o.ordinal <= ? ORDER BY o.ordinal LIMIT ?',tenant.id,actor.scopeRegion,'all',actor.scopeRegion,after,upper,limit+1).toArray();
  const items=rows.slice(0,limit),more=rows.length>limit;let nextCursor:string|null=null;
  if(more){const raw=Buffer.from(JSON.stringify({scope,after:items.at(-1)!.ordinal,upper})).toString('base64url');nextCursor=raw+'.'+createHmac('sha256',secret).update('usage-page-v1.'+raw).digest('base64url');}
  return {events:items.map(row=>{const value=JSON.parse(row.event_json),raw=value.event??value;const event=canonicalUsageEvent(raw);return {event,receivedAt:value.event?value.receivedAt:null,context:this.stream(usageStreamId(event))?.context??null};}),nextCursor,watermark:upper,asOf:Date.now(),pricing:'NOT_CONNECTED' as const};
 }
 alerts(){return this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_usage_conflicts').toArray().map(row=>JSON.parse(row.payload_json) as {code:string;requiresAttention:boolean;eventId:string});}
}
