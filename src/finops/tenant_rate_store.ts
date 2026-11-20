import {pageWindow,type FinopsPage} from './finops_page.js';
import {assertControlAccess,assertExpectedVersion,ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage,TenantRecord} from '../control/tenant_directory.js';
import {canonicalUsageIdentity,usageStreamId,type UsageIdentity} from '../control/usage_event.js';
import {rateHash,validText,verifyRateCard,type RateCard} from './rate_cards.js';
export interface RateActivation {version:number;tenantId:string;tenantVersion:number;region:string;rateCard:RateCard;activatedAt:number;actor:string;reason:string}
export interface UsagePriceContext {
 tenantId:string;tenantVersion:number;region:string;costCenter:string;costCenterVersion:number|null;poolId:string|null;
 activation:RateActivation|null;
}
interface ActivationWrite {expectedVersion:number;rateCardVersion:number;reason:string;requestId:string;idempotencyKey:string}
/** resolvedRate is fetched by a trusted service adapter from the directory, never browser JSON. */
export class TenantRateStore {
 constructor(private readonly storage:DirectoryStorage){
  if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);
  storage.transactionSync(()=>{
   storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_rate_identity (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_rate_activations (version INTEGER PRIMARY KEY, payload_json TEXT NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_rate_requests (id TEXT PRIMARY KEY, digest TEXT NOT NULL, version INTEGER NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_rate_audit (version INTEGER PRIMARY KEY, payload_json TEXT NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS tenant_usage_price_bindings (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)');
  });
 }
 private anchor(tenantId:string,create=false){
  const row=this.storage.sql.exec<{tenant_id:string}>('SELECT tenant_id FROM tenant_rate_identity WHERE id = ?','tenant').toArray()[0];
  if(row&&row.tenant_id!==tenantId)throw new ControlError('TENANT_RATE_IDENTITY_CONFLICT',503);
  if(!row&&create)this.storage.sql.exec('INSERT INTO tenant_rate_identity (id, tenant_id) VALUES (?, ?)', 'tenant',tenantId);
 }
 current(actor:ControlActor,tenant:TenantRecord):RateActivation|null {
  assertControlAccess(actor,'read',tenant.id,tenant.region);this.anchor(tenant.id);
  const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_rate_activations ORDER BY version DESC LIMIT 1').toArray()[0];
  if(!row)return null;const value=JSON.parse(row.payload_json) as RateActivation;
  if(value.tenantId!==tenant.id)throw new ControlError('TENANT_RATE_IDENTITY_CONFLICT',503);
  assertControlAccess(actor,'read',tenant.id,value.region);verifyRateCard(value.rateCard);return value;
 }
 read(actor:ControlActor,tenant:TenantRecord,options:FinopsPage,secret:string){
  const current=this.current(actor,tenant),maximum=current?.version??0;
  const page=pageWindow(actor,'tenant-rates:'+tenant.id,tenant.version,maximum,options,secret);
  const rows=this.storage.sql.exec<{version:number;payload_json:string}>('SELECT version, payload_json FROM tenant_rate_activations WHERE version > ? AND version <= ? ORDER BY version LIMIT ?',page.after,page.upper,page.limit+1).toArray();
  const visible=rows.slice(0,page.limit);const history=visible.map(row=>{const value=JSON.parse(row.payload_json) as RateActivation;if(value.tenantId!==tenant.id)throw new ControlError('TENANT_RATE_IDENTITY_CONFLICT',503);verifyRateCard(value.rateCard);return value;}).filter(value=>actor.scopeRegion==='all'||value.region===actor.scopeRegion);
  return {current,history,watermark:page.upper,nextCursor:page.next(visible.at(-1)?.version??page.after,rows.length>page.limit),effective:current?.tenantVersion===tenant.version,tenantVersion:tenant.version,enforcement:'NOT_CONNECTED' as const};
 }
 audit(actor:ControlActor,tenant:TenantRecord,options:FinopsPage,secret:string){
  assertControlAccess(actor,'administer',tenant.id,tenant.region);const current=this.current(actor,tenant);
  const page=pageWindow(actor,'rate-activation-audit:'+tenant.id,tenant.version,current?.version??0,options,secret);
  const rows=this.storage.sql.exec<{version:number;payload_json:string;activation_json:string}>('SELECT a.version, a.payload_json, v.payload_json AS activation_json FROM tenant_rate_audit a JOIN tenant_rate_activations v ON v.version = a.version WHERE a.version > ? AND a.version <= ? ORDER BY a.version LIMIT ?',page.after,page.upper,page.limit+1).toArray();
  const visible=rows.slice(0,page.limit),items=visible.map(row=>{
   const audit=JSON.parse(row.payload_json),activation=JSON.parse(row.activation_json) as RateActivation;verifyRateCard(activation.rateCard);
   if(activation.tenantId!==tenant.id)throw new ControlError('TENANT_RATE_IDENTITY_CONFLICT',503);
   const previousRow=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_rate_activations WHERE version = ?',row.version-1).toArray()[0];
   const previous=previousRow?JSON.parse(previousRow.payload_json) as RateActivation:null;
   return {...audit,id:'rate-activation:'+tenant.id+':'+row.version,domain:'rate-activation' as const,targetId:tenant.id,region:activation.region,timestamp:new Date(audit.at).toISOString(),previousVersion:row.version-1,version:row.version,changes:[{field:'rateCardVersion',before:previous?String(previous.rateCard.version):null,after:String(activation.rateCard.version)},{field:'rateDigest',before:previous?.rateCard.digest??null,after:activation.rateCard.digest},{field:'tenantVersion',before:previous?String(previous.tenantVersion):null,after:String(activation.tenantVersion)}]};
  }).filter(entry=>actor.scopeRegion==='all'||entry.region===actor.scopeRegion);
  return {items,version:page.upper,nextCursor:page.next(visible.at(-1)?.version??page.after,rows.length>page.limit),coverage:{includedDomains:['rate-activation'],tenantId:tenant.id}};
 }
 activate(actor:ControlActor,tenant:TenantRecord,input:ActivationWrite,resolvedRate:RateCard,now=Date.now()):RateActivation {
  assertControlAccess(actor,'administer',tenant.id,tenant.region);
  if(!input||!validText(input.reason,1000)||!validText(input.requestId,256)||!validText(input.idempotencyKey)||!Number.isSafeInteger(now)||now<0)throw new ControlError('INVALID_RATE_ACTIVATION',422);
  const card=verifyRateCard(resolvedRate);if(card.version!==input.rateCardVersion)throw new ControlError('RATE_VERSION_CONFLICT',409);
  if(card.createdAt>now||card.scope.region!==null&&card.scope.region!==tenant.region)throw new ControlError('RATE_SCOPE_CONFLICT',409);
  const key=rateHash(input.idempotencyKey),digest=rateHash({actor:actor.sub,tenantId:tenant.id,expectedVersion:input.expectedVersion,rateDigest:card.digest,reason:input.reason});
  return this.storage.transactionSync(()=>{
   this.anchor(tenant.id,true);
   const old=this.storage.sql.exec<{digest:string;version:number}>('SELECT digest, version FROM tenant_rate_requests WHERE id = ?',key).toArray()[0];
   if(old){if(old.digest!==digest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_rate_activations WHERE version = ?',old.version).toArray()[0];if(!row)throw new ControlError('TENANT_RATE_STATE_CORRUPT',503);const result=JSON.parse(row.payload_json) as RateActivation;assertControlAccess(actor,'read',tenant.id,result.region);return result;}
   if(tenant.status!=='ACTIVE')throw new ControlError('TENANT_DISABLED',422);
   const current=this.current(actor,tenant);assertExpectedVersion(input.expectedVersion,current?.version??0);
   if((current?.version??0)>=10000)throw new ControlError('RATE_ACTIVATIONS_FULL',503);
   const activation:RateActivation={version:input.expectedVersion+1,tenantId:tenant.id,tenantVersion:tenant.version,region:tenant.region,rateCard:card,activatedAt:now,actor:actor.sub,reason:input.reason};
   this.storage.sql.exec('INSERT INTO tenant_rate_activations (version, payload_json) VALUES (?, ?)',activation.version,JSON.stringify(activation));
   this.storage.sql.exec('INSERT INTO tenant_rate_requests (id, digest, version) VALUES (?, ?, ?)',key,digest,activation.version);
   this.storage.sql.exec('INSERT INTO tenant_rate_audit (version, payload_json) VALUES (?, ?)',activation.version,JSON.stringify({version:activation.version,actor:actor.sub,requestId:input.requestId,reason:input.reason,at:now,rateDigest:card.digest}));return activation;
  });
 }
 /** Must run at the authenticated execution binding boundary, before compute. Null is pinned too. */
 pin(actor:ControlActor,tenant:TenantRecord,identity:UsageIdentity,metadata:{costCenterVersion:number|null;poolId:string|null},now=Date.now()):UsagePriceContext {
  assertControlAccess(actor,'request_capacity',tenant.id,tenant.region);identity=canonicalUsageIdentity(identity);
  if(identity.tenantId!==tenant.id)throw new ControlError('USAGE_BINDING_FORBIDDEN',403);
  if(!metadata||!Number.isSafeInteger(now)||now<0||metadata.costCenterVersion!==null&&(!Number.isSafeInteger(metadata.costCenterVersion)||metadata.costCenterVersion<1)||metadata.poolId!==null&&!validText(metadata.poolId))throw new ControlError('INVALID_USAGE_PRICE_CONTEXT',422);
  return this.storage.transactionSync(()=>{
   this.anchor(tenant.id,true);const existing=this.binding(identity);if(existing)return existing;
   if(tenant.status!=='ACTIVE')throw new ControlError('TENANT_DISABLED',422);
   const sourceTable=this.storage.sql.exec<{name:string}>("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tenant_usage_streams'").toArray()[0];
   if(sourceTable&&this.storage.sql.exec('SELECT id FROM tenant_usage_streams WHERE id = ?',usageStreamId(identity)).toArray().length)throw new ControlError('USAGE_PRICE_PIN_TOO_LATE',409);
   const candidate=this.current(actor,tenant);
   // Metadata edits require explicit reactivation. Do not silently borrow an old region/tenant snapshot.
   const activation=candidate?.tenantVersion===tenant.version?candidate:null;
   if(activation&&activation.activatedAt>now)throw new ControlError('RATE_NOT_YET_ACTIVE',409);
   const context:UsagePriceContext={tenantId:tenant.id,tenantVersion:tenant.version,region:tenant.region,costCenter:tenant.costCenter,costCenterVersion:metadata.costCenterVersion,poolId:metadata.poolId,activation};
   this.storage.sql.exec('INSERT INTO tenant_usage_price_bindings (id, payload_json) VALUES (?, ?)',usageStreamId(identity),JSON.stringify({identity,context}));return context;
  });
 }
 binding(identity:UsageIdentity):UsagePriceContext|null {
  identity=canonicalUsageIdentity(identity);this.anchor(identity.tenantId);
  const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_usage_price_bindings WHERE id = ?',usageStreamId(identity)).toArray()[0];if(!row)return null;
  const value=JSON.parse(row.payload_json) as {identity:UsageIdentity;context:UsagePriceContext};
  if(JSON.stringify(value.identity)!==JSON.stringify(identity))throw new ControlError('USAGE_PRICE_BINDING_CONFLICT',409);return value.context;
 }
}
