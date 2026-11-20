import {pageWindow,type FinopsPage} from './finops_page.js';
/** Internal showback rates. Never infer these values from provider examples or budgets. */
import {createHash} from 'node:crypto';
import {ControlError,assertExpectedVersion,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';
export const MAX_CREDIT=(1n<<127n)-1n;
export const MAX_EXACT=(1n<<255n)-1n;
export interface ExactAmount {numerator:string;denominator:string}
export type Rounding='FLOOR'|'CEIL'|'HALF_EVEN';
export interface ModelRate {
 provider:string;requestedModel:string;reportedModel:string;
 input:{mode:'ALL';rate:ExactAmount|null}|{mode:'CACHE_SPLIT';hit:ExactAmount|null;miss:ExactAmount|null};
 output:ExactAmount|null;
}
export interface RateDefinition {
 schemaVersion:1;unit:'credit-subunit';unitVersion:1;rounding:Rounding;
 scope:{region:string|null;poolId:string|null};
 resources:{cpuNanoCoreMilliseconds:ExactAmount|null;memoryMiBMilliseconds:ExactAmount|null;slotMilliseconds:ExactAmount|null};
 models:ModelRate[];
}
export interface RateCard extends RateDefinition {version:number;digest:string;createdAt:number;actor:string;reason:string}
export function rateHash(value:unknown):string{return createHash('sha256').update(JSON.stringify(value)).digest('hex');}
export function validText(value:unknown,max=128):value is string{return typeof value==='string'&&value.trim()===value&&value.length>0&&value.length<=max&&!/[\u0000-\u001f]/.test(value);}
export function unsigned(value:unknown,max=MAX_EXACT):bigint {
 if(typeof value!=='string'||!/^(0|[1-9][0-9]{0,77})$/.test(value))throw new ControlError('INVALID_EXACT_AMOUNT',422);
 const result=BigInt(value);if(result>max)throw new ControlError('CREDIT_OVERFLOW',422);return result;
}
function gcd(a:bigint,b:bigint):bigint {while(b){const c=a%b;a=b;b=c;}return a;}
export function fraction(n:bigint,d:bigint):ExactAmount {
 if(n<0n||d<=0n)throw new ControlError('INVALID_EXACT_AMOUNT',422);
 const divisor=gcd(n,d);n/=divisor;d/=divisor;
 if(n>MAX_EXACT||d>MAX_EXACT)throw new ControlError('CREDIT_OVERFLOW',422);
 return {numerator:n.toString(),denominator:d.toString()};
}
export function exactAdd(a:ExactAmount,b:ExactAmount):ExactAmount {
 const an=unsigned(a.numerator),ad=unsigned(a.denominator),bn=unsigned(b.numerator),bd=unsigned(b.denominator);
 if(ad===0n||bd===0n)throw new ControlError('INVALID_EXACT_AMOUNT',422);
 const divisor=gcd(ad,bd);return fraction(an*(bd/divisor)+bn*(ad/divisor),ad*(bd/divisor));
}
export function exactPrice(quantity:string,rate:ExactAmount):ExactAmount {
 const q=unsigned(quantity),n=unsigned(rate.numerator,MAX_CREDIT),d=unsigned(rate.denominator,MAX_CREDIT);
 if(d===0n)throw new ControlError('INVALID_EXACT_AMOUNT',422);return fraction(q*n,d);
}
export function roundCredit(value:ExactAmount,mode:Rounding):string {
 const n=unsigned(value.numerator),d=unsigned(value.denominator);if(d===0n||!['FLOOR','CEIL','HALF_EVEN'].includes(mode))throw new ControlError('INVALID_ROUNDING',422);
 let result=n/d;const remainder=n%d;
 if(mode==='CEIL'&&remainder>0n||mode==='HALF_EVEN'&&(remainder*2n>d||remainder*2n===d&&result%2n===1n))result++;
 if(result>MAX_CREDIT)throw new ControlError('CREDIT_OVERFLOW',422);return result.toString();
}
function rate(value:ExactAmount|null):ExactAmount|null {
 if(value===null)return null;if(!value||typeof value!=='object'||Object.keys(value).some(k=>!['numerator','denominator'].includes(k)))throw new ControlError('INVALID_RATE_CARD',422);
 const n=unsigned(value.numerator,MAX_CREDIT),d=unsigned(value.denominator,MAX_CREDIT);if(d===0n)throw new ControlError('INVALID_RATE_CARD',422);
 return fraction(n,d);
}
function keys(value:unknown,expected:string[]):void {
 if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!expected.includes(k))||expected.some(k=>!(k in value)))throw new ControlError('INVALID_RATE_CARD',422);
}
export function canonicalRateDefinition(value:RateDefinition):RateDefinition {
 keys(value,['schemaVersion','unit','unitVersion','rounding','scope','resources','models']);
 if(value.schemaVersion!==1||value.unit!=='credit-subunit'||value.unitVersion!==1||!['FLOOR','CEIL','HALF_EVEN'].includes(value.rounding))throw new ControlError('INVALID_RATE_CARD',422);
 keys(value.scope,['region','poolId']);for(const v of [value.scope.region,value.scope.poolId])if(v!==null&&!validText(v))throw new ControlError('INVALID_RATE_CARD',422);
 keys(value.resources,['cpuNanoCoreMilliseconds','memoryMiBMilliseconds','slotMilliseconds']);
 if(!Array.isArray(value.models)||value.models.length>100)throw new ControlError('INVALID_RATE_CARD',422);
 const seen=new Set<string>();const models=value.models.map(m=>{
  keys(m,['provider','requestedModel','reportedModel','input','output']);
  if([m.provider,m.requestedModel,m.reportedModel].some(v=>!validText(v,256)))throw new ControlError('INVALID_RATE_CARD',422);
  const key=JSON.stringify([m.provider,m.requestedModel,m.reportedModel]);if(seen.has(key))throw new ControlError('DUPLICATE_MODEL_RATE',422);seen.add(key);
  let input:ModelRate['input'];
  if(m.input?.mode==='ALL'){keys(m.input,['mode','rate']);input={mode:'ALL',rate:rate(m.input.rate)};}
  else if(m.input?.mode==='CACHE_SPLIT'){keys(m.input,['mode','hit','miss']);input={mode:'CACHE_SPLIT',hit:rate(m.input.hit),miss:rate(m.input.miss)};}
  else throw new ControlError('INVALID_RATE_CARD',422);
  return {provider:m.provider,requestedModel:m.requestedModel,reportedModel:m.reportedModel,input,output:rate(m.output)};
 }).sort((a,b)=>{const left=JSON.stringify([a.provider,a.requestedModel,a.reportedModel]),right=JSON.stringify([b.provider,b.requestedModel,b.reportedModel]);return left<right?-1:left>right?1:0;});
 return {schemaVersion:1,unit:'credit-subunit',unitVersion:1,rounding:value.rounding,scope:{region:value.scope.region,poolId:value.scope.poolId},resources:{cpuNanoCoreMilliseconds:rate(value.resources.cpuNanoCoreMilliseconds),memoryMiBMilliseconds:rate(value.resources.memoryMiBMilliseconds),slotMilliseconds:rate(value.resources.slotMilliseconds)},models};
}
export function verifyRateCard(value:RateCard):RateCard {
 if(!value||!Number.isSafeInteger(value.version)||value.version<1||!Number.isSafeInteger(value.createdAt)||value.createdAt<0||!validText(value.actor,256)||!validText(value.reason,1000))throw new ControlError('INVALID_RATE_CARD',422);
 const {version,digest,createdAt,actor,reason,...raw}=value;const definition=canonicalRateDefinition(raw);
 const content={...definition,version,createdAt,actor,reason};if(rateHash(content)!==digest)throw new ControlError('RATE_DIGEST_CONFLICT',409);return {...content,digest};
}
export function assertRateAdministrator(actor:ControlActor):void {
 if(!actor||!validText(actor.sub,256)||actor.role!=='SRE_ADMIN'||actor.scopeTenant!=='all'||actor.scopeRegion!=='all')throw new ControlError('CONTROL_FORBIDDEN',403);
}
export interface RateWrite {expectedVersion:number;definition:RateDefinition;reason:string;idempotencyKey:string;requestId:string}
/** Instantiate only inside TenantDirectoryCell: one writer and no mutable price rows. */
export class RateCatalog {
 constructor(private readonly storage:DirectoryStorage){
  if(typeof storage.transactionSync!=='function')throw new ControlError('DURABLE_TRANSACTION_REQUIRED',503);
  storage.transactionSync(()=>{
   storage.sql.exec('CREATE TABLE IF NOT EXISTS directory_rate_cards (version INTEGER PRIMARY KEY, payload_json TEXT NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS directory_rate_requests (id TEXT PRIMARY KEY, digest TEXT NOT NULL, version INTEGER NOT NULL)');
   storage.sql.exec('CREATE TABLE IF NOT EXISTS directory_rate_audit (version INTEGER PRIMARY KEY, payload_json TEXT NOT NULL)');
  });
 }
 get(version:number):RateCard {
  if(!Number.isSafeInteger(version)||version<1)throw new ControlError('INVALID_VERSION',422);
  const row=this.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM directory_rate_cards WHERE version = ?',version).toArray()[0];
  if(!row)throw new ControlError('RATE_CARD_NOT_FOUND',404);const result=verifyRateCard(JSON.parse(row.payload_json));if(result.version!==version)throw new ControlError('RATE_STATE_CORRUPT',503);return result;
 }
 page(actor:ControlActor,options:FinopsPage,secret:string,audit=false){
  assertRateAdministrator(actor);const table=audit?'directory_rate_audit':'directory_rate_cards';
  const maximum=Number(this.storage.sql.exec<{n:number}>(`SELECT COALESCE(MAX(version),0) AS n FROM ${table}`).toArray()[0].n);
  const page=pageWindow(actor,audit?'rate-audit':'rate-cards',1,maximum,options,secret);
  const rows=this.storage.sql.exec<{version:number;payload_json:string}>(`SELECT version, payload_json FROM ${table} WHERE version > ? AND version <= ? ORDER BY version LIMIT ?`,page.after,page.upper,page.limit+1).toArray();
  const visible=rows.slice(0,page.limit);return {items:visible.map(row=>audit?JSON.parse(row.payload_json):verifyRateCard(JSON.parse(row.payload_json))),watermark:page.upper,nextCursor:page.next(visible.at(-1)?.version??page.after,rows.length>page.limit)};
 }
 create(actor:ControlActor,input:RateWrite,now=Date.now()):RateCard {
  assertRateAdministrator(actor);
  if(!input||!validText(input.reason,1000)||!validText(input.idempotencyKey)||!validText(input.requestId,256)||!Number.isSafeInteger(now)||now<0)throw new ControlError('INVALID_RATE_WRITE',422);
  const definition=canonicalRateDefinition(input.definition),key=rateHash(input.idempotencyKey),digest=rateHash({actor:actor.sub,expectedVersion:input.expectedVersion,definition,reason:input.reason});
  return this.storage.transactionSync(()=>{
   const receipt=this.storage.sql.exec<{digest:string;version:number}>('SELECT digest, version FROM directory_rate_requests WHERE id = ?',key).toArray()[0];
   if(receipt){if(receipt.digest!==digest)throw new ControlError('IDEMPOTENCY_CONFLICT',409);return this.get(receipt.version);}
   const current=Number(this.storage.sql.exec<{n:number}>('SELECT COALESCE(MAX(version),0) AS n FROM directory_rate_cards').toArray()[0].n);assertExpectedVersion(input.expectedVersion,current);
   if(current>=10000)throw new ControlError('RATE_CATALOG_FULL',503);
   const content={...definition,version:current+1,createdAt:now,actor:actor.sub,reason:input.reason},card={...content,digest:rateHash(content)};
   this.storage.sql.exec('INSERT INTO directory_rate_cards (version, payload_json) VALUES (?, ?)',card.version,JSON.stringify(card));
   this.storage.sql.exec('INSERT INTO directory_rate_requests (id, digest, version) VALUES (?, ?, ?)',key,digest,card.version);
   this.storage.sql.exec('INSERT INTO directory_rate_audit (version, payload_json) VALUES (?, ?)',card.version,JSON.stringify({version:card.version,actor:actor.sub,reason:input.reason,requestId:input.requestId,at:now,digest:card.digest}));return card;
  });
 }
}
