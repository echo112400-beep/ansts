/** Immutable source quantities. No costs, policy guesses, prompts or model output belong here. */
import {createHash,createHmac,timingSafeEqual} from 'node:crypto';
import {ControlError} from './contracts.js';
export interface UsageIdentity {producerId:string;tenantId:string;taskId:string;attemptId:string;fencing:number;executionInputDigest:string;}
export interface UsageResources {cpuNanoCores:number|null;cpuMilliCores:number|null;memoryMiB:number|null;slots:number;}
export interface ProviderUsage {
 call:number;provider:string|null;requestedModel:string|null;reportedModel:string|null;responseId:string|null;
 inputTokens:string|null;outputTokens:string|null;totalTokens:string|null;cacheHitTokens:string|null;cacheMissTokens:string|null;
 evidenceDigest:string|null;source:'PROVIDER_RESPONSE'|'NO_RESPONSE';completeness:'COMPLETE'|'MISSING'|'UNKNOWN';
}
export interface UsageEvent extends UsageIdentity {
 schemaVersion:1;purpose:'ansts-usage-event-v1';sequence:number;eventId:string;kind:'RESOURCE_OPEN'|'RESOURCE_SEGMENT'|'RESOURCE_CLOSE'|'PROVIDER_REQUEST'|'PROVIDER_USAGE';at:number;resources:UsageResources;
 allocation:{startAt:number;endAt:number;cpuNanoCoreMilliseconds:string|null;memoryMiBMilliseconds:string|null;slotMilliseconds:string}|null;
 provider:ProviderUsage|null;
}
function fail():never{throw new ControlError('INVALID_USAGE_EVENT',422);}
function text(value:unknown,max:number):asserts value is string{if(typeof value!=='string'||!value.trim()||value.length>max||/[\u0000-\u001f]/.test(value))fail();}
function integer(value:number,min=0){if(!Number.isSafeInteger(value)||value<min)fail();}
export function canonicalUsageIdentity(value:UsageIdentity):UsageIdentity {
 if(!value)fail();text(value.producerId,128);text(value.tenantId,128);text(value.taskId,512);text(value.attemptId,512);integer(value.fencing,1);
 if(!/^[a-zA-Z0-9_-]+$/.test(value.producerId)||!(/^[a-f0-9]{64}$/).test(value.executionInputDigest))fail();
 return {producerId:value.producerId,tenantId:value.tenantId,taskId:value.taskId,attemptId:value.attemptId,fencing:value.fencing,executionInputDigest:value.executionInputDigest};
}
export function canonicalUsageResources(value:UsageResources):UsageResources {
 if(!value)fail();integer(value.slots,1);if(value.slots>1000000000)fail();
 if(value.memoryMiB!==null){integer(value.memoryMiB,1);if(value.memoryMiB>1000000000)fail();}
 if(value.cpuNanoCores===null){if(value.cpuMilliCores!==null)fail();}
 else{integer(value.cpuNanoCores,1);if(value.cpuNanoCores>1000000000000000||value.cpuMilliCores!==Math.ceil(value.cpuNanoCores/1000000))fail();}

 return {cpuNanoCores:value.cpuNanoCores,cpuMilliCores:value.cpuMilliCores,memoryMiB:value.memoryMiB,slots:value.slots};
}
function count(value:unknown):asserts value is string {if(typeof value!=='string'||!(/^(0|[1-9][0-9]{0,77})$/).test(value))fail();}
export function canonicalProviderUsage(value:ProviderUsage):ProviderUsage {
 if(!value)fail();integer(value.call,1);
 for(const item of [value.provider,value.requestedModel,value.reportedModel,value.responseId])if(item!==null)text(item,256);
 for(const item of [value.inputTokens,value.outputTokens,value.totalTokens,value.cacheHitTokens,value.cacheMissTokens])if(item!==null)count(item);
 if(!['PROVIDER_RESPONSE','NO_RESPONSE'].includes(value.source)||!['COMPLETE','MISSING','UNKNOWN'].includes(value.completeness))fail();
 if(value.evidenceDigest!==null&&!(typeof value.evidenceDigest==='string'&&/^[a-f0-9]{64}$/.test(value.evidenceDigest)))fail();
 if(value.source==='NO_RESPONSE'&&(value.completeness!=='UNKNOWN'||[value.inputTokens,value.outputTokens,value.totalTokens,value.cacheHitTokens,value.cacheMissTokens,value.evidenceDigest,value.reportedModel,value.responseId].some(item=>item!==null)))fail();
 if(value.source==='PROVIDER_RESPONSE'&&value.evidenceDigest===null)fail();
 if(value.completeness==='COMPLETE'&&(value.source!=='PROVIDER_RESPONSE'||value.inputTokens===null||value.outputTokens===null||value.totalTokens===null||BigInt(value.inputTokens)+BigInt(value.outputTokens)!==BigInt(value.totalTokens)))fail();
 if(value.cacheHitTokens!==null&&value.inputTokens!==null&&BigInt(value.cacheHitTokens)>BigInt(value.inputTokens))fail();
 if(value.cacheMissTokens!==null&&value.inputTokens!==null&&BigInt(value.cacheMissTokens)>BigInt(value.inputTokens))fail();
 if(value.cacheHitTokens!==null&&value.cacheMissTokens!==null&&value.inputTokens!==null&&BigInt(value.cacheHitTokens)+BigInt(value.cacheMissTokens)!==BigInt(value.inputTokens))fail();
 return {call:value.call,provider:value.provider,requestedModel:value.requestedModel,reportedModel:value.reportedModel,responseId:value.responseId,inputTokens:value.inputTokens,outputTokens:value.outputTokens,totalTokens:value.totalTokens,cacheHitTokens:value.cacheHitTokens,cacheMissTokens:value.cacheMissTokens,evidenceDigest:value.evidenceDigest,source:value.source,completeness:value.completeness};
}
export function usageStreamId(identity:UsageIdentity):string{const i=canonicalUsageIdentity(identity);return createHash('sha256').update(JSON.stringify([i.producerId,i.tenantId,i.taskId,i.attemptId,i.fencing])).digest('hex');}
export function usageEventId(identity:UsageIdentity,sequence:number):string{integer(sequence,1);return 'usage:'+createHash('sha256').update(usageStreamId(identity)+':'+sequence).digest('hex');}
export function canonicalUsageEvent(value:UsageEvent):UsageEvent {
 if(!value||value.schemaVersion!==1||value.purpose!=='ansts-usage-event-v1')fail();const identity=canonicalUsageIdentity(value);integer(value.sequence,1);integer(value.at);const resources=canonicalUsageResources(value.resources);
 if(value.eventId!==usageEventId(identity,value.sequence)||!['RESOURCE_OPEN','RESOURCE_SEGMENT','RESOURCE_CLOSE','PROVIDER_REQUEST','PROVIDER_USAGE'].includes(value.kind))fail();
 let allocation:UsageEvent['allocation']=null,provider:ProviderUsage|null=null;
 if(value.kind==='RESOURCE_SEGMENT'){
  if(!value.allocation||value.provider!==null)fail();const a=value.allocation;integer(a.startAt);integer(a.endAt);
  if(a.endAt<=a.startAt||a.endAt!==value.at||Math.floor(a.startAt/86400000)!==Math.floor((a.endAt-1)/86400000))fail();
  for(const quantity of [a.cpuNanoCoreMilliseconds,a.memoryMiBMilliseconds])if(quantity!==null)count(quantity);count(a.slotMilliseconds);
  const duration=BigInt(a.endAt)-BigInt(a.startAt);
  if(resources.cpuNanoCores===null?a.cpuNanoCoreMilliseconds!==null:a.cpuNanoCoreMilliseconds===null||BigInt(a.cpuNanoCoreMilliseconds)!==BigInt(resources.cpuNanoCores)*duration)fail();
  if(resources.memoryMiB===null?a.memoryMiBMilliseconds!==null:a.memoryMiBMilliseconds===null||BigInt(a.memoryMiBMilliseconds)!==BigInt(resources.memoryMiB)*duration)fail();
  if(BigInt(a.slotMilliseconds)!==BigInt(resources.slots)*duration)fail();
  allocation={startAt:a.startAt,endAt:a.endAt,cpuNanoCoreMilliseconds:a.cpuNanoCoreMilliseconds,memoryMiBMilliseconds:a.memoryMiBMilliseconds,slotMilliseconds:a.slotMilliseconds};
 }else if(value.kind.startsWith('PROVIDER_')){if(value.allocation!==null)fail();provider=canonicalProviderUsage(value.provider!);if(value.kind==='PROVIDER_REQUEST'&&provider.source!=='NO_RESPONSE')fail();}
 else if(value.allocation!==null||value.provider!==null)fail();
 return {schemaVersion:1,purpose:'ansts-usage-event-v1',...identity,sequence:value.sequence,eventId:value.eventId,kind:value.kind,at:value.at,resources,allocation,provider};
}
export function usageEventDigest(event:UsageEvent):string{return createHash('sha256').update(JSON.stringify(canonicalUsageEvent(event))).digest('hex');}
function key(value:string){if(typeof value!=='string'||!/^[a-f0-9]{64}$/.test(value))throw new ControlError('USAGE_SIGNING_UNAVAILABLE',503);}
export function signUsageEvent(event:UsageEvent,secret:string):string{key(secret);const payload=Buffer.from(JSON.stringify(canonicalUsageEvent(event))).toString('base64url');return payload+'.'+createHmac('sha256',secret).update('ansts-usage-event-v1.'+payload).digest('base64url');}
export function verifyUsageEvent(proof:string,secret:string):UsageEvent {
 key(secret);if(typeof proof!=='string'||proof.length>16384||!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$/.test(proof))throw new ControlError('USAGE_AUTH_REQUIRED',401);
 const [payload,signature]=proof.split('.'),expected=createHmac('sha256',secret).update('ansts-usage-event-v1.'+payload).digest(),actual=Buffer.from(signature,'base64url');
 if(signature!==actual.toString('base64url')||actual.length!==expected.length||!timingSafeEqual(actual,expected))throw new ControlError('USAGE_AUTH_REQUIRED',401);
 let event;try{event=canonicalUsageEvent(JSON.parse(Buffer.from(payload,'base64url').toString('utf8')));}catch{fail();}
 if(Buffer.from(JSON.stringify(event)).toString('base64url')!==payload)fail();return event;
}
