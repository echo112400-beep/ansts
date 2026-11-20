/** Immutable native usage events and delivery intent share one atomic per-attempt file. */
import {mkdirSync,readdirSync,readFileSync,existsSync,openSync,writeFileSync,fsyncSync,closeSync,renameSync} from 'node:fs';
import {join} from 'node:path';
import {canonicalUsageIdentity,canonicalUsageResources,canonicalUsageEvent,canonicalProviderUsage,usageStreamId,usageEventId,usageEventDigest,type UsageIdentity,type UsageResources,type UsageEvent,type ProviderUsage} from '../control/usage_event.js';
interface Delivery {event:UsageEvent;acknowledged:boolean;attempts:number;nextAttemptAt:number;quarantined:boolean;}
interface State {schemaVersion:1;identity:UsageIdentity;resources:UsageResources;cursor:number;active:boolean;calls:Record<string,{request:ProviderUsage;response:ProviderUsage|null}>;events:Delivery[];}
export class BodyUsageJournal {
 constructor(private readonly directory:string){mkdirSync(directory,{recursive:true});}
 private path(identity:UsageIdentity){return join(this.directory,usageStreamId(identity)+'.json');}
 private load(identity:UsageIdentity):State {
  const file=this.path(identity);if(!existsSync(file))throw new Error('USAGE_STREAM_NOT_FOUND');const state=JSON.parse(readFileSync(file,'utf8')) as State;
  if(state.schemaVersion!==1||JSON.stringify(canonicalUsageIdentity(state.identity))!==JSON.stringify(canonicalUsageIdentity(identity))||!Array.isArray(state.events)||state.events.length<1||state.events[0].event.kind!=='RESOURCE_OPEN'||state.events.at(-1)?.event.sequence!==state.events.length||!state.calls||!Number.isSafeInteger(state.cursor)||typeof state.active!=='boolean')throw new Error('USAGE_STREAM_CORRUPT');return state;
 }
 private save(state:State){
  const data=JSON.stringify(state);if(state.events.length>5000||Buffer.byteLength(data)>8*1024*1024)throw new Error('USAGE_OUTBOX_FULL');
  const file=this.path(state.identity),fd=openSync(file+'.tmp','w',0o600);try{writeFileSync(fd,data);fsyncSync(fd);}finally{closeSync(fd);}renameSync(file+'.tmp',file);const dir=openSync(this.directory,'r');try{fsyncSync(dir);}finally{closeSync(dir);}
 }
 private append(state:State,kind:UsageEvent['kind'],at:number,allocation:UsageEvent['allocation']=null,provider:ProviderUsage|null=null){
  if(state.events.length>=5000)throw new Error('USAGE_OUTBOX_FULL');
  if(state.events.length&&at<state.events.at(-1)!.event.at)throw new Error('USAGE_CLOCK_REGRESSION');
  const sequence=state.events.length+1,event=canonicalUsageEvent({schemaVersion:1,purpose:'ansts-usage-event-v1',...state.identity,resources:state.resources,sequence,eventId:usageEventId(state.identity,sequence),kind,at,allocation,provider});state.events.push({event,acknowledged:false,attempts:0,nextAttemptAt:0,quarantined:false});return event;
 }
 begin(identity:UsageIdentity,resources:UsageResources,now=Date.now()):void {
  identity=canonicalUsageIdentity(identity);resources=canonicalUsageResources(resources);
  if(existsSync(this.path(identity))){const old=this.load(identity);if(JSON.stringify(old.resources)!==JSON.stringify(resources))throw new Error('USAGE_RESOURCE_BINDING_CONFLICT');return;}
  const state:State={schemaVersion:1,identity,resources,cursor:now,active:true,calls:{},events:[]};this.append(state,'RESOURCE_OPEN',now);this.save(state);
 }
 private segments(state:State,now:number){
  if(!Number.isSafeInteger(now)||now<state.cursor)throw new Error('USAGE_CLOCK_REGRESSION');
  while(state.cursor<now){const end=Math.min(now,(Math.floor(state.cursor/86400000)+1)*86400000),duration=BigInt(end)-BigInt(state.cursor);
   this.append(state,'RESOURCE_SEGMENT',end,{startAt:state.cursor,endAt:end,cpuNanoCoreMilliseconds:state.resources.cpuNanoCores===null?null:(BigInt(state.resources.cpuNanoCores)*duration).toString(),memoryMiBMilliseconds:state.resources.memoryMiB===null?null:(BigInt(state.resources.memoryMiB)*duration).toString(),slotMilliseconds:(BigInt(state.resources.slots)*duration).toString()});state.cursor=end;}
 }
 checkpoint(identity:UsageIdentity,now=Date.now()):void {const state=this.load(identity);if(!state.active)return;this.segments(state,now);this.save(state);}
 finish(identity:UsageIdentity,now=Date.now()):void {const state=this.load(identity);if(!state.active)return;this.segments(state,now);this.append(state,'RESOURCE_CLOSE',now);state.active=false;this.save(state);}
 /** Only after native startup positively confirms all previous local executions have stopped. */
 recoverReleased(now=Date.now()):void {for(const state of this.states())if(state.active&&state.resources.cpuNanoCores!==null)this.finish(state.identity,now);}
 checkpointActive(now=Date.now()):void {for(const state of this.states())if(state.active)this.checkpoint(state.identity,now);}
 /** Flush only crossed UTC boundaries before a later provider timestamp becomes immutable.
  * Otherwise the next resource checkpoint would append an older midnight after this response. */
 private beforeProvider(state:State,now:number){
  if(!Number.isSafeInteger(now)||now<0)throw new Error('USAGE_CLOCK_REGRESSION');
  const boundary=Math.floor(now/86400000)*86400000;if(state.active&&boundary>state.cursor)this.segments(state,boundary);
 }
 providerStarted(identity:UsageIdentity,call:number,source:{provider:string;model:string}|undefined,now=Date.now()):void {
  const state=this.load(identity);if(!state.active)throw new Error('USAGE_STREAM_CLOSED');
  const request=canonicalProviderUsage({call,provider:source?.provider??null,requestedModel:source?.model??null,reportedModel:null,responseId:null,inputTokens:null,outputTokens:null,totalTokens:null,cacheHitTokens:null,cacheMissTokens:null,evidenceDigest:null,source:'NO_RESPONSE',completeness:'UNKNOWN'}),key=String(call),old=state.calls[key];
  if(old){if(JSON.stringify(old.request)!==JSON.stringify(request))throw new Error('USAGE_CALL_CONFLICT');throw new Error('USAGE_CALL_ALREADY_STARTED');}
  this.beforeProvider(state,now);state.calls[key]={request,response:null};this.append(state,'PROVIDER_REQUEST',now,null,request);this.save(state);
 }
 providerFinished(identity:UsageIdentity,call:number,response:Omit<ProviderUsage,'call'|'provider'|'requestedModel'>|null,now=Date.now()):void {
  const state=this.load(identity),entry=state.calls[String(call)];if(!entry)throw new Error('USAGE_CALL_NOT_FOUND');
  const usage=canonicalProviderUsage({...entry.request,...(response??{}),call});
  if(entry.response){if(JSON.stringify(entry.response)!==JSON.stringify(usage))throw new Error('USAGE_CALL_CONFLICT');return;}
  this.beforeProvider(state,now);entry.response=usage;this.append(state,'PROVIDER_USAGE',now,null,usage);this.save(state);
 }
 providerUncertain(identity:UsageIdentity,call:number,now=Date.now()):void {const state=this.load(identity);if(state.calls[String(call)]?.response)return;this.providerFinished(identity,call,null,now);}
 private states():State[]{return readdirSync(this.directory).filter(name=>/^[a-f0-9]{64}\.json$/.test(name)).sort().map(name=>{const value=JSON.parse(readFileSync(join(this.directory,name),'utf8')) as State;if(this.path(value.identity)!==join(this.directory,name))throw new Error('USAGE_STREAM_CORRUPT');return this.load(value.identity);});}
 stats(now=Date.now()){
  const states=this.states(),pending=states.flatMap(s=>s.events.filter(e=>!e.acknowledged));
  return {incompleteResourceStreams:states.filter(s=>s.resources.cpuNanoCores===null||s.resources.memoryMiB===null).length,unknownRetainedSlots:states.filter(s=>s.active&&s.resources.cpuNanoCores===null).reduce((sum,s)=>sum+s.resources.slots,0),pendingEvents:pending.length,openStreams:states.filter(s=>s.active).length,quarantinedStreams:states.filter(s=>s.events.some(e=>e.quarantined)).length,unresolvedProviderCalls:states.reduce((sum,s)=>sum+Object.values(s.calls).filter(c=>c.response?c.response.completeness!=='COMPLETE':!s.active).length,0),oldestPendingSeconds:pending.length?Math.max(0,...pending.map(e=>(now-e.event.at)/1000)):0};
 }
 events(identity:UsageIdentity):UsageEvent[]{return this.load(identity).events.map(row=>canonicalUsageEvent(row.event));}
 pending(limit=4,now=Date.now()):UsageEvent[]{
  if(!Number.isSafeInteger(limit)||limit<1||limit>100)throw new Error('INVALID_USAGE_LIMIT');const events:UsageEvent[]=[];
  for(const state of this.states()){const next=state.events.find(row=>!row.acknowledged);if(next&&!next.quarantined&&next.nextAttemptAt<=now)events.push(canonicalUsageEvent(next.event));if(events.length===limit)break;}return events;
 }
 acknowledge(identity:UsageIdentity,sequence:number,digest:string):boolean {
  const state=this.load(identity),entry=state.events[sequence-1];if(!entry||entry.quarantined||entry.event.sequence!==sequence||usageEventDigest(entry.event)!==digest)return false;
  if(state.events.slice(0,sequence-1).some(row=>!row.acknowledged))return false;entry.acknowledged=true;this.save(state);return true;
 }
 defer(identity:UsageIdentity,sequence:number,digest:string,quarantine=false,now=Date.now()):boolean {
  const state=this.load(identity),entry=state.events[sequence-1];if(!entry||entry.acknowledged||usageEventDigest(entry.event)!==digest)return false;
  entry.attempts=Math.min(20,entry.attempts+1);entry.nextAttemptAt=now+Math.min(60000,1000*2**entry.attempts);entry.quarantined=entry.quarantined||quarantine;this.save(state);return true;
 }
}
