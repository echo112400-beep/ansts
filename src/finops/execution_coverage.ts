/** Convert authenticated, complete discovery reads into conservative period evidence. */
import {ControlError} from '../control/contracts.js';
import {inventoryDigest,INVENTORY_PAGE_SIZE,type InventoryPage,type InventorySnapshot,type InventoryObservation} from '../control/execution_inventory.js';
import {canonicalUsageIdentity,usageStreamId,type UsageIdentity} from '../control/usage_event.js';
import {settlementMonth,type SettlementCoverage} from './period_settlement.js';

export type CoverageSources = {body(taskId:string):Promise<InventorySnapshot>} & (
 {scheduler():Promise<InventorySnapshot>;schedulerPage?:never} |
 {scheduler?:never;schedulerPage(after:number):Promise<InventoryPage>}
);
export interface ExecutionCoverageResult {coverage:SettlementCoverage;reasons:string[];sources:{kind:string;taskId:string|null;watermark:string}[];final:false;}
const SCHEDULER_PAGE_LIMIT=128;
function invalid():never {throw new ControlError('EXECUTION_INVENTORY_INVALID',503);}
function checkHeader(value:InventorySnapshot,tenantId:string,kind:'SCHEDULER'|'BODY',now:number,maxEntries:number){
 if(!value||value.schemaVersion!==1||value.purpose!=='ansts-execution-inventory-v1'||value.kind!==kind||value.tenantId!==tenantId||typeof value.sourceId!=='string'||!value.sourceId||!Number.isSafeInteger(value.coverageSince)||value.coverageSince<0||!Number.isSafeInteger(value.observedAt)||value.observedAt>now||value.observedAt<value.coverageSince||!Array.isArray(value.entries)||value.entries.length>maxEntries||!Number.isSafeInteger(value.count)||value.count<0||!Array.isArray(value.reasons)||typeof value.complete!=='boolean')invalid();
}
function checkWatermark(value:InventorySnapshot){
 const meta={kind:value.kind,sourceId:value.sourceId,coverageSince:value.coverageSince},head={count:value.count,digest:value.headDigest};
 if((value.count===0?value.headDigest!=='':!(/^[a-f0-9]{64}$/.test(value.headDigest)))||inventoryDigest({meta,tenantId:value.tenantId,head})!==value.watermark)invalid();
}
function checkChain(value:InventorySnapshot,after=0,previousDigest=''){
 for(const [index,entry] of value.entries.entries()){
  const {digest,...content}=entry;
  if(entry.ordinal!==after+index+1||entry.previousDigest!==previousDigest||entry.observation.tenantId!==value.tenantId||inventoryDigest(content)!==digest||!Number.isSafeInteger(entry.observedAt)||entry.observedAt<0||entry.observedAt>value.observedAt)invalid();
  previousDigest=digest;
 }
 return previousDigest;
}
function checked(value:InventorySnapshot,tenantId:string,kind:'SCHEDULER'|'BODY',now:number):InventorySnapshot {
 checkHeader(value,tenantId,kind,now,10000);
 if(!value.complete)return value;
 if(value.reasons.length||value.entries.length!==value.count||checkChain(value)!==value.headDigest)invalid();
 checkWatermark(value);
 return value;
}
function checkPage(page:InventoryPage,tenantId:string,after:number,now:number){
 checkHeader(page,tenantId,'SCHEDULER',now,INVENTORY_PAGE_SIZE);
 checkWatermark(page);
 const end=after+page.entries.length,nextAfter=end<page.count?end:null;
 if(page.after!==after||after>page.count||page.entries.length!==Math.min(INVENTORY_PAGE_SIZE,page.count-after)||page.nextAfter!==nextAfter||page.complete!==(nextAfter===null)||page.reasons.length)invalid();
}
async function readSchedulerPage(read:(after:number)=>Promise<InventoryPage>,after:number,deadline:number):Promise<InventoryPage>{
 const timeout=new ControlError('SCHEDULER_PAGE_TIME_LIMIT',503),remaining=deadline-Date.now();
 if(remaining<=0)throw timeout;
 let timer:ReturnType<typeof setTimeout>|undefined;
 try {
  return await Promise.race([read(after),new Promise<never>((_,reject)=>{timer=setTimeout(()=>reject(timeout),remaining);})]);
 }finally{if(timer!==undefined)clearTimeout(timer);}
}
async function schedulerHistory(read:(after:number)=>Promise<InventoryPage>,tenantId:string,clock:()=>number,deadline:number):Promise<InventorySnapshot>{
 const first=await readSchedulerPage(read,0,deadline);
 checkPage(first,tenantId,0,clock());
 const incomplete=(reason:string):InventorySnapshot=>({...first,entries:[],complete:false,reasons:[reason]});
 if(first.count>SCHEDULER_PAGE_LIMIT*INVENTORY_PAGE_SIZE)return incomplete('SCHEDULER_PAGE_LIMIT');
 let page=first,after=0,previousDigest='';
 const entries:InventorySnapshot['entries']=[];
 for(;;){
  previousDigest=checkChain(page,after,previousDigest);
  entries.push(...page.entries);
  if(page.nextAfter===null)break;
  after=page.nextAfter;
  try {page=await readSchedulerPage(read,after,deadline);}catch(error){
   if(error instanceof ControlError&&error.status===409)return incomplete('SCHEDULER_INVENTORY_CHANGED');
   return incomplete(error instanceof ControlError&&error.code==='SCHEDULER_PAGE_TIME_LIMIT'?error.code:'SCHEDULER_PAGE_UNAVAILABLE');
  }
  checkPage(page,tenantId,after,clock());
  if(page.watermark!==first.watermark)return incomplete('SCHEDULER_INVENTORY_CHANGED');
 }
 if(previousDigest!==first.headDigest)invalid();
 return {...first,entries,complete:true};
}
/** No caller-supplied list, complete flag, source name, host URL or watermark is accepted. */
export async function collectExecutionCoverage(tenantId:string,periodId:string,source:CoverageSources,clock=()=>Date.now()):Promise<ExecutionCoverageResult> {
 const deadline=Date.now()+10000;
 const period=settlementMonth(periodId),reasons=new Set<string>(),sources:ExecutionCoverageResult['sources']=[],attempts=new Map<string,UsageIdentity>();
 const inspect=(value:InventorySnapshot,kind:'SCHEDULER'|'BODY',taskId:string|null,requiredSince=period.startAt)=>{
  sources.push({kind,taskId,watermark:value.watermark});
  if(!value.complete)reasons.add('INVENTORY_INCOMPLETE');
  if(value.coverageSince>requiredSince)reasons.add('HISTORICAL_COVERAGE_UNAVAILABLE');
  if(value.observedAt<period.endAt)reasons.add('INVENTORY_WATERMARK_BEFORE_PERIOD_END');
  return value;
 };
 const scheduler=inspect(source.schedulerPage?await schedulerHistory(source.schedulerPage,tenantId,clock,deadline):checked(await source.scheduler(),tenantId,'SCHEDULER',clock()),'SCHEDULER',null);
 if(source.schedulerPage&&!scheduler.complete)for(const reason of scheduler.reasons)reasons.add(reason);
 const tasks=new Map<string,InventoryObservation[]>();
 const births=new Map<string,number>();
 const candidates=new Map<string,InventoryObservation[]>();
 // A truncated discovery list cannot be used to decide which Body objects exist.
 if(scheduler.complete){
  for(const {observation:task,baseline,observedAt} of scheduler.entries){
   const history=tasks.get(task.taskId)??[];
   if(!history.length&&!baseline&&['QUEUED','SUBMITTED'].includes(task.state)&&task.fencing===0&&task.sourceAt!==null&&
      task.sourceAt>=period.startAt&&task.sourceAt<=observedAt&&task.sourceAt>=scheduler.coverageSince){
    births.set(task.taskId,task.sourceAt);
   }
   if(baseline||task.sourceAt!==births.get(task.taskId))births.delete(task.taskId);
   history.push(task);tasks.set(task.taskId,history);
   if(task.fencing>0&&(task.sourceAt===null||task.sourceAt<period.endAt)){
    const relevant=candidates.get(task.taskId)??[];relevant.push(task);candidates.set(task.taskId,relevant);
   }
  }
 }
 if(candidates.size>1000)reasons.add('BODY_DISCOVERY_LIMIT');
 else for(const [taskId,relevant] of candidates){
  if(Date.now()>deadline){reasons.add('BODY_DISCOVERY_TIME_LIMIT');break;}
  const history=tasks.get(taskId)!;
  let body:InventorySnapshot;
  try {
   body=checked(await source.body(taskId),tenantId,'BODY',clock());
   const birth=births.get(taskId),first=body.entries[0];
   // A task-local journal cannot predate a task born during the month. Trust only a
   // continuous history starting at its first admission, never an imported baseline.
   const requiredSince=birth!==undefined&&first&&!first.baseline&&first.observation.state==='ADMITTED'&&
    first.observation.version===1&&first.observation.sourceAt!==null&&first.observation.sourceAt>=birth&&
    !body.entries.some(entry=>entry.baseline)?first.observation.sourceAt:period.startAt;
   inspect(body,'BODY',taskId,requiredSince);
  }catch{reasons.add('BODY_INVENTORY_UNAVAILABLE');continue;}
  if(!body.complete)continue;
  if(body.entries.some(entry=>entry.observation.taskId!==taskId)){reasons.add('BODY_TASK_IDENTITY_CONFLICT');continue;}
  const bodyAttempts=new Map<string,typeof body.entries>();
  for(const entry of body.entries){const key=inventoryDigest([entry.observation.attemptId,entry.observation.fencing,entry.observation.nodeId]);const entries=bodyAttempts.get(key)??[];entries.push(entry);bodyAttempts.set(key,entries);}
  for(const task of relevant)if(!body.entries.some(entry=>entry.observation.fencing===task.fencing&&entry.observation.inputDigest===task.inputDigest&&(task.nodeId===null||entry.observation.nodeId===task.nodeId)))reasons.add('SCHEDULER_ATTEMPT_UNPROVEN');
  for(const entries of bodyAttempts.values()){
   const first=entries[0],last=entries.at(-1)!,start=first.observation.sourceAt,end=last.observation.state==='COMPLETED'?last.observation.sourceAt:null;
   if(start!==null&&start>=period.endAt||end!==null&&end<period.startAt)continue;
   if(entries.some(entry=>entry.baseline)||start===null||first.observation.state!=='ADMITTED'){reasons.add('BODY_ATTEMPT_HISTORY_UNPROVEN');continue;}
   if(entries.some((entry,index)=>index>0&&(entry.observation.version!==entries[index-1].observation.version!+1||entry.observation.sourceAt!<entries[index-1].observation.sourceAt!))){reasons.add('BODY_ATTEMPT_SEQUENCE_UNPROVEN');continue;}
   if(!entries.some(entry=>entry.observation.state==='RUNNING')){
    if(entries.some(entry=>!['ADMITTED'].includes(entry.observation.state)))reasons.add('BODY_START_HISTORY_UNPROVEN');
    continue;
   }
   const identity=last.observation;
   if(!history.some(task=>task.fencing===identity.fencing&&task.nodeId===identity.nodeId&&task.inputDigest===identity.inputDigest)){reasons.add('BODY_SCHEDULER_IDENTITY_CONFLICT');continue;}
   try {const value=canonicalUsageIdentity({tenantId,taskId,producerId:identity.nodeId!,attemptId:identity.attemptId!,fencing:identity.fencing,executionInputDigest:identity.inputDigest});const id=usageStreamId(value);if(attempts.has(id))reasons.add('DUPLICATE_EXECUTION_IDENTITY');else attempts.set(id,value);}catch{reasons.add('BODY_USAGE_IDENTITY_INVALID');}
  }
 }
 if(source.schedulerPage){
  if(scheduler.complete){
   try {
    const after=await readSchedulerPage(source.schedulerPage,0,deadline);
    checkPage(after,tenantId,0,clock());
    const digest=checkChain(after);
    if(after.complete&&digest!==after.headDigest)invalid();
    if(after.watermark!==scheduler.watermark)reasons.add('SCHEDULER_INVENTORY_CHANGED');
   } catch {reasons.add('SCHEDULER_RECHECK_UNAVAILABLE');}
  }
 }else{
  const after=checked(await source.scheduler(),tenantId,'SCHEDULER',clock());
  if(!after.complete||after.watermark!==scheduler.watermark)reasons.add('SCHEDULER_INVENTORY_CHANGED');
 }
 const now=clock();if(now<period.endAt)reasons.add('PERIOD_NOT_ENDED');
 if(attempts.size>10000)throw new ControlError('EXECUTION_COVERAGE_LIMIT',503);
 const coverage:SettlementCoverage={schemaVersion:1,purpose:'ansts-settlement-coverage-v1',tenantId,periodId,inventoryWatermark:inventoryDigest(sources),observedAt:Math.min(now,scheduler.observedAt),complete:reasons.size===0,attempts:[...attempts.values()].sort((a,b)=>usageStreamId(a).localeCompare(usageStreamId(b)))};
 return {coverage,reasons:[...reasons].sort(),sources,final:false};
}
