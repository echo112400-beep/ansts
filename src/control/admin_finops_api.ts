import type {DirectoryApiEnv} from './admin_directory_api.js';
import type {ControlActor} from './contracts.js';
import {ControlError} from './contracts.js';
import {aggregateTenantCosts} from '../finops/usage_cost_ledger.js';
const costExportPath=/^\/v1\/admin\/tenants\/([^/]+)\/cost-exports(?:\/([a-f0-9]{64})(?:\/(run|download))?)?$/;
const settlementPath=/^\/v1\/admin\/tenants\/([^/]+)\/settlements\/([^/]+)\/(preview|close|adjustments)$/;
export function isFinopsPath(path:string){return costExportPath.test(path)||path.startsWith('/v1/admin/finops/')||/^\/v1\/admin\/tenants\/[^/]+\/(rate-card|costs|finops-archives)(\/[^/]+)?$/.test(path)||settlementPath.test(path);}
/** Verified Gateway identity only. User-supplied snapshots and actor fields are discarded. */
export async function handleFinopsApi(request:Request,env:DirectoryApiEnv,actor:ControlActor&{readOnly?:boolean},background?:(work:Promise<void>)=>void):Promise<Response>{
 const url=new URL(request.url),reply=(body:unknown,status=200)=>Response.json(body,{status,headers:{'cache-control':'no-store'}});
 try{
  const global=url.pathname.startsWith('/v1/admin/finops/'),rates=url.pathname==='/v1/admin/finops/rate-cards',audit=url.pathname==='/v1/admin/finops/rate-audit',aggregate=url.pathname==='/v1/admin/finops/summary';
  const exportMatch=costExportPath.exec(url.pathname);
  const archiveMatch=/^\/v1\/admin\/finops\/archives(?:\/([a-f0-9]{64}))?$/.exec(url.pathname);
  const match=/^\/v1\/admin\/tenants\/([^/]+)\/(rate-card|costs|finops-archives)(?:\/([a-f0-9]{64}))?$/.exec(url.pathname),activation=match?.[2]==='rate-card',costs=match?.[2]==='costs';
  const periodMatch=settlementPath.exec(url.pathname),manualAdjustment=periodMatch?.[3]==='adjustments',periodClose=periodMatch?.[3]==='close';
  const archive=!!archiveMatch||match?.[2]==='finops-archives',archiveId=archiveMatch?.[1]??match?.[3];
  if(!(rates||audit||aggregate||match||archive||periodMatch||exportMatch))return reply({code:'NOT_FOUND'},404);
  if((periodClose||manualAdjustment)&&request.method!=='POST')return reply({code:'NOT_FOUND'},404);
  const write=!!exportMatch&&request.method==='POST'||periodClose||manualAdjustment||rates&&request.method==='POST'||activation&&request.method==='PUT'||archive&&!archiveId&&request.method==='POST';
  if(!write&&request.method!=='GET')return reply({code:'NOT_FOUND'},404);
  if(archive&&actor.role!=='SRE_ADMIN')return reply({code:'CONTROL_FORBIDDEN'},403);
  if((rates||audit||archive&&global)&&(actor.role!=='SRE_ADMIN'||actor.scopeTenant!=='all'||actor.scopeRegion!=='all')||write&&!exportMatch&&(actor.role!=='SRE_ADMIN'||actor.readOnly))return reply({code:'CONTROL_FORBIDDEN'},403);
  const tenantId=exportMatch?decodeURIComponent(exportMatch[1]):match?decodeURIComponent(match[1]):periodMatch?decodeURIComponent(periodMatch[1]):undefined;
  if(tenantId&&!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(tenantId))return reply({code:'INVALID_TENANT_ID'},422);
  if(tenantId&&actor.scopeTenant!=='all'&&actor.scopeTenant!==tenantId)return reply({code:'TENANT_NOT_FOUND'},404);
  if(periodMatch&&actor.scopeRegion!=='all')return reply({code:'COMPLETE_TENANT_SCOPE_REQUIRED'},403);
  const token=env.ANSTS_DIRECTORY_SERVICE_TOKEN;if(!token||token.length<32)throw new ControlError('DIRECTORY_UNAVAILABLE',503);
  let origin:string|undefined;
  if(env.ANSTS_DIRECTORY_URL){const base=new URL(env.ANSTS_DIRECTORY_URL);if(!['http:','https:'].includes(base.protocol)||base.username||base.password||base.search||base.hash||base.pathname!=='/')throw new ControlError('INVALID_DIRECTORY_ORIGIN',503);origin=base.origin;}
  async function dispatch(action:string,tenant:string|undefined,values:object,timeoutMs=3000){
   const ns=tenant?env.ANSTS_TENANT_CONTROL_CELL:env.ANSTS_TENANT_DIRECTORY_CELL;
   if(!ns&&!origin)throw new ControlError('DIRECTORY_UNAVAILABLE',503);
   const target=origin?origin+(tenant?'/internal/v1/tenant-control/':'/internal/v1/directory/')+action:'https://finops.internal/'+action;
   const forwarded=new Request(target,{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify({actor:{sub:actor.sub,role:actor.role,scopeTenant:actor.scopeTenant,scopeRegion:actor.scopeRegion},tenantId:tenant,...values})});
   let timer:ReturnType<typeof setTimeout>|undefined;const controller=new AbortController();
   try{return await Promise.race([origin?fetch(forwarded,{redirect:'error',signal:controller.signal}):ns!.get(ns!.idFromName(tenant?'tenant-control-v1:'+tenant:'tenant-directory-v1')).fetch(forwarded),new Promise<never>((_,reject)=>{timer=setTimeout(()=>{controller.abort();reject(new ControlError('FINOPS_UNAVAILABLE',503));},timeoutMs);})]);}finally{if(timer)clearTimeout(timer);}
  }
  if(exportMatch){
   const [, ,exportId,operation]=exportMatch;
   const expected=!exportId||operation==='run'?'POST':'GET';
   if(request.method!==expected)return reply({code:'METHOD_NOT_ALLOWED'},405);
   const action=!exportId?'create':operation??'status';
   const response=await dispatch('cost-export-'+action,tenantId,{
    exportId,idempotencyKey:request.headers.get('idempotency-key'),
   },action==='run'?15000:3000);
   if(action==='create'&&response.ok&&background){
    const job=await response.clone().json() as {id?:string;status?:string};
    if(job.status==='PENDING'&&job.id&&/^[a-f0-9]{64}$/.test(job.id)){
     background(dispatch('cost-export-run',tenantId,{exportId:job.id},15000)
      .then(async generated=>{await generated.body?.cancel();}).catch(()=>{}));
    }
   }
   const headers=new Headers({'cache-control':'no-store','x-content-type-options':'nosniff'});
   if(action==='download'&&response.ok){
    headers.set('content-type','text/csv; charset=utf-8');
    headers.set('content-disposition','attachment; filename="cost-export.csv"');
   }else headers.set('content-type','application/json');
   return new Response(response.body,{status:response.status,headers});
  }
  const page={limit:url.searchParams.has('limit')?Number(url.searchParams.get('limit')):undefined,cursor:url.searchParams.get('cursor')??undefined};
  if(aggregate){
   const tenants:string[]=[];let cursor:string|undefined,watermark:number|undefined;
   do{
    const response=await dispatch('list',undefined,{page:{limit:100,cursor}});if(!response.ok)throw new ControlError('DIRECTORY_UNAVAILABLE',503);
    const data=await response.json() as {items:{id:string}[];nextCursor:string|null;version:number};
    if(!Array.isArray(data.items)||data.items.some(t=>!t||typeof t.id!=='string'||!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(t.id))||!Number.isSafeInteger(data.version)||watermark!==undefined&&watermark!==data.version||data.nextCursor!==null&&typeof data.nextCursor!=='string')throw new ControlError('DIRECTORY_UNAVAILABLE',503);
    watermark=data.version;tenants.push(...data.items.map(t=>t.id));if(tenants.length>1000||new Set(tenants).size!==tenants.length||data.nextCursor===cursor)throw new ControlError('AGGREGATE_SCOPE_LIMIT',503);cursor=data.nextCursor??undefined;
   }while(cursor);
   const summaries=new Map<string,{knownSubtotalCreditUnits:string;totalCreditUnits:string|null;complete:boolean}>();
   const groups=new Map<string,{costCenter:string;costCenterVersion:number;rateCardVersion:number;activationVersion:number;known:bigint}>();
   const sources=new Map<string,{tenantId:string;watermark:number;versions:{costCenter:string;costCenterVersion:number;rateCardVersion:number;activationVersion:number}[]}>();
   const deadline=Date.now()+8000;
   for(let offset=0;offset<tenants.length&&Date.now()<deadline;offset+=4)await Promise.all(tenants.slice(offset,offset+4).map(async id=>{
    try{const response=await dispatch('cost-summary',id,{});if(!response.ok)return;const data=await response.json();
     if(data.tenantId!==id||data.unit!=='credit-subunit'||data.unitVersion!==1||typeof data.complete!=='boolean'||!/^\d{1,39}$/.test(data.knownSubtotalCreditUnits)||data.complete&&data.totalCreditUnits!==data.knownSubtotalCreditUnits||!data.complete&&data.totalCreditUnits!==null)return;
     if(!Number.isSafeInteger(data.watermark)||data.watermark<0||!Array.isArray(data.buckets))return;
     const versions=new Map<string,{costCenter:string;costCenterVersion:number;rateCardVersion:number;activationVersion:number}>();
     const contributions=new Map<string,bigint>();let bucketTotal=0n;
     for(const bucket of data.buckets){
      const {costCenter,costCenterVersion,rateCardVersion,activationVersion}=bucket??{};
      if(typeof costCenter!=='string'||![costCenterVersion,rateCardVersion,activationVersion].every(value=>Number.isSafeInteger(value)&&value>0))return;
      const version={costCenter,costCenterVersion,rateCardVersion,activationVersion};
      if(typeof bucket.creditUnits!=='string'||!/^\d{1,39}$/.test(bucket.creditUnits))return;
      const key=JSON.stringify(version),amount=BigInt(bucket.creditUnits);
      bucketTotal+=amount;
      contributions.set(key,(contributions.get(key)??0n)+amount);
      versions.set(key,version);
     }
     if(bucketTotal!==BigInt(data.knownSubtotalCreditUnits))return;
     for(const [key,known] of contributions){
      const existing=groups.get(key);
      groups.set(key,{...versions.get(key)!,known:(existing?.known??0n)+known});
     }
     sources.set(id,{tenantId:id,watermark:data.watermark,versions:[...versions.values()]});
     summaries.set(id,data);
    }catch{/* Missing tenant response is explicit in the aggregate. */}
   }));
   return reply({...aggregateTenantCosts(tenants,summaries),unit:'credit-subunit',unitVersion:1,directoryWatermark:watermark,scope:{tenant:actor.scopeTenant,region:actor.scopeRegion},tenantCount:tenants.length,
    costCenters:[...groups].sort(([a],[b])=>a.localeCompare(b)).map(([,group])=>{
     const {known,...version}=group;return {...version,knownSubtotalCreditUnits:known.toString()};
    }),sources:tenants.flatMap(id=>sources.has(id)?[sources.get(id)]:[])});
  }
  let input:unknown;
  if(write){const raw=await request.text();if(raw.length>65536)return reply({code:'PAYLOAD_TOO_LARGE'},413);const value=JSON.parse(raw);if(!value||typeof value!=='object'||Array.isArray(value))return reply({code:'INVALID_JSON'},400);
   const common={expectedVersion:value.expectedVersion,reason:value.reason,idempotencyKey:request.headers.get('idempotency-key'),requestId:crypto.randomUUID()};
   if(periodClose&&Object.keys(value).some(key=>!['reason','expectedRevision'].includes(key)))return reply({code:'INVALID_PERIOD_CLOSE'},422);
   if(manualAdjustment&&Object.keys(value).some(key=>!['reason','expectedCloseDigest','creditUnits'].includes(key)))return reply({code:'INVALID_MANUAL_ADJUSTMENT'},422);
   input=manualAdjustment?{reason:common.reason,idempotencyKey:common.idempotencyKey,requestId:common.requestId,expectedCloseDigest:value.expectedCloseDigest,creditUnits:value.creditUnits}:archive||periodClose?{reason:common.reason,idempotencyKey:common.idempotencyKey,requestId:common.requestId,expectedRevision:value.expectedRevision}:rates?{...common,definition:value.definition}:{...common,rateCardVersion:value.rateCardVersion};
  }
  const action=periodMatch?(manualAdjustment?'period-adjustment':periodClose?'period-close':'period-preview'):archive?(write?'finops-archive-create':archiveId?'finops-archive-read':'finops-archive-status'):rates?(write?'rate-create':'rate-list'):audit?'rate-audit':activation?(write?'rate-activate':'rate-read'):costs?'cost-read':'';
  const response=await dispatch(action,global?undefined:tenantId,{input,page,archiveId,periodId:periodMatch?.[2]},periodMatch?15000:3000);return new Response(response.body,{status:response.status,headers:{'content-type':'application/json','cache-control':'no-store'}});
 }catch(error){return reply({code:error instanceof ControlError?error.code:error instanceof SyntaxError?'INVALID_JSON':'FINOPS_UNAVAILABLE'},error instanceof ControlError?error.status:error instanceof SyntaxError?400:503);}
}
