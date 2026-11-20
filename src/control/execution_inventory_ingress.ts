/** Dedicated private discovery routes. Cell names and credentials are chosen by the server. */
import type {CloudflareCelldEnv} from '../cloudflare/env.js';
import type {BodyWorkerEnv} from '../cloudflare/body_worker_routes.js';
import {authenticateBodyControl,bodyServiceToken,equalBodyToken} from '../cloudflare/body_auth.js';
import {controlPlaneFetch} from '../cloudflare/control_plane_fetch.js';
import {ControlError,type ControlActor} from './contracts.js';
import {collectExecutionCoverage} from '../finops/execution_coverage.js';
import type {InventoryPage,InventorySnapshot} from './execution_inventory.js';
import type {ExecutionCoverageResult} from '../finops/execution_coverage.js';

export async function bodyInventoryIngress(request:Request,env:BodyWorkerEnv):Promise<Response> {
 if(!authenticateBodyControl(request,env))return Response.json({code:'SERVICE_AUTH_REQUIRED'},{status:401});
 if(request.method!=='GET')return Response.json({code:'METHOD_NOT_ALLOWED'},{status:405});
 const url=new URL(request.url),tenantId=url.searchParams.get('tenantId'),taskId=url.searchParams.get('taskId');
 if(!tenantId||!taskId||tenantId.length>128||taskId.length>512||/[\u0000-\u001f]/.test(tenantId+taskId))return Response.json({code:'INVALID_INVENTORY_SCOPE'},{status:422});
 if(env.ANSTS_BODY_PROXY_URLS)return controlPlaneFetch(env.ANSTS_BODY_PROXY_URLS,url.pathname+url.search,{headers:{authorization:request.headers.get('authorization')??''},signal:AbortSignal.timeout(10000)});
 if(!env.ANSTS_BODY_CELL)return Response.json({code:'EXECUTION_INVENTORY_UNAVAILABLE'},{status:503});
 const ns=env.ANSTS_BODY_CELL;
 return ns.get(ns.idFromName((env.ANSTS_BODY_NAMESPACE_PREFIX??'')+'execution:'+taskId)).fetch(new Request('https://body.internal/v1/body/execution-inventory?tenantId='+encodeURIComponent(tenantId),{headers:{'x-ansts-verified-inventory':'true'}}));
}

export async function executionInventoryIngress(request:Request,env:CloudflareCelldEnv):Promise<Response> {
 const token=env.ANSTS_DIRECTORY_SERVICE_TOKEN;
 if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token))return Response.json({code:'SERVICE_AUTH_REQUIRED'},{status:401});
 if(request.method!=='POST')return Response.json({code:'METHOD_NOT_ALLOWED'},{status:405});
 try {
  const raw=await request.text();if(raw.length>16384)return Response.json({code:'PAYLOAD_TOO_LARGE'},{status:413});const body=JSON.parse(raw);
  if(!body||Object.keys(body).some(k=>!['actor','tenantId','taskId','source','after'].includes(k))||!['SCHEDULER','BODY'].includes(body.source)||typeof body.tenantId!=='string'||!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(body.tenantId))return Response.json({code:'INVALID_INVENTORY_SCOPE'},{status:422});
  if(body.after!==undefined&&(body.source!=='SCHEDULER'||!Number.isSafeInteger(body.after)||body.after<0))return Response.json({code:'INVALID_INVENTORY_SCOPE'},{status:422});
  if(!env.ANSTS_TENANT_DIRECTORY_CELL)return Response.json({code:'DIRECTORY_UNAVAILABLE'},{status:503});
  const directory=env.ANSTS_TENANT_DIRECTORY_CELL;
  const checked=await directory.get(directory.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/tenant',{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify({actor:body.actor,tenantId:body.tenantId})}));
  if(!checked.ok)return checked;await checked.body?.cancel();
  if(body.source==='SCHEDULER'){
   if(body.taskId!==undefined)return Response.json({code:'INVALID_INVENTORY_SCOPE'},{status:422});
   const ns=env.ANSTS_SCHEDULER_CELL;
   const target=new URL('https://scheduler.internal/v1/scheduler/execution-inventory');
   target.searchParams.set('tenantId',body.tenantId);
   if(body.after!==undefined)target.searchParams.set('after',String(body.after));
   return ns.get(ns.idFromName('global-shard-0')).fetch(new Request(target,{headers:{'x-ansts-verified-inventory':'true'}}));
  }
  if(typeof body.taskId!=='string'||!body.taskId||body.taskId.length>512||/[\u0000-\u001f]/.test(body.taskId))return Response.json({code:'INVALID_INVENTORY_SCOPE'},{status:422});
  if(!env.ANSTS_BODY_AUTH_SECRET)return Response.json({code:'EXECUTION_INVENTORY_UNAVAILABLE'},{status:503});
  const path='/internal/v1/execution-inventory/body?tenantId='+encodeURIComponent(body.tenantId)+'&taskId='+encodeURIComponent(body.taskId),headers={authorization:'BodyControl '+bodyServiceToken(env.ANSTS_BODY_AUTH_SECRET,'body:control')};
  if(env.ANSTS_BODY_WORKER_URL)return fetch(new URL(path,env.ANSTS_BODY_WORKER_URL),{headers,redirect:'error',signal:AbortSignal.timeout(10000)});
  return bodyInventoryIngress(new Request('https://body.internal'+path,{headers}),env);
 } catch(error) {return Response.json({code:error instanceof ControlError?error.code:error instanceof SyntaxError?'INVALID_JSON':'EXECUTION_INVENTORY_UNAVAILABLE'},{status:error instanceof ControlError?error.status:error instanceof SyntaxError?400:503});}
}

/** Collect trusted coverage inside the tenant authority so attempt identities stay off the browser request. */
export async function collectTrustedExecutionCoverage(actor:ControlActor,tenantId:string,periodId:string,env:CloudflareCelldEnv):Promise<ExecutionCoverageResult> {
 const token=env.ANSTS_DIRECTORY_SERVICE_TOKEN;
 if(!token||token.length<32)throw new ControlError('DIRECTORY_UNAVAILABLE',503);
 const read=async<Snapshot extends InventorySnapshot>(source:'SCHEDULER'|'BODY',options:{taskId?:string;after?:number}={}):Promise<Snapshot>=>{
  const response=await executionInventoryIngress(new Request('https://fleet.internal/internal/v1/execution-inventory',{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify({actor,tenantId,source,...options})}),env);
  if(!response.ok)throw new ControlError('EXECUTION_INVENTORY_UNAVAILABLE',response.status);
  return response.json() as Promise<Snapshot>;
 };
 return collectExecutionCoverage(tenantId,periodId,{schedulerPage:after=>read<InventoryPage>('SCHEDULER',{after}),body:taskId=>read('BODY',{taskId})});
}

/** Read-only diagnostics; final close and budget release remain separately gated. */
export async function settlementCoverageIngress(request:Request,env:CloudflareCelldEnv):Promise<Response> {
 const token=env.ANSTS_DIRECTORY_SERVICE_TOKEN;
 if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token))return Response.json({code:'SERVICE_AUTH_REQUIRED'},{status:401});
 if(request.method!=='POST')return Response.json({code:'METHOD_NOT_ALLOWED'},{status:405});
 try {
  const raw=await request.text();if(raw.length>16384)return Response.json({code:'PAYLOAD_TOO_LARGE'},{status:413});const body=JSON.parse(raw);
  if(!body||Object.keys(body).some(k=>!['actor','tenantId','periodId'].includes(k))||typeof body.periodId!=='string')return Response.json({code:'INVALID_COVERAGE_REQUEST'},{status:422});
  return Response.json(await collectTrustedExecutionCoverage(body.actor,body.tenantId,body.periodId,env),{headers:{'cache-control':'no-store'}});
 }catch(error){return Response.json({code:error instanceof ControlError?error.code:error instanceof SyntaxError?'INVALID_JSON':'EXECUTION_INVENTORY_UNAVAILABLE'},{status:error instanceof ControlError?error.status:error instanceof SyntaxError?400:503});}
}
