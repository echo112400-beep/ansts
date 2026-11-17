/** Authenticate the meter, pin real Body ownership before execution, then accept immutable quantities. */
import type {CloudflareCelldEnv} from '../cloudflare/env.js';
import {authenticateBody,bodyServiceToken,serviceKeys} from '../cloudflare/body_auth.js';
import {canonicalUsageIdentity,canonicalUsageResources,verifyUsageEvent,type UsageIdentity,type UsageResources} from './usage_event.js';
import {ControlError} from './contracts.js';
export async function handleBodyUsageIngress(request:Request,env:CloudflareCelldEnv):Promise<Response>{
 const reply=(value:unknown,status=200)=>Response.json(value,{status,headers:{'cache-control':'no-store'}}),host=authenticateBody(request,env),bindingRoute=new URL(request.url).pathname==='/internal/v1/body-usage-binding';
 if(!host)return reply({code:'BODY_AUTH_REQUIRED'},401);if(request.method!=='POST')return reply({code:'METHOD_NOT_ALLOWED'},405);
 if(!env.ANSTS_TENANT_DIRECTORY_CELL||!env.ANSTS_TENANT_CONTROL_CELL||!env.ANSTS_DIRECTORY_SERVICE_TOKEN||!env.ANSTS_ADMISSION_SERVICE_TOKEN)return reply({code:'USAGE_UNAVAILABLE'},503);
 try{
  const raw=await request.text();if(raw.length>65536)return reply({code:'PAYLOAD_TOO_LARGE'},413);const body=JSON.parse(raw);if(!body||typeof body!=='object'||Array.isArray(body))return reply({code:'INVALID_USAGE_REQUEST'},422);
  let event,identity:UsageIdentity,resources:UsageResources;
  if(bindingRoute){if(Object.keys(body).some(k=>!['identity','resources'].includes(k)))return reply({code:'INVALID_USAGE_REQUEST'},422);identity=canonicalUsageIdentity(body.identity);resources=canonicalUsageResources(body.resources);}
  else{
   if(Object.keys(body).some(k=>k!=='proof'))return reply({code:'INVALID_USAGE_REQUEST'},422);
   for(const key of serviceKeys(env))try{event=verifyUsageEvent(body.proof,bodyServiceToken(key,host));break;}catch(error){if(!(error instanceof ControlError)||error.code!=='USAGE_AUTH_REQUIRED')throw error;}
   if(!event)return reply({code:'USAGE_AUTH_REQUIRED'},401);identity=canonicalUsageIdentity(event);resources=event.resources;
  }
  if(identity.producerId!==host)return reply({code:'USAGE_BINDING_FORBIDDEN'},403);
  const tenantId=identity.tenantId;if(!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(tenantId))return reply({code:'INVALID_TENANT_ID'},422);
  const actor={sub:'system:usage:'+host,role:'OPERATOR',scopeTenant:tenantId,scopeRegion:'all'},directory=env.ANSTS_TENANT_DIRECTORY_CELL;
  const checked=await directory.get(directory.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/tenant',{method:'POST',headers:{authorization:'Bearer '+env.ANSTS_DIRECTORY_SERVICE_TOKEN,'content-type':'application/json'},body:JSON.stringify({actor,tenantId})}));if(!checked.ok)return checked;await checked.body?.cancel();
  const ns=env.ANSTS_TENANT_CONTROL_CELL,stub=ns.get(ns.idFromName('tenant-control-v1:'+tenantId));
  const control=(action:string,values:unknown)=>stub.fetch(new Request('https://tenant.internal/'+action,{method:'POST',headers:{authorization:'Bearer '+env.ANSTS_ADMISSION_SERVICE_TOKEN,'content-type':'application/json'},body:JSON.stringify({actor,tenantId,...values as object})}));
  const existing=await control('admission-usage-binding',{identity});if(!existing.ok)return existing;const binding=(await existing.json() as {binding:{resources:UsageResources}|null}).binding;
  if(binding&&JSON.stringify(canonicalUsageResources(binding.resources))!==JSON.stringify(resources))return reply({code:'USAGE_STREAM_BINDING_CONFLICT'},409);
  if(!binding){
   if(!env.ANSTS_BODY_CELL)return reply({code:'USAGE_EXECUTION_AUTHORITY_UNAVAILABLE'},503);
   const bodies=env.ANSTS_BODY_CELL,id=bodies.idFromName((env.ANSTS_BODY_NAMESPACE_PREFIX??'')+'execution:'+identity.taskId);
   const current=await bodies.get(id).fetch(new Request('https://body.internal/v1/body/state'));if(!current.ok)return reply({code:'USAGE_EXECUTION_AUTHORITY_UNAVAILABLE'},503);
   const {execution}=await current.json() as {execution?:Record<string,unknown>};
   if(!execution||execution.node_id!==host||execution.task_id!==identity.taskId||execution.tenant_id!==tenantId||execution.attempt_id!==identity.attemptId||execution.fencing_token!==identity.fencing||execution.input_digest!==identity.executionInputDigest||!['RUNNING','UNCERTAIN','RESULT_PENDING','COMPLETED'].includes(String(execution.phase)))return reply({code:'USAGE_EXECUTION_BINDING_REQUIRED'},409);
   const pinned=await control('admission-usage-bind',{identity,resources});if(!pinned.ok)return pinned;await pinned.body?.cancel();
  }
  if(bindingRoute)return reply({bound:true});
  return control('admission-usage-ingest',{proof:body.proof,identity});
 }catch(error){return reply({code:error instanceof ControlError?error.code:error instanceof SyntaxError?'INVALID_JSON':'USAGE_UNAVAILABLE'},error instanceof ControlError?error.status:error instanceof SyntaxError?400:503);}
}
