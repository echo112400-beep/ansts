/** Host-scoped observation ingress; it cannot prepare, authorize or release arbitrary reservations. */
import {authenticateBody,bodyServiceToken,serviceKeys} from '../cloudflare/body_auth.js';
import type {CloudflareCelldEnv} from '../cloudflare/env.js';
import {bodyEvidenceDigest,verifyBodyEvidence,canonicalBodyObservationLookup} from './body_execution_evidence.js';
import {ControlError} from './contracts.js';
export async function receiveBodyObservation(request:Request,env:CloudflareCelldEnv):Promise<Response> {
 const reply=(value:unknown,status=200)=>Response.json(value,{status,headers:{'cache-control':'no-store'}});
 const host=authenticateBody(request,env);
 if(!host)return reply({code:'BODY_AUTH_REQUIRED'},401);
 if(request.method!=='POST')return reply({code:'METHOD_NOT_ALLOWED'},405);
 if(!env.ANSTS_TENANT_CONTROL_CELL||!env.ANSTS_TENANT_DIRECTORY_CELL||!env.ANSTS_ADMISSION_SERVICE_TOKEN||!env.ANSTS_DIRECTORY_SERVICE_TOKEN)return reply({code:'TENANT_CONTROL_UNAVAILABLE'},503);
 try {
  const raw=await request.text();if(raw.length>16384)return reply({code:'PAYLOAD_TOO_LARGE'},413);
  let body;try{body=JSON.parse(raw);}catch{return reply({code:'INVALID_JSON'},400);}
  if(!body||typeof body!=='object'||Array.isArray(body)||Object.keys(body).some(key=>key!=='proof'))return reply({code:'INVALID_EVIDENCE_REQUEST'},400);
  let fact;
  for(const key of serviceKeys(env))try{fact=verifyBodyEvidence(body.proof,bodyServiceToken(key,host));break;}catch(error){if(!(error instanceof ControlError)||error.code!=='BODY_EVIDENCE_AUTH_REQUIRED')throw error;}
  if(!fact)return reply({code:'BODY_EVIDENCE_AUTH_REQUIRED'},401);
  if(fact.hostId!==host)return reply({code:'BODY_SCOPE_FORBIDDEN'},403);
  if(!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(fact.tenantId))return reply({code:'INVALID_TENANT_ID'},422);
  const actor={sub:'system:body:'+host,role:'OPERATOR',scopeTenant:fact.tenantId,scopeRegion:'all'};
  const directories=env.ANSTS_TENANT_DIRECTORY_CELL;
  const checked=await directories.get(directories.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/tenant',{method:'POST',headers:{authorization:'Bearer '+env.ANSTS_DIRECTORY_SERVICE_TOKEN,'content-type':'application/json'},body:JSON.stringify({actor,tenantId:fact.tenantId})}));
  if(!checked.ok)return checked;await checked.body?.cancel();
  const ns=env.ANSTS_TENANT_CONTROL_CELL;
  const response=await ns.get(ns.idFromName('tenant-control-v1:'+fact.tenantId)).fetch(new Request('https://tenant.internal/admission-observe-execution',{method:'POST',headers:{authorization:'Bearer '+env.ANSTS_ADMISSION_SERVICE_TOKEN,'content-type':'application/json'},body:JSON.stringify({actor,tenantId:fact.tenantId,reservationId:fact.reservationId,proof:body.proof})}));
  if(!response.ok)return response;
  const result=await response.json() as {sourceVerified?:boolean};
  if(result.sourceVerified!==true)return reply({code:'INVALID_EVIDENCE_RECEIPT'},503);
  return reply({sourceVerified:true,digest:bodyEvidenceDigest(fact)});
 }catch(error){return reply({code:error instanceof ControlError?error.code:'BODY_EVIDENCE_DELIVERY_UNAVAILABLE'},error instanceof ControlError?error.status:503);}
}

/** Discover only an already authorized shadow observation binding for this Host/attempt. */
export async function lookupBodyObservation(request:Request,env:CloudflareCelldEnv):Promise<Response> {
 const reply=(value:unknown,status=200)=>Response.json(value,{status,headers:{'cache-control':'no-store'}}),host=authenticateBody(request,env);
 if(!host)return reply({code:'BODY_AUTH_REQUIRED'},401);
 if(request.method!=='POST')return reply({code:'METHOD_NOT_ALLOWED'},405);
 if(!env.ANSTS_TENANT_DIRECTORY_CELL||!env.ANSTS_TENANT_CONTROL_CELL||!env.ANSTS_DIRECTORY_SERVICE_TOKEN||!env.ANSTS_ADMISSION_SERVICE_TOKEN)return reply({code:'TENANT_CONTROL_UNAVAILABLE'},503);
 try {
  const raw=await request.text();if(raw.length>16384)return reply({code:'PAYLOAD_TOO_LARGE'},413);
  let body;try{body=JSON.parse(raw);}catch{return reply({code:'INVALID_JSON'},400);}
  if(!body||typeof body!=='object'||Array.isArray(body)||Object.keys(body).some(key=>!['tenantId','taskId','attemptId','fencing','executionInputDigest','resources'].includes(key)))return reply({code:'INVALID_EVIDENCE_REQUEST'},400);
  const input=canonicalBodyObservationLookup(body),actor={sub:'system:body:'+host,role:'OPERATOR',scopeTenant:input.tenantId,scopeRegion:'all'};
  const directories=env.ANSTS_TENANT_DIRECTORY_CELL;
  const checked=await directories.get(directories.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/tenant',{method:'POST',headers:{authorization:'Bearer '+env.ANSTS_DIRECTORY_SERVICE_TOKEN,'content-type':'application/json'},body:JSON.stringify({actor,tenantId:input.tenantId})}));
  if(!checked.ok)return checked;await checked.body?.cancel();
  const ns=env.ANSTS_TENANT_CONTROL_CELL;
  return ns.get(ns.idFromName('tenant-control-v1:'+input.tenantId)).fetch(new Request('https://tenant.internal/admission-observation-binding',{method:'POST',headers:{authorization:'Bearer '+env.ANSTS_ADMISSION_SERVICE_TOKEN,'content-type':'application/json'},body:JSON.stringify({actor,tenantId:input.tenantId,target:host,input})}));
 }catch(error){return reply({code:error instanceof ControlError?error.code:'BODY_BINDING_UNAVAILABLE'},error instanceof ControlError?error.status:503);}
}
