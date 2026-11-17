/** Reports carry only Host-scoped credentials. Global directory credentials stay in the Worker. */
import type {CloudflareCelldEnv} from '../cloudflare/env.js';
import {authenticateBody,bodyServiceToken,equalBodyToken,serviceKeys} from '../cloudflare/body_auth.js';
import {verifyCapacityReport} from './capacity_report.js';
import {ControlError} from './contracts.js';
export async function handleCapacityIngress(request:Request,env:CloudflareCelldEnv):Promise<Response>{
 const path=new URL(request.url).pathname,reporting=path==='/internal/v1/capacity/report';
 const reply=(value:unknown,status=200)=>Response.json(value,{status,headers:{'cache-control':'no-store'}});
 const host=reporting?authenticateBody(request,env):null,token=env.ANSTS_DIRECTORY_SERVICE_TOKEN;
 if(reporting?!host:!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token))return reply({code:reporting?'BODY_AUTH_REQUIRED':'SERVICE_AUTH_REQUIRED'},401);
 if(!env.ANSTS_CAPACITY_INVENTORY_CELL||!token||token.length<32)return reply({code:'CAPACITY_UNAVAILABLE'},503);
 if(request.method!=='POST')return reply({code:'METHOD_NOT_ALLOWED'},405);
 try{
  const raw=await request.text();if(raw.length>65536)return reply({code:'PAYLOAD_TOO_LARGE'},413);let body=JSON.parse(raw),action=path.slice('/internal/v1/capacity/'.length);
  if(reporting){
   if(!body||typeof body!=='object'||Array.isArray(body)||Object.keys(body).some(key=>key!=='proof'))return reply({code:'INVALID_CAPACITY_REPORT'},422);
   let report;for(const key of serviceKeys(env))try{report=verifyCapacityReport(body.proof,bodyServiceToken(key,host!));break;}catch(error){if(!(error instanceof ControlError)||error.code!=='CAPACITY_REPORT_AUTH_REQUIRED')throw error;}
   if(!report)return reply({code:'CAPACITY_REPORT_AUTH_REQUIRED'},401);if(report.daemonId!==host)return reply({code:'CAPACITY_REPORT_SCOPE_FORBIDDEN'},403);
   body={daemonId:host,report};
  }else if(!['snapshot','audit','pool-write','host-write'].includes(action))return reply({code:'NOT_FOUND'},404);
  const ns=env.ANSTS_CAPACITY_INVENTORY_CELL;
  return ns.get(ns.idFromName('capacity-inventory-v1')).fetch(new Request('https://capacity.internal/'+action,{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify(body)}));
 }catch(error){return reply({code:error instanceof ControlError?error.code:error instanceof SyntaxError?'INVALID_JSON':'CAPACITY_UNAVAILABLE'},error instanceof ControlError?error.status:error instanceof SyntaxError?400:503);}
}
