/** Independent durable shadow receiver. Records messages; never reserves physical capacity or executes. */
import {AdmissionInbox,type AdmissionMessage} from '../control/admission_messages.js';
import {assertControlAccess,ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage,TenantRecord} from '../control/tenant_directory.js';
import type {DurableObjectNamespaceLike,DurableObjectStateLike} from './env.js';
import {equalBodyToken} from './body_auth.js';
interface MailboxEnv {ANSTS_ADMISSION_SERVICE_TOKEN?:string;ANSTS_DIRECTORY_SERVICE_TOKEN?:string;ANSTS_TENANT_DIRECTORY_CELL?:DurableObjectNamespaceLike}
export class AdmissionShadowMailboxCell {
 private readonly inbox:AdmissionInbox|null;
 constructor(ctx:DurableObjectStateLike&{storage:{transactionSync?:DirectoryStorage['transactionSync']}},private readonly env:MailboxEnv={}) {
  this.inbox=typeof ctx.storage.transactionSync==='function'?new AdmissionInbox({sql:ctx.storage.sql,transactionSync:ctx.storage.transactionSync.bind(ctx.storage)}):null;
 }
 async fetch(request:Request):Promise<Response>{
  const respond=(body:unknown,status=200)=>Response.json(body,{status,headers:{'cache-control':'no-store'}}),token=this.env.ANSTS_ADMISSION_SERVICE_TOKEN;
  if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token))return respond({code:'SERVICE_AUTH_REQUIRED'},401);
  if(request.method!=='POST'||new URL(request.url).pathname!=='/record')return respond({code:'NOT_FOUND'},404);
  if(!this.inbox||!this.env.ANSTS_TENANT_DIRECTORY_CELL||!this.env.ANSTS_DIRECTORY_SERVICE_TOKEN)return respond({code:'ADMISSION_MAILBOX_UNAVAILABLE'},503);
  try{
   const raw=await request.text();if(raw.length>65536)return respond({code:'PAYLOAD_TOO_LARGE'},413);
   const body=JSON.parse(raw) as {actor:ControlActor;tenantId:string;message:AdmissionMessage};
   if(!body||typeof body.tenantId!=='string'||!body.actor||typeof body.actor.sub!=='string'||!body.actor.sub.trim()||!['VIEWER','OPERATOR','SRE_ADMIN'].includes(body.actor.role)||!body.message)return respond({code:'INVALID_ADMISSION_MESSAGE'},422);
   const ns=this.env.ANSTS_TENANT_DIRECTORY_CELL;
   const resolved=await ns.get(ns.idFromName('tenant-directory-v1')).fetch(new Request('https://directory.internal/tenant',{method:'POST',headers:{authorization:'Bearer '+this.env.ANSTS_DIRECTORY_SERVICE_TOKEN,'content-type':'application/json'},body:JSON.stringify({actor:body.actor,tenantId:body.tenantId})}));
   if(!resolved.ok)return new Response(resolved.body,{status:resolved.status,headers:{'content-type':'application/json','cache-control':'no-store'}});
   const tenant=await resolved.json() as TenantRecord;if(tenant.id!==body.tenantId)return respond({code:'ADMISSION_RECEIVER_MISMATCH'},403);
   assertControlAccess(body.actor,'request_capacity',tenant.id,tenant.region);assertControlAccess(body.actor,'request_capacity',tenant.id,body.message.region);
   return respond({receipt:this.inbox.receive(body.message,tenant.id,body.message.receiver),protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});
  }catch(error){if(error instanceof ControlError)return respond({code:error.code},error.status);return respond({code:error instanceof SyntaxError?'INVALID_JSON':'ADMISSION_MAILBOX_UNAVAILABLE'},error instanceof SyntaxError?400:503);}
 }
}
