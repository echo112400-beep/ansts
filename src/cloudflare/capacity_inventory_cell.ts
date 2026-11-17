import type {DurableObjectStateLike} from './env.js';
import {equalBodyToken} from './body_auth.js';
import {CapacityInventory,type HostWrite,type PoolWrite} from '../cluster/capacity_inventory.js';
import {ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';
import type {CapacityReport} from '../control/capacity_report.js';
export class CapacityInventoryCell {
 private readonly inventory:CapacityInventory|null;
 constructor(ctx:DurableObjectStateLike&{storage:{transactionSync?:DirectoryStorage['transactionSync']}},private readonly env:{ANSTS_DIRECTORY_SERVICE_TOKEN?:string}={}){this.inventory=typeof ctx.storage.transactionSync==='function'?new CapacityInventory({sql:ctx.storage.sql,transactionSync:ctx.storage.transactionSync.bind(ctx.storage)}):null;}
 async fetch(request:Request):Promise<Response>{
  const reply=(value:unknown,status=200)=>Response.json(value,{status,headers:{'cache-control':'no-store'}}),token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
  if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token))return reply({code:'SERVICE_AUTH_REQUIRED'},401);
  if(!this.inventory)return reply({code:'DURABLE_TRANSACTION_REQUIRED'},503);
  if(request.method!=='POST')return reply({code:'METHOD_NOT_ALLOWED'},405);
  try{
   const raw=await request.text();if(raw.length>65536)return reply({code:'PAYLOAD_TOO_LARGE'},413);
   const body=JSON.parse(raw) as {actor:ControlActor;input:HostWrite&PoolWrite;report:CapacityReport;daemonId:string};
   const path=new URL(request.url).pathname;
   if(path==='/report'){const result=this.inventory.report(body.daemonId,body.report,Date.now());return reply(result,result.outcome==='CONFLICT'?409:200);}
   if(path==='/snapshot')return reply(this.inventory.snapshot(body.actor,Date.now()));
   if(path==='/audit')return reply({audit:this.inventory.audit(body.actor)});
   if(!body.input||typeof body.input!=='object')return reply({code:'INVALID_CAPACITY_INPUT'},422);
   if(path==='/pool-write'){const result=this.inventory.writePool(body.actor,body.input,Date.now());const response=reply(result.record);response.headers.set('x-ansts-operation-id',result.operationId);return response;}
   if(path==='/host-write'){const result=this.inventory.writeHost(body.actor,body.input,Date.now());const response=reply(result.record);response.headers.set('x-ansts-operation-id',result.operationId);return response;}
   return reply({code:'NOT_FOUND'},404);
  }catch(error){return reply({code:error instanceof ControlError?error.code:error instanceof SyntaxError?'INVALID_JSON':'CAPACITY_UNAVAILABLE'},error instanceof ControlError?error.status:error instanceof SyntaxError?400:503);}
 }
}
