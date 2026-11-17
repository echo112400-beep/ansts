import type {DirectoryApiEnv} from './admin_directory_api.js';
import type {DurableObjectNamespaceLike} from '../cloudflare/env.js';
import {assertCapacityAdmin} from '../cluster/capacity_inventory.js';
import {ControlError,type ControlActor} from './contracts.js';
export interface CapacityApiEnv extends DirectoryApiEnv {ANSTS_CAPACITY_INVENTORY_CELL?:DurableObjectNamespaceLike;}
export async function handleCapacityApi(request:Request,env:CapacityApiEnv,actor:ControlActor&{readOnly?:boolean}):Promise<Response>{
 const reply=(value:unknown,status=200)=>Response.json(value,{status,headers:{'cache-control':'no-store'}});
 try{
  assertCapacityAdmin(actor);const url=new URL(request.url),path=url.pathname;
  const kind=path==='/v1/admin/capacity/pools'?'pool':path==='/v1/admin/capacity/hosts'?'host':null,reading=request.method==='GET';
  if(!kind&&path!=='/v1/admin/capacity/audit')return reply({code:'NOT_FOUND'},404);
  if(!reading&&(request.method!=='POST'&&request.method!=='PUT'||!kind))return reply({code:'METHOD_NOT_ALLOWED'},405);
  if(!reading&&actor.readOnly)return reply({code:'CONTROL_FORBIDDEN'},403);
  const token=env.ANSTS_DIRECTORY_SERVICE_TOKEN;if(!token||token.length<32)return reply({code:'CAPACITY_UNAVAILABLE'},503);
  let input;
  if(!reading){const raw=await request.text();if(raw.length>65536)return reply({code:'PAYLOAD_TOO_LARGE'},413);const value=JSON.parse(raw);if(!value||typeof value!=='object'||Array.isArray(value))return reply({code:'INVALID_JSON'},400);
   input={id:value.id,name:value.name,region:value.region,poolId:value.poolId,daemonIds:value.daemonIds,limit:value.limit,status:value.status,expectedVersion:value.expectedVersion,reason:value.reason,idempotencyKey:request.headers.get('idempotency-key'),requestId:crypto.randomUUID()};}
  const action=reading?(path.endsWith('/audit')?'audit':'snapshot'):kind+'-write';let dispatch:(request:Request)=>Promise<Response>,destination:string;
  if(env.ANSTS_DIRECTORY_URL){const base=new URL(env.ANSTS_DIRECTORY_URL);if(!['http:','https:'].includes(base.protocol)||base.username||base.password||base.search||base.hash||base.pathname!=='/')return reply({code:'INVALID_DIRECTORY_ORIGIN'},503);destination=base.origin+'/internal/v1/capacity/'+action;dispatch=request=>fetch(request,{redirect:'error',signal:AbortSignal.timeout(3000)});}
  else{const ns=env.ANSTS_CAPACITY_INVENTORY_CELL;if(!ns)return reply({code:'CAPACITY_UNAVAILABLE'},503);const stub=ns.get(ns.idFromName('capacity-inventory-v1'));destination='https://capacity.internal/'+action;dispatch=request=>stub.fetch(request);}
  const response=await dispatch(new Request(destination,{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify({actor:{sub:actor.sub,role:actor.role,scopeTenant:actor.scopeTenant,scopeRegion:actor.scopeRegion},input})}));
  return new Response(response.body,{status:response.status,headers:{'content-type':'application/json','cache-control':'no-store',...(response.headers.get('x-ansts-operation-id')?{'x-ansts-operation-id':response.headers.get('x-ansts-operation-id')!}:{})}});
 }catch(error){return reply({code:error instanceof ControlError?error.code:error instanceof SyntaxError?'INVALID_JSON':'CAPACITY_UNAVAILABLE'},error instanceof ControlError?error.status:error instanceof SyntaxError?400:503);}
}
