import type {DurableObjectNamespaceLike} from '../cloudflare/env.js';
import type {DirectoryApiEnv} from './admin_directory_api.js';
import {assertControlAccess,ControlError,type ControlActor} from './contracts.js';

export interface RegionApiEnv extends DirectoryApiEnv {
  ANSTS_REGION_CATALOG_CELL?: DurableObjectNamespaceLike;
}
export const regionAdminPath='/v1/admin/regions';
export const regionSimulationPath=regionAdminPath+'/simulate';
export const regionProbePath=regionAdminPath+'/probe';
export const regionPlacementTrialPath=regionAdminPath+'/placement-trial';
export const regionInternalPath='/internal/v1/regions';

export async function handleRegionCatalog(request:Request,env:RegionApiEnv,actor?:ControlActor&{readOnly?:boolean}):Promise<Response>{
  const reply=(data:unknown,status:number)=>Response.json(data,{status,headers:{'cache-control':'no-store'}});
  const token=env.ANSTS_DIRECTORY_SERVICE_TOKEN;
  if(!token||token.length<32)return reply({code:'REGION_CATALOG_UNAVAILABLE'},503);
  try{
    let action:string, forwarded:Request;
    if(actor){
      const simulate=new URL(request.url).pathname===regionSimulationPath;
      const probe=new URL(request.url).pathname===regionProbePath;
      const trial=new URL(request.url).pathname===regionPlacementTrialPath;
      if((simulate||probe||trial)&&request.method!=='POST')return reply({code:'METHOD_NOT_ALLOWED'},405);
      const read=request.method==='GET'||simulate;
      if(!read&&!probe&&!trial&&request.method!=='PUT')return reply({code:'METHOD_NOT_ALLOWED'},405);
      if(!read&&actor.readOnly)return reply({code:'CONTROL_FORBIDDEN'},403);
      assertControlAccess(actor,read?'read':'administer','all','all');
      action=trial?'placement-trial':probe?'probe':simulate?'simulate':read?'list':'write';
      const raw=read&&!simulate?'{}':await request.text();
      if(raw.length>65536)return reply({code:'PAYLOAD_TOO_LARGE'},413);
      const value=JSON.parse(raw);
      if(!value||typeof value!=='object'||Array.isArray(value))return reply({code:'INVALID_JSON'},400);
      const credentials={sub:actor.sub,role:actor.role,scopeTenant:actor.scopeTenant,scopeRegion:actor.scopeRegion};
      let destination='https://region.internal/'+action;
      if(env.ANSTS_DIRECTORY_URL){
        const base=new URL(env.ANSTS_DIRECTORY_URL);
        if(!['http:','https:'].includes(base.protocol)||base.username||base.password||base.search||base.hash||base.pathname!=='/')
          return reply({code:'INVALID_DIRECTORY_ORIGIN'},503);
        destination=base.origin+regionInternalPath+'/'+action;
      }
      forwarded=new Request(destination,{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},
        body:JSON.stringify({actor:credentials,input:value.input,expectedVersion:value.expectedVersion}),signal:AbortSignal.timeout(3000)});
      if(env.ANSTS_DIRECTORY_URL)return await fetch(forwarded,{redirect:'error'});
    }else{
      action=new URL(request.url).pathname.slice(regionInternalPath.length+1);
      if(!['list','write','simulate','probe','placement-trial'].includes(action))return reply({code:'NOT_FOUND'},404);
      forwarded=new Request('https://region.internal/'+action,request);
    }
    const ns=env.ANSTS_REGION_CATALOG_CELL;
    if(!ns)return reply({code:'REGION_CATALOG_UNAVAILABLE'},503);
    return await ns.get(ns.idFromName('region-catalog-v1')).fetch(forwarded);
  }catch(error){
    return error instanceof ControlError?reply({code:error.code},error.status):
      error instanceof SyntaxError?reply({code:'INVALID_JSON'},400):reply({code:'REGION_CATALOG_UNAVAILABLE'},503);
  }
}
