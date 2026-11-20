import {RegionPlacementSimulation} from '../cluster/region_placement_simulation.js';
import {SimulatedRegionAdapter} from '../cluster/region_adapter.js';
import {RegionProbeRegistry,type RegionProbeConfig} from '../cluster/region_probe.js';
import {simulateRegionPlacement,type PlacementHealth} from '../cluster/region_placement.js';
import {RegionCatalog,regionTrust,type RegionRecord} from '../cluster/region_catalog.js';
import {assertControlAccess,ControlError,type ControlActor} from '../control/contracts.js';
import type {DirectoryStorage} from '../control/tenant_directory.js';
import type {DurableObjectStateLike} from './env.js';
import {equalBodyToken} from './body_auth.js';

export class RegionCatalogCell {
  private trial: {runner:RegionPlacementSimulation;regions:RegionRecord[]}|null=null;
  private readonly catalog: RegionCatalog | null;

  constructor(ctx: DurableObjectStateLike, private readonly env: {ANSTS_DIRECTORY_SERVICE_TOKEN?:string;
    ANSTS_REGION_PROBE_SERVICE?:Extract<RegionProbeConfig,{kind:'service-binding'}>['binding'];
    ANSTS_REGION_PROBE_IDENTITY_REF?:string;
    ANSTS_REGION_PROBE_TOKEN?:string;
    ANSTS_RUNTIME_PROFILE?:string;
    ANSTS_REGION_PLACEMENT_TRIAL?:string;
  }) {
    const storage=ctx.storage as typeof ctx.storage & Partial<DirectoryStorage>;
    this.catalog=typeof storage.transactionSync==='function'?new RegionCatalog(storage as DirectoryStorage):null;
  }

  async fetch(request: Request): Promise<Response> {
    const reply=(data:unknown,status=200)=>Response.json(data,{status,headers:{'cache-control':'no-store'}});
    const token=this.env.ANSTS_DIRECTORY_SERVICE_TOKEN;
    if(!token||token.length<32||!equalBodyToken(request.headers.get('authorization')??'','Bearer '+token))
      return reply({code:'SERVICE_AUTH_REQUIRED'},401);
    if(!this.catalog)return reply({code:'REGION_CATALOG_UNAVAILABLE'},503);
    if(request.method!=='POST')return reply({code:'METHOD_NOT_ALLOWED'},405);
    try{
      const path=new URL(request.url).pathname;
      if(!['/list','/write','/simulate','/probe','/placement-trial'].includes(path))return reply({code:'NOT_FOUND'},404);
      const raw=await request.text();
      if(raw.length>65536)return reply({code:'PAYLOAD_TOO_LARGE'},413);
      const body=JSON.parse(raw) as {actor?:ControlActor;input?:unknown;expectedVersion?:number}|null;
      if(!body?.actor)return reply({code:'INVALID_REGION_RECORD'},422);
      assertControlAccess(body.actor,(path==='/write'||path==='/probe'||path==='/placement-trial')?'administer':'read','all','all');
      const now=Date.now();
      if(path==='/list')return reply({regions:this.catalog.list().map(region=>({...region,trust:regionTrust(region,now)}))});
      if(path==='/probe'){
        const input=body.input as {regionId?:unknown}|null;
        if(!input||typeof input.regionId!=='string'||Object.keys(input).length!==1)
          return reply({code:'INVALID_REGION_PROBE_INPUT'},422);
        const region=this.catalog.list().find(row=>row.id===input.regionId);
        if(!region)return reply({code:'REGION_NOT_FOUND'},404);
        const {ANSTS_REGION_PROBE_SERVICE:binding,ANSTS_REGION_PROBE_IDENTITY_REF:identity,
          ANSTS_REGION_PROBE_TOKEN:credential}=this.env;
        if(!binding||region.adapterRef!=='service-probe'||!identity||region.identityRef!==identity||!credential)
          return reply({code:'REGION_ADAPTER_NOT_CONFIGURED'},503);
        const registry=new RegionProbeRegistry('production',new Map([['service-probe',{kind:'service-binding',binding}]]));
        return reply({regionId:region.id,observedAt:Date.now(),...await registry.probe(region.adapterRef,credential)});
      }
      if(path==='/placement-trial'){
        if(this.env.ANSTS_RUNTIME_PROFILE!=='development'||this.env.ANSTS_REGION_PLACEMENT_TRIAL!=='true')
          return reply({code:'REGION_TRIAL_DISABLED'},409);
        const input=body.input as {action?:unknown;id?:unknown;model?:unknown;dataType?:unknown;jurisdiction?:unknown}|null;
        const id=(value:unknown):value is string=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(value);
        if(!input||!id(input.id)||!['submit','drain'].includes(String(input.action)))
          return reply({code:'INVALID_PLACEMENT_INPUT'},422);
        // Synthetic fixtures are deliberately independent of real catalog evidence.
        if(!this.trial){
          const regions:RegionRecord[]=['trial-a','trial-b'].map(regionId=>({
            id:regionId,jurisdiction:'fixture',failureDomain:regionId,identityRef:regionId,adapterRef:regionId,
            models:['fixture-model'],dataTypes:['fixture-data'],environment:'production',
            evidence:{ref:'synthetic-only',verifiedAt:0,expiresAt:Number.MAX_SAFE_INTEGER},version:1,
          }));
          this.trial={regions,runner:new RegionPlacementSimulation(new Map(regions.map((region,index)=>[
            region.id,{identity:region.id,adapter:new SimulatedRegionAdapter(region.id,region.models,region.dataTypes,index+1)},
          ])))};
        }
        if(input.action==='drain'){
          this.trial.runner.drain(input.id);
          return reply({mode:'SIMULATION',draining:input.id});
        }
        if(!id(input.model)||!id(input.dataType)||!id(input.jurisdiction))return reply({code:'INVALID_PLACEMENT_INPUT'},422);
        return reply(this.trial.runner.submit(input.id,this.trial.regions,{
          model:input.model,dataType:input.dataType,jurisdictions:[input.jurisdiction],
        },now));
      }
      if(path==='/simulate'){
        const input=body.input as {jurisdictions?:unknown;model?:unknown;dataType?:unknown;health?:unknown}|null;
        const id=(value:unknown):value is string=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$/.test(value);
        if(!input||!Array.isArray(input.jurisdictions)||input.jurisdictions.length>64||!input.jurisdictions.every(id)||
          !(input.model===null||id(input.model))||!(input.dataType===null||id(input.dataType))||
          !Array.isArray(input.health)||input.health.length>256)return reply({code:'INVALID_PLACEMENT_INPUT'},422);
        const health=new Map<string,PlacementHealth>();
        for(const row of input.health){
          if(!row||!id(row.regionId)||health.has(row.regionId)||typeof row.healthy!=='boolean'||
            !Number.isFinite(row.rttMs)||row.rttMs<0||!Number.isSafeInteger(row.expiresAt))
            return reply({code:'INVALID_PLACEMENT_INPUT'},422);
          health.set(row.regionId,{healthy:row.healthy,rttMs:row.rttMs,expiresAt:row.expiresAt});
        }
        const {jurisdictions,model,dataType}=input;
        return reply({...simulateRegionPlacement(this.catalog.list(),{jurisdictions,model,dataType},health,now),
          healthSource:'HYPOTHETICAL'});
      }
      if(!body.input||body.expectedVersion===undefined)return reply({code:'INVALID_REGION_RECORD'},422);
      const region=this.catalog.write(body.input as Omit<RegionRecord,'version'>,body.expectedVersion,now);
      return reply({region:{...region,trust:regionTrust(region,now)}});
    }catch(error){
      if(error instanceof ControlError)return reply({code:error.code},error.status);
      if(error instanceof SyntaxError)return reply({code:'INVALID_JSON'},400);
      return reply({code:'REGION_CATALOG_UNAVAILABLE'},503);
    }
  }
}
