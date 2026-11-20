import type {RegionRecord} from './region_catalog.js';
import type {RegionAdapter} from './region_adapter.js';
import {simulateRegionPlacement,type PlacementPolicy} from './region_placement.js';
import {ControlError} from '../control/contracts.js';

export class RegionPlacementSimulation {
  private readonly accepted=new Map<string,{signature:string;regionId:string}>();

  constructor(private readonly adapters:ReadonlyMap<string,{adapter:RegionAdapter;identity:string}>) {}

  submit(id:string, regions:readonly RegionRecord[], policy:PlacementPolicy&{model:string;dataType:string}, now:number) {
    const signature=JSON.stringify([[...new Set(policy.jurisdictions)].sort(),policy.model,policy.dataType]);
    const previous=this.accepted.get(id);
    if(previous){
      if(previous.signature!==signature)throw new ControlError('IDEMPOTENCY_CONFLICT',409);
      return {mode:'SIMULATION' as const,regionId:previous.regionId,replayed:true};
    }
    const health=new Map<string,{healthy:boolean;rttMs:number;expiresAt:number}>();
    for(const region of regions){
      const entry=this.adapters.get(region.id);
      if(entry?.adapter.mode!=='SIMULATION')continue;
      try{health.set(region.id,{...entry.adapter.probe(entry.identity),expiresAt:now+1});}catch(error){
        if(!(error instanceof ControlError)||error.code!=='REGION_IDENTITY_REJECTED')throw error;
      }
    }
    const decision=simulateRegionPlacement(regions,policy,health,now);
    if(!decision.selectedRegion)return {mode:'SIMULATION' as const,regionId:null,replayed:false,decision};
    const entry=this.adapters.get(decision.selectedRegion)!;
    entry.adapter.submit(entry.identity,{id,model:policy.model,dataType:policy.dataType});
    this.accepted.set(id,{signature,regionId:decision.selectedRegion});
    return {mode:'SIMULATION' as const,regionId:decision.selectedRegion,replayed:false,decision};
  }

  drain(regionId:string) {
    const entry=this.adapters.get(regionId);
    if(entry?.adapter.mode!=='SIMULATION')throw new ControlError('REGION_ADAPTER_NOT_CONFIGURED',404);
    entry.adapter.drain(entry.identity);
  }
}
