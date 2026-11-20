import {regionTrust,type RegionRecord} from './region_catalog.js';

export interface PlacementPolicy {
  jurisdictions: readonly string[];
  model: string | null;
  dataType: string | null;
}
export interface PlacementHealth {
  healthy: boolean;
  rttMs: number;
  expiresAt: number;
}

export function simulateRegionPlacement(
  regions: readonly RegionRecord[], policy: PlacementPolicy,
  health: ReadonlyMap<string,PlacementHealth>, now: number,
) {
  const candidates=regions.map(region=>{
    const reasons:string[]=[];
    const trust=regionTrust(region,now);
    if(trust!=='CURRENT')reasons.push(trust);
    if(policy.jurisdictions.length&&(!region.jurisdiction||!policy.jurisdictions.includes(region.jurisdiction)))
      reasons.push('JURISDICTION_NOT_ALLOWED');
    if(policy.model&&!region.models.includes(policy.model))reasons.push('MODEL_UNSUPPORTED');
    if(policy.dataType&&!region.dataTypes.includes(policy.dataType))reasons.push('DATA_TYPE_UNSUPPORTED');
    const probe=health.get(region.id);
    let rttMs:number|null=null;
    if(!reasons.length){
      if(!probe||!Number.isFinite(probe.expiresAt)||probe.expiresAt<=now)reasons.push('HEALTH_UNKNOWN');
      else if(!probe.healthy)reasons.push('UNHEALTHY');
      else if(!Number.isFinite(probe.rttMs)||probe.rttMs<0)reasons.push('INVALID_RTT');
      else rttMs=probe.rttMs;
    }
    return {regionId:region.id,reasons,rttMs};
  });
  const ranked=candidates.filter(candidate=>candidate.rttMs!==null)
    .sort((a,b)=>a.rttMs!-b.rttMs!||(a.regionId<b.regionId?-1:a.regionId>b.regionId?1:0));
  return {mode:'SIMULATION' as const,selectedRegion:ranked[0]?.regionId??null,
    ranked:ranked.map(candidate=>candidate.regionId),candidates};
}
