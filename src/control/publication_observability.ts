import {ControlError} from './contracts.js';
export interface MonitoringEvidence {collectedAt:number;targets:{id:string;health:string;scrapedAt:number}[]}
/** Evidence must be collected by the server; never accept browser-supplied health as approval. */
export function assertPublicationObservability(evidence:MonitoringEvidence,required:string[],now=Date.now(),maxAgeMs=15000):void {
 const denied=()=>{throw new ControlError('PUBLICATION_MONITORING_REQUIRED',503);};
 if(!required.length||required.length>32||new Set(required).size!==required.length||!Number.isSafeInteger(now)||!Number.isSafeInteger(maxAgeMs)||maxAgeMs<1||maxAgeMs>60000||!Number.isSafeInteger(evidence?.collectedAt)||evidence.collectedAt>now||now-evidence.collectedAt>maxAgeMs||!Array.isArray(evidence.targets))denied();
 for(const id of required) {
  const matches=evidence.targets.filter(target=>target.id===id);
  if(matches.length!==1)denied();
  const target=matches[0];
  if(target.health!=='up'||!Number.isFinite(target.scrapedAt)||target.scrapedAt>now||now-target.scrapedAt>maxAgeMs)denied();
 }
}
/** Prometheus origin and required target IDs come only from deployment configuration. */
export async function collectPublicationObservability(origin:string,required:string[],signal?:AbortSignal):Promise<MonitoringEvidence> {
 try {
  const url=new URL(origin);
  if(!['http:','https:'].includes(url.protocol)||url.username||url.password||url.pathname!=='/'||url.search||url.hash)throw new Error();
  const response=await fetch(url.origin+'/api/v1/targets',{redirect:'error',signal:signal?AbortSignal.any([signal,AbortSignal.timeout(3000)]):AbortSignal.timeout(3000)});
  if(!response.ok)throw new Error();const raw=await response.text();if(raw.length>2*1024*1024)throw new Error();
  const value=JSON.parse(raw);if(value.status!=='success'||!Array.isArray(value.data?.activeTargets))throw new Error();
  const evidence:MonitoringEvidence={collectedAt:Date.now(),targets:value.data.activeTargets.map((target:{labels?:{job?:string;instance?:string};health?:string;lastScrape?:string})=>({id:(target.labels?.job??'')+'/'+(target.labels?.instance??''),health:target.health??'unknown',scrapedAt:Date.parse(target.lastScrape??'')})).filter((target:{id:string})=>required.includes(target.id))};
  assertPublicationObservability(evidence,required);return evidence;
 }catch{throw new ControlError('PUBLICATION_MONITORING_REQUIRED',503);}
}
