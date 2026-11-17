import {timingSafeEqual} from 'node:crypto';
/** Read-only monitoring identity, intentionally distinct from user and execution credentials. */
export function monitoringAuthorized(authorization:string|undefined,token:string|undefined):boolean {
 if(!token||!/^[a-f0-9]{64}$/.test(token))return false;
 const actual=Buffer.from(authorization??''),expected=Buffer.from('Bearer '+token);
 return actual.length===expected.length&&timingSafeEqual(actual,expected);
}
export function gaugeMetrics(values:Record<string,number>):string {
 return Object.entries(values).map(([name,value])=>{
  if(!/^[a-zA-Z_:][a-zA-Z0-9_:]*$/.test(name)||!Number.isFinite(value))throw new Error('INVALID_METRIC');
  return `# TYPE ${name} gauge\n${name} ${value}\n`;
 }).join('');
}
const requests=new Map<string,number>();let seconds=0,count=0;
export function recordGatewayRequest(path:string,method:string,status:number,elapsedMs:number):void {
 const route=path==='/metrics'?'metrics':path.startsWith('/v1/admin/')?'management':path.startsWith('/v1/auth/')?'identity':path.startsWith('/v1/tasks')?'tasks':path.startsWith('/v1/console/')?'console':['/health','/healthz','/readyz'].includes(path)?'health':'other';
 const verb=['GET','POST','PUT','DELETE'].includes(method)?method:'OTHER';
 const group=status>=100&&status<600?Math.floor(status/100)+'xx':'other';
 const labels=`route="${route}",method="${verb}",status_class="${group}"`;
 requests.set(labels,(requests.get(labels)??0)+1);count++;seconds+=Math.max(0,elapsedMs)/1000;
}
export function gatewayMetrics():string {
 return '# TYPE ansts_gateway_requests_total counter\n'+Array.from(requests,([labels,value])=>`ansts_gateway_requests_total{${labels}} ${value}\n`).join('')+
 '# TYPE ansts_gateway_request_duration_seconds summary\n'+`ansts_gateway_request_duration_seconds_count ${count}\nansts_gateway_request_duration_seconds_sum ${seconds}\n`;
}
const admissionReasons=['accepted','replayed','invalid_request','not_enabled','draining','duplicate_running','recovery_required','capacity_exhausted','grant_rejected','stale_lease','outcome_uncertain','other_rejection'] as const;
export type AdmissionReason=typeof admissionReasons[number];
export function admissionRejection(status:number,error:unknown):AdmissionReason {
 if(status>=200&&status<300)return 'replayed';
 const reasons:Record<string,AdmissionReason>={INVALID_REQUEST_BODY:'invalid_request',EXECUTION_IDENTITY_REQUIRED:'invalid_request',INVALID_EXECUTION_SPEC:'invalid_request',INVALID_TIMEOUT:'invalid_request',SANDBOX_IMAGE_NOT_ENABLED:'not_enabled',AGENT_MODEL_NOT_CONFIGURED:'not_enabled',BODY_DRAINING:'draining',SANDBOX_ALREADY_RUNNING:'duplicate_running',BODY_RECOVERY_REQUIRED:'recovery_required',BODY_CAPACITY_EXHAUSTED:'capacity_exhausted',BODY_RESOURCE_EXHAUSTED:'capacity_exhausted',EXECUTION_GRANT_REJECTED:'grant_rejected',STALE_EXECUTION_LEASE:'stale_lease',EXECUTION_OUTCOME_UNCERTAIN:'outcome_uncertain',BODY_EXECUTION_UNCERTAIN:'outcome_uncertain'};
 return typeof error==='string'&&Object.hasOwn(reasons,error)?reasons[error]:'other_rejection';
}
export class AdmissionMetrics {
 private readonly counts=new Map<AdmissionReason,number>(admissionReasons.map(reason=>[reason,0]));
 record(reason:AdmissionReason):void {if(!this.counts.has(reason))throw new Error('INVALID_ADMISSION_REASON');this.counts.set(reason,this.counts.get(reason)!+1);}
 render():string {return '# TYPE ansts_body_admission_decisions_total counter\n'+Array.from(this.counts,([reason,value])=>`ansts_body_admission_decisions_total{reason="${reason}"} ${value}\n`).join('');}
}
