/** Host inventory snapshots, not allocation grants or billing utilization. */
import {createHash,createHmac,timingSafeEqual} from 'node:crypto';
import {ControlError} from './contracts.js';
export interface CapacityResources {cpuMilliCores:number;memoryMiB:number;slots:number;}
export interface CapacityReport {
 schemaVersion:1;purpose:'ansts-capacity-report-v1';hostId:string;daemonId:string;epoch:number;sequence:number;observedAt:number;
 total:CapacityResources;reserved:CapacityResources;draining:boolean;recoveryRequired:boolean;
}
export const CAPACITY_REPORT_TTL_MS=15000;
export function capacityId(value:unknown):asserts value is string {if(typeof value!=='string'||!/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}$/.test(value))throw new ControlError('INVALID_CAPACITY_ID',422);}
export function capacityResources(value:CapacityResources,positive=false):CapacityResources {
 if(!value||typeof value!=='object'||(['cpuMilliCores','memoryMiB','slots'] as const).some(key=>!Number.isSafeInteger(value[key])||value[key]<(positive?1:0)||value[key]>1000000000))throw new ControlError('INVALID_CAPACITY_RESOURCES',422);
 return {cpuMilliCores:value.cpuMilliCores,memoryMiB:value.memoryMiB,slots:value.slots};
}
export function canonicalCapacityReport(value:CapacityReport):CapacityReport {
 if(!value||value.schemaVersion!==1||value.purpose!=='ansts-capacity-report-v1')throw new ControlError('INVALID_CAPACITY_REPORT',422);
 capacityId(value.hostId);capacityId(value.daemonId);
 for(const number of [value.epoch,value.sequence])if(!Number.isSafeInteger(number)||number<1)throw new ControlError('INVALID_CAPACITY_REPORT',422);
 if(!Number.isSafeInteger(value.observedAt)||value.observedAt<0||typeof value.draining!=='boolean'||typeof value.recoveryRequired!=='boolean')throw new ControlError('INVALID_CAPACITY_REPORT',422);
 return {schemaVersion:1,purpose:'ansts-capacity-report-v1',hostId:value.hostId,daemonId:value.daemonId,epoch:value.epoch,sequence:value.sequence,observedAt:value.observedAt,total:capacityResources(value.total,true),reserved:capacityResources(value.reserved),draining:value.draining,recoveryRequired:value.recoveryRequired};
}
function validKey(key:string){if(typeof key!=='string'||!/^[a-f0-9]{64}$/.test(key))throw new ControlError('CAPACITY_SIGNING_UNAVAILABLE',503);}
export function capacityReportDigest(report:CapacityReport){return createHash('sha256').update(JSON.stringify(canonicalCapacityReport(report))).digest('hex');}
export function signCapacityReport(report:CapacityReport,key:string):string {
 validKey(key);const payload=Buffer.from(JSON.stringify(canonicalCapacityReport(report))).toString('base64url');return payload+'.'+createHmac('sha256',key).update('ansts-capacity-report-v1.'+payload).digest('base64url');
}
export function verifyCapacityReport(proof:string,key:string):CapacityReport {
 validKey(key);if(typeof proof!=='string'||proof.length>8192||!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$/.test(proof))throw new ControlError('CAPACITY_REPORT_AUTH_REQUIRED',401);
 const [payload,signature]=proof.split('.'),expected=createHmac('sha256',key).update('ansts-capacity-report-v1.'+payload).digest(),actual=Buffer.from(signature,'base64url');
 if(signature!==actual.toString('base64url')||actual.length!==expected.length||!timingSafeEqual(actual,expected))throw new ControlError('CAPACITY_REPORT_AUTH_REQUIRED',401);
 let report;try{report=canonicalCapacityReport(JSON.parse(Buffer.from(payload,'base64url').toString('utf8')));}catch{throw new ControlError('INVALID_CAPACITY_REPORT',422);}
 if(Buffer.from(JSON.stringify(report)).toString('base64url')!==payload)throw new ControlError('INVALID_CAPACITY_REPORT',422);return report;
}
