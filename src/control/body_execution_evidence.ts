/** Authenticated Body facts. These observations do not authorize quota-enforced execution or report cost. */
import {createHash,createHmac,timingSafeEqual} from 'node:crypto';
import {ControlError,validateQuota,type ResourceQuota} from './contracts.js';
export interface BodyObservationBinding {
 reservationId:string;generation:number;policyVersion:number;tenantVersion:number;requestDigest:string;executionInputDigest:string;
}
export interface BodyObservationLookup {
 tenantId:string;taskId:string;attemptId:string;fencing:number;executionInputDigest:string;resources:ResourceQuota;
}
export interface BodyExecutionEvidence extends BodyObservationBinding {
 schemaVersion:1;purpose:'ansts-body-execution-observation-v1';hostId:string;tenantId:string;taskId:string;attemptId:string;fencing:number;
 resources:ResourceQuota;executionObserved:boolean;sequence:number;phase:'UNCERTAIN'|'RUNNING'|'NOT_STARTED'|'TERMINATED';observedAt:number;
}
function id(value:unknown,maxLength=128):asserts value is string{if(typeof value!=='string'||!value.trim()||value.length>maxLength)throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);}
export function canonicalBodyObservationLookup(value:BodyObservationLookup):BodyObservationLookup {
 if(!value||typeof value!=='object')throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);
 id(value.tenantId);id(value.taskId,512);id(value.attemptId,512);
 if(!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]*$/.test(value.tenantId)||!Number.isSafeInteger(value.fencing)||value.fencing<1||typeof value.executionInputDigest!=='string'||!/^[a-f0-9]{64}$/.test(value.executionInputDigest))throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);
 validateQuota(value.resources);
 if(value.resources.concurrentAttempts!==1||value.resources.queuedTasks!==0)throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);
 return {tenantId:value.tenantId,taskId:value.taskId,attemptId:value.attemptId,fencing:value.fencing,executionInputDigest:value.executionInputDigest,resources:{concurrentAttempts:1,cpuMilliCores:value.resources.cpuMilliCores,memoryMiB:value.resources.memoryMiB,queuedTasks:0}};
}
export function canonicalBodyEvidence(value:BodyExecutionEvidence):BodyExecutionEvidence {
 if(!value||value.schemaVersion!==1||value.purpose!=='ansts-body-execution-observation-v1'||!['UNCERTAIN','RUNNING','NOT_STARTED','TERMINATED'].includes(value.phase))throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);
 for(const field of [value.hostId,value.tenantId,value.reservationId])id(field);
 id(value.taskId,512);id(value.attemptId,512);
 if(!/^[a-zA-Z0-9_-]{1,128}$/.test(value.hostId)||![value.requestDigest,value.executionInputDigest].every(field=>typeof field==='string'&&/^[a-f0-9]{64}$/.test(field)))throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);
 for(const field of [value.fencing,value.generation,value.policyVersion,value.tenantVersion,value.sequence])if(!Number.isSafeInteger(field)||field<1)throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);
 if(!Number.isSafeInteger(value.observedAt)||value.observedAt<0)throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);
 if(typeof value.executionObserved!=='boolean'||value.phase==='RUNNING'&&!value.executionObserved||value.phase==='NOT_STARTED'&&value.executionObserved)throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);
 validateQuota(value.resources);if(value.resources.concurrentAttempts!==1||value.resources.queuedTasks!==0)throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);
 return {schemaVersion:1,purpose:'ansts-body-execution-observation-v1',hostId:value.hostId,tenantId:value.tenantId,taskId:value.taskId,attemptId:value.attemptId,fencing:value.fencing,reservationId:value.reservationId,generation:value.generation,policyVersion:value.policyVersion,tenantVersion:value.tenantVersion,requestDigest:value.requestDigest,executionInputDigest:value.executionInputDigest,resources:{concurrentAttempts:1,cpuMilliCores:value.resources.cpuMilliCores,memoryMiB:value.resources.memoryMiB,queuedTasks:0},executionObserved:value.executionObserved,sequence:value.sequence,phase:value.phase,observedAt:value.observedAt};
}
function validKey(key:string){if(typeof key!=='string'||!/^[a-f0-9]{64}$/.test(key))throw new ControlError('BODY_EVIDENCE_KEY_UNAVAILABLE',503);}
export function bodyEvidenceDigest(value:BodyExecutionEvidence):string{return createHash('sha256').update(JSON.stringify(canonicalBodyEvidence(value))).digest('hex');}
export function signBodyEvidence(value:BodyExecutionEvidence,key:string):string {
 validKey(key);const payload=Buffer.from(JSON.stringify(canonicalBodyEvidence(value))).toString('base64url');return payload+'.'+createHmac('sha256',key).update('ansts-body-execution-observation-v1.'+payload).digest('base64url');
}
export function verifyBodyEvidence(proof:string,key:string):BodyExecutionEvidence {
 validKey(key);if(typeof proof!=='string'||proof.length>8192||!proof.match(/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]{43}$/))throw new ControlError('BODY_EVIDENCE_AUTH_REQUIRED',401);
 const [payload,signature]=proof.split('.'),expected=createHmac('sha256',key).update('ansts-body-execution-observation-v1.'+payload).digest(),actual=Buffer.from(signature,'base64url');
 if(signature!==actual.toString('base64url')||actual.length!==expected.length||!timingSafeEqual(actual,expected))throw new ControlError('BODY_EVIDENCE_AUTH_REQUIRED',401);
 let value:BodyExecutionEvidence;try{value=canonicalBodyEvidence(JSON.parse(Buffer.from(payload,'base64url').toString('utf8')));}catch{throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);}
 if(Buffer.from(JSON.stringify(value)).toString('base64url')!==payload)throw new ControlError('INVALID_EXECUTION_EVIDENCE',422);return value;
}
