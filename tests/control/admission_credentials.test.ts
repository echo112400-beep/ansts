import {test} from 'node:test';
import assert from 'node:assert/strict';
import {issueAdmissionCredential,verifyAdmissionCredential} from '../../src/control/admission_credentials.js';
import type {AdmissionReservation} from '../../src/control/admission_reservations.js';
const now=Date.parse('2026-11-16T12:00:00Z'),key='a'.repeat(64);
const reservation:AdmissionReservation={id:'r1',tenantId:'tenant-1',taskId:'t1',attemptId:'a1',requestDigest:'digest',fencing:7,generation:1,version:3,phase:'START_AUTHORIZED',policy:{schemaVersion:1,tenantId:'tenant-1',version:4,tenantVersion:2,region:'dev',costCenter:'eng',actor:'admin',reason:'test',createdAt:new Date(now).toISOString(),resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:1024,queuedTasks:0},budget:{period:'UTC_DAY',limitCreditUnits:'10',softThresholdBps:8000}},resources:{concurrentAttempts:1,cpuMilliCores:500,memoryMiB:512,queuedTasks:0},estimatedCreditUnits:'5',period:'UTC_DAY:2026-11-16',expiresAt:now+60000,createdAt:now,updatedAt:now,startAuthorized:true,executionObserved:false,target:'body-1',capacityReceipt:'capacity-1',terminalEvidence:null,actualCreditUnits:null,rateSnapshot:null};
test('authenticated shadow credential pins authoritative execution fields and exact target',()=>{
 const token=issueAdmissionCredential(reservation,key,now),claims=verifyAdmissionCredential(token,key,reservation,now,'body-1');
 assert.equal(claims.purpose,'ansts-admission-shadow-v1');assert.equal(claims.requestDigest,'digest');assert.equal(claims.resources.memoryMiB,512);
 assert.throws(()=>verifyAdmissionCredential(token,key,reservation,now,'body-2'),{code:'ADMISSION_NOT_AUTHORIZED'});
 for(const changed of [{tenantId:'other'},{taskId:'other'},{attemptId:'other'},{generation:2},{fencing:8},{version:4},{requestDigest:'other'},{resources:{...reservation.resources,memoryMiB:1024}}])assert.throws(()=>verifyAdmissionCredential(token,key,{...reservation,...changed},now,'body-1'),{code:'STALE_ADMISSION_CREDENTIAL'});
});
test('tampering, wrong key, missing key and malformed credentials fail closed',()=>{
 const token=issueAdmissionCredential(reservation,key,now),[payload,signature]=token.split('.');
 const json=JSON.parse(Buffer.from(payload,'base64url').toString());json.resources.cpuMilliCores=100000;
 const forged=Buffer.from(JSON.stringify(json)).toString('base64url')+'.'+signature;
 assert.throws(()=>verifyAdmissionCredential(forged,key,reservation,now,'body-1'),{code:'INVALID_ADMISSION_CREDENTIAL'});
 assert.throws(()=>verifyAdmissionCredential(token,'b'.repeat(64),reservation,now,'body-1'),{code:'INVALID_ADMISSION_CREDENTIAL'});
 assert.throws(()=>issueAdmissionCredential(reservation,'',now),{code:'ADMISSION_SIGNING_UNAVAILABLE'});
 for(const invalid of ['','abc.def',token+'.extra','x'.repeat(8193)])assert.throws(()=>verifyAdmissionCredential(invalid,key,reservation,now,'body-1'),{code:'INVALID_ADMISSION_CREDENTIAL'});
});
test('expiry and authoritative revocation prevent credential use and cannot be inferred offline',()=>{
 const token=issueAdmissionCredential(reservation,key,now);
 assert.throws(()=>verifyAdmissionCredential(token,key,reservation,reservation.expiresAt,'body-1'),{code:'ADMISSION_EXPIRED'});
 for(const phase of ['UNCERTAIN','CANCELLED','RUNNING','SETTLED'] as const)assert.throws(()=>verifyAdmissionCredential(token,key,{...reservation,phase},now,'body-1'),{code:'ADMISSION_NOT_AUTHORIZED'});
 assert.throws(()=>verifyAdmissionCredential(token,key,{...reservation,generation:2,phase:'CANCELLED'},now,'body-1'),{code:'STALE_ADMISSION_CREDENTIAL'});
 assert.throws(()=>issueAdmissionCredential({...reservation,phase:'RESERVED'},key,now),{code:'ADMISSION_NOT_AUTHORIZED'});
});
