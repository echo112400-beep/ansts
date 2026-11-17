import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {bodyEvidenceDigest,signBodyEvidence,verifyBodyEvidence,type BodyExecutionEvidence} from '../../src/control/body_execution_evidence.js';
import {BodyExecutionEvidenceLedger} from '../../src/runtime/body_execution_evidence_ledger.js';
import {issueAdmissionCredential,verifyAdmissionCredential} from '../../src/control/admission_credentials.js';
import {TenantPolicyStore} from '../../src/finops/tenant_policy_store.js';
import {AdmissionReservationStore,type AdmissionReservation} from '../../src/control/admission_reservations.js';
import type {DirectoryStorage,DirectorySql,TenantRecord} from '../../src/control/tenant_directory.js';
const load=createRequire(join(process.cwd(),'package.json'));
interface Database {exec(q:string):void;prepare(q:string):{all(...bindings:unknown[]):unknown[]};close():void}
const {DatabaseSync}=load('node:sqlite') as {DatabaseSync:new(path:string)=>Database};
const actor={sub:'github:1',role:'SRE_ADMIN' as const,scopeTenant:'all',scopeRegion:'all'},tenant:TenantRecord={id:'tenant',name:'Tenant',region:'dev',costCenter:'eng',version:1,status:'ACTIVE'};
function fixture(path=':memory:'){
 const db=new DatabaseSync(path);const sql:DirectorySql={exec<T>(query:string,...values:unknown[]){const rows=db.prepare(query).all(...values) as T[];return {toArray:()=>rows};}};
 const storage:DirectoryStorage={sql,transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}}};
 return {db,storage,store:new AdmissionReservationStore(storage),policies:new TenantPolicyStore(storage)};
}
function prepare(s:ReturnType<typeof fixture>){
 s.policies.write(actor,tenant,{expectedVersion:0,resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'evidence test',idempotencyKey:'policy',requestId:'test'});
 let r=s.store.prepare(actor,tenant,{reservationId:'r1',taskId:'task',attemptId:'attempt',fencing:7,expectedPolicyVersion:1,resources:{concurrentAttempts:1,cpuMilliCores:250,memoryMiB:128,queuedTasks:0},estimatedCreditUnits:'10',expiresAt:Date.now()+60000,executionInputDigest:'f'.repeat(64)},'prepare',Date.now());
 for(const to of ['RESERVED','START_AUTHORIZED'] as const)r=s.store.transition(actor,tenant,{reservationId:r.id,expectedVersion:r.version,generation:r.generation,fencing:r.fencing,to,reason:'test binding',...(to==='RESERVED'?{target:'host-1',capacityReceipt:'legacy-observation'}:{})},to,Date.now());return r;
}
function fact(r:AdmissionReservation,phase:BodyExecutionEvidence['phase']='RUNNING',sequence=2):BodyExecutionEvidence{return {schemaVersion:1,purpose:'ansts-body-execution-observation-v1',hostId:'host-1',tenantId:r.tenantId,taskId:r.taskId,attemptId:r.attemptId,fencing:r.fencing,reservationId:r.id,generation:r.generation,policyVersion:r.policy.version,tenantVersion:r.policy.tenantVersion,requestDigest:r.requestDigest,executionInputDigest:r.executionInputDigest!,resources:r.resources,executionObserved:phase==='RUNNING'||phase==='TERMINATED',phase,sequence,observedAt:Date.now()};}
test('Body facts are authenticated and immutable terminal observations survive host reconstruction',()=>{
 const s=fixture(),directory=mkdtempSync(join(tmpdir(),'ansts-evidence-'));try{
  const r=prepare(s),begin={...fact(r,'UNCERTAIN',1),executionObserved:false};let ledger=new BodyExecutionEvidenceLedger(directory);ledger.begin(begin);ledger.advance(begin,'RUNNING');ledger.advance(begin,'UNCERTAIN');
  ledger=new BodyExecutionEvidenceLedger(directory);assert.equal(ledger.read('tenant','task','attempt')?.executionObserved,true);assert.throws(()=>ledger.advance(begin,'NOT_STARTED'),/BODY_EVIDENCE_TERMINAL_CONFLICT/);
  const final=ledger.advance(begin,'TERMINATED'),proof=signBodyEvidence(final,'a'.repeat(64));assert.deepEqual(verifyBodyEvidence(proof,'a'.repeat(64)),final);
  assert.throws(()=>verifyBodyEvidence(proof,'b'.repeat(64)),{code:'BODY_EVIDENCE_AUTH_REQUIRED'});assert.throws(()=>verifyBodyEvidence('x'+proof.slice(1),'a'.repeat(64)),{code:'BODY_EVIDENCE_AUTH_REQUIRED'});
  assert.throws(()=>new BodyExecutionEvidenceLedger(directory).advance(begin,'RUNNING'),/BODY_EVIDENCE_TERMINAL_CONFLICT/);
 }finally{s.db.close();rmSync(directory,{recursive:true,force:true});}
});
test('verified Engine termination releases resource quota while unpriced budget remains reserved',()=>{
 const s=fixture();try{
  const r=prepare(s),running=s.store.applyBodyEvidence(actor,tenant,fact(r));assert.equal(running.phase,'RUNNING');
  const final=fact(r,'TERMINATED',3),settling=s.store.applyBodyEvidence(actor,tenant,final);assert.equal(settling.phase,'SETTLING');assert.equal(settling.actualCreditUnits,null);
  assert.deepEqual(s.store.applyBodyEvidence(actor,tenant,final),settling);
  const input={reservationId:'r2',taskId:'task2',attemptId:'attempt2',fencing:8,expectedPolicyVersion:1,resources:r.resources,estimatedCreditUnits:'91',expiresAt:Date.now()+60000};
  assert.throws(()=>s.store.prepare(actor,tenant,input,'over-budget',Date.now()),{code:'BUDGET_QUOTA_EXCEEDED'});
  assert.equal(s.store.prepare(actor,tenant,{...input,estimatedCreditUnits:'1'},'within-budget',Date.now()).phase,'PREPARING');
 }finally{s.db.close();}
});
test('out-of-order facts cannot resurrect execution and binding mismatch is never consumed',()=>{
 const s=fixture();try{
  const r=prepare(s);assert.throws(()=>s.store.applyBodyEvidence(actor,tenant,{...fact(r),executionInputDigest:'a'.repeat(64)}),{code:'EXECUTION_EVIDENCE_BINDING_CONFLICT'});
  const final=fact(r,'TERMINATED',3);s.store.applyBodyEvidence(actor,tenant,final);
  assert.throws(()=>s.store.applyBodyEvidence(actor,tenant,fact(r)),{code:'STALE_EXECUTION_EVIDENCE'});
  assert.throws(()=>s.store.applyBodyEvidence(actor,tenant,{...final,observedAt:final.observedAt+1}),{code:'EXECUTION_EVIDENCE_CONFLICT'});
  assert.equal(s.store.read(actor,tenant,'r1').phase,'SETTLING');
 }finally{s.db.close();}
});
test('positive prestart cancellation refunds once and survives native authority reconstruction',()=>{
 const directory=mkdtempSync(join(tmpdir(),'ansts-evidence-state-'));let s=fixture(join(directory,'state.sqlite'));try{
  const r=prepare(s),stopped=fact(r,'NOT_STARTED',2);const result=s.store.applyBodyEvidence(actor,tenant,stopped);assert.equal(result.phase,'CANCELLED');assert.equal(result.generation,2);
  s.db.close();s=fixture(join(directory,'state.sqlite'));assert.deepEqual(s.store.applyBodyEvidence(actor,tenant,stopped),result);
  assert.throws(()=>s.store.applyBodyEvidence({...actor,role:'VIEWER'},tenant,stopped),{code:'CONTROL_FORBIDDEN'});
 }finally{s.db.close();rmSync(directory,{recursive:true,force:true});}
});
test('evidence commit and message intent roll back together when outbox storage fails',()=>{
 const s=fixture();try{
  const r=prepare(s),final=fact(r,'TERMINATED',3),original=s.storage.sql.exec.bind(s.storage.sql);
  s.storage.sql.exec=(query,...values)=>{if(query.startsWith('INSERT OR REPLACE INTO admission_outbox'))throw new Error('disk failed');return original(query,...values);};
  assert.throws(()=>s.store.applyBodyEvidence(actor,tenant,final),/disk failed/);s.storage.sql.exec=original;assert.deepEqual(s.store.read(actor,tenant,'r1'),r);assert.equal(s.store.applyBodyEvidence(actor,tenant,final).phase,'SETTLING');
 }finally{s.db.close();}
});

test('observation outbox survives restart and stale acknowledgements cannot hide newer facts',()=>{
 const s=fixture(),directory=mkdtempSync(join(tmpdir(),'ansts-evidence-outbox-'));try{
  const r=prepare(s),begin={...fact(r,'UNCERTAIN',1),executionObserved:false};
  let ledger=new BodyExecutionEvidenceLedger(directory);ledger.begin(begin);
  assert.deepEqual(ledger.pending(),[begin]);
  const firstDigest=bodyEvidenceDigest(begin);assert.equal(ledger.acknowledge(begin,firstDigest),true);
  ledger=new BodyExecutionEvidenceLedger(directory);assert.deepEqual(ledger.pending(),[]);
  const running=ledger.advance(begin,'RUNNING');assert.deepEqual(ledger.pending(),[running]);
  assert.equal(ledger.acknowledge(begin,firstDigest),false);assert.deepEqual(ledger.pending(),[running]);
  const terminal=ledger.advance(begin,'TERMINATED');ledger=new BodyExecutionEvidenceLedger(directory);
  assert.deepEqual(ledger.pending(),[terminal]);
  assert.equal(ledger.acknowledge(terminal,bodyEvidenceDigest(terminal)),true);
  assert.equal(ledger.acknowledge(terminal,bodyEvidenceDigest(terminal)),true);
  assert.deepEqual(new BodyExecutionEvidenceLedger(directory).read('tenant','task','attempt'),terminal);
  assert.deepEqual(new BodyExecutionEvidenceLedger(directory).pending(),[]);
  ledger.begin(begin);assert.deepEqual(ledger.pending(),[]);
  assert.throws(()=>ledger.pending(101),/INVALID_EVIDENCE_LIMIT/);
 }finally{s.db.close();rmSync(directory,{recursive:true,force:true});}
});

test('legacy long execution identities survive admission, message, credential and observation roundtrips',()=>{
 const s=fixture();try{
  const original=prepare(s);s.store.applyBodyEvidence(actor,tenant,fact(original,'TERMINATED',3));const taskId='task-'+ 'x'.repeat(295),attemptId=taskId+':8';
  let r=s.store.prepare(actor,tenant,{reservationId:'long-r',taskId,attemptId,fencing:8,expectedPolicyVersion:1,resources:original.resources,estimatedCreditUnits:'1',expiresAt:Date.now()+60000,executionInputDigest:'e'.repeat(64)},'long-prepare',Date.now());
  assert.equal(s.store.listMessages(actor,tenant,100).some(item=>item.message.attemptId===attemptId),true);
  for(const to of ['RESERVED','START_AUTHORIZED'] as const)r=s.store.transition(actor,tenant,{reservationId:r.id,expectedVersion:r.version,generation:r.generation,fencing:r.fencing,to,reason:'long identity',...(to==='RESERVED'?{target:'host-1',capacityReceipt:'legacy-observation'}:{})},'long-'+to,Date.now());
  const credential=issueAdmissionCredential(r,'a'.repeat(64),Date.now());assert.equal(verifyAdmissionCredential(credential,'a'.repeat(64),r,Date.now(),'host-1').taskId,taskId);
  const evidence=fact(r,'TERMINATED',3),proof=signBodyEvidence(evidence,'a'.repeat(64));
  assert.equal(s.store.applyBodyEvidence(actor,tenant,verifyBodyEvidence(proof,'a'.repeat(64))).phase,'SETTLING');
  assert.throws(()=>signBodyEvidence({...evidence,attemptId:'x'.repeat(513)},'a'.repeat(64)),{code:'INVALID_EXECUTION_EVIDENCE'});
 }finally{s.db.close();}
});

test('observation binding lookup is scoped and read-only, including expired running attempts',(t)=>{
 const s=fixture();try{
  const r=prepare(s),input={tenantId:tenant.id,taskId:r.taskId,attemptId:r.attemptId,fencing:r.fencing,executionInputDigest:r.executionInputDigest!,resources:r.resources};
  const binding=s.store.observationBinding(actor,tenant,'host-1',input);assert.equal(binding?.reservationId,r.id);
  assert.equal(s.store.observationBinding(actor,tenant,'host-2',input),null);
  for(const change of [{taskId:'other'},{attemptId:'other'},{fencing:8},{executionInputDigest:'a'.repeat(64)},{resources:{...r.resources,cpuMilliCores:251}}])assert.equal(s.store.observationBinding(actor,tenant,'host-1',{...input,...change}),null);
  assert.throws(()=>s.store.observationBinding({...actor,scopeTenant:'other'},tenant,'host-1',input),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>s.store.observationBinding({...actor,role:'VIEWER'},tenant,'host-1',input),{code:'CONTROL_FORBIDDEN'});
  assert.deepEqual(s.store.read(actor,tenant,r.id),r);
  t.mock.method(Date,'now',()=>r.expiresAt+1);assert.deepEqual(s.store.observationBinding(actor,tenant,'host-1',input),binding);t.mock.restoreAll();
  s.store.applyBodyEvidence(actor,tenant,fact(r,'TERMINATED',3));assert.deepEqual(s.store.observationBinding(actor,tenant,'host-1',input),binding);
  const nextInput={reservationId:'unstarted',taskId:'unstarted',attemptId:'unstarted',fencing:8,expectedPolicyVersion:1,resources:r.resources,estimatedCreditUnits:'1',expiresAt:Date.now()+60000,executionInputDigest:'a'.repeat(64)};
  const next=s.store.prepare(actor,tenant,nextInput,'unstarted',Date.now());s.store.transition(actor,tenant,{reservationId:next.id,expectedVersion:1,generation:1,fencing:8,to:'RESERVED',target:'host-1',capacityReceipt:'observation-only',reason:'No start authorization'},'reserve-unstarted',Date.now());
  assert.equal(s.store.observationBinding(actor,tenant,'host-1',{...input,taskId:next.taskId,attemptId:next.attemptId,fencing:8,executionInputDigest:next.executionInputDigest!}),null);
 }finally{s.db.close();}
});
