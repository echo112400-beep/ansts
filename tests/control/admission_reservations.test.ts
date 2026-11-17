import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Worker} from 'node:worker_threads';
import {AdmissionReservationStore,type PrepareAdmission,type AdmissionTransition,type AdmissionReservation} from '../../src/control/admission_reservations.js';
import {TenantPolicyStore,type QuotaPolicyWrite} from '../../src/finops/tenant_policy_store.js';
import type {DirectoryStorage,DirectorySql,TenantRecord} from '../../src/control/tenant_directory.js';
const load=createRequire(join(process.cwd(),'package.json'));
interface Database {exec(q:string):void;prepare(q:string):{all(...bindings:unknown[]):unknown[]};close():void}
const {DatabaseSync}=load('node:sqlite') as {DatabaseSync:new(path:string)=>Database};
function open(path=':memory:'){
 const db=new DatabaseSync(path);
 const sql:DirectorySql={exec<T>(query:string,...values:unknown[]){const rows=db.prepare(query).all(...values) as T[];return {toArray:()=>rows};}};
 const storage:DirectoryStorage={sql,transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}}};
 return {db,storage,store:new AdmissionReservationStore(storage),policies:new TenantPolicyStore(storage)};
}
const actor={sub:'github:1',role:'SRE_ADMIN' as const,scopeTenant:'all',scopeRegion:'all'};
const tenant:TenantRecord={id:'a',name:'A',region:'dev',costCenter:'eng',version:1,status:'ACTIVE'};
const now=Date.parse('2026-11-16T23:59:59Z');
const policy:QuotaPolicyWrite={expectedVersion:0,resources:{concurrentAttempts:2,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'test capacity',idempotencyKey:'policy',requestId:'r1'};
function prepare(id='r1',overrides:Partial<PrepareAdmission>={}):PrepareAdmission{return {reservationId:id,taskId:'task-'+id,attemptId:'attempt-'+id,fencing:7,expectedPolicyVersion:1,resources:{concurrentAttempts:1,cpuMilliCores:500,memoryMiB:256,queuedTasks:0},estimatedCreditUnits:'60',expiresAt:now+60000,...overrides};}
function command(r:AdmissionReservation,to:AdmissionTransition['to'],extra:Partial<AdmissionTransition>={}):AdmissionTransition{return {reservationId:r.id,expectedVersion:r.version,generation:r.generation,fencing:r.fencing,to,reason:'protocol validation',...extra};}
function reserved(s:ReturnType<typeof open>){let r=s.store.prepare(actor,tenant,prepare(),'prepare',now);return s.store.transition(actor,tenant,command(r,'RESERVED',{target:'body-1',capacityReceipt:'capacity-1'}),'reserve',now);}
function authorized(s:ReturnType<typeof open>){return s.store.transition(actor,tenant,command(reserved(s),'START_AUTHORIZED'),'authorize',now);}
function evidence(r:AdmissionReservation,kind:'NOT_STARTED'|'TERMINATED'='TERMINATED'){return {kind,id:'evidence-'+r.id,target:r.target!,attemptId:r.attemptId,fencing:r.fencing,generation:r.generation};}
test('admission holds and immutable policy snapshots survive each phase restart and settlement replay',()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-admission-')),path=join(dir,'state.sqlite');let s=open(path);
 const restart=()=>{s.db.close();s=open(path);};
 try{
  s.policies.write(actor,tenant,policy);let r=s.store.prepare(actor,tenant,prepare(),'prepare',now);restart();assert.deepEqual(s.store.read(actor,tenant,r.id),r);
  for(const to of ['RESERVED','START_AUTHORIZED','RUNNING','SETTLING','SETTLED'] as const){
   const extra:Partial<AdmissionTransition>=to==='RESERVED'?{target:'body-1',capacityReceipt:'capacity-1'}:to==='SETTLING'?{evidence:evidence(r)}:to==='SETTLED'?{actualCreditUnits:'25'}:{};
   const input=command(r,to,extra);r=s.store.transition(actor,tenant,input,to,now);restart();assert.deepEqual(s.store.read(actor,tenant,r.id),r);assert.deepEqual(s.store.transition(actor,tenant,input,to,now+100000),r);
  }
  assert.deepEqual(s.store.prepare(actor,{...tenant,status:'DISABLED'},prepare(),'prepare',now+100000).phase,'PREPARING');
  assert.equal(s.store.prepare(actor,tenant,prepare('r2',{estimatedCreditUnits:'75'}),'new',now).phase,'PREPARING');
  assert.throws(()=>s.store.prepare(actor,tenant,prepare('r3',{estimatedCreditUnits:'1'}),'exceeded',now),{code:'BUDGET_QUOTA_EXCEEDED'});
 }finally{s.db.close();rmSync(dir,{recursive:true,force:true});}
});
test('quota and budget are atomic, pending budget crosses UTC boundaries, lower policy retains old occupancy',()=>{
 const s=open();try{
  s.policies.write(actor,tenant,policy);const r=s.store.prepare(actor,tenant,prepare(),'one',now);
  assert.throws(()=>s.store.prepare(actor,tenant,prepare('r2'),'two',now+2000),{code:'BUDGET_QUOTA_EXCEEDED'});
  assert.throws(()=>s.store.prepare(actor,tenant,prepare('r2',{estimatedCreditUnits:'1',resources:{...prepare().resources,cpuMilliCores:501}}),'cpu',now),{code:'RESOURCE_QUOTA_EXCEEDED'});
  s.policies.write(actor,tenant,{...policy,expectedVersion:1,resources:{...policy.resources,concurrentAttempts:0},idempotencyKey:'lower'});
  assert.equal(s.store.read(actor,tenant,r.id).policy.version,1);
  assert.equal(s.store.read(actor,tenant,r.id).policy.resources.concurrentAttempts,2);
  assert.throws(()=>s.store.prepare(actor,tenant,prepare('r2',{expectedPolicyVersion:2,estimatedCreditUnits:'1'}),'lowered',now),{code:'RESOURCE_QUOTA_EXCEEDED'});
 }finally{s.db.close();}
});
test('authorization uncertainty never expires into released capacity; evidence and generations are pinned',()=>{
 const s=open();try{
  s.policies.write(actor,tenant,policy);let r=authorized(s);
  r=s.store.transition(actor,tenant,command(r,'UNCERTAIN'),'uncertain',now+120000);
  assert.equal(s.store.read(actor,tenant,r.id).phase,'UNCERTAIN');
  assert.throws(()=>s.store.prepare(actor,tenant,prepare('r2',{expiresAt:now+240000}),'blocked',now+120000),{code:'BUDGET_QUOTA_EXCEEDED'});
  assert.throws(()=>s.store.transition(actor,tenant,command(r,'CANCELLED'),'ttl-is-not-evidence',now+120000),{code:'EXECUTION_EVIDENCE_REQUIRED'});
  assert.throws(()=>s.store.transition(actor,tenant,command(r,'SETTLING',{evidence:{...evidence(r),fencing:6}}),'old-evidence',now+120000),{code:'EXECUTION_EVIDENCE_REQUIRED'});
  assert.throws(()=>s.store.transition(actor,tenant,command(r,'SETTLING',{generation:2,evidence:evidence(r)}),'old-generation',now+120000),{code:'STALE_ADMISSION_GENERATION'});
  r=s.store.transition(actor,tenant,command(r,'SETTLING',{evidence:evidence(r)}),'stop-confirmed',now+120000);
  assert.throws(()=>s.store.transition(actor,tenant,command(r,'SETTLED'),'missing-cost',now+120000),{code:'ACTUAL_COST_REQUIRED'});
  assert.equal(s.store.read(actor,tenant,r.id).phase,'SETTLING');
 }finally{s.db.close();}
});
test('pre-start revocation is persistent, stale or expired start rejected, attempts cannot be rebound',()=>{
 const s=open();try{
  s.policies.write(actor,tenant,policy);let r=reserved(s);
  assert.throws(()=>s.store.transition(actor,tenant,command(r,'START_AUTHORIZED'),'expired',r.expiresAt),{code:'ADMISSION_EXPIRED'});
  const previous=r;r=s.store.transition(actor,tenant,command(r,'CANCELLED'),'revoke',r.expiresAt);
  assert.equal(r.generation,2);assert.equal(r.startAuthorized,false);
  assert.throws(()=>s.store.transition(actor,tenant,command(previous,'START_AUTHORIZED'),'stale',now),{code:'VERSION_CONFLICT'});
  assert.throws(()=>s.store.prepare(actor,tenant,prepare('different',{attemptId:r.attemptId}),'rebind',now),{code:'ATTEMPT_ALREADY_RESERVED'});
  assert.equal(s.store.prepare(actor,tenant,prepare('r2'),'fresh',now).phase,'PREPARING');
 }finally{s.db.close();}
});
test('tenant role, metadata activation and idempotency are enforced without storing extraneous input',()=>{
 const s=open();try{
  s.policies.write(actor,tenant,policy);
  assert.throws(()=>s.store.prepare({...actor,role:'VIEWER'},tenant,prepare(),'viewer',now),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>s.store.prepare(actor,{...tenant,version:2},prepare(),'metadata',now),{code:'POLICY_METADATA_NOT_ACTIVATED'});
  assert.throws(()=>s.store.prepare(actor,{...tenant,status:'DISABLED'},prepare(),'disabled',now),{code:'TENANT_DISABLED'});
  const r=s.store.prepare({...actor,role:'OPERATOR'},tenant,{...prepare(),secret:'must-not-persist'} as PrepareAdmission,'valid',now);
  assert.throws(()=>s.store.prepare(actor,tenant,prepare('r2'),'valid',now),{code:'IDEMPOTENCY_CONFLICT'});
  assert.throws(()=>s.store.read({...actor,scopeRegion:'other'},tenant,r.id),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>s.store.read(actor,{...tenant,id:'other'},r.id),{code:'ADMISSION_IDENTITY_CONFLICT'});
  const saved=s.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_admission_state WHERE id = ?','admission').toArray()[0].payload_json;
  assert.equal(saved.includes('must-not-persist'),false);
 }finally{s.db.close();}
});
test('reservation transaction failure does not consume idempotency or capacity',()=>{
 const s=open();try{
  s.policies.write(actor,tenant,policy);const original=s.storage.sql.exec.bind(s.storage.sql);
  s.storage.sql.exec=(query,...bindings)=>{if(query.startsWith('INSERT OR REPLACE INTO tenant_admission_state'))throw new Error('disk failure');return original(query,...bindings);};
  assert.throws(()=>s.store.prepare(actor,tenant,prepare(),'retry',now),/disk failure/);
  s.storage.sql.exec=original;assert.equal(s.store.prepare(actor,tenant,prepare(),'retry',now).version,1);
 }finally{s.db.close();}
});
test('running acknowledgement cannot be erased by uncertainty and false not-started evidence',()=>{
 const s=open();try{
  s.policies.write(actor,tenant,policy);let r=authorized(s);
  r=s.store.transition(actor,tenant,command(r,'RUNNING'),'running',now);
  r=s.store.transition(actor,tenant,command(r,'UNCERTAIN'),'disconnected',now+120000);
  assert.throws(()=>s.store.transition(actor,tenant,command(r,'CANCELLED',{evidence:evidence(r,'NOT_STARTED')}),'false-stop',now+120000),{code:'EXECUTION_EVIDENCE_REQUIRED'});
  assert.throws(()=>s.store.transition(actor,tenant,command(r,'SETTLING',{evidence:evidence(r,'NOT_STARTED')}),'false-settle',now+120000),{code:'EXECUTION_EVIDENCE_REQUIRED'});
  assert.equal(s.store.transition(actor,tenant,command(r,'SETTLING',{evidence:evidence(r)}),'terminated',now+120000).phase,'SETTLING');
 }finally{s.db.close();}
});
test('settlement keeps spent budget occupied under a changed window and late costs block new admissions',()=>{
 const s=open();try{
  s.policies.write(actor,tenant,policy);let r=authorized(s);
  r=s.store.transition(actor,tenant,command(r,'SETTLING',{evidence:evidence(r)}),'stop',now);
  r=s.store.transition(actor,tenant,command(r,'SETTLED',{actualCreditUnits:'101'}),'settled',now);
  s.policies.write(actor,tenant,{...policy,expectedVersion:1,budget:{...policy.budget,period:'UTC_MONTH'},idempotencyKey:'month'});
  assert.throws(()=>s.store.prepare(actor,tenant,prepare('r2',{expectedPolicyVersion:2,estimatedCreditUnits:'0'}),'late-cost',now),{code:'BUDGET_QUOTA_EXCEEDED'});
  assert.equal(s.store.read(actor,tenant,r.id).actualCreditUnits,'101');
 }finally{s.db.close();}
});
test('uncertain state and receipt survive native storage recovery without TTL release',()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-uncertain-')),path=join(dir,'state.sqlite');let s=open(path);
 try{
  s.policies.write(actor,tenant,policy);const started=authorized(s),input=command(started,'UNCERTAIN');
  const expected=s.store.transition(actor,tenant,input,'unknown',now);s.db.close();s=open(path);
  assert.deepEqual(s.store.read(actor,tenant,expected.id),expected);
  assert.deepEqual(s.store.transition(actor,tenant,input,'unknown',now+86400000),expected);
  assert.throws(()=>s.store.prepare(actor,tenant,prepare('r2',{expiresAt:now+86460000}),'next-day',now+86400000),{code:'BUDGET_QUOTA_EXCEEDED'});
 }finally{s.db.close();rmSync(dir,{recursive:true,force:true});}
});

test('independent native SQLite authorities racing for the last slot commit only one reservation',{timeout:20000},async()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-race-')),path=join(dir,'state.sqlite');const s=open(path);
 s.policies.write(actor,tenant,{...policy,resources:{...policy.resources,concurrentAttempts:1}});s.db.close();
 const {buildSync}=load('esbuild') as {buildSync(options:unknown):{outputFiles:{text:string}[]}};
 const source=buildSync({stdin:{resolveDir:process.cwd(),contents:`
  import {parentPort,workerData} from 'node:worker_threads';
  import {DatabaseSync} from 'node:sqlite';
  import {AdmissionReservationStore} from './src/control/admission_reservations.ts';
  const db=new DatabaseSync(workerData.path);db.exec('PRAGMA busy_timeout=10000');
  const storage={sql:{exec(query,...bindings){const rows=db.prepare(query).all(...bindings);return {toArray:()=>rows}}},transactionSync(fn){db.exec('BEGIN IMMEDIATE');try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}}};
  const store=new AdmissionReservationStore(storage);parentPort.postMessage('ready');
  Atomics.wait(new Int32Array(workerData.gate),0,0);
  try{store.prepare(workerData.actor,workerData.tenant,workerData.input,'parallel-'+workerData.input.reservationId,workerData.now);parentPort.postMessage('committed');}
  catch(error){parentPort.postMessage(error.code??error.message);}
  finally{db.close();}
 `},bundle:true,platform:'node',format:'cjs',write:false}).outputFiles[0].text;
 const gate=new SharedArrayBuffer(4);const workers:Worker[]=[];
 try{
  const ready:Promise<void>[]=[],results:Promise<string>[]=[];
  for(let index=0;index<4;index++){
   const worker=new Worker(source,{eval:true,workerData:{path,gate,actor,tenant,now,input:prepare('race-'+index,{estimatedCreditUnits:'0'})}});workers.push(worker);
   ready.push(new Promise((resolve,reject)=>{worker.on('message',message=>{if(message==='ready')resolve();});worker.on('error',reject);worker.on('exit',code=>{if(code)reject(new Error('worker initialization failed'));});}));
   results.push(new Promise((resolve,reject)=>{worker.on('message',message=>{if(message!=='ready')resolve(message);});worker.on('error',reject);worker.on('exit',code=>{if(code)reject(new Error('worker execution failed'));});}));
  }
  await Promise.all(ready);Atomics.store(new Int32Array(gate),0,1);Atomics.notify(new Int32Array(gate),0);
  const actual=await Promise.all(results);assert.equal(actual.filter(result=>result==='committed').length,1);assert.equal(actual.filter(result=>result==='RESOURCE_QUOTA_EXCEEDED').length,3);
 }finally{await Promise.all(workers.map(worker=>worker.terminate()));rmSync(dir,{recursive:true,force:true});}
});
test('failed start authorization rolls back both phase and operation receipt',()=>{
 const s=open();try{
  s.policies.write(actor,tenant,policy);const r=reserved(s),input=command(r,'START_AUTHORIZED');
  const original=s.storage.sql.exec.bind(s.storage.sql);
  s.storage.sql.exec=(query,...bindings)=>{if(query.startsWith('INSERT OR REPLACE INTO tenant_admission_state'))throw new Error('disk failure');return original(query,...bindings);};
  assert.throws(()=>s.store.transition(actor,tenant,input,'start',now),/disk failure/);
  s.storage.sql.exec=original;assert.deepEqual(s.store.read(actor,tenant,r.id),r);
  assert.equal(s.store.transition(actor,tenant,input,'start',now).phase,'START_AUTHORIZED');
 }finally{s.db.close();}
});
