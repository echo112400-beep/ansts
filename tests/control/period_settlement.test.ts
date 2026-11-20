import {AdmissionReservationStore} from '../../src/control/admission_reservations.js';
import {signBodyEvidence,verifyBodyEvidence} from '../../src/control/body_execution_evidence.js';
import {TenantPolicyStore} from '../../src/finops/tenant_policy_store.js';
import {joinedTransactions} from '../../src/finops/finops_page.js';
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {PeriodSettlement,settlementMonth,type SettlementCoverage} from '../../src/finops/period_settlement.js';
import {UsageInbox} from '../../src/finops/usage_inbox.js';
import {UsageCostLedger} from '../../src/finops/usage_cost_ledger.js';
import {TenantRateStore} from '../../src/finops/tenant_rate_store.js';
import {RateCatalog,rateHash,type RateDefinition} from '../../src/finops/rate_cards.js';
import {BodyUsageJournal} from '../../src/runtime/body_usage_journal.js';
import type {DirectoryStorage,TenantRecord} from '../../src/control/tenant_directory.js';
import type {ControlActor} from '../../src/control/contracts.js';
import type {UsageIdentity} from '../../src/control/usage_event.js';
const admin:ControlActor={sub:'github:1',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},viewer:ControlActor={...admin,sub:'github:2',role:'VIEWER',scopeTenant:'tenant'};
const tenant:TenantRecord={id:'tenant',name:'Tenant',region:'dev',costCenter:'center',status:'ACTIVE',version:1};
const may=settlementMonth('2026-05'),june=settlementMonth('2026-06'),july=settlementMonth('2026-07'),august=settlementMonth('2026-08');
const resources={cpuNanoCores:1,cpuMilliCores:1,memoryMiB:1,slots:1},zero={numerator:'0',denominator:'1'};
function definition():RateDefinition{return {schemaVersion:1,unit:'credit-subunit',unitVersion:1,rounding:'HALF_EVEN',scope:{region:null,poolId:null},resources:{cpuNanoCoreMilliseconds:{numerator:'1',denominator:'2'},memoryMiBMilliseconds:zero,slotMilliseconds:zero},models:[{provider:'deepseek',requestedModel:'requested',reportedModel:'actual',input:{mode:'ALL',rate:{numerator:'1',denominator:'1'}},output:zero}]};}
function fixture(priced=true){
 const directory=mkdtempSync(join(tmpdir(),'ansts-period-')),{DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');let db=new DatabaseSync(join(directory,'authority.sqlite'));
 const storage=joinedTransactions({sql:{exec<T>(q:string,...args:unknown[]){const rows=db.prepare(q).all(...args) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const value=fn();db.exec('COMMIT');return value;}catch(error){db.exec('ROLLBACK');throw error;}}});
 const inbox=new UsageInbox(storage),ledger=new UsageCostLedger(storage),rates=new TenantRateStore(storage),catalog=new RateCatalog(storage),source=new BodyUsageJournal(join(directory,'source'));let periods=new PeriodSettlement(storage);
 if(priced){const card=catalog.create(admin,{expectedVersion:0,definition:definition(),reason:'Fixture internal price',idempotencyKey:'rate',requestId:'rate'},may.startAt-200);rates.activate(admin,tenant,{expectedVersion:0,rateCardVersion:card.version,reason:'Fixture activation',idempotencyKey:'activation',requestId:'activation'},card,may.startAt-100);}
 const identity=(name:string):UsageIdentity=>({producerId:'body-1',tenantId:'tenant',taskId:name,attemptId:name+'-attempt',fencing:1,executionInputDigest:'a'.repeat(64)});
 const bind=(id:UsageIdentity,start:number,allocation=resources)=>{const price=rates.pin(admin,tenant,id,{costCenterVersion:1,poolId:null},start-1);inbox.bind(id,allocation,{tenantVersion:1,region:'dev',costCenter:'center',rateCardVersion:price.activation?.rateCard.version??null},start-1);};
 const deliver=(id:UsageIdentity,limit=Infinity,project=true)=>{for(const event of source.events(id).filter(e=>e.sequence<=limit)){inbox.receive(event,id,may.endAt+50);if(project)ledger.project(event.eventId);}};
 const task=(name:string,start=may.startAt+1000,duration=2)=>{const id=identity(name);bind(id,start);source.begin(id,resources,start);source.finish(id,start+duration);deliver(id);return id;};
 const cover=(periodId:string,ids:UsageIdentity[],now:number):SettlementCoverage=>({schemaVersion:1,purpose:'ansts-settlement-coverage-v1',tenantId:'tenant',periodId,inventoryWatermark:'isolated-inventory-v1',observedAt:now,complete:true,attempts:ids});
 const close=(periodId:string,ids:UsageIdentity[],now:number,key=periodId)=>{const coverage=cover(periodId,ids,now),preview=periods.preview(admin,tenant,periodId,coverage,now);assert.deepEqual(preview.reasons,[]);return periods.close(admin,tenant,periodId,{expectedRevision:preview.revision,reason:'Verified isolated month',requestId:'close-request',idempotencyKey:key},coverage,now);};
 return {storage,inbox,ledger,rates,catalog,source,identity,bind,task,deliver,cover,close,get periods(){return periods;},reopen(){db.close();db=new DatabaseSync(join(directory,'authority.sqlite'));periods=new PeriodSettlement(storage);return periods;},finish(){db.close();rmSync(directory,{recursive:true,force:true});}};
}
function closeInput(revision:string,key='close'){return {expectedRevision:revision,reason:'Close verified period',requestId:'request',idempotencyKey:key};}
test('reservation cost derives exact UTC contributions from its terminal receipt and complete priced stream',()=>{
 const f=fixture();try{
  const id=f.identity('cross-day'),start=may.endAt-1,finished=start+4;
  const quota={concurrentAttempts:1,cpuMilliCores:1,memoryMiB:1,queuedTasks:0};
  new TenantPolicyStore(f.storage).write(admin,tenant,{expectedVersion:0,resources:quota,
   budget:{period:'UTC_DAY',limitCreditUnits:'10',softThresholdBps:8000},reason:'Fixture policy',requestId:'policy',idempotencyKey:'policy'});
  const admissions=new AdmissionReservationStore(f.storage);
  let reservation=admissions.prepare(admin,tenant,{reservationId:'reservation',expectedPolicyVersion:1,resources:quota,
   estimatedCreditUnits:'5',expiresAt:start+60000,taskId:id.taskId,attemptId:id.attemptId,fencing:id.fencing,executionInputDigest:id.executionInputDigest},'prepare',start-10);
  for(const to of ['RESERVED','START_AUTHORIZED','UNCERTAIN'] as const){
   reservation=admissions.transition(admin,tenant,{reservationId:reservation.id,expectedVersion:reservation.version,
    generation:reservation.generation,fencing:id.fencing,to,reason:'Fixture transition',target:id.producerId,capacityReceipt:'capacity'},to,start-5);
  }
  f.bind(id,start);f.source.begin(id,resources,start);f.source.finish(id,finished);f.deliver(id,2);
  const actual=()=>f.periods.previewReservationCost(admin,tenant,admissions,reservation.id);
  assert.deepEqual(actual().reasons,['VERIFIED_TERMINATION_REQUIRED']);
  const key='a'.repeat(64);
  const proof=signBodyEvidence({schemaVersion:1,purpose:'ansts-body-execution-observation-v1',hostId:id.producerId,
   tenantId:id.tenantId,taskId:id.taskId,attemptId:id.attemptId,fencing:id.fencing,reservationId:reservation.id,
   generation:reservation.generation,policyVersion:reservation.policy.version,tenantVersion:tenant.version,requestDigest:reservation.requestDigest,
   executionInputDigest:id.executionInputDigest,resources:quota,executionObserved:true,sequence:1,phase:'TERMINATED',observedAt:finished},key);
  reservation=admissions.applyBodyEvidence(admin,tenant,verifyBodyEvidence(proof,key));
  assert.ok(actual().reasons.includes('OPEN_USAGE_STREAM'));
  f.deliver(id,Infinity,false);
  assert.deepEqual(actual().reasons,['UNPROJECTED_USAGE']);
  f.deliver(id);f.task('unrelated');
  const result=actual();assert.equal(result.complete,true);assert.equal(result.phase,'SETTLING');
  const cpu=result.buckets.filter(b=>b.descriptor.dimension==='cpuNanoCoreMilliseconds').sort((a,b)=>a.descriptor.periodId.localeCompare(b.descriptor.periodId));
  assert.deepEqual(cpu.map(b=>[b.descriptor.periodId,b.exactCredit]),[['2026-05-31',{numerator:'1',denominator:'2'}],['2026-06-01',{numerator:'3',denominator:'2'}]]);
  f.reopen();f.deliver(id);assert.deepEqual(actual(),result);
  assert.equal(admissions.read(admin,tenant,reservation.id).actualCreditUnits,null);
  assert.throws(()=>f.periods.previewReservationCost(viewer,tenant,admissions,reservation.id),{code:'CONTROL_FORBIDDEN'});
 }finally{f.finish();}
});
test('verified costs replace estimates exactly once and post cumulative rounding deltas to their UTC usage periods',()=>{
 const f=fixture();try{
  const admissions=new AdmissionReservationStore(f.storage),policies=new TenantPolicyStore(f.storage);
  const quota={concurrentAttempts:1,cpuMilliCores:1,memoryMiB:1,queuedTasks:0};
  const policy={resources:quota,reason:'Fixture policy',requestId:'policy'};
  policies.write(admin,tenant,{...policy,expectedVersion:0,idempotencyKey:'day-policy',budget:{period:'UTC_DAY',limitCreditUnits:'10',softThresholdBps:8000}});
  const prepare=(name:string,now:number,estimate='5',expectedPolicyVersion=1)=>{
   const id=f.identity(name);
   return admissions.prepare(admin,tenant,{reservationId:name,taskId:id.taskId,attemptId:id.attemptId,fencing:id.fencing,
    executionInputDigest:id.executionInputDigest,resources:quota,expectedPolicyVersion,estimatedCreditUnits:estimate,expiresAt:now+60000},'prepare-'+name,now);
  };
  function finish(name:string,start:number,duration:number){
   const id=f.identity(name);
   let r=prepare(name,start-10);
   for(const to of ['RESERVED','START_AUTHORIZED'] as const){
    r=admissions.transition(admin,tenant,{reservationId:r.id,expectedVersion:r.version,generation:r.generation,fencing:r.fencing,
     to,reason:'Fixture start',target:id.producerId,capacityReceipt:'capacity'},name+'-'+to,start-5);
   }
   const token='a'.repeat(64);
   const proof=signBodyEvidence({schemaVersion:1,purpose:'ansts-body-execution-observation-v1',hostId:id.producerId,
    tenantId:tenant.id,taskId:id.taskId,attemptId:id.attemptId,fencing:id.fencing,reservationId:r.id,generation:r.generation,
    policyVersion:r.policy.version,tenantVersion:tenant.version,requestDigest:r.requestDigest,executionInputDigest:id.executionInputDigest,
    resources:quota,executionObserved:true,sequence:1,phase:'TERMINATED',observedAt:start+duration},token);
   admissions.applyBodyEvidence(admin,tenant,verifyBodyEvidence(proof,token));
   f.task(name,start,duration);
  }
  const preview=(id:string)=>f.periods.previewReservationCost(admin,tenant,admissions,id);
  const settle=(id:string,key=id,revision?:string)=>{
   const candidate=preview(id);
   return admissions.settleUsageCost(admin,tenant,{reservationId:id,expectedVersion:candidate.version,expectedRevision:revision??candidate.revision!},
    key,Date.now(),()=>preview(id));
  };
  finish('first',may.endAt-1,4);
  finish('second',may.endAt-1,1);
  assert.throws(()=>prepare('blocked-by-estimates',june.startAt+10,'1'),{code:'BUDGET_QUOTA_EXCEEDED'});
  assert.throws(()=>settle('first','stale','0'.repeat(64)),{code:'RESERVATION_COST_REVISION_CONFLICT'});
  const original=f.storage.sql.exec.bind(f.storage.sql);
  f.storage.sql.exec=(query,...args)=>{
   if(query.startsWith('INSERT OR REPLACE INTO admission_outbox'))throw new Error('release write failed');
   return original(query,...args);
  };
  assert.throws(()=>settle('first'),/release write failed/);
  f.storage.sql.exec=original;
  assert.throws(()=>prepare('blocked-after-rollback',june.startAt+10,'1'),{code:'BUDGET_QUOTA_EXCEEDED'});
  const candidate=preview('first');
  const first=settle('first');
  assert.deepEqual(first.budgetSettlement?.chargesByDay,[{day:'2026-05-31',creditUnits:'0'},{day:'2026-06-01',creditUnits:'2'}]);
  assert.equal(settle('second').actualCreditUnits,'1');
  finish('third',june.startAt+86400000+1000,2);
  assert.equal(settle('third').actualCreditUnits,'1');
  f.reopen();
  const receiptInput={reservationId:'first',expectedVersion:candidate.version,expectedRevision:candidate.revision!};
  assert.deepEqual(admissions.settleUsageCost(admin,tenant,receiptInput,'first',Date.now(),()=>{throw new Error('replay read costs');}),first);
  assert.throws(()=>admissions.settleUsageCost(admin,tenant,receiptInput,'fresh-key',Date.now(),()=>preview('first')),{code:'VERSION_CONFLICT'});
  assert.throws(()=>prepare('may-check',may.endAt-1,'10'),{code:'BUDGET_QUOTA_EXCEEDED'});
  assert.throws(()=>prepare('june-day-check',june.startAt+10,'9'),{code:'BUDGET_QUOTA_EXCEEDED'});
  policies.write(admin,tenant,{...policy,expectedVersion:1,idempotencyKey:'month-policy',budget:{period:'UTC_MONTH',limitCreditUnits:'10',softThresholdBps:8000}});
  assert.throws(()=>prepare('june-month-blocked',june.startAt+10,'8',2),{code:'BUDGET_QUOTA_EXCEEDED'});
  assert.equal(prepare('june-month-available',june.startAt+10,'7',2).phase,'PREPARING');
 }finally{f.finish();}
});
test('month keys are strict UTC intervals and reject malformed, pre-epoch and unsupported years',()=>{
 assert.equal(may.endAt,june.startAt);for(const bad of ['2026-5','2026-00','2026-13','1969-12','9999-12','2026-05-01'])assert.throws(()=>settlementMonth(bad),{code:'INVALID_SETTLEMENT_PERIOD'});
});
test('coverage is required independently of apparently complete accepted usage',()=>{
 const f=fixture();try{const id=f.task('one'),now=may.endAt+100,preview=f.periods.preview(viewer,tenant,'2026-05',null,now);assert.equal(preview.totalCreditUnits,null);assert.ok(preview.reasons.includes('EXECUTION_COVERAGE_UNVERIFIED'));
  assert.throws(()=>f.periods.close(admin,tenant,'2026-05',closeInput(preview.revision),null,now),{code:'PERIOD_INCOMPLETE'});
  const malformed={...f.cover('2026-05',[id],now),attempts:[id,id]};assert.throws(()=>f.periods.preview(admin,tenant,'2026-05',malformed,now),{code:'INVALID_SETTLEMENT_COVERAGE'});
 }finally{f.finish();}
});
test('closed receipt freezes source digests, exact daily buckets, immutable prices and input watermarks',()=>{
 const f=fixture();try{const first=f.task('first',may.startAt+1000,1),second=f.task('second',may.startAt+2000,1),now=may.endAt+100;const result=f.close('2026-05',[first,second],now);assert.equal(result.totalCreditUnits,'1');assert.equal(result.watermarks.usage,6);assert.equal(result.entryDigests.length,2);assert.equal(result.buckets.find(b=>b.descriptor.dimension==='cpuNanoCoreMilliseconds')!.exactCredit.numerator,'1');assert.equal(result.coverage.attempts.length,2);assert.equal(rateHash(result.basis),result.revision);assert.ok(result.basis.streamEvidence.every(proof=>rateHash(proof.state)===proof.digest));assert.deepEqual(result.basis.entryDigests,result.entryDigests);assert.deepEqual(f.periods.readClosed(viewer,tenant,'2026-05'),result);
 }finally{f.finish();}
});
test('close checks SRE and full tenant scope; Viewer may only inspect authorized complete-tenant statements',()=>{
 const f=fixture();try{const id=f.task('task'),now=may.endAt+100,coverage=f.cover('2026-05',[id],now),preview=f.periods.preview(viewer,tenant,'2026-05',coverage,now);
  assert.throws(()=>f.periods.close(viewer,tenant,'2026-05',closeInput(preview.revision),coverage,now),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>f.periods.preview({...admin,scopeTenant:'other'},tenant,'2026-05',coverage,now),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>f.periods.preview({...admin,scopeRegion:'dev'},tenant,'2026-05',coverage,now),{code:'COMPLETE_TENANT_SCOPE_REQUIRED'});
 }finally{f.finish();}
});
test('close refuses active months, expected but missing attempts and uncovered source streams',()=>{
 const f=fixture();try{const id=f.task('task'),now=may.endAt+100;
  const active=f.periods.preview(admin,tenant,'2026-06',null,now);assert.ok(active.reasons.includes('PERIOD_NOT_ENDED'));
  const missing=f.periods.preview(admin,tenant,'2026-05',f.cover('2026-05',[id,f.identity('missing')],now),now);assert.ok(missing.reasons.includes('EXPECTED_ATTEMPT_MISSING'));
  const uncovered=f.periods.preview(admin,tenant,'2026-05',f.cover('2026-05',[],now),now);assert.ok(uncovered.reasons.includes('ATTEMPT_NOT_IN_COVERAGE'));
 }finally{f.finish();}
});
test('closing is idempotent across receipt loss, later source rows, reconstruction and different keys',()=>{
 const f=fixture();try{const id=f.task('task'),now=may.endAt+100,coverage=f.cover('2026-05',[id],now),preview=f.periods.preview(admin,tenant,'2026-05',coverage,now),input=closeInput(preview.revision);const closed=f.periods.close(admin,tenant,'2026-05',input,coverage,now);
  f.task('future',june.startAt+1000);f.reopen();assert.deepEqual(f.periods.close(admin,tenant,'2026-05',input,null,now+2000),closed);assert.deepEqual(f.periods.close(admin,tenant,'2026-05',{...input,idempotencyKey:'another'},null,now+2000),closed);
  assert.throws(()=>f.periods.close(admin,tenant,'2026-05',{...input,reason:'changed'},coverage,now),{code:'IDEMPOTENCY_CONFLICT'});assert.equal(f.storage.sql.exec('SELECT id FROM tenant_period_closes').toArray().length,1);
 }finally{f.finish();}
});
test('preview/source metadata changes cause CAS conflict without a closing or idempotency record',()=>{
 const f=fixture();try{const first=f.task('first'),now=may.endAt+100,coverage=f.cover('2026-05',[first],now),preview=f.periods.preview(admin,tenant,'2026-05',coverage,now);f.task('second');
  assert.throws(()=>f.periods.close(admin,tenant,'2026-05',closeInput(preview.revision),coverage,now),{code:'VERSION_CONFLICT'});assert.equal(f.storage.sql.exec('SELECT id FROM tenant_period_closes').toArray().length,0);
 }finally{f.finish();}
});
test('statement and close receipt roll back together if the durable receipt write fails',()=>{
 const f=fixture();try{const id=f.task('task'),now=may.endAt+100,coverage=f.cover('2026-05',[id],now),preview=f.periods.preview(admin,tenant,'2026-05',coverage,now),original=f.storage.sql.exec.bind(f.storage.sql);
  f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT INTO tenant_period_close_requests'))throw new Error('receipt disk failed');return original(q,...args);};
  assert.throws(()=>f.periods.close(admin,tenant,'2026-05',closeInput(preview.revision),coverage,now),/receipt disk failed/);f.storage.sql.exec=original;assert.equal(f.periods.readClosed(admin,tenant,'2026-05'),null);assert.equal(f.storage.sql.exec('SELECT id FROM tenant_period_close_requests').toArray().length,0);assert.equal(f.periods.close(admin,tenant,'2026-05',closeInput(preview.revision),coverage,now).totalCreditUnits,'1');
 }finally{f.finish();}
});
test('UTC resource segments crossing month end belong to their own days and wait for complete terminal evidence',()=>{
 const f=fixture();try{const id=f.identity('boundary');f.bind(id,may.endAt-1);f.source.begin(id,resources,may.endAt-1);f.source.finish(id,may.endAt+1);f.deliver(id,2);
  const now=june.endAt+100,partial=f.periods.preview(admin,tenant,'2026-05',f.cover('2026-05',[id],now),now);assert.ok(partial.reasons.includes('OPEN_USAGE_STREAM'));f.deliver(id);
  const old=f.close('2026-05',[id],now),next=f.close('2026-06',[id],now);assert.equal(old.totalCreditUnits,'0');assert.equal(next.totalCreditUnits,'0');assert.deepEqual(old.buckets.map(b=>b.descriptor.periodId),['2026-05-31','2026-05-31','2026-05-31']);assert.ok(next.buckets.every(b=>b.descriptor.periodId==='2026-06-01'));
 }finally{f.finish();}
});
test('unknown prices, unprojected evidence, empty streams and unresolved provider calls cannot close',()=>{
 for(const variant of ['price','projection','open','provider']){const f=fixture(variant!=='price');try{
  const id=f.identity(variant),start=may.startAt+1000;f.bind(id,start);if(variant!=='open'){f.source.begin(id,resources,start);if(variant==='provider')f.source.providerStarted(id,1,{provider:'deepseek',model:'requested'},start+1);f.source.finish(id,start+3);f.deliver(id,Infinity,variant!=='projection');}
  const now=may.endAt+100,coverage=f.cover('2026-05',[id],now),preview=f.periods.preview(admin,tenant,'2026-05',coverage,now);assert.equal(preview.totalCreditUnits,null);assert.ok(preview.reasons.includes(variant==='price'?'PRICE_SNAPSHOT_MISSING':variant==='projection'?'UNPROJECTED_USAGE':variant==='open'?'STREAM_WITHOUT_EVENTS':'PROVIDER_USAGE_INCOMPLETE'));assert.throws(()=>f.periods.close(admin,tenant,'2026-05',closeInput(preview.revision),coverage,now),{code:'PERIOD_INCOMPLETE'});
 }finally{f.finish();}}
});
test('accepted sequence/window corruption is detected before final closing',()=>{
 const f=fixture();try{const id=f.task('task'),stream=f.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_usage_streams').toArray()[0],value=JSON.parse(stream.payload_json);value.sequence++;f.storage.sql.exec('UPDATE tenant_usage_streams SET payload_json = ?',JSON.stringify(value));const now=may.endAt+100,preview=f.periods.preview(admin,tenant,'2026-05',f.cover('2026-05',[id],now),now);assert.ok(preview.reasons.includes('SOURCE_SEQUENCE_INCOMPLETE'));assert.equal(preview.totalCreditUnits,null);
 }finally{f.finish();}
});
test('every accepted execution remains in its source period independently of task labels',()=>{
 const f=fixture();try{const failed=f.task('failed',may.startAt+1000,2),cancelled=f.task('cancelled',may.startAt+2000,2);const closed=f.close('2026-05',[failed,cancelled],may.endAt+100);assert.equal(closed.totalCreditUnits,'2');assert.equal(closed.entryDigests.length,2);
 }finally{f.finish();}
});
test('real admission reservations remain held across UTC periods and closing refusal cannot release their budget',()=>{
 const f=fixture();try{
  const policies=new TenantPolicyStore(f.storage),admissions=new AdmissionReservationStore(f.storage);
  policies.write(admin,tenant,{expectedVersion:0,resources:{concurrentAttempts:2,cpuMilliCores:20,memoryMiB:20,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'Cross-period policy',requestId:'policy',idempotencyKey:'policy'});
  const input={reservationId:'uncertain',taskId:'other',attemptId:'other-attempt',fencing:1,expectedPolicyVersion:1,resources:{concurrentAttempts:1,cpuMilliCores:1,memoryMiB:1,queuedTasks:0},estimatedCreditUnits:'17',expiresAt:may.startAt+200000};
  admissions.prepare(admin,tenant,input,'prepare',may.startAt-10);admissions.transition(admin,tenant,{reservationId:'uncertain',expectedVersion:1,generation:1,fencing:1,to:'UNCERTAIN',reason:'Unknown execution'},'uncertain',may.startAt+10);
  const before=f.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_admission_state').toArray()[0].payload_json;
  const now=may.endAt+100,coverage=f.cover('2026-05',[],now),preview=f.periods.preview(admin,tenant,'2026-05',coverage,now);assert.ok(preview.reasons.includes('UNSETTLED_ADMISSION_RESERVATIONS'));assert.throws(()=>f.periods.close(admin,tenant,'2026-05',closeInput(preview.revision),coverage,now),{code:'PERIOD_INCOMPLETE'});
  assert.equal(f.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_admission_state').toArray()[0].payload_json,before);
  assert.throws(()=>admissions.prepare(admin,tenant,{...input,reservationId:'new',taskId:'new-task',attemptId:'new-attempt',estimatedCreditUnits:'84',expiresAt:now+100000},'new',now),{code:'BUDGET_QUOTA_EXCEEDED'});
 }finally{f.finish();}
});
test('late evidence appends an associated adjustment without rewriting original statement, source or price',()=>{
 const f=fixture();try{const base=f.task('base',may.startAt+1000,1),original=f.close('2026-05',[base],may.endAt+100),late=f.task('late',may.startAt+2000,1);const before=f.periods.preview(admin,tenant,'2026-06',f.cover('2026-06',[late],june.endAt+100),june.endAt+100);assert.ok(before.reasons.includes('LATE_EVENT_NOT_ROUTED'));
  assert.equal(f.periods.reconcileLate(admin,tenant,june.startAt+1000),1);assert.equal(f.periods.reconcileLate(admin,tenant,june.startAt+2000),0);const adjustments=f.periods.adjustments(viewer,tenant,{},'s'.repeat(64));assert.equal(adjustments.items[0].originCloseDigest,original.digest);assert.equal(adjustments.items[0].targetPeriodId,'2026-06');assert.equal(adjustments.items[0].entry!.source.taskId,late.taskId);
  const juneClose=f.close('2026-06',[late],june.endAt+100);assert.equal(original.totalCreditUnits,'0');assert.equal(juneClose.totalCreditUnits,'1');assert.deepEqual(f.periods.readClosed(admin,tenant,'2026-05'),original);
 }finally{f.finish();}
});
test('late rounding deltas accumulate against previously settled exact quantities across several months',()=>{
 const f=fixture();try{const first=f.task('first',may.startAt+1000,1),original=f.close('2026-05',[first],may.endAt+100);const charged=[BigInt(original.totalCreditUnits)];
  for(const [index,period] of [june,july,august].entries()){const late=f.task('late-'+index,may.startAt+2000+index*1000,1);f.periods.reconcileLate(admin,tenant,period.startAt+1000);charged.push(BigInt(f.close(period.id,[late],period.endAt+100).totalCreditUnits));}
  assert.deepEqual(charged,[0n,1n,1n,0n]);assert.equal(charged.reduce((sum,n)=>sum+n,0n),2n);assert.deepEqual(f.periods.readClosed(admin,tenant,'2026-05'),original);
 }finally{f.finish();}
});
test('late unknown price remains pending and never becomes a zero-valued adjustment',()=>{
 const f=fixture(false);try{f.close('2026-05',[],may.endAt+100);const late=f.task('unknown');f.periods.reconcileLate(admin,tenant,june.startAt+1000);const now=june.endAt+100,preview=f.periods.preview(admin,tenant,'2026-06',f.cover('2026-06',[late],now),now);assert.equal(preview.totalCreditUnits,null);assert.ok(preview.reasons.includes('PRICE_SNAPSHOT_MISSING'));
 }finally{f.finish();}
});
test('conflicting source after close appends attention in an open period and preserves original closing bytes',()=>{
 const f=fixture();try{const id=f.task('task'),original=f.close('2026-05',[id],may.endAt+100),event=f.source.events(id)[0];assert.equal(f.inbox.receive({...event,at:event.at+1},id,june.startAt+1000).outcome,'CONFLICT');assert.equal(f.periods.reconcileLate(admin,tenant,june.startAt+2000),1);assert.equal(f.periods.reconcileLate(admin,tenant,june.startAt+3000),0);
  const rows=f.periods.adjustments(admin,tenant,{},'s'.repeat(64));assert.equal(rows.items[0].kind,'SOURCE_CONFLICT');assert.equal(original.basis.streamEvidence[0].state.quarantined,false);const now=june.endAt+100,preview=f.periods.preview(admin,tenant,'2026-06',f.cover('2026-06',[],now),now);assert.ok(preview.reasons.includes('SOURCE_CONFLICT_AFTER_CLOSE'));assert.deepEqual(f.periods.readClosed(admin,tenant,'2026-05'),original);
 }finally{f.finish();}
});
test('late routing and closing idempotency survive native SQLite close/reopen',()=>{
 const f=fixture();try{const original=f.close('2026-05',[],may.endAt+100),late=f.task('late');f.periods.reconcileLate(admin,tenant,june.startAt+1000);const before=f.periods.adjustments(admin,tenant,{},'s'.repeat(64));f.reopen();assert.deepEqual(f.periods.readClosed(admin,tenant,'2026-05'),original);assert.deepEqual(f.periods.adjustments(admin,tenant,{},'s'.repeat(64)),before);assert.equal(f.periods.reconcileLate(admin,tenant,june.startAt+2000),0);assert.equal(f.close('2026-06',[late],june.endAt+100).totalCreditUnits,'1');
 }finally{f.finish();}
});
test('adjustment pages freeze append watermarks and enforce actor-bound signed cursors',()=>{
 const f=fixture();try{f.close('2026-05',[],may.endAt+100);f.task('a');f.task('b');f.periods.reconcileLate(admin,tenant,june.startAt+1000);const first=f.periods.adjustments(admin,tenant,{limit:1},'s'.repeat(64));assert.ok(first.nextCursor);f.task('c');f.periods.reconcileLate(admin,tenant,june.startAt+2000);const next=f.periods.adjustments(admin,tenant,{limit:1,cursor:first.nextCursor!},'s'.repeat(64));assert.equal(next.nextCursor,null);assert.equal(next.watermark,first.watermark);assert.throws(()=>f.periods.adjustments(viewer,tenant,{cursor:first.nextCursor!},'s'.repeat(64)),{code:'CURSOR_STALE'});
 }finally{f.finish();}
});
test('unrouted post-close source conflict blocks a later close even when its original execution no longer overlaps',()=>{
 const f=fixture();try{const id=f.task('task');f.close('2026-05',[id],may.endAt+100);const event=f.source.events(id)[0];f.inbox.receive({...event,at:event.at+1},id,june.startAt+1000);
  const now=june.endAt+100,coverage=f.cover('2026-06',[],now),preview=f.periods.preview(admin,tenant,'2026-06',coverage,now);assert.ok(preview.reasons.includes('SOURCE_CONFLICT_NOT_ROUTED'));assert.throws(()=>f.periods.close(admin,tenant,'2026-06',closeInput(preview.revision),coverage,now),{code:'PERIOD_INCOMPLETE'});
 }finally{f.finish();}
});
test('month-start exclusive resource end does not falsely place a previous-month attempt in the new month',()=>{
 const f=fixture();try{f.task('ends-at-boundary',may.endAt-1,1);const now=june.endAt+100,preview=f.periods.preview(admin,tenant,'2026-06',f.cover('2026-06',[],now),now);assert.deepEqual(preview.reasons,[]);assert.equal(preview.totalCreditUnits,'0');
 }finally{f.finish();}
});
test('outstanding prior adjustment period prevents charging another delta against an unsettled rounding base',()=>{
 const f=fixture();try{const base=f.task('base',may.startAt+1000,1);f.close('2026-05',[base],may.endAt+100);f.task('late-june',may.startAt+2000,1);f.periods.reconcileLate(admin,tenant,june.startAt+1000);const late=f.task('late-july',may.startAt+3000,1);f.periods.reconcileLate(admin,tenant,july.startAt+1000);
  const now=july.endAt+100,preview=f.periods.preview(admin,tenant,'2026-07',f.cover('2026-07',[late],now),now);assert.ok(preview.reasons.includes('PRIOR_ADJUSTMENT_PERIOD_OPEN'));assert.equal(preview.totalCreditUnits,null);
 }finally{f.finish();}
});
test('late adjustment creation rolls back atomically and does not lose its deterministic event identity',()=>{
 const f=fixture();try{f.close('2026-05',[],may.endAt+100);f.task('late');const original=f.storage.sql.exec.bind(f.storage.sql);f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT INTO tenant_period_adjustments'))throw new Error('late disk failed');return original(q,...args);};
  assert.throws(()=>f.periods.reconcileLate(admin,tenant,june.startAt+1000),/late disk failed/);f.storage.sql.exec=original;assert.equal(f.periods.adjustments(admin,tenant,{},'s'.repeat(64)).items.length,0);assert.equal(f.periods.reconcileLate(admin,tenant,june.startAt+2000),1);
 }finally{f.finish();}
});
test('closed periods cannot be re-previewed as a changed final invoice, and corrupted closing bytes are rejected',()=>{
 const f=fixture();try{const id=f.task('task');f.close('2026-05',[id],may.endAt+100);assert.throws(()=>f.periods.preview(admin,tenant,'2026-05',null,may.endAt+100),{code:'PERIOD_ALREADY_CLOSED'});const row=f.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_period_closes').toArray()[0];const value=JSON.parse(row.payload_json);value.totalCreditUnits='99';f.storage.sql.exec('UPDATE tenant_period_closes SET payload_json = ?',JSON.stringify(value));assert.throws(()=>f.periods.readClosed(admin,tenant,'2026-05'),{code:'PERIOD_STATEMENT_CORRUPT'});
 }finally{f.finish();}
});
test('provider response across UTC month boundary flushes resource segments first and bills token evidence in response month',()=>{
 const f=fixture();try{const id=f.identity('provider-boundary'),start=may.endAt-2;f.bind(id,start);f.source.begin(id,resources,start);f.source.providerStarted(id,1,{provider:'deepseek',model:'requested'},start+1);
  f.source.providerFinished(id,1,{reportedModel:'actual',responseId:'boundary',inputTokens:'2',outputTokens:'0',totalTokens:'2',cacheHitTokens:null,cacheMissTokens:null,evidenceDigest:'b'.repeat(64),source:'PROVIDER_RESPONSE',completeness:'COMPLETE'},may.endAt+1);f.source.finish(id,may.endAt+2);f.deliver(id);
  const now=june.endAt+100,old=f.close('2026-05',[id],now),next=f.close('2026-06',[id],now);assert.equal(old.totalCreditUnits,'1');assert.equal(next.totalCreditUnits,'3');assert.ok(!old.buckets.some(b=>b.descriptor.dimension==='inputTokens'));assert.equal(next.buckets.find(b=>b.descriptor.dimension==='inputTokens')!.creditUnits,'2');
 }finally{f.finish();}
});
