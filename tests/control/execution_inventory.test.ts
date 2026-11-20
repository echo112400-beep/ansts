import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {ExecutionInventory,type InventoryObservation,type InventorySnapshot} from '../../src/control/execution_inventory.js';
import {collectExecutionCoverage} from '../../src/finops/execution_coverage.js';
import {AnstsSchedulerCell} from '../../src/cloudflare/scheduler_cell.js';
import {AnstsBodyCell} from '../../src/cloudflare/body_cell.js';
import {TaskSpec,TaskState} from '../../src/models.js';
import {bodyInventoryIngress,executionInventoryIngress,settlementCoverageIngress} from '../../src/control/execution_inventory_ingress.js';
import {bodyServiceToken} from '../../src/cloudflare/body_auth.js';
import type {DirectoryStorage} from '../../src/control/tenant_directory.js';
import type {CloudflareCelldEnv,DurableObjectStateLike} from '../../src/cloudflare/env.js';
const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');
const start=Date.UTC(2026,4,1),end=Date.UTC(2026,5,1),now=end+100;
function database(){
 const db=new DatabaseSync(':memory:');
 const storage:DirectoryStorage={sql:{exec<T>(query:string,...args:unknown[]){const rows=db.prepare(query).all(...args) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const value=fn();db.exec('COMMIT');return value;}catch(error){db.exec('ROLLBACK');throw error;}}};
 const ctx={id:{toString:()=> 'source-cell'},storage:{...storage,sql:{exec<T>(query:string,...args:unknown[]){const rows=storage.sql.exec<T>(query,...args).toArray();return {toArray:()=>rows,one:()=>rows[0]??null};}},get:async()=>undefined,put:async()=>{},delete:async()=>true,setAlarm:async()=>{},getAlarm:async()=>null},blockConcurrencyWhile:async<T>(f:()=>Promise<T>)=>f()} satisfies DurableObjectStateLike;
 return {db,storage,ctx};
}
function observation(extra:Partial<InventoryObservation>={}):InventoryObservation{return {tenantId:'tenant',taskId:'task',inputDigest:'a'.repeat(64),state:'QUEUED',fencing:0,nodeId:null,attemptId:null,version:null,sourceAt:start+1,...extra};}
function inventories(){const scheduler=database(),body=database(),s=new ExecutionInventory(scheduler.storage,'SCHEDULER','scheduler',()=>[],start-10),b=new ExecutionInventory(body.storage,'BODY','body',()=>[],start-10);
 const sched=observation({state:'LEASED',fencing:1,nodeId:'body-1'});s.record(sched,()=>{},start+1);
 for(const [index,state] of ['ADMITTED','RUNNING','RESULT_PENDING','COMPLETED'].entries())b.record({...sched,state,attemptId:'attempt-1',version:index+1,sourceAt:start+index+1},()=>{},start+index+1);
 return {s,b,scheduler,body,sources:{scheduler:async()=>s.snapshot('tenant',now),body:async()=>b.snapshot('tenant',now)},close(){scheduler.db.close();body.db.close();}};}
test('source history and state mutation roll back atomically, replay deduplicates, tenant snapshots are isolated',()=>{
 const f=database();try{const journal=new ExecutionInventory(f.storage,'SCHEDULER','source',()=>[],start);f.db.exec('CREATE TABLE state (id TEXT)');
  const original=f.storage.sql.exec;f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT INTO execution_inventory_events'))throw Error('disk full');return original(q,...args);};
  assert.throws(()=>journal.record(observation(),()=>f.db.exec("INSERT INTO state VALUES ('task')"),start+1),/disk full/);f.storage.sql.exec=original;
  assert.equal(f.storage.sql.exec('SELECT * FROM state').toArray().length,0);assert.equal(journal.snapshot('tenant',now).count,0);
  journal.record(observation(),()=>{},start+1);journal.record(observation(),()=>{},start+2);assert.equal(journal.snapshot('tenant',now).count,1);assert.equal(journal.snapshot('other',now).count,0);
  const before=journal.snapshot('tenant',now);const rebuilt=new ExecutionInventory(f.storage,'SCHEDULER','source',()=>{throw Error('must not bootstrap twice');},now);assert.deepEqual(rebuilt.snapshot('tenant',now),before);
 }finally{f.db.close();}
});
test('missing/tampered journal rows and foreign source identity cannot establish coverage',()=>{
 const f=database();try{const journal=new ExecutionInventory(f.storage,'BODY','body',()=>[],start);journal.record(observation(),()=>{},start+1);
  f.storage.sql.exec('DELETE FROM execution_inventory_events WHERE tenant_id = ?', 'tenant');assert.throws(()=>journal.snapshot('tenant',now),{code:'EXECUTION_INVENTORY_CORRUPT'});
  assert.throws(()=>new ExecutionInventory(f.storage,'SCHEDULER','body',()=>[],start),{code:'EXECUTION_INVENTORY_CORRUPT'});
 }finally{f.db.close();}
});
test('native Scheduler keeps discovery after task purge and constructor rehydration',async()=>{
 const f=database();try{let cell=new AnstsSchedulerCell(f.ctx);const task=new TaskSpec({task_id:'task',tenant_id:'tenant',prompt:'private input'});await cell.submitTask(task);task.state=TaskState.COMPLETED;await cell.alarm();
  const read=async()=>{const response=await cell.fetch(new Request('https://cell/v1/scheduler/execution-inventory?tenantId=tenant',{headers:{'x-ansts-verified-inventory':'true'}}));assert.equal(response.status,200);return response.json() as Promise<InventorySnapshot>;};
  const before=await read();assert.ok(before.entries.some(e=>e.observation.state==='COMPLETED'));assert.ok(!JSON.stringify(before).includes('private input'));
  const purge=await cell.fetch(new Request('https://cell/v1/scheduler/purge',{method:'POST',body:JSON.stringify({states:['COMPLETED']})}));assert.equal(purge.status,200);assert.equal(f.storage.sql.exec('SELECT * FROM ansts_tasks').toArray().length,0);
  cell=new AnstsSchedulerCell(f.ctx);assert.equal((await read()).watermark,before.watermark);
  assert.equal((await cell.fetch(new Request('https://cell/v1/scheduler/execution-inventory?tenantId=tenant'))).status,403);
 }finally{f.db.close();}
});
test('native Body history survives phase changes and reconstruction; no result payload is copied',async()=>{
 const f=database();try{let cell=new AnstsBodyCell(f.ctx);const identity={tenant_id:'tenant',task_id:'task',attempt_id:'attempt',node_id:'body-1',fencing_token:1,input_digest:'a'.repeat(64)};
  for(const [index,route] of ['acquire','start','finish','acknowledge'].entries()){const response=await cell.fetch(new Request('https://cell/v1/body/'+route,{method:'POST',body:JSON.stringify({...identity,expected_version:index,result:{secret:'private result'}})}));assert.equal(response.status,200);}
  cell=new AnstsBodyCell(f.ctx);const response=await cell.fetch(new Request('https://cell/v1/body/execution-inventory?tenantId=tenant',{headers:{'x-ansts-verified-inventory':'true'}})),value=await response.json() as InventorySnapshot;assert.equal(response.status,200);assert.deepEqual(value.entries.map(e=>e.observation.state),['ADMITTED','RUNNING','RESULT_PENDING','COMPLETED']);assert.ok(!JSON.stringify(value).includes('private result'));
 }finally{f.db.close();}
});
test('trusted full inventories produce canonical attempts, stable revision, and no final bill',async()=>{
 const f=inventories();try{const result=await collectExecutionCoverage('tenant','2026-05',f.sources,()=>now);assert.deepEqual(result.reasons,[]);assert.equal(result.coverage.complete,true);assert.equal(result.coverage.attempts[0].producerId,'body-1');assert.equal(result.final,false);
  f.s.record(observation({state:'COMPLETED',fencing:1,nodeId:null}),()=>{},start+8);const next=await collectExecutionCoverage('tenant','2026-05',f.sources,()=>now);assert.equal(next.coverage.complete,true);assert.equal(next.coverage.attempts.length,1);
 }finally{f.close();}
});
test('a new task can prove a non-empty month with a Body journal created after the month starts',async()=>{
 const scheduler=database(),body=database();
 try{
  const s=new ExecutionInventory(scheduler.storage,'SCHEDULER','scheduler',()=>[],start-100);
  const b=new ExecutionInventory(body.storage,'BODY','body',()=>[],start+20);
  const queued=observation({sourceAt:start+10}),leased={...queued,state:'LEASED',fencing:1,nodeId:'body-1'};
  s.record({...queued,state:'SUBMITTED'},()=>{},start+10);s.record(queued,()=>{},start+11);s.record(leased,()=>{},start+20);
  for(const [index,state] of ['ADMITTED','RUNNING','RESULT_PENDING','COMPLETED'].entries()){
   b.record({...leased,state,attemptId:'new-attempt',version:index+1,sourceAt:start+21+index},()=>{},start+21+index);
  }
  const sources={schedulerPage:async(after:number)=>s.page('tenant',after,now),body:async()=>b.snapshot('tenant',now)};
  const verified=await collectExecutionCoverage('tenant','2026-05',sources,()=>now);
  assert.deepEqual(verified.reasons,[]);
  assert.equal(verified.coverage.complete,true);
  assert.equal(verified.coverage.attempts[0].attemptId,'new-attempt');
  b.importBaseline(()=>{},()=>[],start+50);
  const imported=await collectExecutionCoverage('tenant','2026-05',sources,()=>now);
  assert.equal(imported.coverage.complete,false);
  assert.ok(imported.reasons.includes('HISTORICAL_COVERAGE_UNAVAILABLE'));
 }finally{scheduler.db.close();body.db.close();}
});
test('pre-cutover periods, imported baselines, missing Body and changing Scheduler fail closed',async()=>{
 const f=inventories();try{
  const old=await collectExecutionCoverage('tenant','2026-04',f.sources,()=>now);assert.ok(old.reasons.includes('HISTORICAL_COVERAGE_UNAVAILABLE'));
  const missing=await collectExecutionCoverage('tenant','2026-05',{...f.sources,body:async()=>{throw Error('offline');}},()=>now);assert.equal(missing.coverage.complete,false);assert.ok(missing.reasons.includes('BODY_INVENTORY_UNAVAILABLE'));
  let reads=0;const changed=await collectExecutionCoverage('tenant','2026-05',{...f.sources,scheduler:async()=>{if(reads++)f.s.record(observation({state:'CANCELLED',fencing:1,nodeId:'body-1'}),()=>{},now-1);return f.s.snapshot('tenant',now);}},()=>now);assert.ok(changed.reasons.includes('SCHEDULER_INVENTORY_CHANGED'));
  f.b.importBaseline(()=>{},()=>[observation({state:'RUNNING',fencing:1,nodeId:'body-1',attemptId:'legacy',version:1})],start+10);const imported=await collectExecutionCoverage('tenant','2026-05',f.sources,()=>now);assert.ok(imported.reasons.includes('HISTORICAL_COVERAGE_UNAVAILABLE'));assert.ok(imported.reasons.includes('BODY_ATTEMPT_HISTORY_UNPROVEN'));
 }finally{f.close();}
});
test('truncated, foreign and forged inventory responses cannot become full coverage',async()=>{
 const f=inventories();try{
  const partial=await collectExecutionCoverage('tenant','2026-05',{...f.sources,scheduler:async()=>({...f.s.snapshot('tenant',now),complete:false,reasons:['INVENTORY_SCAN_LIMIT']})},()=>now);assert.equal(partial.coverage.complete,false);assert.deepEqual(partial.coverage.attempts,[]);
  await assert.rejects(()=>collectExecutionCoverage('tenant','2026-05',{...f.sources,scheduler:async()=>({...f.s.snapshot('tenant',now),count:0})},()=>now),{code:'EXECUTION_INVENTORY_INVALID'});
  const foreign=await collectExecutionCoverage('tenant','2026-05',{...f.sources,body:async()=>f.b.snapshot('other',now)},()=>now);assert.equal(foreign.coverage.complete,false);
 }finally{f.close();}
});
test('private ingress rejects browser, Host, arbitrary source fields and caller complete claims',async()=>{
 const token='d'.repeat(64),secret='a'.repeat(64),env={ANSTS_DIRECTORY_SERVICE_TOKEN:token,ANSTS_BODY_AUTH_SECRET:secret} as CloudflareCelldEnv;
 const post=(payload:object,auth='Bearer '+token)=>new Request('https://fleet/internal/v1/execution-inventory',{method:'POST',headers:{authorization:auth},body:JSON.stringify(payload)});
 assert.equal((await executionInventoryIngress(post({tenantId:'tenant',source:'SCHEDULER'},'Body '+bodyServiceToken(secret,'body-1')),env)).status,401);
 assert.equal((await executionInventoryIngress(post({tenantId:'tenant',source:'SCHEDULER',shard:'injected'}),env)).status,422);
 assert.equal((await settlementCoverageIngress(post({tenantId:'tenant',periodId:'2026-05',complete:true}),env)).status,422);
 assert.equal((await bodyInventoryIngress(new Request('https://body/internal/v1/execution-inventory/body?tenantId=tenant&taskId=task',{headers:{authorization:'Body '+bodyServiceToken(secret,'body-1'),'x-ansts-verified-inventory':'true'}}),env)).status,401);
});
