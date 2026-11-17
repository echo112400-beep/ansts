import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {TenantPolicyStore,type QuotaPolicyWrite} from '../../src/finops/tenant_policy_store.js';
import type {DirectoryStorage,DirectorySql,TenantRecord} from '../../src/control/tenant_directory.js';
const load=createRequire(join(process.cwd(),'package.json'));
interface Database {exec(q:string):void;prepare(q:string):{all(...bindings:unknown[]):unknown[]};close():void}
const {DatabaseSync}=load('node:sqlite') as {DatabaseSync:new(path:string)=>Database};
function open(path:string){const db=new DatabaseSync(path);const sql:DirectorySql={exec<T>(query:string,...values:unknown[]){const rows=db.prepare(query).all(...values) as T[];return {toArray:()=>rows};}};const storage:DirectoryStorage={sql,transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}}};return {db,storage,store:new TenantPolicyStore(storage)};}
const actor={sub:'github:1',role:'SRE_ADMIN' as const,scopeTenant:'all',scopeRegion:'all'};
const tenant:TenantRecord={id:'a',name:'A',region:'dev',costCenter:'eng',version:1,status:'ACTIVE'};
const input:QuotaPolicyWrite={expectedVersion:0,resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'9007199254740993',softThresholdBps:8000},reason:'allocate internal capacity',idempotencyKey:'create',requestId:'r1'};
test('tenant policy versions and receipts persist, lower limits retain historical snapshots',()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-policy-'));const path=join(dir,'state.sqlite');let s=open(path);
 try{
  const first=s.store.write(actor,tenant,input);s.db.close();s=open(path);
  assert.deepEqual(s.store.write(actor,tenant,{...input,requestId:'r2'}),first);
  const second=s.store.write(actor,tenant,{...input,expectedVersion:1,resources:{...input.resources,concurrentAttempts:0},idempotencyKey:'lower'});
  assert.equal(second.version,2);assert.equal(s.store.read(actor,tenant).history[0].resources.concurrentAttempts,1);
  assert.equal(s.store.read(actor,tenant).current?.budget.limitCreditUnits,'9007199254740993');
  assert.equal(s.store.read(actor,tenant).enforcement,'NOT_CONNECTED');
  assert.throws(()=>s.store.write(actor,tenant,{...input,idempotencyKey:'stale'}),{code:'VERSION_CONFLICT'});
  assert.throws(()=>s.store.write(actor,tenant,{...input,reason:'different'}),{code:'IDEMPOTENCY_CONFLICT'});
 }finally{s.db.close();rmSync(dir,{recursive:true,force:true});}
});
test('policy authority binds tenant identity and verifies both region and role',()=>{
 const s=open(':memory:');try{
  s.store.write(actor,tenant,input);
  assert.throws(()=>s.store.write({...actor,role:'OPERATOR'},tenant,input),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>s.store.read({...actor,scopeTenant:'b'},tenant),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>s.store.read({...actor,scopeRegion:'other'},tenant),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>s.store.read(actor,{...tenant,id:'b'}),{code:'TENANT_CONTROL_IDENTITY_CONFLICT'});
  assert.throws(()=>s.store.write(actor,{...tenant,status:'DISABLED'},{...input,expectedVersion:1,idempotencyKey:'disabled'}),{code:'TENANT_DISABLED'});
  assert.equal(s.store.read({...actor,role:'VIEWER'},tenant).current?.version,1);
 }finally{s.db.close();}
});
test('policy commit failure cannot publish history or consume an idempotency key',()=>{
 const s=open(':memory:');try{
  const original=s.storage.sql.exec.bind(s.storage.sql);s.storage.sql.exec=(query,...values)=>{if(query.startsWith('INSERT'))throw new Error('disk failure');return original(query,...values);};
  assert.throws(()=>s.store.write(actor,tenant,input),/disk failure/);s.storage.sql.exec=original;
  assert.equal(s.store.read(actor,tenant).history.length,0);assert.equal(s.store.write(actor,tenant,input).version,1);
 }finally{s.db.close();}
});
test('policy history cursor binds scope, metadata and policy snapshot',()=>{
 const s=open(':memory:');try{
  for(let version=0;version<3;version++)s.store.write(actor,tenant,{...input,expectedVersion:version,idempotencyKey:'page-'+version});
  const first=s.store.read(actor,tenant,{limit:1});assert.equal(first.history.length,1);assert.equal(first.current?.version,3);assert.equal(first.historyTotal,3);
  assert.equal(s.store.read(actor,tenant,{limit:1,cursor:first.nextCursor!}).history[0].version,2);
  assert.throws(()=>s.store.read({...actor,sub:'github:other'},tenant,{cursor:first.nextCursor!}),{code:'CURSOR_STALE'});
  assert.throws(()=>s.store.read(actor,{...tenant,version:2},{cursor:first.nextCursor!}),{code:'CURSOR_STALE'});
  assert.throws(()=>s.store.read(actor,tenant,{limit:101}),{code:'INVALID_PAGE_LIMIT'});
  s.store.write(actor,tenant,{...input,expectedVersion:3,idempotencyKey:'page-new'});
  assert.throws(()=>s.store.read(actor,tenant,{cursor:first.nextCursor!}),{code:'CURSOR_STALE'});
 }finally{s.db.close();}
});
