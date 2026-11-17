import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TenantDirectory, DirectoryStorage, DirectorySql } from '../../src/control/tenant_directory.js';
const loadBuiltin = createRequire(join(process.cwd(), 'package.json'));
interface Database { exec(q:string):void; prepare(q:string):{all(...bindings:unknown[]):unknown[]}; close():void }
const { DatabaseSync } = loadBuiltin('node:sqlite') as {DatabaseSync: new(path:string)=>Database};
function open(path:string) {
  const db = new DatabaseSync(path);
  const sql: DirectorySql = {exec<T>(q:string,...bindings:unknown[]) {
    const rows = db.prepare(q).all(...bindings) as T[];
    return {toArray:()=>rows};
  }};
  const storage: DirectoryStorage = {sql,transactionSync<T>(fn:()=>T):T {
    db.exec('BEGIN IMMEDIATE');
    try { const result=fn(); db.exec('COMMIT'); return result; }
    catch(e) { db.exec('ROLLBACK'); throw e; }
  }};
  const directory=new TenantDirectory(storage);
  if(!directory.costCenters(admin).length) for(const id of ['eng','research']) directory.mutateCostCenter(admin,{id,name:id,status:'ACTIVE',expectedVersion:0,reason:'test setup',requestId:id,idempotencyKey:id});
  return {db,storage,directory};
}
const admin={sub:'github:1',role:'SRE_ADMIN' as const,scopeTenant:'all',scopeRegion:'all'};
const input={id:'a',region:'dev',name:'Team A',costCenter:'eng',status:'ACTIVE' as const,expectedVersion:0,reason:'initial setup',requestId:'req1',idempotencyKey:'key1'};
test('SQLite restart preserves tenant, receipt and audit; stale writes cannot commit', () => {
  const dir=mkdtempSync(join(tmpdir(),'ansts-dir-')); const path=join(dir,'state.sqlite');
  let s=open(path);
  try {
    const first=s.directory.mutate(admin,input); s.db.close(); s=open(path);
    assert.deepEqual(s.directory.mutate(admin,{...input,requestId:'req2'}),first);
    assert.equal(s.directory.audit(admin).length,1);
    assert.throws(()=>s.directory.mutate(admin,{...input,idempotencyKey:'other'}),{code:'VERSION_CONFLICT'});
    assert.throws(()=>s.directory.mutate(admin,{...input,name:'changed'}),{code:'IDEMPOTENCY_CONFLICT'});
    assert.equal(s.directory.list(admin).tenants.length,1);
  } finally {s.db.close();rmSync(dir,{recursive:true,force:true});}
});
test('scope checks cover old and new region and readonly roles', () => {
  const s=open(':memory:');
  try {
    s.directory.mutate(admin,input);
    assert.equal(s.directory.list({...admin,scopeTenant:'b'}).tenants.length,0);
    assert.throws(()=>s.directory.mutate({...admin,role:'VIEWER'},input),{code:'CONTROL_FORBIDDEN'});
    assert.throws(()=>s.directory.mutate({...admin,scopeRegion:'other'},{...input,region:'other',expectedVersion:1}),{code:'CONTROL_FORBIDDEN'});
  } finally {s.db.close();}
});
test('commit failure rolls back tenant, receipt and audit together', () => {
  const s=open(':memory:');
  try {
    const original=s.storage.sql.exec.bind(s.storage.sql);
    s.storage.sql.exec=(q,...args)=> {if(q.startsWith('INSERT')) throw new Error('disk failure');return original(q,...args);};
    assert.throws(()=>s.directory.mutate(admin,input),/disk failure/);
    s.storage.sql.exec=original;
    assert.equal(s.directory.list(admin).tenants.length,0);
    assert.equal(s.directory.audit(admin).length,0);
    assert.equal(s.directory.mutate(admin,input).version,1);
  } finally {s.db.close();}
});

test('pagination is scoped, bounded, and rejects a changed snapshot',()=>{
  const s=open(':memory:');
  try {
    for (const id of ['c','a','b']) s.directory.mutate(admin,{...input,id,idempotencyKey:id});
    const first=s.directory.listPage(admin,{limit:2});
    assert.deepEqual(first.items.map(t=>t.id),['a','b']);
    assert.equal(s.directory.listPage(admin,{cursor:first.nextCursor!,limit:2}).items[0].id,'c');
    assert.throws(()=>s.directory.listPage({...admin,scopeTenant:'a'},{cursor:first.nextCursor!}),{code:'CURSOR_STALE'});
    assert.throws(()=>s.directory.listPage(admin,{limit:101}),{code:'INVALID_PAGE_LIMIT'});
    s.directory.mutate(admin,{...input,id:'d',idempotencyKey:'d'});
    assert.throws(()=>s.directory.listPage(admin,{cursor:first.nextCursor!}),{code:'CURSOR_STALE'});
  } finally {s.db.close();}
});
test('cost center reassignment retains history and audit diff through disable',()=>{
  const s=open(':memory:');
  try {
    s.directory.mutate(admin,input);
    s.directory.mutate(admin,{...input,costCenter:'research',expectedVersion:1,idempotencyKey:'reassign'});
    const disabled=s.directory.mutate(admin,{...input,costCenter:'research',status:'DISABLED',expectedVersion:2,idempotencyKey:'disable'});
    assert.deepEqual(disabled.costCenterHistory?.map(h=>h.costCenter),['eng','research']);
    const audit=s.directory.auditPage(admin,{limit:2});
    assert.deepEqual(audit.items[1].changes,[{field:'costCenter',before:'eng',after:'research'}]);
    assert.equal(s.directory.auditPage(admin,{cursor:audit.nextCursor!}).items.length,1);
    assert.throws(()=>s.directory.auditPage({...admin,role:'VIEWER'}),{code:'CONTROL_FORBIDDEN'});
  } finally {s.db.close();}
});

test('cost center assignments reject unknown or disabled centers and retain existing history',()=>{
  const s=open(':memory:');
  try {
    assert.throws(()=>s.directory.mutate(admin,{...input,costCenter:'unknown'}),{code:'COST_CENTER_NOT_ACTIVE'});
    s.directory.mutate(admin,input);
    s.directory.mutateCostCenter(admin,{id:'eng',name:'Engineering',status:'DISABLED',expectedVersion:1,reason:'retire',requestId:'retire',idempotencyKey:'retire'});
    assert.throws(()=>s.directory.mutate(admin,{...input,id:'new',idempotencyKey:'new'}),{code:'COST_CENTER_NOT_ACTIVE'});
    assert.equal(s.directory.mutate(admin,{...input,name:'Renamed',expectedVersion:1,idempotencyKey:'rename'}).version,2);
    assert.throws(()=>s.directory.mutateCostCenter({...admin,scopeTenant:'a'},{id:'x',name:'X',status:'ACTIVE',expectedVersion:0,reason:'invalid',requestId:'x',idempotencyKey:'x'}),{code:'CONTROL_FORBIDDEN'});
    assert.deepEqual(s.directory.costCenters({...admin,scopeTenant:'a'}).map(c=>c.id),['eng']);
    assert.deepEqual(s.directory.costCenters({...admin,scopeTenant:'other'}),[]);
  } finally {s.db.close();}
});
test('cost center metadata uses durable CAS and idempotent receipts',()=>{
  const s=open(':memory:');
  try {
    const mutation={id:'new',name:'New',status:'ACTIVE' as const,expectedVersion:0,reason:'register',requestId:'new',idempotencyKey:'new'};
    const first=s.directory.mutateCostCenter(admin,mutation);
    assert.deepEqual(s.directory.mutateCostCenter(admin,mutation),first);
    assert.throws(()=>s.directory.mutateCostCenter(admin,{...mutation,name:'Other'}),{code:'IDEMPOTENCY_CONFLICT'});
    assert.throws(()=>s.directory.mutateCostCenter(admin,{...mutation,idempotencyKey:'next'}),{code:'VERSION_CONFLICT'});
    assert.equal(s.directory.costCenters(admin).filter(c=>c.id==='new').length,1);
  } finally {s.db.close();}
});

test('unified audit binds domain and scope to cursor and includes cost center changes',()=>{
 const s=open(':memory:');
 try {
  s.directory.mutate(admin,input);
  const all=s.directory.unifiedAuditPage(admin,{limit:1});
  assert.equal(all.items.length,1);assert.ok(all.nextCursor);
  assert.throws(()=>s.directory.unifiedAuditPage(admin,{domain:'tenant',cursor:all.nextCursor!}),{code:'CURSOR_STALE'});
  const center=s.directory.unifiedAuditPage(admin,{domain:'cost-center'});
  assert.equal(center.items.length,2);assert.equal(center.items[0].domain,'cost-center');
  assert.ok(center.items[0].changes.length>0);
  const scoped=s.directory.unifiedAuditPage({...admin,scopeTenant:'a'});
  assert.equal(scoped.items.length,1);assert.equal(scoped.items[0].targetId,'a');
  assert.throws(()=>s.directory.unifiedAuditPage({...admin,role:'VIEWER'}),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>s.directory.unifiedAuditPage(admin,{domain:'invalid'}),{code:'INVALID_AUDIT_DOMAIN'});
 } finally {s.db.close();}
});

test('operation receipts survive restart, stay immutable and conceal out-of-scope IDs',()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-operations-'));const path=join(dir,'state.sqlite');let s=open(path);
 try {
  s.directory.mutate(admin,input);
  const id=s.directory.operationId('tenant',input.id,input.idempotencyKey);
  const first=s.directory.operation(admin,id);assert.equal(first.status,'SUCCEEDED');assert.equal(first.resultVersion,1);
  s.directory.mutate(admin,{...input,name:'New name',expectedVersion:1,idempotencyKey:'next'});
  s.db.close();s=open(path);assert.deepEqual(s.directory.operation(admin,id),first);
  assert.throws(()=>s.directory.operation({...admin,scopeTenant:'other'},id),{code:'OPERATION_NOT_FOUND'});
  assert.throws(()=>s.directory.operation({...admin,scopeRegion:'other'},id),{code:'OPERATION_NOT_FOUND'});
  assert.throws(()=>s.directory.operation({...admin,role:'VIEWER'},id),{code:'CONTROL_FORBIDDEN'});
  const center=s.directory.operationId('cost-center','eng','eng');
  assert.equal(s.directory.operation(admin,center).targetId,'eng');
  assert.throws(()=>s.directory.operation({...admin,scopeTenant:'a'},center),{code:'OPERATION_NOT_FOUND'});
  assert.throws(()=>s.directory.operation(admin,'tenant:'+ '0'.repeat(64)),{code:'OPERATION_NOT_FOUND'});
 } finally {s.db.close();rmSync(dir,{recursive:true,force:true});}
});

test('archives verify payload, retain every receipt and replay safely across later writes',()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-archives-'));const path=join(dir,'state.sqlite');let s=open(path);
 try {
  s.directory.mutate(admin,input);
  const version=s.directory.archiveStatus(admin).currentVersion;
  const request={expectedVersion:version,reason:'verified backup',requestId:'backup',idempotencyKey:'backup'};
  const manifest=s.directory.archive(admin,request);
  assert.equal(manifest.receiptRetention,'ALL_RETAINED');assert.equal(manifest.safeReplayWatermark,null);
  assert.equal(s.directory.readArchive(admin,manifest.id).snapshot.receipts!==undefined,true);
  s.directory.mutate(admin,{...input,name:'later',expectedVersion:1,idempotencyKey:'later'});
  s.db.close();s=open(path);
  assert.deepEqual(s.directory.archive(admin,request),manifest);
  assert.equal(s.directory.readArchive(admin,manifest.id).snapshot.tenants[0].name,'Team A');
  assert.equal(s.directory.mutate(admin,input).version,1);
  assert.throws(()=>s.directory.archive(admin,{...request,idempotencyKey:'stale'}),{code:'VERSION_CONFLICT'});
  assert.throws(()=>s.directory.archive(admin,{...request,reason:'changed'}),{code:'IDEMPOTENCY_CONFLICT'});
  assert.throws(()=>s.directory.readArchive({...admin,scopeTenant:'a'},manifest.id),{code:'CONTROL_FORBIDDEN'});
  s.storage.sql.exec('UPDATE control_directory_archives SET payload_json = ? WHERE id = ?','{}',manifest.id);
  assert.throws(()=>s.directory.readArchive(admin,manifest.id),{code:'ARCHIVE_INTEGRITY_FAILED'});
 } finally {s.db.close();rmSync(dir,{recursive:true,force:true});}
});

test('adding archive tables to an existing directory preserves tenant receipts',()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-archive-migration-'));const path=join(dir,'state.sqlite');let s=open(path);
 try {
  const first=s.directory.mutate(admin,input);
  s.db.exec('DROP TABLE control_directory_archive_requests');s.db.exec('DROP TABLE control_directory_archives');
  s.db.close();s=open(path);
  assert.deepEqual(s.directory.mutate(admin,input),first);
  assert.equal(s.directory.archiveStatus(admin).items.length,0);
  const archive=s.directory.archive(admin,{expectedVersion:s.directory.archiveStatus(admin).currentVersion,reason:'after migration',requestId:'migrate',idempotencyKey:'migrate'});
  assert.equal(s.directory.readArchive(admin,archive.id).snapshot.tenants[0].id,input.id);
 }finally{s.db.close();rmSync(dir,{recursive:true,force:true});}
});

test('archive and request receipt roll back together when receipt write fails',()=>{
 const s=open(':memory:');
 try {
  const request={expectedVersion:s.directory.archiveStatus(admin).currentVersion,reason:'atomic backup',requestId:'atomic',idempotencyKey:'atomic'};
  const original=s.storage.sql.exec.bind(s.storage.sql);
  s.storage.sql.exec=(query,...bindings)=>{if(query.startsWith('INSERT INTO control_directory_archive_requests'))throw new Error('receipt disk failure');return original(query,...bindings);};
  assert.throws(()=>s.directory.archive(admin,request),/receipt disk failure/);
  s.storage.sql.exec=original;
  assert.equal(s.directory.archiveStatus(admin).items.length,0);
  assert.equal(s.directory.archive(admin,request).sourceVersion,request.expectedVersion);
 }finally{s.db.close();}
});

test('directory RBAC matrix enforces role, tenant and region for reads and writes',()=>{
 for(const role of ['VIEWER','OPERATOR','SRE_ADMIN'] as const) for(const [scopeTenant,scopeRegion,visible,writable] of [['all','all',true,true],['a','dev',true,true],['a','all',true,true],['all','dev',true,true],['other','all',false,false],['all','other',false,false],['other','dev',false,false],['a','other',false,false]] as const) {
  const s=open(':memory:');
  try {
   s.directory.mutate(admin,input);
   const actor={sub:'github:2',role,scopeTenant,scopeRegion};
   assert.equal(s.directory.listPage(actor).items.length,visible?1:0);
   assert.equal(s.directory.costCenters(actor).some(center=>center.id==='eng'),visible);
   const write=()=>s.directory.mutate(actor,{...input,name:'Renamed',expectedVersion:1,idempotencyKey:'matrix'});
   if(role==='SRE_ADMIN'&&writable)assert.equal(write().version,2);else assert.throws(write,{code:'CONTROL_FORBIDDEN'});
   if(role==='SRE_ADMIN')assert.equal(s.directory.unifiedAuditPage(actor,{domain:'tenant'}).items.length,visible?(writable?2:1):0);else assert.throws(()=>s.directory.unifiedAuditPage(actor),{code:'CONTROL_FORBIDDEN'});
   const centerWrite=()=>s.directory.mutateCostCenter(actor,{id:'extra',name:'Extra',status:'ACTIVE',expectedVersion:0,reason:'matrix',requestId:'matrix',idempotencyKey:'matrix'});
   if(role==='SRE_ADMIN'&&scopeTenant==='all'&&scopeRegion==='all')assert.equal(centerWrite().version,1);else assert.throws(centerWrite,{code:'CONTROL_FORBIDDEN'});
  }finally{s.db.close();}
 }
});
test('directory replay is stable across UTC day and month boundaries',()=>{
 const originalDate=globalThis.Date;let now=originalDate.UTC(2026,10,15,23,59,59);
 const clock=new Proxy(originalDate,{construct(target,args,newTarget){return Reflect.construct(target,args.length?args:[now],newTarget);},get(target,key,receiver){return key==='now'?()=>now:Reflect.get(target,key,receiver);}});
 globalThis.Date=clock;
 const s=open(':memory:');
 try {
  const first=s.directory.mutate(admin,input);now+=120000;
  assert.deepEqual(s.directory.mutate(admin,input),first);
  now+=32*86400000;assert.deepEqual(s.directory.mutate(admin,input),first);
  assert.equal(s.directory.audit(admin).length,1);
  assert.throws(()=>s.directory.mutate(admin,{...input,name:'Different payload'}),{code:'IDEMPOTENCY_CONFLICT'});
 }finally{globalThis.Date=originalDate;s.db.close();}
});
test('audit and archive persist operation evidence without raw idempotency credentials',()=>{
 const s=open(':memory:');
 try {
  const privateKey='raw-private-idempotency-credential-never-export';
  s.directory.mutate(admin,{...input,idempotencyKey:privateKey,requestId:'evidence-request'});
  const archive=s.directory.archive(admin,{expectedVersion:s.directory.archiveStatus(admin).currentVersion,reason:'redaction check',requestId:'archive-evidence',idempotencyKey:'private-archive-key'});
  const exported=s.directory.readArchive(admin,archive.id);
  assert.equal(JSON.stringify(exported).includes(privateKey),false);
  assert.equal(JSON.stringify(s.directory.unifiedAuditPage(admin)).includes(privateKey),false);
  assert.deepEqual(s.directory.audit(admin)[0].changes.map(change=>change.field).sort(),['costCenter','name','region','status']);
 }finally{s.db.close();}
});
