import {createHmac} from 'node:crypto';
import {verifyAuthorityArchive,restoreIntoEmptyOfflineDatabase,restoreIntoFreshCelldObject,type AuthorityArchive} from '../../src/finops/authority_archive.js';
import {rateHash} from '../../src/finops/rate_cards.js';
import {UsageCostLedger} from '../../src/finops/usage_cost_ledger.js';
import {handleDirectoryApi} from '../../src/control/admin_directory_api.js';
import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {TenantDirectoryCell} from '../../src/cloudflare/tenant_directory_cell.js';
import {TenantControlCell} from '../../src/cloudflare/tenant_control_cell.js';
import {handleFinopsApi} from '../../src/control/admin_finops_api.js';
import {handleBodyUsageIngress} from '../../src/control/body_usage_ingress.js';
import type {DirectoryStorage} from '../../src/control/tenant_directory.js';
import type {CloudflareCelldEnv,DurableObjectNamespaceLike,DurableObjectStateLike} from '../../src/cloudflare/env.js';
import {bodyServiceToken} from '../../src/cloudflare/body_auth.js';
import {BodyUsageJournal} from '../../src/runtime/body_usage_journal.js';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import type {ControlActor} from '../../src/control/contracts.js';
const admin:ControlActor={sub:'github:1',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},viewer:ControlActor={sub:'github:2',role:'VIEWER',scopeTenant:'tenant',scopeRegion:'dev'};
const directory='d'.repeat(64),admission='e'.repeat(64),root='f'.repeat(64),key=bodyServiceToken(root,'body-1');
const resources={cpuNanoCores:1,cpuMilliCores:1,memoryMiB:1,slots:1},identity={producerId:'body-1',tenantId:'tenant',taskId:'task',attemptId:'attempt',fencing:7,executionInputDigest:'a'.repeat(64)};
const zero={numerator:'0',denominator:'1'},half={numerator:'1',denominator:'2'};
const definition={schemaVersion:1,unit:'credit-subunit',unitVersion:1,rounding:'HALF_EVEN',scope:{region:null,poolId:null},resources:{cpuNanoCoreMilliseconds:half,memoryMiBMilliseconds:zero,slotMilliseconds:zero},models:[]};
function namespace(fn:(request:Request)=>Promise<Response>):DurableObjectNamespaceLike{return {idFromName:name=>({toString:()=>name}),get:()=>({id:{toString:()=>''},fetch:request=>fn(new Request(request))})};}
function fixture(){
 const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');const dbs:any[]=[];
 function storage(){const db=new DatabaseSync(':memory:');dbs.push(db);const result:DirectoryStorage={sql:{exec<T>(q:string,...args:unknown[]){const rows=db.prepare(q).all(...args) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const value=fn();db.exec('COMMIT');return value;}catch(error){db.exec('ROLLBACK');throw error;}}};return result;}
 const directoryStorage=storage(),tenantStorage=storage();let phase='RUNNING',offline=false;
 const env={ANSTS_DIRECTORY_SERVICE_TOKEN:directory,ANSTS_ADMISSION_SERVICE_TOKEN:admission,ANSTS_ADMISSION_SIGNING_KEY:'c'.repeat(64),ANSTS_BODY_AUTH_SECRET:root} as CloudflareCelldEnv;
 const ctx=(store:DirectoryStorage)=>{let alarm:number|null=null;return {storage:{...store,getAlarm:async()=>alarm,setAlarm:async(at:number)=>{alarm=at;}}} as unknown as DurableObjectStateLike;};
 let directories=new TenantDirectoryCell(ctx(directoryStorage),env);env.ANSTS_TENANT_DIRECTORY_CELL=namespace(req=>directories.fetch(req));
 let tenant=new TenantControlCell(ctx(tenantStorage),env);env.ANSTS_TENANT_CONTROL_CELL=namespace(req=>offline?Promise.resolve(Response.json({code:'OFFLINE'},{status:503})):tenant.fetch(req));
 env.ANSTS_BODY_CELL=namespace(async()=>Response.json({execution:{phase,node_id:'body-1',tenant_id:'tenant',task_id:'task',attempt_id:'attempt',fencing_token:7,input_digest:identity.executionInputDigest}}));
 async function direct(action:string,input:unknown){const response=await directories.fetch(new Request('https://directory/'+action,{method:'POST',headers:{authorization:'Bearer '+directory},body:JSON.stringify({actor:admin,input})}));assert.equal(response.status,200);return response.json();}
 async function setup(){await direct('cost-center-mutate',{id:'center',name:'Center',status:'ACTIVE',expectedVersion:0,reason:'test',requestId:'req',idempotencyKey:'center'});await direct('mutate',{id:'tenant',name:'Tenant',region:'dev',costCenter:'center',status:'ACTIVE',expectedVersion:0,reason:'test',requestId:'req',idempotencyKey:'tenant'});}
 const api=(path:string,body?:unknown,actor:ControlActor&{readOnly?:boolean}=admin,method=body?'POST':'GET',idempotencyKey='request')=>handleFinopsApi(new Request('https://gateway'+path,{method,headers:{'content-type':'application/json','idempotency-key':idempotencyKey},body:body?JSON.stringify(body):undefined}),env,actor);
 const control=(action:string,body:object,actor=admin,token=admission)=>tenant.fetch(new Request('https://tenant/'+action,{method:'POST',headers:{authorization:'Bearer '+token},body:JSON.stringify({actor,tenantId:'tenant',...body})}));
 const bind=()=>handleBodyUsageIngress(new Request('https://fleet/internal/v1/body-usage-binding',{method:'POST',headers:{authorization:'Body '+key,'x-ansts-body-id':'body-1'},body:JSON.stringify({identity,resources})}),env);
 const dir=mkdtempSync(join(tmpdir(),'ansts-finops-service-')),source=new BodyUsageJournal(dir);
 return {env,source,storage:tenantStorage,api,control,bind,setup,setPhase(value:string){phase=value;},setOffline(value:boolean){offline=value;},restart(){directories=new TenantDirectoryCell(ctx(directoryStorage),env);tenant=new TenantControlCell(ctx(tenantStorage),env);},close(){for(const db of dbs)db.close();rmSync(dir,{recursive:true,force:true});}};
}
async function activate(f:ReturnType<typeof fixture>){
 assert.equal((await f.api('/v1/admin/finops/rate-cards',{expectedVersion:0,definition,reason:'Explicit fixture price'})).status,200);
 const response=await f.api('/v1/admin/tenants/tenant/rate-card',{expectedVersion:0,rateCardVersion:1,reason:'Explicit fixture activation',rateCard:{...definition,resources:{}}},admin,'PUT','activate');assert.equal(response.status,200);return response.json();
}
async function authorizeReservation(f:ReturnType<typeof fixture>){
 const quota={concurrentAttempts:1,cpuMilliCores:1,memoryMiB:1,queuedTasks:0};
 const policy=await f.control('policy-write',{input:{expectedVersion:0,resources:quota,
  budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'Fixture policy',idempotencyKey:'policy',requestId:'policy'}},admin,directory);
 assert.equal(policy.status,200);
 const prepared=await f.control('admission-prepare',{input:{reservationId:'reservation',taskId:identity.taskId,attemptId:identity.attemptId,
  fencing:identity.fencing,executionInputDigest:identity.executionInputDigest,expectedPolicyVersion:1,resources:quota,
  estimatedCreditUnits:'10',expiresAt:Date.now()+100000},idempotencyKey:'prepare'});
 assert.equal(prepared.status,200);
 let {reservation}=await prepared.json();
 for(const to of ['RESERVED','START_AUTHORIZED']){
  const changed=await f.control('admission-transition',{input:{reservationId:reservation.id,expectedVersion:reservation.version,
   generation:reservation.generation,fencing:identity.fencing,to,reason:'Fixture authorization',
   target:identity.producerId,capacityReceipt:'fixture-receipt'},idempotencyKey:to});
  assert.equal(changed.status,200);
  ({reservation}=await changed.json());
 }
}
test('actual directory/control services resolve immutable rates, ignore forged snapshots, and enforce read/write scope',async()=>{
 const f=fixture();try{await f.setup();
  assert.equal((await f.api('/v1/admin/finops/rate-cards',undefined,viewer)).status,403);
  assert.equal((await f.api('/v1/admin/finops/rate-cards',{expectedVersion:0,definition,reason:'test'},{...admin,readOnly:true})).status,403);
  const activated=await activate(f);assert.equal(activated.activation.rateCard.resources.cpuNanoCoreMilliseconds.numerator,'1');
  assert.equal((await f.api('/v1/admin/tenants/tenant/rate-card',undefined,viewer)).status,200);
  assert.equal((await f.api('/v1/admin/tenants/tenant/costs',undefined,{...viewer,scopeTenant:'other'})).status,404);
  assert.equal((await f.api('/v1/admin/finops/rate-audit')).status,200);
  assert.equal((await f.control('admission-usage-bind',{identity,resources,priceEligible:true},admin,directory)).status,401);
 }finally{f.close();}
});
test('production Host ingress pins execution price then atomically projects accepted signed events; object reconstruction replays once',async()=>{
 const f=fixture();try{await f.setup();await activate(f);assert.equal((await f.bind()).status,200);
  const pinned=await(await f.control('admission-usage-binding',{identity})).json();assert.equal(pinned.binding.context.rateCardVersion,1);
  const start=Date.now();f.source.begin(identity,resources,start);f.source.checkpoint(identity,start+1);f.source.finish(identity,start+2);
  const {signUsageEvent}=await import('../../src/control/usage_event.js');
  for(const event of f.source.events(identity)){const response=await f.control('admission-usage-ingest',{identity,proof:signUsageEvent(event,key)});assert.equal(response.status,200);}
  const before=await(await f.api('/v1/admin/tenants/tenant/costs?limit=1',undefined,viewer)).json();assert.equal(before.summary.totalCreditUnits,'1');assert.ok(before.nextCursor);
  f.restart();for(const event of f.source.events(identity))assert.equal((await f.control('admission-usage-ingest',{identity,proof:signUsageEvent(event,key)})).status,200);
  assert.deepEqual(await(await f.api('/v1/admin/tenants/tenant/costs?limit=1',undefined,viewer)).json(),before);
  assert.equal((await f.api('/v1/admin/tenants/tenant/costs?cursor='+encodeURIComponent(before.nextCursor+'x'),undefined,viewer)).status,422);
  const sum=await(await f.api('/v1/admin/finops/summary',undefined,viewer)).json();assert.equal(sum.totalCreditUnits,'1');assert.equal(sum.tenantCount,1);
  assert.deepEqual(sum.costCenters,[{costCenter:'center',costCenterVersion:1,rateCardVersion:1,
   activationVersion:1,knownSubtotalCreditUnits:'1'}]);
  assert.deepEqual(sum.sources,[{tenantId:'tenant',watermark:before.watermark,versions:[{costCenter:'center',costCenterVersion:1,rateCardVersion:1,activationVersion:1}]}]);
  f.setOffline(true);const missing=await(await f.api('/v1/admin/finops/summary',undefined,viewer)).json();assert.equal(missing.totalCreditUnits,null);assert.deepEqual(missing.missingTenantIds,['tenant']);
 }finally{f.close();}
});
test('pin and usage binding roll back together; accepted event and cost entry roll back on projection failure',async()=>{
 const f=fixture();try{await f.setup();await activate(f);const original=f.storage.sql.exec.bind(f.storage.sql);
  f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT OR REPLACE INTO tenant_usage_streams'))throw new Error('binding storage failure');return original(q,...args);};
  assert.equal((await f.bind()).status,503);f.storage.sql.exec=original;assert.equal(f.storage.sql.exec('SELECT id FROM tenant_usage_price_bindings').toArray().length,0);assert.equal((await f.bind()).status,200);
  f.source.begin(identity,resources,Date.now());const event=f.source.events(identity)[0],{signUsageEvent}=await import('../../src/control/usage_event.js');
  f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT INTO tenant_usage_cost_entries'))throw new Error('projection storage failure');return original(q,...args);};
  assert.equal((await f.control('admission-usage-ingest',{identity,proof:signUsageEvent(event,key)})).status,503);f.storage.sql.exec=original;
  assert.equal(f.storage.sql.exec('SELECT id FROM tenant_usage_events').toArray().length,0);assert.equal((await f.control('admission-usage-ingest',{identity,proof:signUsageEvent(event,key)})).status,200);
 }finally{f.close();}
});
test('late completed execution and pre-existing unpriced usage are not retroactively pinned after rate activation',async()=>{
 const f=fixture();try{await f.setup();await activate(f);f.setPhase('COMPLETED');assert.equal((await f.bind()).status,200);
  assert.equal(f.storage.sql.exec('SELECT id FROM tenant_usage_price_bindings').toArray().length,0);
  const {signUsageEvent}=await import('../../src/control/usage_event.js');const start=Date.now();f.source.begin(identity,resources,start);f.source.finish(identity,start+2);
  for(const event of f.source.events(identity))assert.equal((await f.control('admission-usage-ingest',{identity,proof:signUsageEvent(event,key)})).status,200);
  const costs=await(await f.api('/v1/admin/tenants/tenant/costs')).json();assert.equal(costs.summary.totalCreditUnits,null);assert.ok(costs.summary.reasons.includes('PRICE_SNAPSHOT_MISSING'));
  f.setPhase('RUNNING');assert.equal((await f.bind()).status,200);assert.equal(f.storage.sql.exec('SELECT id FROM tenant_usage_price_bindings').toArray().length,0);
 }finally{f.close();}
});
test('new start-authorized reservations pin price with usage binding and retain the original activation on replay',async()=>{
 const f=fixture();try{
  await f.setup();await activate(f);await authorizeReservation(f);
  const original=f.storage.sql.exec.bind(f.storage.sql);
  f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT OR REPLACE INTO tenant_usage_streams'))throw new Error('binding storage failure');return original(q,...args);};
  assert.equal((await f.bind()).status,503);
  f.storage.sql.exec=original;
  assert.equal(f.storage.sql.exec('SELECT id FROM tenant_usage_price_bindings').toArray().length,0);
  const requests=await Promise.all([f.bind(),f.bind()]);
  assert.deepEqual(requests.map(response=>response.status),[200,200]);
  const pinned=await (await f.control('admission-usage-binding',{identity})).json();
  assert.equal(pinned.binding.context.rateCardVersion,1);
  assert.equal((await f.api('/v1/admin/finops/rate-cards',{expectedVersion:1,definition,reason:'Second version'},admin,'POST','second-rate')).status,200);
  assert.equal((await f.api('/v1/admin/tenants/tenant/rate-card',{expectedVersion:1,rateCardVersion:2,reason:'Second activation'},admin,'PUT','second-activation')).status,200);
  f.setPhase('RUNNING');
  const lower=await f.control('policy-write',{input:{expectedVersion:1,
   resources:{concurrentAttempts:0,cpuMilliCores:1,memoryMiB:1,queuedTasks:0},
   budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},
   reason:'Lower quota during execution',idempotencyKey:'lower-during-execution',requestId:'lower-during-execution'}},admin,directory);
  assert.equal(lower.status,200);
  f.restart();
  assert.equal((await f.bind()).status,200);
  assert.deepEqual(await (await f.control('admission-usage-binding',{identity})).json(),pinned);
  const admission=await (await f.control('admission-read',{reservationId:'reservation'})).json();
  assert.equal(admission.reservation.policy.version,1);
  const headroom=await (await f.control('admission-capacity-headroom',{})).json();
  assert.equal(headroom.policyVersion,2);
  assert.equal(headroom.used.slots,1);
  assert.equal(headroom.remaining.slots,0);

  assert.equal(f.storage.sql.exec('SELECT id FROM tenant_usage_price_bindings').toArray().length,1);
 }finally{f.close();}
});
test('legacy and late reservation bindings remain unpriced even after an eligible-looking retry',async()=>{
 for(const mode of ['legacy','completed','uncertain'] as const){
  const f=fixture();try{
   await f.setup();await activate(f);await authorizeReservation(f);
   if(mode==='legacy'){
    const row=f.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM tenant_admission_state WHERE id = ?','admission').toArray()[0];
    const state=JSON.parse(row.payload_json);
    delete state.reservations[0].priceAtUsageBinding;
    f.storage.sql.exec('UPDATE tenant_admission_state SET payload_json = ? WHERE id = ?',JSON.stringify(state),'admission');
   }else if(mode==='completed'){
    f.setPhase('COMPLETED');
   }else{
    const transition=await f.control('admission-transition',{input:{reservationId:'reservation',expectedVersion:3,generation:1,
     fencing:identity.fencing,to:'UNCERTAIN',reason:'Recover uncertain attempt'},idempotencyKey:'uncertain'});
    assert.equal(transition.status,200);
   }
   assert.equal((await f.bind()).status,200);
   f.setPhase('RUNNING');
   assert.equal((await f.bind()).status,200);
   const pinned=await (await f.control('admission-usage-binding',{identity})).json();
   assert.equal(pinned.binding.context.rateCardVersion,null);
   assert.equal(f.storage.sql.exec('SELECT id FROM tenant_usage_price_bindings').toArray().length,0);
  }finally{f.close();}
 }
});
test('rate history cursors freeze upper version and reject changed identities',async()=>{
 const f=fixture();try{await f.setup();await activate(f);
  assert.equal((await f.api('/v1/admin/finops/rate-cards',{expectedVersion:1,definition,reason:'Second version'},admin,'POST','second')).status,200);
  const first=await(await f.api('/v1/admin/finops/rate-cards?limit=1')).json();assert.equal(first.watermark,2);
  assert.equal((await f.api('/v1/admin/finops/rate-cards',{expectedVersion:2,definition,reason:'Third version'},admin,'POST','third')).status,200);
  const next=await(await f.api('/v1/admin/finops/rate-cards?cursor='+encodeURIComponent(first.nextCursor))).json();assert.deepEqual(next.items.map((r:any)=>r.version),[2]);assert.equal(next.nextCursor,null);
  assert.equal((await f.api('/v1/admin/finops/rate-cards?cursor='+encodeURIComponent(first.nextCursor),undefined,{...admin,sub:'github:other'})).status,409);
 }finally{f.close();}
});
test('real Gateway requires authentication for finops paths and preserves verified Viewer scope',async()=>{
 const f=fixture();try{
  await f.setup();const {handleUnifiedGatewayRequest}=await import('../../src/cloudflare/gateway_cell.js');const {ConsoleRbacAuthGuard}=await import('../../src/dashboard/console_rbac_guard.js');const secret='test-finops-gateway-signing-secret-1234567890';
  const env={...f.env,ANSTS_USER_AUTH_SECRET:secret};const guard=new ConsoleRbacAuthGuard(secret),token=guard.issueToken(viewer);
  assert.equal((await handleUnifiedGatewayRequest(new Request('https://gateway/v1/admin/finops/rate-cards'),env))?.status,401);
  assert.equal((await handleUnifiedGatewayRequest(new Request('https://gateway/v1/admin/tenants/tenant/costs',{headers:{authorization:'Bearer '+token}}),env))?.status,200);
  assert.equal((await handleUnifiedGatewayRequest(new Request('https://gateway/v1/admin/finops/rate-cards',{headers:{authorization:'Bearer '+token}}),env))?.status,403);
 }finally{f.close();}
});
function offline(){
 const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');const db=new DatabaseSync(':memory:');const storage:DirectoryStorage={sql:{exec<T>(q:string,...args:unknown[]){const rows=db.prepare(q).all(...args) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){db.exec('BEGIN');try{const value=fn();db.exec('COMMIT');return value;}catch(error){db.exec('ROLLBACK');throw error;}}};return {db,storage};
}
async function backup(f:ReturnType<typeof fixture>,path:string,key='backup'){
 const status=await(await f.api(path)).json();const input={expectedRevision:status.sourceRevision,reason:'Independent recovery verification'};
 const created=await f.api(path,input,admin,'POST',key);assert.equal(created.status,200);const manifest=await created.json();const response=await f.api(path+'/'+manifest.id);assert.equal(response.status,200);return {archive:await response.json() as AuthorityArchive,input,manifest};
}
test('unified management audit includes directory templates and explicitly tenant-scoped activation history',async()=>{
 const f=fixture();try{await f.setup();await activate(f);
  const run=(query:string,actor=admin)=>handleDirectoryApi(new Request('https://gateway/v1/admin/audit?'+query),f.env,actor);
  const all=await(await run('domain=all')).json();assert.ok(all.items.some((entry:any)=>entry.domain==='rate-card'));assert.deepEqual(all.coverage.tenantSelectionRequired,['rate-activation']);
  const tenantAudit=await(await run('domain=rate-activation&tenantId=tenant&limit=1')).json();assert.equal(tenantAudit.items[0].domain,'rate-activation');assert.equal(tenantAudit.items[0].actor,admin.sub);
  assert.equal((await f.api('/v1/admin/tenants/tenant/rate-card',{expectedVersion:1,rateCardVersion:1,reason:'Explicit reactivation'},admin,'PUT','second-activation')).status,200);const secondAudit=await(await run('domain=rate-activation&tenantId=tenant')).json();assert.equal(secondAudit.items[1].changes.find((c:any)=>c.field==='rateCardVersion').before,'1');assert.ok(tenantAudit.items[0].changes.some((c:any)=>c.field==='rateDigest'));
  assert.equal((await run('domain=rate-activation')).status,422);assert.equal((await run('domain=rate-activation&tenantId=tenant',viewer)).status,403);
  assert.equal((await run('domain=rate-activation&tenantId=tenant',{...admin,scopeTenant:'other'})).status,404);
 }finally{f.close();}
});
test('directory authority archive authenticates, keeps receipts/audit and restores only into a new offline database',async()=>{
 const f=fixture(),target=offline();try{await f.setup();await activate(f);
  const path='/v1/admin/finops/archives';assert.equal((await f.api(path,undefined,viewer)).status,403);
  const {archive,input,manifest}=await backup(f,path);assert.equal(manifest.tableCounts.directory_rate_cards,1);assert.equal(manifest.operationalResume,'DISABLED');verifyAuthorityArchive(archive,directory);
  assert.deepEqual(await(await f.api(path,input,admin,'POST','backup')).json(),manifest);
  assert.equal((await f.api(path,{...input,reason:'different'},admin,'POST','backup')).status,409);
  const result=restoreIntoEmptyOfflineDatabase(target.storage,archive,directory,{kind:'DIRECTORY',tenantId:null});assert.equal(result.sourceRevision,manifest.sourceRevision);
  const restored=new TenantDirectoryCell({storage:target.storage} as unknown as DurableObjectStateLike,{ANSTS_DIRECTORY_SERVICE_TOKEN:directory});
  const response=await restored.fetch(new Request('https://directory/rate-list',{method:'POST',headers:{authorization:'Bearer '+directory},body:JSON.stringify({actor:admin,page:{limit:1}})}));assert.equal(response.status,200);assert.equal((await response.json()).items[0].version,1);
  assert.throws(()=>restoreIntoEmptyOfflineDatabase(target.storage,archive,directory,{kind:'DIRECTORY',tenantId:null}),{code:'FINOPS_RESTORE_REQUIRES_EMPTY_DATABASE'});
  assert.throws(()=>verifyAuthorityArchive(archive,'a'.repeat(64)),{code:'FINOPS_ARCHIVE_AUTH_REQUIRED'});
 }finally{target.db.close();f.close();}
});
test('celld archive recovery accepts only empty runtime state and known engine tables',async()=>{
 const f=fixture(),target=offline();try{
  await f.setup();
  const {archive}=await backup(f,'/v1/admin/finops/archives');
  const scope={kind:'DIRECTORY',tenantId:null} as const;
  target.db.exec("CREATE TABLE _cf_METADATA (value TEXT); INSERT INTO _cf_METADATA VALUES ('engine'); CREATE TABLE _cf_ALARM (value INTEGER)");
  assert.throws(()=>restoreIntoEmptyOfflineDatabase(target.storage,archive,directory,scope),{code:'FINOPS_RESTORE_REQUIRES_EMPTY_DATABASE'});
  let alarm:number|null=1;
  let values=new Map<string,unknown>();
  const storage={...target.storage,getAlarm:async()=>alarm,list:async()=>values};
  const restore=()=>restoreIntoFreshCelldObject(storage,archive,directory,scope);
  await assert.rejects(restore,{code:'FINOPS_RESTORE_REQUIRES_EMPTY_DATABASE'});
  alarm=null;
  values.set('existing',1);
  await assert.rejects(restore,{code:'FINOPS_RESTORE_REQUIRES_EMPTY_DATABASE'});
  values.clear();
  target.db.exec('CREATE TABLE sqliteXbusiness (value TEXT)');
  await assert.rejects(restore,{code:'FINOPS_RESTORE_REQUIRES_EMPTY_DATABASE'});
  target.db.exec('DROP TABLE sqliteXbusiness');
  assert.equal((await restore()).operationalResume,'DISABLED');
  assert.equal(target.storage.sql.exec<{value:string}>('SELECT value FROM _cf_METADATA').toArray()[0].value,'engine');
  await assert.rejects(restore,{code:'FINOPS_RESTORE_REQUIRES_EMPTY_DATABASE'});
 }finally{target.db.close();f.close();}
});
test('tenant backup restores exact source/price/derived entries and UNCERTAIN reservations with pending delivery receipts',async()=>{
 const f=fixture(),target=offline();try{await f.setup();await activate(f);assert.equal((await f.bind()).status,200);const at=Date.now();f.source.begin(identity,resources,at);f.source.finish(identity,at+2);
  const {signUsageEvent}=await import('../../src/control/usage_event.js');for(const event of f.source.events(identity))assert.equal((await f.control('admission-usage-ingest',{identity,proof:signUsageEvent(event,key)})).status,200);
  const policy=await f.control('policy-write',{input:{expectedVersion:0,resources:{concurrentAttempts:1,cpuMilliCores:10,memoryMiB:10,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'fixture',idempotencyKey:'policy',requestId:'policy'}},admin,directory);assert.equal(policy.status,200);
  const prepared=await f.control('admission-prepare',{input:{reservationId:'pending',taskId:'other-task',attemptId:'other-attempt',fencing:1,expectedPolicyVersion:1,resources:{concurrentAttempts:1,cpuMilliCores:1,memoryMiB:1,queuedTasks:0},estimatedCreditUnits:'10',expiresAt:Date.now()+100000},idempotencyKey:'prepare'});assert.equal(prepared.status,200);
  assert.equal((await f.control('admission-transition',{input:{reservationId:'pending',expectedVersion:1,generation:1,fencing:1,to:'UNCERTAIN',reason:'Fixture uncertain'},idempotencyKey:'uncertain'})).status,200);
  const before=await(await f.api('/v1/admin/tenants/tenant/costs')).json(),{archive}=await backup(f,'/v1/admin/tenants/tenant/finops-archives');
  assert.ok(before.summary.reasons.includes('UNSETTLED_ADMISSION_RESERVATIONS'));
  const result=restoreIntoEmptyOfflineDatabase(target.storage,archive,directory,{kind:'TENANT',tenantId:'tenant'});assert.equal(result.operationalResume,'DISABLED');
  const summary=new UsageCostLedger(target.storage).summary(admin,archive.snapshot.tenant!);assert.deepEqual(summary,before.summary);
  for(const table of ['tenant_usage_events','tenant_usage_price_bindings','tenant_usage_cost_entries','tenant_admission_state','admission_outbox','admission_delivery_control'])assert.deepEqual(target.storage.sql.exec(`SELECT * FROM ${table} ORDER BY id`).toArray(),f.storage.sql.exec(`SELECT * FROM ${table} ORDER BY id`).toArray());
  assert.throws(()=>restoreIntoEmptyOfflineDatabase(target.storage,archive,directory,{kind:'TENANT',tenantId:'other'}),{code:'FINOPS_RESTORE_SCOPE_CONFLICT'});
 }finally{target.db.close();f.close();}
});
test('archive corruption, stale revision and live-scope reads fail closed without publishing a receipt',async()=>{
 const f=fixture(),target=offline();try{await f.setup();await activate(f);const path='/v1/admin/finops/archives';
  assert.equal((await f.api(path,{expectedRevision:'0'.repeat(64),reason:'stale'})).status,409);
  const {archive}=await backup(f,path);const corrupt=structuredClone(archive);corrupt.snapshot.reason='tampered';assert.throws(()=>restoreIntoEmptyOfflineDatabase(target.storage,corrupt,directory,{kind:'DIRECTORY',tenantId:null}));assert.equal(target.storage.sql.exec("SELECT name FROM sqlite_master WHERE type = 'table'").toArray().length,0);
  assert.equal((await f.api('/v1/admin/tenants/tenant/finops-archives',undefined,{...admin,scopeRegion:'dev'})).status,403);
  assert.equal((await f.api('/v1/admin/finops/archives/'+archive.manifest.id,undefined,viewer)).status,403);
  assert.equal((await f.api('/v1/admin/finops/restore',archive,admin,'POST')).status,404);
 }finally{target.db.close();f.close();}
});
test('restore refuses signed but inconsistent business rows and rolls back all schema and data',async()=>{
 const f=fixture(),target=offline();try{await f.setup();await activate(f);const {archive}=await backup(f,'/v1/admin/finops/archives');
  const row=archive.snapshot.tables.find(t=>t.name==='directory_rate_cards')!.rows[0];const value=JSON.parse(row[1] as string);value.version=9;row[1]=JSON.stringify(value);
  const payload=JSON.stringify(archive.snapshot);archive.manifest.digest=rateHash(archive.snapshot);archive.manifest.id=archive.manifest.digest;archive.manifest.bytes=Buffer.byteLength(payload);archive.manifest.sourceRevision=rateHash(archive.snapshot.tables);const {signature,...manifest}=archive.manifest;archive.manifest.signature=createHmac('sha256',directory).update('finops-authority-backup-v1.'+JSON.stringify(manifest)).digest('base64url');
  assert.throws(()=>restoreIntoEmptyOfflineDatabase(target.storage,archive,directory,{kind:'DIRECTORY',tenantId:null}));assert.equal(target.storage.sql.exec("SELECT name FROM sqlite_master WHERE type = 'table'").toArray().length,0);
 }finally{target.db.close();f.close();}
});
test('archive publication failure rolls back snapshot and idempotency receipt without changing business state',async()=>{
 const f=fixture();try{await f.setup();await activate(f);const path='/v1/admin/tenants/tenant/finops-archives',status=await(await f.api(path)).json(),original=f.storage.sql.exec.bind(f.storage.sql);
  f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT INTO finops_authority_archive_requests'))throw new Error('archive receipt write failed');return original(q,...args);};
  assert.equal((await f.api(path,{expectedRevision:status.sourceRevision,reason:'Atomic backup'})).status,503);f.storage.sql.exec=original;
  assert.equal(f.storage.sql.exec('SELECT id FROM finops_authority_archives').toArray().length,0);assert.equal(f.storage.sql.exec('SELECT id FROM finops_authority_archive_requests').toArray().length,0);assert.equal(f.storage.sql.exec('SELECT version FROM tenant_rate_activations').toArray().length,1);
 }finally{f.close();}
});
test('restore rejects non-whitelisted schema before creating any table',async()=>{
 const f=fixture(),target=offline();try{await f.setup();await activate(f);const {archive}=await backup(f,'/v1/admin/finops/archives');archive.snapshot.tables[0].name='arbitrary_sql' as typeof archive.snapshot.tables[0]['name'];
  assert.throws(()=>restoreIntoEmptyOfflineDatabase(target.storage,archive,directory,{kind:'DIRECTORY',tenantId:null}),{code:'FINOPS_ARCHIVE_INVALID'});assert.equal(target.storage.sql.exec("SELECT name FROM sqlite_master WHERE type = 'table'").toArray().length,0);
 }finally{target.db.close();f.close();}
});
test('signed late ingest rolls back with its adjustment, replays once, and survives v2 archive restore',async()=>{
 const f=fixture(),legacy=offline(),current=offline();try{await f.setup();await activate(f);const path='/v1/admin/tenants/tenant/finops-archives';const old=await backup(f,path,'before-periods');assert.equal(old.manifest.schemaVersion,1);
  const {PeriodSettlement}=await import('../../src/finops/period_settlement.js'),periods=new PeriodSettlement(f.storage);
  const record={id:'tenant',name:'Tenant',region:'dev',costCenter:'center',status:'ACTIVE' as const,version:1};
  const now=Date.UTC(2025,1,1),periodId='2025-01';
  const coverage={schemaVersion:1 as const,purpose:'ansts-settlement-coverage-v1' as const,tenantId:'tenant',periodId,
   inventoryWatermark:'isolated-fixture',observedAt:now,complete:true,attempts:[]};
  const preview=periods.preview(admin,record,periodId,coverage,now);
  const closed=periods.close(admin,record,periodId,{expectedRevision:preview.revision,reason:'Fixture close',requestId:'close',idempotencyKey:'close'},coverage,now);
  assert.equal((await f.bind()).status,200);
  const start=Date.UTC(2025,0,18);f.source.begin(identity,resources,start);f.source.finish(identity,start+2);
  const {signUsageEvent}=await import('../../src/control/usage_event.js');
  const ingest=(event:Parameters<typeof signUsageEvent>[0])=>f.control('admission-usage-ingest',{identity,proof:signUsageEvent(event,key)});
  const [open,segment,close]=f.source.events(identity);
  assert.equal((await ingest(open)).status,200);
  const original=f.storage.sql.exec.bind(f.storage.sql);
  f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT INTO tenant_period_adjustments'))throw new Error('adjustment write failed');return original(q,...args);};
  assert.equal((await ingest(segment)).status,503);
  f.storage.sql.exec=original;
  for(const table of ['tenant_usage_events','tenant_usage_cost_entries'])assert.equal(f.storage.sql.exec('SELECT id FROM '+table+' WHERE id = ?',segment.eventId).toArray().length,0);
  for(const event of [segment,close,segment])assert.equal((await ingest(event)).status,200);
  assert.equal(periods.reconcileLate(admin,record),0);
  const modern=await backup(f,path,'after-periods');assert.equal(modern.manifest.schemaVersion,2);assert.equal(modern.manifest.tableCounts.tenant_period_closes,1);
  restoreIntoEmptyOfflineDatabase(current.storage,modern.archive,directory,{kind:'TENANT',tenantId:'tenant'});assert.deepEqual(new PeriodSettlement(current.storage).readClosed(admin,record,periodId),closed);assert.equal(new PeriodSettlement(current.storage).adjustments(admin,record,{},directory).items.length,1);
  restoreIntoEmptyOfflineDatabase(legacy.storage,old.archive,directory,{kind:'TENANT',tenantId:'tenant'});assert.equal(legacy.storage.sql.exec("SELECT name FROM sqlite_master WHERE name = 'tenant_period_closes'").toArray().length,0);
 }finally{legacy.db.close();current.db.close();f.close();}
});
test('conflicting signed usage after close records attention once without changing the frozen statement',async()=>{
 const f=fixture();try{
  await f.setup();await activate(f);assert.equal((await f.bind()).status,200);
  const {PeriodSettlement}=await import('../../src/finops/period_settlement.js'),periods=new PeriodSettlement(f.storage);
  const {signUsageEvent}=await import('../../src/control/usage_event.js');
  const tenant={id:'tenant',name:'Tenant',region:'dev',costCenter:'center',status:'ACTIVE' as const,version:1};
  const periodId='2025-01',start=Date.UTC(2025,0,18),closedAt=Date.UTC(2025,1,1);
  f.source.begin(identity,resources,start);f.source.finish(identity,start+2);
  const ingest=(event:Parameters<typeof signUsageEvent>[0])=>f.control('admission-usage-ingest',{identity,proof:signUsageEvent(event,key)});
  const events=f.source.events(identity);
  for(const event of events)assert.equal((await ingest(event)).status,200);
  const coverage={schemaVersion:1 as const,purpose:'ansts-settlement-coverage-v1' as const,tenantId:tenant.id,periodId,
   inventoryWatermark:'fixture',observedAt:closedAt,complete:true,attempts:[identity]};
  const preview=periods.preview(admin,tenant,periodId,coverage,closedAt);
  const statement=periods.close(admin,tenant,periodId,{expectedRevision:preview.revision,reason:'Close',requestId:'close',idempotencyKey:'close'},coverage,closedAt);
  for(let attempt=0;attempt<2;attempt++){
   const response=await ingest({...events[0],at:start+1});
   assert.equal(response.status,409);
   assert.equal((await response.json()).receipt.outcome,'CONFLICT');
  }
  const {items}=periods.adjustments(admin,tenant,{},directory);
  assert.equal(items.length,1);
  assert.equal(items[0].originCloseDigest,statement.digest);
  assert.equal(items[0].kind,'SOURCE_CONFLICT');
  assert.deepEqual(items[0].pending,['SOURCE_CONFLICT_AFTER_CLOSE']);
  assert.deepEqual(periods.readClosed(admin,tenant,periodId),statement);
 }finally{f.close();}
});
test('manual debit and credit use the authenticated API, survive restore, and enter the next closing without rewriting the origin',async()=>{
 const f=fixture(),target=offline();try{
  await f.setup();
  const {PeriodSettlement,settlementMonth}=await import('../../src/finops/period_settlement.js');
  const periods=new PeriodSettlement(f.storage),tenant={id:'tenant',name:'Tenant',region:'dev',costCenter:'center',status:'ACTIVE' as const,version:1};
  const origin='2025-01',closedAt=settlementMonth(origin).endAt;
  const cover=(periodId:string,observedAt:number)=>({schemaVersion:1 as const,purpose:'ansts-settlement-coverage-v1' as const,
   tenantId:tenant.id,periodId,inventoryWatermark:'fixture',observedAt,complete:true,attempts:[]});
  const original=periods.close(admin,tenant,origin,{expectedRevision:periods.preview(admin,tenant,origin,cover(origin,closedAt),closedAt).revision,
   reason:'Close',requestId:'close-origin',idempotencyKey:'close-origin'},cover(origin,closedAt),closedAt);
  const path='/v1/admin/tenants/tenant/settlements/'+origin+'/adjustments';
  const input={creditUnits:'-5',reason:'Correct overcharge',expectedCloseDigest:original.digest};
  assert.equal((await f.api(path,input,viewer)).status,403);
  assert.equal((await f.api(path,{...input,expectedCloseDigest:'0'.repeat(64)})).status,409);
  const response=await f.api(path,input,admin,'POST','credit');assert.equal(response.status,200);
  const credit=await response.json();
  assert.deepEqual(await (await f.api(path,input,admin,'POST','credit')).json(),credit);
  assert.equal((await f.api(path,{...input,creditUnits:'-6'},admin,'POST','credit')).status,409);
  assert.equal((await f.api(path,{...input,creditUnits:'2',reason:'Add missing fee'},admin,'POST','debit')).status,200);
  const month=credit.targetPeriodId,end=settlementMonth(month).endAt;
  const preview=periods.preview(admin,tenant,month,cover(month,end),end);
  assert.equal(preview.manualAdjustmentCreditUnits,'-3');assert.equal(preview.totalCreditUnits,'-3');
  const closed=periods.close(admin,tenant,month,{expectedRevision:preview.revision,reason:'Close target',requestId:'target',idempotencyKey:'target'},cover(month,end),end);
  assert.equal(closed.totalCreditUnits,'-3');
  const {archive}=await backup(f,'/v1/admin/tenants/tenant/finops-archives');
  restoreIntoEmptyOfflineDatabase(target.storage,archive,directory,{kind:'TENANT',tenantId:'tenant'});
  const restored=new PeriodSettlement(target.storage);
  assert.deepEqual(restored.readClosed(admin,tenant,origin),original);
  assert.deepEqual(restored.readClosed(admin,tenant,month),closed);
  assert.equal(restored.adjustments(admin,tenant,{},directory).items.length,2);
 }finally{target.db.close();f.close();}
});
test('Gateway refuses non-empty close until the original signed missing events are replayed',async t=>{
 const f=fixture(),scheduler=offline(),body=offline();try{
  const {ExecutionInventory}=await import('../../src/control/execution_inventory.js');
  const {handleUnifiedGatewayRequest}=await import('../../src/cloudflare/gateway_cell.js');
  const {ConsoleRbacAuthGuard}=await import('../../src/dashboard/console_rbac_guard.js');
  const {signUsageEvent}=await import('../../src/control/usage_event.js');
  const start=Date.UTC(2025,0,5),periodId='2025-01';
  await f.setup();
  const clock=t.mock.method(Date,'now',()=>start-100);
  try{await activate(f);assert.equal((await f.bind()).status,200);}finally{clock.mock.restore();}
  const schedulerJournal=new ExecutionInventory(scheduler.storage,'SCHEDULER','scheduler',()=>[],Date.UTC(2024,11,1));
  const bodyJournal=new ExecutionInventory(body.storage,'BODY','body',()=>[],start);
  const queued={tenantId:identity.tenantId,taskId:identity.taskId,inputDigest:identity.executionInputDigest,
   fencing:0,nodeId:null,attemptId:null,version:null,sourceAt:start-5,state:'QUEUED'};
  schedulerJournal.record(queued,()=>{},start-5);
  const leased={...queued,fencing:identity.fencing,nodeId:identity.producerId,state:'LEASED'};
  schedulerJournal.record(leased,()=>{},start);
  for(const [index,state] of ['ADMITTED','RUNNING','RESULT_PENDING','COMPLETED'].entries()){
   bodyJournal.record({...leased,state,attemptId:identity.attemptId,version:index+1,sourceAt:start+index},()=>{},start+index);
  }
  f.env.ANSTS_SCHEDULER_CELL=namespace(async request=>Response.json(schedulerJournal.page('tenant',Number(new URL(request.url).searchParams.get('after')))));
  f.env.ANSTS_BODY_CELL=namespace(async()=>Response.json(bodyJournal.snapshot('tenant')));
  const secret='isolated-gateway-finops-close-signing-key-1234',token=new ConsoleRbacAuthGuard(secret).issueToken(admin);
  const gateway=(action:string,input?:object)=>handleUnifiedGatewayRequest(new Request('https://gateway/v1/admin/tenants/tenant/settlements/'+periodId+'/'+action,{
   method:input?'POST':'GET',headers:{authorization:'Bearer '+token,'idempotency-key':'recovered-close'},body:input?JSON.stringify(input):undefined
  }),{...f.env,ANSTS_USER_AUTH_SECRET:secret});
  const missing=await (await gateway('preview'))!.json();
  assert.equal(missing.coverage.complete,true);assert.ok(missing.reasons.includes('STREAM_WITHOUT_EVENTS'));
  const close=(expectedRevision:string)=>gateway('close',{expectedRevision,reason:'Close after signed usage recovery'});
  const blocked=await close(missing.revision);
  assert.equal(blocked?.status,409);assert.equal((await blocked!.json()).code,'PERIOD_INCOMPLETE');
  f.source.begin(identity,resources,start+1);f.source.finish(identity,start+3);
  for(const event of f.source.events(identity)){
   const accepted=await f.control('admission-usage-ingest',{identity,proof:signUsageEvent(event,key)});
   assert.equal(accepted.status,200);
  }
  const stale=await close(missing.revision);
  assert.equal(stale?.status,409);assert.equal((await stale!.json()).code,'VERSION_CONFLICT');
  const recovered=await (await gateway('preview'))!.json();
  assert.equal(recovered.complete,true);assert.equal(recovered.totalCreditUnits,'1');
  const final=await close(recovered.revision);
  assert.equal(final?.status,200);
  const statement=await final!.json();assert.equal(statement.final,true);assert.equal(statement.totalCreditUnits,'1');
  f.restart();
  assert.deepEqual(await (await close(recovered.revision))!.json(),statement);
 }finally{scheduler.db.close();body.db.close();f.close();}
});
import {safeCostCsvCell} from '../../src/finops/cost_csv.js';

test('cost export persists and rechecks caller and scope on generation and download',async()=>{
 const f=fixture();
 const base='/v1/admin/tenants/tenant/cost-exports';
 try{
  await f.setup();
  const created=await f.api(base,{},viewer);
  assert.equal(created.status,202);
  const pending=await created.json(),path=base+'/'+pending.id;
  assert.equal(pending.status,'PENDING');
  assert.deepEqual(await (await f.api(base,{},viewer)).json(),pending);
  assert.equal((await f.api(path+'/download',undefined,viewer)).status,409);
  assert.equal((await f.api(path+'/run',{}, {...viewer,sub:'another'})).status,404);
  assert.equal((await f.api(path+'/run',{}, {...viewer,scopeTenant:'other'})).status,404);
  f.restart();
  assert.deepEqual(await (await f.api(path,undefined,viewer)).json(),pending);
  const ready=await f.api(path+'/run',{},viewer);
  assert.equal(ready.status,200);
  assert.equal((await ready.json()).status,'READY');
  const download=await f.api(path+'/download',undefined,viewer);
  assert.equal(download.status,200);
  assert.match(download.headers.get('content-disposition')??'',/^attachment;/);
  assert.match(await download.text(),/^"event_id","watermark"/);
  assert.deepEqual(f.storage.sql.exec<{action:string}>('SELECT action FROM cost_export_audit ORDER BY ordinal')
   .toArray().map(row=>row.action),['CREATE','GENERATE','DOWNLOAD']);

  assert.equal((await f.api(path+'/download',undefined,{...viewer,scopeRegion:'all'})).status,404);
  const ns=f.env.ANSTS_TENANT_DIRECTORY_CELL!;
  const updated=await ns.get(ns.idFromName('tenant-directory-v1')).fetch(new Request('https://directory/mutate',{
   method:'POST',headers:{authorization:'Bearer '+directory},
   body:JSON.stringify({actor:admin,input:{id:'tenant',name:'Tenant updated',region:'dev',costCenter:'center',
    status:'ACTIVE',expectedVersion:1,reason:'version check',requestId:'version',idempotencyKey:'version'}}),
  }));
  assert.equal(updated.status,200);
  assert.equal((await f.api(path+'/download',undefined,viewer)).status,404);
 }finally{f.close();}
});

test('cost CSV quotes fields and neutralizes formula prefixes',()=>{
 for(const value of ['=1+1',' +1','-1','@a','\tvalue','\rvalue','\nvalue','\ufeff=1'])
  assert.equal(safeCostCsvCell(value),'"'+ "'" +value+'"');
 assert.equal(safeCostCsvCell('a,"b"'),'"a,""b"""');
});
test('cost export traverses the full frozen ledger cursor without omitting or duplicating events',async()=>{
 const f=fixture();
 const base='/v1/admin/tenants/tenant/cost-exports';
 try{
  await f.setup();
  await activate(f);
  assert.equal((await f.bind()).status,200);
  const start=Date.now()-1000;
  f.source.begin(identity,resources,start);
  for(let offset=1;offset<=105;offset++)f.source.checkpoint(identity,start+offset);
  f.source.finish(identity,start+106);
  const {signUsageEvent}=await import('../../src/control/usage_event.js');
  const events=f.source.events(identity);
  assert.ok(events.length>100);
  for(const event of events)assert.equal((await f.control('admission-usage-ingest',{identity,proof:signUsageEvent(event,key)})).status,200);
  const first=await (await f.api('/v1/admin/tenants/tenant/costs?limit=100',undefined,viewer)).json();
  assert.equal(first.items.length,100);
  assert.ok(first.nextCursor);
  const job=await (await f.api(base,{},viewer)).json(),path=base+'/'+job.id;
  assert.equal((await f.api(path+'/run',{},viewer)).status,200);
  const download=await f.api(path+'/download',undefined,viewer);
  const csv=await download.text();
  const rows=csv.trimEnd().split('\r\n').slice(1);
  const counts=new Map<string,number>();
  for(const row of rows){
   const parsed=/^"([^"]+)","(\d+)"/.exec(row);
   assert.ok(parsed);
   assert.equal(Number(parsed[2]),first.watermark);
   counts.set(parsed[1],(counts.get(parsed[1])??0)+1);
  }
  assert.deepEqual(counts,new Map(events.map(event=>[event.eventId,event.kind==='RESOURCE_SEGMENT'?3:1])));
  f.restart();
  assert.equal(await (await f.api(path+'/download',undefined,viewer)).text(),csv);
 }finally{f.close();}
});
test('gateway can generate an export after returning its pending receipt',async()=>{
 const f=fixture();
 const base='/v1/admin/tenants/tenant/cost-exports';
 try{
  await f.setup();
  const work:Promise<void>[]=[];
  const response=await handleFinopsApi(new Request('https://gateway'+base,{
   method:'POST',headers:{'idempotency-key':'background-run'},
  }),f.env,viewer,job=>work.push(job));
  assert.equal(response.status,202);
  const job=await response.json();
  assert.equal(job.status,'PENDING');
  assert.equal(work.length,1);
  await Promise.all(work);
  const path=base+'/'+job.id;
  assert.equal((await (await f.api(path,undefined,viewer)).json()).status,'READY');
  assert.equal((await f.api(path+'/download',undefined,{...viewer,sub:'other'})).status,404);
  assert.equal((await f.api(path+'/download',undefined,viewer)).status,200);
 }finally{f.close();}
});
