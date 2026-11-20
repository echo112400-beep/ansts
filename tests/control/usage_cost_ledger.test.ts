import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import type {ControlActor} from '../../src/control/contracts.js';
import type {DirectoryStorage,TenantRecord} from '../../src/control/tenant_directory.js';
import {BodyUsageJournal} from '../../src/runtime/body_usage_journal.js';
import {UsageInbox} from '../../src/finops/usage_inbox.js';
import {RateCatalog,MAX_CREDIT,MAX_EXACT,exactAdd,exactPrice,roundCredit,type RateDefinition,type RateCard} from '../../src/finops/rate_cards.js';
import {TenantRateStore} from '../../src/finops/tenant_rate_store.js';
import {UsageCostLedger,aggregateTenantCosts} from '../../src/finops/usage_cost_ledger.js';
const admin:ControlActor={sub:'github:1',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'};
const viewer:ControlActor={sub:'github:2',role:'VIEWER',scopeTenant:'tenant',scopeRegion:'dev'};
const tenant:TenantRecord={id:'tenant',name:'Tenant',region:'dev',costCenter:'center',status:'ACTIVE',version:1};
const identity={producerId:'body-1',tenantId:'tenant',taskId:'task',attemptId:'attempt',fencing:1,executionInputDigest:'a'.repeat(64)};
const resources={cpuNanoCores:1,cpuMilliCores:1,memoryMiB:1,slots:1};
const zero={numerator:'0',denominator:'1'},half={numerator:'1',denominator:'2'};
function definition():RateDefinition{return {schemaVersion:1,unit:'credit-subunit',unitVersion:1,rounding:'HALF_EVEN',scope:{region:null,poolId:null},resources:{cpuNanoCoreMilliseconds:half,memoryMiBMilliseconds:zero,slotMilliseconds:zero},models:[{provider:'deepseek',requestedModel:'requested',reportedModel:'actual',input:{mode:'ALL',rate:half},output:zero}]};}
function fixture(){
 const dir=mkdtempSync(join(tmpdir(),'ansts-cost-'));
 const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');let db=new DatabaseSync(join(dir,'ledger.sqlite'));
 const storage:DirectoryStorage={sql:{exec<T>(q:string,...args:unknown[]){const rows=db.prepare(q).all(...args) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}}};
 const catalog=new RateCatalog(storage),rates=new TenantRateStore(storage),inbox=new UsageInbox(storage),ledger=new UsageCostLedger(storage),source=new BodyUsageJournal(join(dir,'source'));
 let counter=0;
 const create=(value=definition(),expectedVersion=0)=>catalog.create(admin,{expectedVersion,definition:value,reason:'Internal approved test rate',idempotencyKey:'rate-'+(++counter),requestId:'request'},10);
 const activate=(card:RateCard,expectedVersion=0)=>rates.activate(admin,tenant,{expectedVersion,rateCardVersion:card.version,reason:'Explicit activation',requestId:'request',idempotencyKey:'activation-'+(++counter)},card,20);
 const pin=(id=identity)=>{const price=rates.pin(admin,tenant,id,{costCenterVersion:1,poolId:null},30);inbox.bind(id,resources,{tenantVersion:1,region:'dev',costCenter:'center',rateCardVersion:price.activation?.rateCard.version??null},30);};
 const receive=(id=identity)=>{for(const event of source.events(id)){inbox.receive(event,id,100);ledger.project(event.eventId);}};
 return {dir,db,storage,catalog,rates,inbox,ledger,source,create,activate,pin,receive,reopen(){db.close();db=new DatabaseSync(join(dir,'ledger.sqlite'));return {catalog:new RateCatalog(storage),rates:new TenantRateStore(storage),inbox:new UsageInbox(storage),ledger:new UsageCostLedger(storage)};},close(){db.close();rmSync(dir,{recursive:true,force:true});}};
}
test('exact amounts preserve fractions, explicit tie-to-even and integer credit overflow rejection',()=>{
 assert.deepEqual(exactPrice('3',half),{numerator:'3',denominator:'2'});
 assert.deepEqual(exactAdd(half,half),{numerator:'1',denominator:'1'});
 assert.equal(roundCredit(half,'HALF_EVEN'),'0');assert.equal(roundCredit({numerator:'3',denominator:'2'},'HALF_EVEN'),'2');
 assert.equal(roundCredit(half,'FLOOR'),'0');assert.equal(roundCredit(half,'CEIL'),'1');
 assert.throws(()=>roundCredit({numerator:(MAX_CREDIT+1n).toString(),denominator:'1'},'FLOOR'),{code:'CREDIT_OVERFLOW'});
 assert.throws(()=>exactPrice(MAX_EXACT.toString(),{numerator:'2',denominator:'1'}),{code:'CREDIT_OVERFLOW'});
 assert.throws(()=>exactPrice('1',{numerator:'1',denominator:'0'}),{code:'INVALID_EXACT_AMOUNT'});
 assert.throws(()=>exactPrice('01',half),{code:'INVALID_EXACT_AMOUNT'});
});
test('directory rate versions are immutable, admin-only, CAS and idempotency-safe with audited atomic writes',()=>{
 const f=fixture();try{
  const input={expectedVersion:0,definition:definition(),reason:'Approved internal rate',idempotencyKey:'fixed-key',requestId:'request'};
  assert.throws(()=>f.catalog.create(viewer,input),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>f.catalog.create({...admin,scopeTenant:'tenant'},input),{code:'CONTROL_FORBIDDEN'});
  const first=f.catalog.create(admin,input,10);assert.deepEqual(f.catalog.create(admin,input,100),first);
  assert.throws(()=>f.catalog.create(admin,{...input,reason:'Changed'}),{code:'IDEMPOTENCY_CONFLICT'});
  assert.throws(()=>f.create(),{code:'VERSION_CONFLICT'});
  const next=f.create({...definition(),rounding:'CEIL'},1);assert.equal(next.version,2);assert.deepEqual(f.catalog.get(1),first);
  assert.equal(f.storage.sql.exec<{n:number}>('SELECT COUNT(*) AS n FROM directory_rate_audit').toArray()[0].n,2);
 }finally{f.close();}
});
test('rate schema rejects unknown dimensions, zero denominators, wildcard guesses and duplicate model selectors',()=>{
 const f=fixture();try{
  assert.throws(()=>f.create({...definition(),resources:{...definition().resources,estimatedTokens:half}} as RateDefinition),{code:'INVALID_RATE_CARD'});
  assert.throws(()=>f.create({...definition(),resources:{...definition().resources,slotMilliseconds:{numerator:'1',denominator:'0'}}}),{code:'INVALID_RATE_CARD'});
  assert.throws(()=>f.create({...definition(),models:[...definition().models,...definition().models]}),{code:'DUPLICATE_MODEL_RATE'});
  assert.throws(()=>f.create({...definition(),models:[{...definition().models[0],reportedModel:''}]}),{code:'INVALID_RATE_CARD'});
  assert.throws(()=>f.create({...definition(),unit:'usd'} as unknown as RateDefinition),{code:'INVALID_RATE_CARD'});
 }finally{f.close();}
});
test('rate creation and activation failures roll back version, receipt and audit together',()=>{
 const f=fixture();try{
  const original=f.storage.sql.exec.bind(f.storage.sql);f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT INTO directory_rate_audit'))throw new Error('write failed');return original(q,...args);};
  assert.throws(()=>f.create(),/write failed/);f.storage.sql.exec=original;const card=f.create();assert.equal(card.version,1);
  f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT INTO tenant_rate_audit'))throw new Error('write failed');return original(q,...args);};
  assert.throws(()=>f.activate(card),/write failed/);f.storage.sql.exec=original;assert.equal(f.rates.current(admin,tenant),null);assert.equal(f.activate(card).version,1);
 }finally{f.close();}
});
test('explicit activation pins old and null snapshots; later rate and metadata changes never reprice execution',()=>{
 const f=fixture();try{
  f.pin();assert.equal(f.rates.binding(identity)!.activation,null);
  const card=f.create();f.activate(card);assert.equal(f.rates.pin(admin,tenant,identity,{costCenterVersion:2,poolId:'new'},40).activation,null);
  const next={...identity,taskId:'task2',attemptId:'attempt2'};f.pin(next);assert.equal(f.rates.binding(next)!.activation!.rateCard.version,1);
  f.activate(f.create({...definition(),rounding:'CEIL'},1),1);assert.equal(f.rates.binding(next)!.activation!.rateCard.version,1);
  const changed={...tenant,version:2,costCenter:'other'};const third={...identity,taskId:'task3'};
  assert.equal(f.rates.pin(admin,changed,third,{costCenterVersion:1,poolId:null},50).activation,null);
  assert.throws(()=>f.rates.binding({...next,executionInputDigest:'b'.repeat(64)}),{code:'USAGE_PRICE_BINDING_CONFLICT'});
 }finally{f.close();}
});
test('activation verifies directory digest, tenant scope and exact published version',()=>{
 const f=fixture();try{
  const card=f.create();const input={expectedVersion:0,rateCardVersion:1,reason:'Activate',requestId:'request',idempotencyKey:'activation'};
  assert.throws(()=>f.rates.activate(viewer,tenant,input,card,20),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>f.rates.activate(admin,tenant,input,{...card,rounding:'CEIL'},20),{code:'RATE_DIGEST_CONFLICT'});
  assert.throws(()=>f.rates.activate(admin,tenant,{...input,rateCardVersion:2},card,20),{code:'RATE_VERSION_CONFLICT'});
  assert.throws(()=>f.rates.activate(admin,{...tenant,status:'DISABLED'},input,card,20),{code:'TENANT_DISABLED'});
  const scoped=f.create({...definition(),scope:{region:'other',poolId:null}},1);
  assert.throws(()=>f.activate(scoped),{code:'RATE_SCOPE_CONFLICT'});
 }finally{f.close();}
});
test('resource entries round only after aggregation, replay once and recompute after native SQLite object reconstruction',()=>{
 const f=fixture();try{
  f.activate(f.create());f.pin();f.source.begin(identity,resources,1000);f.source.checkpoint(identity,1001);f.source.finish(identity,1002);f.receive();
  const events=f.source.events(identity);const before=f.ledger.summary(viewer,tenant);assert.equal(before.totalCreditUnits,'1');assert.equal(before.final,false);
  assert.equal(before.buckets.find(b=>b.dimension==='cpuNanoCoreMilliseconds')!.exactCredit.numerator,'1');
  assert.deepEqual(f.ledger.project(events[1].eventId),f.ledger.project(events[1].eventId));
  const rebuilt=new UsageCostLedger(f.storage);assert.deepEqual(rebuilt.summary(viewer,tenant),before);
  // Drop only the derived projection in this isolated test: accepted raw events and pinned prices suffice to rebuild.
  f.storage.sql.exec('DELETE FROM tenant_usage_cost_entries');for(const event of events)rebuilt.project(event.eventId);
  assert.deepEqual(rebuilt.summary(viewer,tenant),before);
  assert.throws(()=>rebuilt.summary({...viewer,scopeTenant:'other'},tenant),{code:'CONTROL_FORBIDDEN'});
 }finally{f.close();}
});
test('new rates do not backfill historical events without an execution snapshot',()=>{
 const f=fixture();try{
  f.inbox.bind(identity,resources,{tenantVersion:1,region:'dev',costCenter:'center',rateCardVersion:null});
  f.source.begin(identity,resources,1000);f.source.finish(identity,1010);f.receive();
  const first=f.ledger.summary(admin,tenant);assert.equal(first.totalCreditUnits,null);assert.ok(first.reasons.includes('PRICE_SNAPSHOT_MISSING'));
  f.activate(f.create());assert.throws(()=>f.pin(),{code:'USAGE_PRICE_PIN_TOO_LATE'});for(const event of f.source.events(identity))f.ledger.project(event.eventId);
  assert.deepEqual(f.ledger.summary(admin,tenant),first);
 }finally{f.close();}
});
test('actual provider counts use exact reported model and cache split without charging input twice',()=>{
 const f=fixture();try{
  const def=definition();def.resources.cpuNanoCoreMilliseconds=zero;def.models[0].input={mode:'CACHE_SPLIT',hit:{numerator:'1',denominator:'4'},miss:{numerator:'2',denominator:'1'}};def.models[0].output={numerator:'3',denominator:'1'};
  f.activate(f.create(def));f.pin();f.source.begin(identity,resources,1000);f.source.providerStarted(identity,1,{provider:'deepseek',model:'requested'},1001);
  f.source.providerFinished(identity,1,{reportedModel:'actual',responseId:'response',inputTokens:'12',outputTokens:'8',totalTokens:'20',cacheHitTokens:'4',cacheMissTokens:'8',evidenceDigest:'b'.repeat(64),source:'PROVIDER_RESPONSE',completeness:'COMPLETE'},1002);f.source.finish(identity,1010);f.receive();
  const summary=f.ledger.summary(admin,tenant);assert.equal(summary.totalCreditUnits,'41');assert.ok(!summary.buckets.some(b=>b.dimension==='inputTokens'));assert.deepEqual(summary.buckets.filter(b=>b.dimension.endsWith('Tokens')).map(b=>b.dimension).sort(),['cacheHitTokens','cacheMissTokens','outputTokens']);
 }finally{f.close();}
});
test('cache quantity missing, reported-model mismatch and unresolved calls keep costs pending',()=>{
 for(const variant of ['cache','model','lost']){const f=fixture();try{
  const def=definition();def.models[0].input={mode:'CACHE_SPLIT',hit:half,miss:half};f.activate(f.create(def));f.pin();f.source.begin(identity,resources,1000);f.source.providerStarted(identity,1,{provider:'deepseek',model:'requested'},1001);
  if(variant!=='lost')f.source.providerFinished(identity,1,{reportedModel:variant==='model'?'unapproved':'actual',responseId:'response',inputTokens:'12',outputTokens:'8',totalTokens:'20',cacheHitTokens:null,cacheMissTokens:null,evidenceDigest:'b'.repeat(64),source:'PROVIDER_RESPONSE',completeness:'COMPLETE'},1002);
  f.source.finish(identity,1010);f.receive();const summary=f.ledger.summary(admin,tenant);assert.equal(summary.totalCreditUnits,null);assert.ok(summary.reasons.includes(variant==='lost'?'PROVIDER_USAGE_INCOMPLETE':variant==='model'?'MODEL_RATE_MISSING':'QUANTITY_MISSING'));
 }finally{f.close();}}
});
test('ledger append is atomic and requires accepted source bytes; unprojected, open and quarantined streams stay incomplete',()=>{
 const f=fixture();try{
  f.activate(f.create());f.pin();f.source.begin(identity,resources,1000);const open=f.source.events(identity)[0];assert.throws(()=>f.ledger.project(open.eventId),{code:'ACCEPTED_USAGE_REQUIRED'});f.inbox.receive(open,identity);
  assert.ok(f.ledger.summary(admin,tenant).reasons.includes('UNPROJECTED_USAGE'));
  const original=f.storage.sql.exec.bind(f.storage.sql);f.storage.sql.exec=(q,...args)=>{if(q.startsWith('INSERT INTO tenant_usage_cost_entries'))throw new Error('storage failed');return original(q,...args);};
  assert.throws(()=>f.ledger.project(open.eventId),/storage failed/);f.storage.sql.exec=original;
  f.ledger.project(open.eventId);assert.ok(f.ledger.summary(admin,tenant).reasons.includes('OPEN_USAGE_STREAM'));
  f.inbox.receive({...open,at:1001},identity);assert.ok(f.ledger.summary(admin,tenant).reasons.includes('USAGE_QUARANTINED'));
 }finally{f.close();}
});
test('UTC resource segments remain in source day, each pricing dimension has an explicit rounding bucket',()=>{
 const f=fixture();try{
  f.activate(f.create());f.pin();const midnight=Date.UTC(2026,10,17);f.source.begin(identity,resources,midnight-1);f.source.finish(identity,midnight+1);f.receive();
  const summary=f.ledger.summary(admin,tenant);assert.equal(summary.totalCreditUnits,'0');assert.deepEqual([...new Set(summary.buckets.map(b=>b.periodId))],['2026-11-16','2026-11-17']);
  assert.equal(summary.roundingScope,'UTC_DAY_COST_CENTER_VERSION_RATE_ACTIVATION_DIMENSION');
 }finally{f.close();}
});
test('missing ownership, unknown pool selector and legacy unknown resources never become complete zero cost',()=>{
 for(const variant of ['ownership','pool','legacy']){const f=fixture();try{
  const def=definition();if(variant==='pool')def.scope.poolId='pool-a';f.activate(f.create(def));
  f.rates.pin(admin,tenant,identity,{costCenterVersion:variant==='ownership'?null:1,poolId:null},30);
  const allocation=variant==='legacy'?{cpuNanoCores:null,cpuMilliCores:null,memoryMiB:null,slots:1}:resources;
  f.inbox.bind(identity,allocation,{tenantVersion:1,region:'dev',costCenter:'center',rateCardVersion:1});f.source.begin(identity,allocation,1000);f.source.finish(identity,1010);f.receive();
  const summary=f.ledger.summary(admin,tenant);assert.equal(summary.totalCreditUnits,null);assert.ok(summary.reasons.includes(variant==='ownership'?'COST_CENTER_VERSION_MISSING':variant==='pool'?'RATE_SCOPE_UNKNOWN_OR_MISMATCH':'QUANTITY_MISSING'));
 }finally{f.close();}}
});
test('aggregate availability and pending are explicit; partial subtotal never masquerades as a complete zero',()=>{
 const complete={knownSubtotalCreditUnits:'2',totalCreditUnits:'2',complete:true};
 const results=new Map<string,{knownSubtotalCreditUnits:string;totalCreditUnits:string|null;complete:boolean}>([['a',complete]]);const partial=aggregateTenantCosts(['a','b'],results);assert.equal(partial.totalCreditUnits,null);assert.equal(partial.knownSubtotalCreditUnits,'2');assert.deepEqual(partial.missingTenantIds,['b']);
 results.set('b',{knownSubtotalCreditUnits:'0',totalCreditUnits:null,complete:false});assert.equal(aggregateTenantCosts(['a','b'],results).complete,false);
 results.set('b',complete);assert.equal(aggregateTenantCosts(['a','b'],results).totalCreditUnits,'4');
 assert.throws(()=>aggregateTenantCosts(['a','a'],results),{code:'INVALID_AGGREGATE_SCOPE'});
});
test('binding without a first source event cannot report a complete empty total',()=>{
 const f=fixture();try{f.activate(f.create());f.pin();const summary=f.ledger.summary(viewer,tenant);assert.equal(summary.totalCreditUnits,null);assert.ok(summary.reasons.includes('OPEN_USAGE_STREAM'));}finally{f.close();}
});
test('large accepted provider evidence fails pricing closed without committing a partial entry',()=>{
 const f=fixture();try{
  f.activate(f.create());f.pin();f.source.begin(identity,resources,1000);f.source.providerStarted(identity,1,{provider:'deepseek',model:'requested'},1001);
  const count=(MAX_EXACT+1n).toString();f.source.providerFinished(identity,1,{reportedModel:'actual',responseId:'response',inputTokens:count,outputTokens:'0',totalTokens:count,cacheHitTokens:null,cacheMissTokens:null,evidenceDigest:'b'.repeat(64),source:'PROVIDER_RESPONSE',completeness:'COMPLETE'},1002);f.source.finish(identity,1010);
  for(const event of f.source.events(identity)){f.inbox.receive(event,identity);if(event.kind==='PROVIDER_USAGE'){assert.throws(()=>f.ledger.project(event.eventId),{code:'CREDIT_OVERFLOW'});}else f.ledger.project(event.eventId);}
  const summary=f.ledger.summary(admin,tenant);assert.equal(summary.totalCreditUnits,null);assert.ok(summary.reasons.includes('UNPROJECTED_USAGE'));
 }finally{f.close();}
});
test('closing and reopening the native database retains rate audit, explicit activation, pinned nulls and immutable projection',()=>{
 const f=fixture();try{
  f.pin();const first=f.create();f.activate(first);const priced={...identity,taskId:'priced',attemptId:'priced'};f.pin(priced);
  for(const id of [identity,priced]){f.source.begin(id,resources,1000);f.source.finish(id,1002);f.receive(id);}
  const before=f.ledger.summary(admin,tenant),pending=f.ledger.project(f.source.events(identity)[1].eventId);
  const recovered=f.reopen();assert.deepEqual(recovered.catalog.get(1),first);assert.equal(recovered.rates.current(admin,tenant)!.rateCard.digest,first.digest);assert.equal(recovered.rates.binding(identity)!.activation,null);assert.equal(recovered.rates.binding(priced)!.activation!.rateCard.version,1);assert.deepEqual(recovered.ledger.summary(admin,tenant),before);
  f.storage.sql.exec('DELETE FROM tenant_usage_cost_entries');for(const id of [identity,priced])for(const event of f.source.events(id))recovered.ledger.project(event.eventId);
  assert.deepEqual(recovered.ledger.project(f.source.events(identity)[1].eventId),pending);assert.deepEqual(recovered.ledger.summary(admin,tenant),before);
 }finally{f.close();}
});
test('tenant authority rejects cross-tenant pins and incompatible authenticated usage context',()=>{
 const f=fixture();try{
  f.activate(f.create());f.rates.pin(admin,tenant,identity,{costCenterVersion:1,poolId:null},30);
  assert.throws(()=>f.rates.pin(admin,{...tenant,id:'other'},{...identity,tenantId:'other'},{costCenterVersion:1,poolId:null},30),{code:'TENANT_RATE_IDENTITY_CONFLICT'});
  f.inbox.bind(identity,resources,{tenantVersion:1,region:'dev',costCenter:'different-center',rateCardVersion:null});f.source.begin(identity,resources,1000);const open=f.source.events(identity)[0];f.inbox.receive(open,identity);assert.throws(()=>f.ledger.project(open.eventId),{code:'USAGE_PRICE_CONTEXT_CONFLICT'});
 }finally{f.close();}
});
