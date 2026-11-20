import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {CapacityInventory,type HostWrite,type PoolWrite} from '../../src/cluster/capacity_inventory.js';
import {canonicalCapacityReport,signCapacityReport,verifyCapacityReport,type CapacityReport} from '../../src/control/capacity_report.js';
import {CapacityReporter,physicalHostIdFromMachineId} from '../../src/runtime/capacity_reporter.js';
import {CapacityInventoryCell} from '../../src/cloudflare/capacity_inventory_cell.js';
import {handleCapacityApi} from '../../src/control/admin_capacity_api.js';
import {handleCapacityIngress} from '../../src/control/capacity_ingress.js';
import {bodyServiceToken} from '../../src/cloudflare/body_auth.js';
import type {DirectoryStorage} from '../../src/control/tenant_directory.js';
import type {CloudflareCelldEnv,DurableObjectNamespaceLike,DurableObjectStateLike} from '../../src/cloudflare/env.js';
const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite') as {DatabaseSync:new(path:string)=>{exec(q:string):void;prepare(q:string):{all(...args:unknown[]):unknown[]};close():void}};
const admin={sub:'github:1',role:'SRE_ADMIN' as const,scopeTenant:'all',scopeRegion:'all'},limit={cpuMilliCores:1000,memoryMiB:1024,slots:2};
const pool:PoolWrite={id:'pool',name:'Pool',region:'dev',status:'ACTIVE',limit,expectedVersion:0,reason:'Capacity fixture',requestId:'pool',idempotencyKey:'pool'};
const host:HostWrite={id:'physical-1',poolId:'pool',daemonIds:['d1','d2'],limit,status:'ACTIVE',expectedVersion:0,reason:'Bind same physical Host',requestId:'host',idempotencyKey:'host'};
function fixture(path=':memory:'){
 const db=new DatabaseSync(path),storage:DirectoryStorage={sql:{exec<T>(query:string,...values:unknown[]){const rows=db.prepare(query).all(...values) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}}};
 const inventory=new CapacityInventory(storage);return {db,storage,inventory};
}
function report(daemonId='d1',sequence=1,overrides:Partial<CapacityReport>={}):CapacityReport{return {schemaVersion:1,purpose:'ansts-capacity-report-v1',hostId:'physical-1',daemonId,epoch:1,sequence,observedAt:10000,total:limit,reserved:{cpuMilliCores:100,memoryMiB:128,slots:1},draining:false,recoveryRequired:false,...overrides};}
function setup(s:ReturnType<typeof fixture>){s.inventory.writePool(admin,pool,1000);s.inventory.writeHost(admin,host,1000);}
test('multiple Daemons on one Host never multiply capacity; offline snapshots retain occupancy',()=>{
 const s=fixture();try{setup(s);assert.equal(s.inventory.snapshot(admin,10000).hosts[0].health,'UNVERIFIED');s.inventory.report('d1',report(),10000);assert.equal(s.inventory.snapshot(admin,10000).hosts[0].available.cpuMilliCores,0);
  s.inventory.report('d2',report('d2'),10000);const view=s.inventory.snapshot(admin,10000);assert.deepEqual(view.pools[0].capacity,limit);assert.deepEqual(view.hosts[0].retainedReserved,{cpuMilliCores:200,memoryMiB:256,slots:2});assert.equal(view.hosts[0].available.cpuMilliCores,800);
  s.inventory.report('d1',report('d1',2,{observedAt:24000,reserved:{cpuMilliCores:0,memoryMiB:0,slots:0}}),24000);
  const stale=s.inventory.snapshot(admin,25000).hosts[0];assert.equal(stale.health,'STALE');assert.deepEqual(stale.available,{cpuMilliCores:0,memoryMiB:0,slots:0});assert.equal(stale.retainedReserved.slots,1);
  assert.equal(s.inventory.snapshot(admin,9999).hosts[0].available.cpuMilliCores,0);
 }finally{s.db.close();}
});
test('unique Host ownership and Daemon membership cannot be moved across pools or silently removed',()=>{
 const s=fixture();try{setup(s);s.inventory.writePool(admin,{...pool,id:'p2',idempotencyKey:'p2'},1000);
  assert.throws(()=>s.inventory.writeHost(admin,{...host,poolId:'p2',expectedVersion:1,idempotencyKey:'move'},1001),{code:'HOST_BINDING_IMMUTABLE'});
  assert.throws(()=>s.inventory.writeHost(admin,{...host,daemonIds:['d1'],expectedVersion:1,idempotencyKey:'remove'},1001),{code:'HOST_BINDING_IMMUTABLE'});
  assert.throws(()=>s.inventory.writeHost(admin,{...host,id:'another',poolId:'p2',idempotencyKey:'duplicate'},1001),{code:'DAEMON_ALREADY_REGISTERED'});
  assert.throws(()=>s.inventory.report('d3',report('d3'),10000),{code:'CAPACITY_HOST_NOT_REGISTERED'});
 }finally{s.db.close();}
});
test('administrator ceilings, drain and recovery conditions constrain newly available capacity',()=>{
 const s=fixture();try{setup(s);s.inventory.report('d1',report(),10000);s.inventory.report('d2',report('d2'),10000);
  s.inventory.writeHost(admin,{...host,limit:{cpuMilliCores:150,memoryMiB:128,slots:1},expectedVersion:1,idempotencyKey:'lower'},10001);
  const over=s.inventory.snapshot(admin,10001).hosts[0];assert.equal(over.health,'OVERCOMMITTED');assert.equal(over.retainedReserved.cpuMilliCores,200);assert.equal(over.available.cpuMilliCores,0);
  s.inventory.report('d1',report('d1',2,{draining:true,observedAt:10002}),10002);assert.equal(s.inventory.snapshot(admin,10002).hosts[0].health,'DRAINING');
  s.inventory.report('d2',report('d2',2,{recoveryRequired:true,observedAt:10002}),10002);assert.equal(s.inventory.snapshot(admin,10002).hosts[0].health,'QUARANTINED');
 }finally{s.db.close();}
});
test('replays cannot refresh freshness and stale epochs cannot overwrite current occupancy',()=>{
 const s=fixture();try{setup(s);s.inventory.report('d1',report(),10000);s.inventory.report('d2',report('d2'),10000);
  assert.equal(s.inventory.report('d1',report(),24000).outcome,'REPLAY');assert.equal(s.inventory.snapshot(admin,25000).hosts[0].health,'STALE');
  s.inventory.report('d1',report('d1',1,{epoch:2,observedAt:24001}),24001);
  assert.throws(()=>s.inventory.report('d1',report('d1',100,{observedAt:24002}),24002),{code:'STALE_CAPACITY_REPORT'});
  assert.throws(()=>s.inventory.report('d2',report('d2',2,{observedAt:50000}),25000),{code:'CAPACITY_REPORT_NOT_FRESH'});
 }finally{s.db.close();}
});
test('same report identity with different payload persists quarantine across restart',()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-capacity-'));let s=fixture(join(dir,'state.sqlite'));try{setup(s);s.inventory.report('d1',report(),10000);s.inventory.report('d2',report('d2'),10000);
  assert.equal(s.inventory.report('d1',report('d1',1,{reserved:{cpuMilliCores:0,memoryMiB:0,slots:0}}),10001).outcome,'CONFLICT');s.db.close();s=fixture(join(dir,'state.sqlite'));
  const view=s.inventory.snapshot(admin,10001);assert.equal(view.conflictCount,1);assert.equal(view.hosts[0].health,'QUARANTINED');assert.equal(view.hosts[0].retainedReserved.slots,2);
  s.inventory.report('d1',report('d1',1,{epoch:2,observedAt:10002}),10002);assert.equal(s.inventory.snapshot(admin,10002).hosts[0].health,'QUARANTINED');
 }finally{s.db.close();rmSync(dir,{recursive:true,force:true});}
});
test('CAS, actor-scoped replay and audit commit atomically with administrator changes',()=>{
 const s=fixture();try{const first=s.inventory.writePool(admin,pool,1000);assert.deepEqual(s.inventory.writePool(admin,{...pool,requestId:'replay'},1001),first);assert.equal(s.inventory.audit(admin).length,1);
  assert.throws(()=>s.inventory.writePool(admin,{...pool,name:'changed'},1001),{code:'IDEMPOTENCY_CONFLICT'});
  assert.throws(()=>s.inventory.writePool(admin,{...pool,idempotencyKey:'stale'},1001),{code:'VERSION_CONFLICT'});
  const original=s.storage.sql.exec.bind(s.storage.sql);s.storage.sql.exec=(q,...values)=>{if(q.startsWith('INSERT OR REPLACE INTO capacity_inventory'))throw new Error('disk failed');return original(q,...values);};
  assert.throws(()=>s.inventory.writeHost(admin,host,1001),/disk failed/);s.storage.sql.exec=original;assert.equal(s.inventory.snapshot(admin,1001).hosts.length,0);assert.equal(s.inventory.audit(admin).length,1);assert.equal(s.inventory.writeHost(admin,host,1002).record.version,1);
  for(const actor of [{...admin,role:'VIEWER' as const},{...admin,scopeTenant:'tenant'},{...admin,scopeRegion:'dev'}])assert.throws(()=>s.inventory.snapshot(actor,1000),{code:'CONTROL_FORBIDDEN'});
 }finally{s.db.close();}
});
test('reporter stable physical identity and durable epoch/sequence reject corrupt source identity',()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-capacity-reporter-')),file=join(dir,'reporter.json'),key='a'.repeat(64),id=physicalHostIdFromMachineId('a'.repeat(32));try{
  assert.equal(id,physicalHostIdFromMachineId('a'.repeat(32)+'\n'));assert.throws(()=>physicalHostIdFromMachineId(''),/INVALID_PHYSICAL_HOST_IDENTITY/);
  let reporter=new CapacityReporter(file,id,'d1',key);const input={total:limit,reserved:{cpuMilliCores:0,memoryMiB:0,slots:0},draining:false,recoveryRequired:false};
  const first=reporter.capture(input,10000);assert.equal(verifyCapacityReport(first.proof,key).sequence,1);assert.equal(reporter.capture(input,10001).report.sequence,2);
  reporter=new CapacityReporter(file,id,'d1',key);const next=reporter.capture(input,10002);assert.equal(next.report.epoch,2);assert.equal(next.report.sequence,1);
  assert.throws(()=>verifyCapacityReport(first.proof,'b'.repeat(64)),{code:'CAPACITY_REPORT_AUTH_REQUIRED'});assert.throws(()=>new CapacityReporter(file,'different','d1',key),/CAPACITY_REPORTER_IDENTITY_CONFLICT/);
 }finally{rmSync(dir,{recursive:true,force:true});}
});
test('capacity ingress authenticates report purpose and binds reporter to its service identity',async()=>{
 const s=fixture();setup(s);const root='f'.repeat(64),token='d'.repeat(64),key=bodyServiceToken(root,'d1');
 const cell=new CapacityInventoryCell({storage:s.storage} as unknown as DurableObjectStateLike,{ANSTS_DIRECTORY_SERVICE_TOKEN:token});
 const ns:DurableObjectNamespaceLike={idFromName:name=>({toString:()=>name}),get:()=>({id:{toString:()=>''},fetch:r=>cell.fetch(new Request(r))})};
 const env={ANSTS_DIRECTORY_SERVICE_TOKEN:token,ANSTS_BODY_AUTH_SECRET:root,ANSTS_CAPACITY_INVENTORY_CELL:ns} as CloudflareCelldEnv;
 const request=(body:unknown,auth='Body '+key)=>new Request('https://fleet/internal/v1/capacity/report',{method:'POST',headers:{authorization:auth,'x-ansts-body-id':'d1'},body:JSON.stringify(body)});
 try{
  assert.equal((await handleCapacityIngress(request({proof:'bad'},'Bearer '+token),env)).status,401);
  assert.equal((await handleCapacityIngress(request({proof:signCapacityReport(report('d2'),key)}),env)).status,403);
  const now=Date.now();assert.equal((await handleCapacityIngress(request({proof:signCapacityReport(report('d1',1,{observedAt:now}),key)}),env)).status,200);
  const viewer=await handleCapacityApi(new Request('https://gateway/v1/admin/capacity/pools'),env,{...admin,role:'VIEWER'});assert.equal(viewer.status,403);
  const read=await handleCapacityApi(new Request('https://gateway/v1/admin/capacity/hosts'),env,admin);assert.equal(read.status,200);assert.equal((await read.json()).hosts.length,1);
  const write=await handleCapacityApi(new Request('https://gateway/v1/admin/capacity/pools',{method:'POST',headers:{'idempotency-key':'readonly'},body:JSON.stringify(pool)}),env,{...admin,readOnly:true});assert.equal(write.status,403);
 }finally{s.db.close();}
});

test('native Daemon signs actual local resource snapshots and advances epoch across restart',async()=>{
 const {BodyDaemonServer}=await import('../../src/runtime/body_daemon_server.js');
 const dir=mkdtempSync(join(tmpdir(),'ansts-capacity-daemon-')),key='a'.repeat(64),hostId=physicalHostIdFromMachineId('b'.repeat(32));
 const options={nodeId:'daemon-1',physicalHostId:hostId,token:key,storageDir:dir,bodyWorkerUrl:'http://127.0.0.1:9',host:'127.0.0.1',port:0,executionSlots:2,resources:{cpu:1,memoryMb:1024}};
 let daemon=new BodyDaemonServer(options);
 try{
  let {port}=await daemon.start();const capture=async(auth=true)=>fetch('http://127.0.0.1:'+port+'/v1/daemon/capacity-report',{method:'POST',headers:{'content-type':'application/json',...(auth?{authorization:'Body '+key,'x-ansts-body-id':'daemon-1'}:{})},body:'{}'});
  assert.equal((await capture(false)).status,401);const first=await capture();assert.equal(first.status,200);const report=verifyCapacityReport((await first.json()).proof,key);assert.deepEqual(report.total,limit);assert.equal(report.reserved.slots,0);assert.equal(report.hostId,hostId);
  await daemon.stop();daemon=new BodyDaemonServer(options);({port}=await daemon.start());const next=verifyCapacityReport((await (await capture()).json()).proof,key);assert.equal(next.epoch,report.epoch+1);assert.equal(next.sequence,1);
 }finally{await daemon.stop();rmSync(dir,{recursive:true,force:true});}
});

test('capacity request API enforces tenant, region and operator write scope', async () => {
 const fixtureState = fixture();
 try {
  setup(fixtureState);
  const token = 'capacity-service-token'.padEnd(32, '0');
  const cell = new CapacityInventoryCell(
   {storage:fixtureState.storage} as DurableObjectStateLike,
   {ANSTS_DIRECTORY_SERVICE_TOKEN:token},
  );
  const env = {
   ANSTS_DIRECTORY_SERVICE_TOKEN:token,
   ANSTS_CAPACITY_INVENTORY_CELL:{idFromName:()=> 'inventory',get:()=>cell},
  } as unknown as Parameters<typeof handleCapacityApi>[1];
  const operator = {sub:'operator',role:'OPERATOR' as const,scopeTenant:'team',scopeRegion:'dev'};
  const path = 'https://gateway.test/v1/admin/capacity/requests?poolId=pool&tenantId=';
  const submit = (tenant: string, principal: Parameters<typeof handleCapacityApi>[2]) =>
   handleCapacityApi(new Request(path+tenant,{
    method:'POST',headers:{'idempotency-key':'once'},
    body:JSON.stringify({resources:{...limit,slots:1},priority:999,tenantId:'other'}),
   }),env,principal);

  const control = (name:string, input?:object, principal:Parameters<typeof handleCapacityApi>[2]=admin) =>
   handleCapacityApi(new Request('https://gateway.test/v1/admin/capacity/'+name+'?poolId=pool',
    input?{method:'PUT',body:JSON.stringify(input)}:undefined),env,principal);
  assert.equal((await control('allocation-control',undefined,operator)).status,403);
  assert.deepEqual(await (await control('allocation-control')).json(),{version:0,enabled:true});
  assert.deepEqual(await (await control('allocation-control',{expectedVersion:0,enabled:false})).json(),
   {version:1,enabled:false});
  assert.equal((await control('allocation-control',{expectedVersion:0,enabled:true})).status,409);
  assert.deepEqual(await (await control('queue-policy',{expectedVersion:0,maxWaitMs:2000,weights:{team:3},priorities:{team:20}})).json(),
   {version:1,maxWaitMs:2000,weights:{team:3},priorities:{team:20},guaranteedShares:{}});
  assert.equal((await submit('other',operator)).status,403);
  assert.equal((await submit('team',{...operator,scopeRegion:'elsewhere'})).status,403);
  assert.equal((await submit('team',{...operator,role:'VIEWER'})).status,403);
  assert.equal((await submit('team',{...operator,readOnly:true})).status,403);
  const created = await submit('team',operator);
  assert.equal(created.status,200);
  const request = await created.json() as {id:string;tenantId:string;priority:number};
  assert.equal(request.tenantId,'team');
  assert.equal(request.priority,0);
  assert.deepEqual(await (await submit('team',operator)).json(),request);
  assert.equal((await handleCapacityApi(new Request(path+'team'),env,{...operator,scopeTenant:'other'})).status,403);
  const viewer = {...operator,role:'VIEWER' as const};
  assert.deepEqual(await (await handleCapacityApi(new Request(path+'team'),env,viewer)).json(),{requests:[{...request,queueReason:'ALLOCATION_PAUSED'}]});
  const cancel = () => new Request(path+'team&id='+request.id,{method:'DELETE'});
  assert.equal((await handleCapacityApi(cancel(),env,viewer)).status,403);
  assert.equal((await handleCapacityApi(cancel(),env,operator)).status,200);
  assert.equal((await (await submit('team',operator)).json() as {state:string}).state,'CANCELLED');
 } finally {
  fixtureState.db.close();
 }
});



test('grant issuance reads tenant authority and reports held capacity to the queue',async()=>{
 const s=fixture();
 try{
  setup(s);
  const now=Date.now();
  for(const daemon of ['d1','d2'])s.inventory.report(daemon,report(daemon,1,{
   observedAt:now,reserved:{cpuMilliCores:0,memoryMiB:0,slots:0},
  }),now);
  const token='directory-token'.padEnd(36,'0'),admissionToken='admission-token'.padEnd(36,'0');
  const queried:string[]=[];
  let alarmAt:number|null=null;
  const storage={...s.storage,getAlarm:async()=>alarmAt,setAlarm:async(at:number)=>{alarmAt=at;}};
  const cell=new CapacityInventoryCell({storage} as DurableObjectStateLike,{
   ANSTS_DIRECTORY_SERVICE_TOKEN:token,ANSTS_ADMISSION_SERVICE_TOKEN:admissionToken,
   ANSTS_TENANT_CONTROL_CELL:{
    idFromName:(id:string)=>id,
    get:()=>({fetch:async(request:Request)=>{
     assert.equal(request.headers.get('authorization'),'Bearer '+admissionToken);
     const {tenantId}=await request.json() as {tenantId:string};
     queried.push(tenantId);
     return Response.json({used:{cpuMilliCores:0,memoryMiB:0,slots:0},
      remaining:tenantId==='limited'?{...limit,slots:0}:limit});
    }}),
   } as unknown as DurableObjectNamespaceLike,
  });
  const post=(path:string,body:object)=>cell.fetch(new Request('https://capacity.internal/'+path,{
   method:'POST',headers:{authorization:'Bearer '+token},body:JSON.stringify(body),
  }));
  const unsupported=await post('request-submit',{
   actor:admin,requestInput:{poolId:'pool',tenantId:'eligible',key:'multi-slot',resources:limit},
  });
  assert.equal(unsupported.status,422);
  const guarantee=await post('queue-policy-write',{actor:admin,input:{
   id:'pool',expectedVersion:0,maxWaitMs:1000,weights:{},guaranteedShares:{eligible:{...limit,slots:1}},
  }});
  assert.equal(guarantee.status,200);
  const borrower=await post('request-submit',{actor:admin,requestInput:{
   poolId:'pool',tenantId:'borrower',key:'guarantee-check',resources:{...limit,slots:1},
  }});
  assert.equal(borrower.status,200);
  const borrowerView=await post('pool-catalog',{actor:admin,requestInput:{tenantId:'borrower'}});
  const borrowerPool=(await borrowerView.json() as {pools:{available:typeof limit;guaranteedShare:null}[]}).pools[0];
  assert.deepEqual(borrowerPool.available,{cpuMilliCores:0,memoryMiB:0,slots:1});
  assert.equal(borrowerPool.guaranteedShare,null);
  assert.deepEqual(await (await post('grant-issue',{actor:admin,input:{id:'pool'}})).json(),{grant:null});
  const {id:borrowerId}=await borrower.json() as {id:string};
  assert.equal((await post('request-cancel',{actor:admin,requestInput:{poolId:'pool',tenantId:'borrower',id:borrowerId}})).status,200);
  queried.length=0;
  let eligibleId='';
  for(const tenantId of ['limited','eligible']){
   const result=await post('request-submit',{actor:admin,requestInput:{poolId:'pool',tenantId,key:'request',resources:{...limit,slots:1}}});
   assert.equal(result.status,200);
   const submitted=await result.json() as {id:string};
   if(tenantId==='eligible')eligibleId=submitted.id;
  }
  assert.ok(alarmAt!==null);
  alarmAt=null;
  await cell.alarm();
  assert.ok(alarmAt!==null);
  const issued=await post('grant-read',{actor:admin,requestInput:{poolId:'pool',tenantId:'eligible',id:eligibleId}});
  assert.equal(issued.status,200);
  const {grant}=await issued.json() as {grant:{hostId:string;requestId:string;target:string;capacityReceipt:string}};
  assert.equal(grant.hostId,'physical-1');
  assert.equal(grant.target,'d1');
  assert.equal(grant.capacityReceipt,'capacity-grant:'+grant.requestId);
  const readGrant = (tenantId:string) => post('grant-read',{actor:admin,requestInput:{poolId:'pool',tenantId,id:grant.requestId}});
  assert.deepEqual(await (await readGrant('eligible')).json(),{grant});
  assert.deepEqual(await (await readGrant('limited')).json(),{grant:null});
  const catalog=await post('pool-catalog',{actor:admin,requestInput:{tenantId:'limited'}});
  const view=await catalog.json() as {pools:{available:typeof limit;activeGrants:number}[]};
  assert.equal(view.pools[0].activeGrants,1);
  assert.deepEqual(view.pools[0].available,{cpuMilliCores:0,memoryMiB:0,slots:1});
  assert.deepEqual(queried.sort(),['eligible','limited']);
  const listed=await post('request-list',{actor:admin,requestInput:{poolId:'pool',tenantId:'limited'}});
  assert.equal((await listed.json() as {requests:{queueReason:string}[]}).requests[0].queueReason,'HOST_RESOURCES_UNAVAILABLE');
  assert.deepEqual(await (await post('grant-issue',{actor:admin,input:{id:'pool'}})).json(),{grant:null});
  const binding={reservationId:'reservation',attemptId:'attempt',generation:1,fencing:7};
  const readBound=(candidate= binding)=>post('grant-read',{
   actor:admin,requestInput:{poolId:'pool',tenantId:'eligible',id:grant.requestId,binding:candidate},
  });
  assert.deepEqual(await (await readBound()).json(),{grant:null});
  const reservation={
   id:binding.reservationId,attemptId:binding.attemptId,generation:binding.generation,fencing:binding.fencing,
   tenantId:'eligible',policy:{region:'dev'},phase:'PREPARING',expiresAt:Date.now()+10_000,
   resources:{cpuMilliCores:limit.cpuMilliCores,memoryMiB:limit.memoryMiB,concurrentAttempts:1,queuedTasks:0},
  };
  const wrongDaemon=await post('grant-bind',{requestId:grant.requestId,target:'d2',reservation});
  assert.equal(wrongDaemon.status,409);
  assert.deepEqual(await wrongDaemon.json(),{code:'CAPACITY_TARGET_UNAVAILABLE'});
  const bind=await post('grant-bind',{requestId:grant.requestId,target:grant.target,reservation});
  assert.deepEqual(await bind.json(),{capacityReceipt:grant.capacityReceipt});
  assert.deepEqual(await (await readBound()).json(),{grant});
  assert.deepEqual(await (await readBound({...binding,fencing:6})).json(),{grant:null});
  const restartedAt=Date.now();
  s.inventory.report('d1',report('d1',1,{
   epoch:2,observedAt:restartedAt,reserved:{cpuMilliCores:0,memoryMiB:0,slots:0},
  }),restartedAt);
  assert.deepEqual(await (await readGrant('eligible')).json(),{grant:null});
  assert.deepEqual(await (await readBound()).json(),{grant:null});
  const staleBind=await post('grant-bind',{requestId:grant.requestId,target:grant.target,reservation:{
   tenantId:'eligible',policy:{region:'dev'},
  }});
  assert.equal(staleBind.status,409);
  assert.deepEqual(await staleBind.json(),{code:'CAPACITY_TARGET_UNAVAILABLE'});
 }finally{s.db.close();}
});











