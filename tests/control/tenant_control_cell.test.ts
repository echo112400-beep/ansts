import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {TenantControlCell,type TenantControlEnv} from '../../src/cloudflare/tenant_control_cell.js';
import {AdmissionInbox,type AdmissionMessage} from '../../src/control/admission_messages.js';
import {signBodyEvidence,type BodyExecutionEvidence} from '../../src/control/body_execution_evidence.js';
import {bodyServiceToken} from '../../src/cloudflare/body_auth.js';
import {TenantPolicyStore} from '../../src/finops/tenant_policy_store.js';
import type {DirectoryStorage,DirectorySql,TenantRecord} from '../../src/control/tenant_directory.js';
import type {AdmissionReservation} from '../../src/control/admission_reservations.js';
const require=createRequire(join(process.cwd(),'package.json'));
interface Database {exec(q:string):void;prepare(q:string):{all(...bindings:unknown[]):unknown[]};close():void}
const {DatabaseSync}=require('node:sqlite') as {DatabaseSync:new(path:string)=>Database};
const directoryToken='d'.repeat(64),admissionToken='e'.repeat(64),signingKey='a'.repeat(64);
const actor={sub:'github:1',role:'SRE_ADMIN' as const,scopeTenant:'all',scopeRegion:'all'};
const tenant:TenantRecord={id:'tenant-a',name:'A',region:'dev',costCenter:'eng',version:1,status:'ACTIVE'};
function fixture(){
 const db=new DatabaseSync(':memory:');const sql:DirectorySql={exec<T>(query:string,...bindings:unknown[]){const rows=db.prepare(query).all(...bindings) as T[];return {toArray:()=>rows};}};
 const storage:DirectoryStorage={sql,transactionSync<T>(fn:()=>T){db.exec('BEGIN');try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}}};
 new TenantPolicyStore(storage).write(actor,tenant,{expectedVersion:0,resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'native service setup',idempotencyKey:'policy',requestId:'request'});
 db.exec('CREATE TABLE test_alarm (id TEXT PRIMARY KEY, scheduled_at INTEGER)');let alarmFailure=false;
 const alarmStorage={...storage,async getAlarm(){return sql.exec<{scheduled_at:number}>('SELECT scheduled_at FROM test_alarm WHERE id = ?', 'alarm').toArray()[0]?.scheduled_at??null;},async setAlarm(at:number){if(alarmFailure)throw new Error('alarm registration failed');sql.exec('INSERT OR REPLACE INTO test_alarm (id, scheduled_at) VALUES (?, ?)', 'alarm',at);}};
 let metadata={...tenant};const state={storage:alarmStorage} as unknown as ConstructorParameters<typeof TenantControlCell>[0];
 const env:TenantControlEnv={ANSTS_DIRECTORY_SERVICE_TOKEN:directoryToken,ANSTS_ADMISSION_SERVICE_TOKEN:admissionToken,ANSTS_ADMISSION_SIGNING_KEY:signingKey,ANSTS_TENANT_DIRECTORY_CELL:{idFromName:(name:string)=>({toString:()=>name}),get:()=>({id:{toString:()=>''},async fetch(request:Request|string|URL){assert.ok(request instanceof Request);assert.equal(request.headers.get('authorization'),'Bearer '+directoryToken);return Response.json(metadata);}})}};
 let cell=new TenantControlCell(state,env);
 async function call(path:string,body:Record<string,unknown>={},token=admissionToken){return cell.fetch(new Request('https://tenant.internal/'+path,{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify({actor,tenantId:tenant.id,...body})}));}
 const prepare={reservationId:'r1',taskId:'task-1',attemptId:'attempt-1',fencing:7,expectedPolicyVersion:1,resources:{concurrentAttempts:1,cpuMilliCores:500,memoryMiB:256,queuedTasks:0},estimatedCreditUnits:'20',expiresAt:Date.now()+60000};
 async function step(r:AdmissionReservation,to:string,extra:Record<string,unknown>={}){const response=await call('admission-transition',{idempotencyKey:to,input:{reservationId:r.id,expectedVersion:r.version,generation:r.generation,fencing:r.fencing,to,reason:'service protocol validation',...extra}});assert.equal(response.status,200);return (await response.json() as {reservation:AdmissionReservation}).reservation;}
 async function authorize(){let response=await call('admission-prepare',{idempotencyKey:'prepare',input:prepare});assert.equal(response.status,200);let r=(await response.json() as {reservation:AdmissionReservation}).reservation;r=await step(r,'RESERVED',{target:'body-1',capacityReceipt:'capacity-1'});return step(r,'START_AUTHORIZED');}
 const receiverDatabases:Database[]=[];const receivers=new Map<string,AdmissionInbox>();let failMailbox=false;
 function connectMailbox(){env.ANSTS_ADMISSION_MAILBOX={idFromName:(name:string)=>({toString:()=>name}),get:()=>({id:{toString:()=>''},async fetch(request:Request|string|URL){if(failMailbox)return Response.json({code:'OFFLINE'},{status:503});assert.ok(request instanceof Request);const body=await request.json() as {message:AdmissionMessage};let inbox=receivers.get(body.message.receiver);if(!inbox){const receiver=new DatabaseSync(':memory:');receiverDatabases.push(receiver);const receiverStorage:DirectoryStorage={sql:{exec<T>(query:string,...values:unknown[]){const rows=receiver.prepare(query).all(...values) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){receiver.exec('BEGIN');try{const result=fn();receiver.exec('COMMIT');return result;}catch(error){receiver.exec('ROLLBACK');throw error;}}};inbox=new AdmissionInbox(receiverStorage);receivers.set(body.message.receiver,inbox);}return Response.json({receipt:inbox.receive(body.message,tenant.id,body.message.receiver),protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});}})};}
 return {db,env,call,prepare,step,authorize,connectMailbox,failMailbox(value:boolean){failMailbox=value;},failAlarm(value:boolean){alarmFailure=value;},closeMailboxes(){receiverDatabases.forEach(db=>db.close());},async fireAlarm(){sql.exec('DELETE FROM test_alarm');await cell.alarm();},reopen(){cell=new TenantControlCell(state,env);},metadata(value:TenantRecord){metadata=value;}};
}
test('tenant service separates policy/coordinator credentials and keeps Viewer mutations forbidden',async()=>{
 const s=fixture();try{
  assert.equal((await s.call('admission-read',{reservationId:'r1'},directoryToken)).status,401);
  assert.equal((await s.call('policy-read',{},admissionToken)).status,401);
  assert.equal((await s.call('policy-read',{},directoryToken)).status,200);
  assert.equal((await s.call('admission-prepare',{actor:{...actor,role:'VIEWER'},idempotencyKey:'view',input:s.prepare})).status,403);
  assert.equal((await s.call('admission-prepare',{idempotencyKey:'create',input:s.prepare})).status,200);
  assert.equal((await s.call('admission-read',{actor:{...actor,role:'VIEWER'},reservationId:'r1'})).status,200);
  assert.equal((await s.call('admission-read',{actor:{...actor,scopeTenant:'other'},reservationId:'r1'})).status,403);
  s.env.ANSTS_ADMISSION_SERVICE_TOKEN=directoryToken;assert.equal((await s.call('admission-read',{reservationId:'r1'},directoryToken)).status,503);
 }finally{s.db.close();}
});
test('native tenant service issues target-bound shadow credentials and rejects them after durable uncertainty',async()=>{
 const s=fixture();try{
  const r=await s.authorize();let response=await s.call('admission-credential',{reservationId:r.id});assert.equal(response.status,200);
  const issued=await response.json() as {credential:string;protocolMode:string;enforcement:string};assert.equal(issued.protocolMode,'SHADOW');assert.equal(issued.enforcement,'NOT_CONNECTED');
  assert.equal((await s.call('admission-verify',{reservationId:r.id,credential:issued.credential,target:'body-1'})).status,200);
  assert.equal((await s.call('admission-verify',{reservationId:r.id,credential:issued.credential,target:'body-other'})).status,409);
  assert.equal((await s.call('admission-credential',{actor:{...actor,role:'VIEWER'},reservationId:r.id})).status,403);
  const uncertain=await s.step(r,'UNCERTAIN');s.reopen();
  response=await s.call('admission-read',{reservationId:r.id});assert.deepEqual((await response.json() as {reservation:AdmissionReservation}).reservation,uncertain);
  assert.equal((await s.call('admission-verify',{reservationId:r.id,credential:issued.credential,target:'body-1'})).status,409);
  assert.equal((await s.call('admission-credential',{reservationId:r.id})).status,409);
 }finally{s.db.close();}
});
test('execution and settlement cannot be asserted through the coordinator route without trusted adapters',async()=>{
 const s=fixture();try{
  const r=await s.authorize();
  for(const to of ['RUNNING','SETTLING','SETTLED'])assert.equal((await s.call('admission-transition',{idempotencyKey:to,input:{reservationId:r.id,to}})).status,409);
  assert.equal((await s.call('admission-transition',{idempotencyKey:'fake',input:{reservationId:r.id,to:'CANCELLED',evidence:{kind:'NOT_STARTED'}}})).status,409);
  const response=await s.call('admission-read',{reservationId:r.id});assert.equal((await response.json() as {reservation:AdmissionReservation}).reservation.phase,'START_AUTHORIZED');
 }finally{s.db.close();}
});
test('directory activation and missing signing configuration fail closed without modifying reservation',async()=>{
 const s=fixture();try{
  const r=await s.authorize();s.env.ANSTS_ADMISSION_SIGNING_KEY='';
  assert.equal((await s.call('admission-credential',{reservationId:r.id})).status,503);
  s.env.ANSTS_ADMISSION_SIGNING_KEY=signingKey;s.metadata({...tenant,version:2});
  assert.equal((await s.call('admission-credential',{reservationId:r.id})).status,409);
  s.metadata({...tenant,id:'other'});assert.equal((await s.call('admission-read',{reservationId:r.id})).status,503);
 }finally{s.db.close();}
});
test('alarm registration fails before reservation commit and initialization can be retried safely',async()=>{
 const s=fixture();try{
  s.failAlarm(true);assert.equal((await s.call('admission-prepare',{idempotencyKey:'prepare',input:s.prepare})).status,503);
  assert.equal((await s.call('admission-read',{reservationId:'r1'})).status,404);
  const before=await (await s.call('admission-delivery-status')).json() as {delivery:{configuration:{mode:string}}};assert.equal(before.delivery.configuration.mode,'UNINITIALIZED');
  s.failAlarm(false);assert.equal((await s.call('admission-prepare',{idempotencyKey:'prepare',input:s.prepare})).status,200);
  const after=await (await s.call('admission-delivery-status')).json() as {delivery:{configuration:{mode:string}};alarmAt:number};assert.equal(after.delivery.configuration.mode,'RUNNING');assert.ok(after.alarmAt>Date.now());
 }finally{s.db.close();}
});
test('automatic shadow delivery pre-arms a watchdog and acknowledgement leaves execution untouched',async()=>{
 const s=fixture();try{
  s.connectMailbox();const ns=s.env.ANSTS_ADMISSION_MAILBOX!;
  s.env.ANSTS_ADMISSION_MAILBOX={idFromName:ns.idFromName,get(id){const stub=ns.get(id);return {id:stub.id,async fetch(input,init){const status=await (await s.call('admission-delivery-status')).json() as {alarmAt:number};assert.ok(status.alarmAt>Date.now()+30000);return stub.fetch(input,init);}};}};
  assert.equal((await s.call('admission-prepare',{idempotencyKey:'prepare',input:s.prepare})).status,200);await s.fireAlarm();
  const pending=await (await s.call('admission-outbox')).json() as {messages:unknown[]};assert.equal(pending.messages.length,0);
  const reservation=await (await s.call('admission-read',{reservationId:'r1'})).json() as {reservation:AdmissionReservation};assert.equal(reservation.reservation.phase,'PREPARING');
  const status=await (await s.call('admission-delivery-status')).json() as {delivery:{runtime:{acknowledgedMessages:number}}};assert.equal(status.delivery.runtime.acknowledgedMessages,1);
 }finally{s.closeMailboxes();s.db.close();}
});
test('delivery pause/resume is scoped, CAS-protected and replay cannot reactivate a later pause',async()=>{
 const s=fixture();try{
  s.connectMailbox();const control={expectedVersion:0,enabled:false,reason:'manual fixture',idempotencyKey:'pause'};
  assert.equal((await s.call('admission-delivery-control',{actor:{...actor,role:'OPERATOR'},control})).status,403);
  assert.equal((await s.call('admission-delivery-control',{control})).status,200);
  assert.equal((await s.call('admission-prepare',{idempotencyKey:'prepare',input:s.prepare})).status,200);s.reopen();await s.fireAlarm();
  let pending=await (await s.call('admission-outbox')).json() as {messages:unknown[]};assert.equal(pending.messages.length,1);
  const resume={expectedVersion:1,enabled:true,reason:'resume delivery',idempotencyKey:'resume'};
  assert.equal((await s.call('admission-delivery-control',{control:resume})).status,200);await s.fireAlarm();pending=await (await s.call('admission-outbox')).json() as {messages:unknown[]};assert.equal(pending.messages.length,0);
  assert.equal((await s.call('admission-delivery-control',{control:{...control,idempotencyKey:'stale'}})).status,409);
  assert.equal((await s.call('admission-delivery-control',{control:{...control,expectedVersion:2,idempotencyKey:'pause-again'}})).status,200);
  const replay=await (await s.call('admission-delivery-control',{control:resume})).json() as {configuration:{version:number};current:{configuration:{version:number;mode:string}}};assert.equal(replay.configuration.version,2);assert.equal(replay.current.configuration.version,3);assert.equal(replay.current.configuration.mode,'PAUSED');
 }finally{s.closeMailboxes();s.db.close();}
});
test('automatic receiver retry survives cell reconstruction and never releases uncertain occupancy',async()=>{
 const s=fixture(),originalNow=Date.now;try{
  s.connectMailbox();let r=await s.authorize();r=await s.step(r,'UNCERTAIN');s.failMailbox(true);await s.fireAlarm();
  let pending=await (await s.call('admission-outbox')).json() as {messages:{status:string;attempts:number}[]};assert.equal(pending.messages[0].status,'PENDING');assert.equal(pending.messages[0].attempts,1);
  s.reopen();s.failMailbox(false);const at=originalNow()+2000;Date.now=()=>at;await s.fireAlarm();
  pending=await (await s.call('admission-outbox')).json() as {messages:{status:string;attempts:number}[]};assert.equal(pending.messages.length,0);
  const stored=await (await s.call('admission-read',{reservationId:r.id})).json() as {reservation:AdmissionReservation};assert.deepEqual(stored.reservation,r);
  const status=await (await s.call('admission-delivery-status')).json() as {delivery:{runtime:{retriedMessages:number;acknowledgedMessages:number}}};assert.equal(status.delivery.runtime.retriedMessages,1);assert.equal(status.delivery.runtime.acknowledgedMessages,1);
 }finally{Date.now=originalNow;s.closeMailboxes();s.db.close();}
});
test('directory outage beyond platform retry count remains scheduled and preserves uncertain reservations',async()=>{
 const s=fixture(),originalNow=Date.now;try{
  s.connectMailbox();let r=await s.authorize();r=await s.step(r,'UNCERTAIN');
  const directory=s.env.ANSTS_TENANT_DIRECTORY_CELL!;s.env.ANSTS_TENANT_DIRECTORY_CELL={idFromName:directory.idFromName,get(id){return {id,async fetch(){return Response.json({code:'OFFLINE'},{status:503});}};}};
  const initial=originalNow();for(let retry=0;retry<8;retry++){const at=initial+retry*31000;Date.now=()=>at;await s.fireAlarm();}
  s.env.ANSTS_TENANT_DIRECTORY_CELL=directory;
  const status=await (await s.call('admission-delivery-status')).json() as {alarmAt:number;delivery:{runtime:{lastError:string}}};assert.ok(status.alarmAt>Date.now());assert.equal(status.delivery.runtime.lastError,'DIRECTORY_UNAVAILABLE');
  const stored=await (await s.call('admission-read',{reservationId:r.id})).json() as {reservation:AdmissionReservation};assert.deepEqual(stored.reservation,r);
  Date.now=()=>initial+9*31000;await s.fireAlarm();const pending=await (await s.call('admission-outbox')).json() as {messages:unknown[]};assert.equal(pending.messages.length,0);
 }finally{Date.now=originalNow;s.closeMailboxes();s.db.close();}
});
test('enabled idle delivery keeps a durable heartbeat and pause stops renewal',async()=>{
 const s=fixture();try{
  s.connectMailbox();assert.equal((await s.call('admission-prepare',{idempotencyKey:'prepare',input:s.prepare})).status,200);await s.fireAlarm();await s.fireAlarm();
  let status=await (await s.call('admission-delivery-status')).json() as {alarmAt:number|null};assert.ok(status.alarmAt!>Date.now()+30000);
  assert.equal((await s.call('admission-delivery-control',{control:{expectedVersion:1,enabled:false,reason:'pause idle heartbeat',idempotencyKey:'pause-idle'}})).status,200);await s.fireAlarm();
  status=await (await s.call('admission-delivery-status')).json() as {alarmAt:number|null};assert.equal(status.alarmAt,null);
 }finally{s.closeMailboxes();s.db.close();}
});
test('tenant evidence API verifies pinned host and input before changing execution state',async()=>{
 const s=fixture();try{
  const root='f'.repeat(64);s.env.ANSTS_BODY_AUTH_SECRET=root;
  assert.equal((await s.call('admission-prepare',{idempotencyKey:'pinned',input:{...s.prepare,executionInputDigest:'e'.repeat(64)}})).status,200);
  let r=(await (await s.call('admission-read',{reservationId:'r1'})).json() as {reservation:AdmissionReservation}).reservation;r=await s.step(r,'RESERVED',{target:'body-1',capacityReceipt:'capacity-1'});r=await s.step(r,'START_AUTHORIZED');
  const evidence:BodyExecutionEvidence={schemaVersion:1,purpose:'ansts-body-execution-observation-v1',hostId:'body-1',tenantId:tenant.id,taskId:r.taskId,attemptId:r.attemptId,fencing:r.fencing,reservationId:r.id,generation:r.generation,policyVersion:r.policy.version,tenantVersion:r.policy.tenantVersion,requestDigest:r.requestDigest,executionInputDigest:r.executionInputDigest!,resources:r.resources,executionObserved:true,sequence:2,phase:'RUNNING',observedAt:Date.now()};
  assert.equal((await s.call('admission-observe-execution',{reservationId:r.id,proof:signBodyEvidence(evidence,'a'.repeat(64))})).status,401);
  const proof=signBodyEvidence(evidence,bodyServiceToken(root,'body-1'));
  assert.equal((await s.call('admission-observe-execution',{actor:{...actor,role:'VIEWER'},reservationId:r.id,proof})).status,403);
  const response=await s.call('admission-observe-execution',{reservationId:r.id,proof});assert.equal(response.status,200);const result=await response.json() as {sourceVerified:boolean;reservation:AdmissionReservation};assert.equal(result.sourceVerified,true);assert.equal(result.reservation.phase,'RUNNING');
 }finally{s.db.close();}
});

test('capacity reservation cannot authorize start after its grant becomes unavailable',async()=>{
 const s=fixture();
 try{
  const capacityReceipt='capacity-grant:'+'a'.repeat(64);
  let live=false;
  s.env.ANSTS_CAPACITY_INVENTORY_CELL={
   idFromName:name=>({toString:()=>name}),
   get:()=>({id:{toString:()=>''},async fetch(request){
    assert.ok(request instanceof Request);
    assert.equal(request.headers.get('authorization'),'Bearer '+directoryToken);
    if(new URL(request.url).pathname==='/grant-bind')return Response.json({capacityReceipt});
    const {requestInput}=await request.json() as {requestInput:{binding:unknown}};
    assert.deepEqual(requestInput.binding,{
     reservationId:s.prepare.reservationId,attemptId:s.prepare.attemptId,generation:1,fencing:7,
    });
    return Response.json({grant:live?{capacityReceipt,target:'body-1'}:null});
   }}),
  };
  const prepared=await s.call('admission-prepare',{idempotencyKey:'prepare',input:s.prepare});
  assert.equal(prepared.status,200);
  const {reservation}=await prepared.json() as {reservation:AdmissionReservation};
  const reserved=await s.step(reservation,'RESERVED',{target:'body-1',capacityReceipt});
  s.reopen();
  const failed=await s.call('admission-transition',{idempotencyKey:'START_AUTHORIZED',input:{
   reservationId:reserved.id,expectedVersion:reserved.version,generation:reserved.generation,
   fencing:reserved.fencing,to:'START_AUTHORIZED',reason:'check grant before authorizing',
  }});
  assert.equal(failed.status,409);
  assert.deepEqual(await failed.json(),{code:'CAPACITY_TARGET_UNAVAILABLE'});
  const read=await s.call('admission-read',{reservationId:reserved.id});
  assert.deepEqual((await read.json() as {reservation:AdmissionReservation}).reservation,reserved);
  live=true;
  assert.equal((await s.step(reserved,'START_AUTHORIZED')).phase,'START_AUTHORIZED');
 }finally{s.db.close();}
});

test('cancel recovers a capacity binding whose acknowledgement was lost',async()=>{
 const s=fixture();
 try{
  const capacityReceipt='capacity-grant:'+'b'.repeat(64);
  let held=false,releaseAttempts=0;
  s.env.ANSTS_CAPACITY_INVENTORY_CELL={
   idFromName:name=>({toString:()=>name}),
   get:()=>({id:{toString:()=>''},async fetch(request){
    assert.ok(request instanceof Request);
    assert.equal(request.headers.get('authorization'),'Bearer '+directoryToken);
    const body=await request.json() as {reservation:AdmissionReservation};
    if(new URL(request.url).pathname==='/grant-bind'){
     held=true;
     throw new Error('binding acknowledgement lost');
    }
    assert.equal(body.reservation.capacityReceipt,capacityReceipt);
    assert.equal(body.reservation.phase,'CANCELLED');
    assert.equal(body.reservation.generation,2);
    assert.equal(body.reservation.startAuthorized,false);
    if(++releaseAttempts===1)return Response.json({code:'OFFLINE'},{status:503});
    held=false;
    return Response.json({released:true});
   }}),
  };
  const prepared=await s.call('admission-prepare',{idempotencyKey:'prepare',input:s.prepare});
  assert.equal(prepared.status,200);
  const {reservation}=await prepared.json() as {reservation:AdmissionReservation};
  const failed=await s.call('admission-transition',{idempotencyKey:'reserve',input:{
   reservationId:reservation.id,expectedVersion:reservation.version,generation:reservation.generation,
   fencing:reservation.fencing,to:'RESERVED',reason:'bind capacity',target:'body-1',capacityReceipt,
  }});
  assert.equal(failed.status,503);
  assert.equal(held,true);
  s.reopen();
  const cancelled=await s.step(reservation,'CANCELLED');
  assert.equal(cancelled.capacityReceipt,null);
  await s.fireAlarm();
  assert.equal(held,true);
  s.reopen();
  await s.fireAlarm();
  assert.equal(held,false);
  await s.fireAlarm();
  assert.equal(releaseAttempts,2);
 }finally{s.db.close();}
});
