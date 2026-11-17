import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {AdmissionReservationStore,type AdmissionReservation} from '../../src/control/admission_reservations.js';
import {AdmissionInbox,messageForReservation,type AdmissionDelivery} from '../../src/control/admission_messages.js';
import {dispatchAdmissionMessage} from '../../src/control/admission_dispatcher.js';
import {TenantPolicyStore} from '../../src/finops/tenant_policy_store.js';
import type {DirectoryStorage,DirectorySql,TenantRecord} from '../../src/control/tenant_directory.js';
const load=createRequire(join(process.cwd(),'package.json'));
interface Database {exec(q:string):void;prepare(q:string):{all(...bindings:unknown[]):unknown[]};close():void}
const {DatabaseSync}=load('node:sqlite') as {DatabaseSync:new(path:string)=>Database};
function open(path=':memory:'){
 const db=new DatabaseSync(path);const sql:DirectorySql={exec<T>(query:string,...bindings:unknown[]){const rows=db.prepare(query).all(...bindings) as T[];return {toArray:()=>rows};}};
 const storage:DirectoryStorage={sql,transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}}};
 return {db,storage,store:new AdmissionReservationStore(storage),inbox:new AdmissionInbox(storage),policies:new TenantPolicyStore(storage)};
}
const actor={sub:'github:1',role:'SRE_ADMIN' as const,scopeTenant:'all',scopeRegion:'all'};
const tenant:TenantRecord={id:'tenant-1',name:'Test',region:'dev',costCenter:'eng',version:1,status:'ACTIVE'};
const now=Date.parse('2026-11-16T23:59:59Z');
const input={reservationId:'r1',taskId:'task-1',attemptId:'attempt-1',fencing:7,expectedPolicyVersion:1,resources:{concurrentAttempts:1,cpuMilliCores:500,memoryMiB:256,queuedTasks:0},estimatedCreditUnits:'10',expiresAt:now+60000};
function prepare(s:ReturnType<typeof open>){s.policies.write(actor,tenant,{expectedVersion:0,resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'outbox verification',idempotencyKey:'policy',requestId:'request'});return s.store.prepare(actor,tenant,input,'prepare',now);}
function transition(s:ReturnType<typeof open>,r:AdmissionReservation,to:'RESERVED'|'START_AUTHORIZED'|'UNCERTAIN'|'CANCELLED') {return s.store.transition(actor,tenant,{reservationId:r.id,expectedVersion:r.version,generation:r.generation,fencing:r.fencing,to,reason:'outbox test',...(to==='RESERVED'?{target:'body-1',capacityReceipt:'shadow-capacity'}:{})},to,now);}
function claim(s:ReturnType<typeof open>,at=now):AdmissionDelivery {const item=s.store.claimMessage(actor,tenant,at);assert.ok(item);return item;}
test('outbox write failure rolls back reservation, audit and idempotency; legacy recovery is stable',()=>{
 const s=open();try{
  prepare(s);const before=s.store.read(actor,tenant,'r1'),original=s.storage.sql.exec.bind(s.storage.sql);
  s.storage.sql.exec=(query,...bindings)=>{if(query.startsWith('INSERT OR REPLACE INTO admission_outbox'))throw new Error('outbox disk failed');return original(query,...bindings);};
  assert.throws(()=>transition(s,before,'RESERVED'),/outbox disk failed/);s.storage.sql.exec=original;
  assert.deepEqual(s.store.read(actor,tenant,'r1'),before);assert.equal(transition(s,before,'RESERVED').phase,'RESERVED');
  s.storage.sql.exec('DELETE FROM admission_outbox');const reserved=s.store.read(actor,tenant,'r1');transition(s,reserved,'START_AUTHORIZED');
  s.storage.sql.exec('DELETE FROM admission_outbox');const first=claim(s);assert.equal(first.message.kind,'START_EXECUTION');
  assert.equal(s.store.claimMessage(actor,tenant,now),null);assert.equal(claim(s,now+10000).message.eventId,first.message.eventId);
 }finally{s.db.close();}
});
test('receiver commit with lost reply is retried after sender restart without duplicate effect',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-message-'));let sender=open(join(dir,'sender.sqlite')),receiver=open(join(dir,'receiver.sqlite'));let at=now;
 try{
  prepare(sender);let deliveries=0;
  const transport={async deliver(message:Parameters<AdmissionInbox['receive']>[0]){deliveries++;const receipt=receiver.inbox.receive(message,tenant.id,'capacity');if(deliveries===1)throw new Error('reply lost after receiver commit');return receipt;}};
  assert.equal((await dispatchAdmissionMessage(sender.store,actor,tenant,transport,()=>at))?.status,'PENDING');
  sender.db.close();receiver.db.close();sender=open(join(dir,'sender.sqlite'));receiver=open(join(dir,'receiver.sqlite'));at+=1000;
  assert.equal((await dispatchAdmissionMessage(sender.store,actor,tenant,transport,()=>at))?.status,'ACKNOWLEDGED');
  assert.equal(deliveries,2);assert.equal(sender.store.claimMessage(actor,tenant,at),null);
  const state=JSON.parse(receiver.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM admission_inbox WHERE id = ?','inbox').toArray()[0].payload_json);
  assert.equal(state.receipts.length,1);assert.equal(state.streams.length,1);assert.equal(sender.store.read(actor,tenant,'r1').phase,'PREPARING');
 }finally{sender.db.close();receiver.db.close();rmSync(dir,{recursive:true,force:true});}
});
test('sender crash after claim recovers same message and old delivery lease cannot acknowledge new retry',()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-lease-'));let s=open(join(dir,'sender.sqlite'));const receiver=open();
 try{
  prepare(s);const first=claim(s);s.db.close();s=open(join(dir,'sender.sqlite'));
  assert.equal(s.store.claimMessage(actor,tenant,now+9999),null);const second=claim(s,now+10000);assert.equal(second.attempts,2);
  const receipt=receiver.inbox.receive(first.message,tenant.id,'capacity');
  assert.throws(()=>s.store.finishMessage(actor,tenant,first.message.eventId,first.attempts,receipt,now+10000),{code:'STALE_DELIVERY_LEASE'});
  assert.equal(s.store.finishMessage(actor,tenant,second.message.eventId,second.attempts,receipt,now+10000).status,'ACKNOWLEDGED');
 }finally{s.db.close();receiver.db.close();rmSync(dir,{recursive:true,force:true});}
});
test('revocation tombstone fences reserve arriving after release and late sender acknowledgement is rejected',()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-tombstone-')),s=open();let receiver=open(join(dir,'receiver.sqlite'));try{
  const r=prepare(s),old=claim(s),cancelled=transition(s,r,'CANCELLED');const release=claim(s);
  assert.equal(release.message.kind,'RELEASE_CAPACITY');assert.equal(release.message.generation,cancelled.generation);
  assert.equal(receiver.inbox.receive(release.message,tenant.id,'capacity').outcome,'RECORDED');
  receiver.db.close();receiver=open(join(dir,'receiver.sqlite'));
  const oldReply=receiver.inbox.receive(old.message,tenant.id,'capacity');assert.equal(oldReply.outcome,'STALE');
  assert.throws(()=>s.store.finishMessage(actor,tenant,old.message.eventId,old.attempts,oldReply,now),{code:'STALE_DELIVERY_LEASE'});
  const json=receiver.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM admission_inbox WHERE id = ?','inbox').toArray()[0].payload_json;
  assert.equal(JSON.parse(json).streams[0].revoked,true);
 }finally{s.db.close();receiver.db.close();rmSync(dir,{recursive:true,force:true});}
});
test('same event with different content quarantines conflict durably while preserving original receipt',()=>{
 const receiver=open(),s=open();try{
  const r=prepare(s),message=messageForReservation(r)!;const original=receiver.inbox.receive(message,tenant.id,'capacity');
  const conflict=receiver.inbox.receive({...message,resources:{...message.resources,cpuMilliCores:501}},tenant.id,'capacity');assert.equal(conflict.outcome,'CONFLICT');
  assert.deepEqual(receiver.inbox.receive(message,tenant.id,'capacity'),original);
  const json=JSON.parse(receiver.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM admission_inbox WHERE id = ?','inbox').toArray()[0].payload_json);assert.equal(json.conflicts.length,1);assert.equal(json.receipts.length,1);
  const item=claim(s);assert.equal(s.store.finishMessage(actor,tenant,item.message.eventId,item.attempts,{...original,outcome:'CONFLICT'},now).status,'QUARANTINED');
 }finally{s.db.close();receiver.db.close();}
});
test('expired start is quarantined, uncertainty queues a query and never releases quota',()=>{
 const s=open();try{
  let r=prepare(s);r=transition(s,r,'RESERVED');r=transition(s,r,'START_AUTHORIZED');
  assert.equal(s.store.claimMessage(actor,tenant,input.expiresAt),null);assert.equal(s.store.listMessages(actor,tenant)[0].lastError,'EXPIRED_INTENT');
  r=s.store.transition(actor,tenant,{reservationId:r.id,expectedVersion:r.version,generation:r.generation,fencing:r.fencing,to:'UNCERTAIN',reason:'lost reply'},'uncertain',input.expiresAt);
  assert.equal(claim(s,input.expiresAt).message.kind,'QUERY_EXECUTION');
  assert.throws(()=>s.store.prepare(actor,tenant,{...input,reservationId:'r2',attemptId:'a2',expiresAt:input.expiresAt+60000},'no-release',input.expiresAt),{code:'RESOURCE_QUOTA_EXCEEDED'});
  assert.equal(s.store.read(actor,tenant,'r1').phase,'UNCERTAIN');
 }finally{s.db.close();}
});
test('delivery role/scope and receipt destination are checked without storing arbitrary adapter errors',async()=>{
 const s=open();try{
  prepare(s);assert.throws(()=>s.store.claimMessage({...actor,role:'VIEWER'},tenant,now),{code:'CONTROL_FORBIDDEN'});
  assert.throws(()=>s.store.listMessages({...actor,scopeTenant:'other'},tenant),{code:'CONTROL_FORBIDDEN'});
  assert.equal(s.store.listMessages({...actor,scopeRegion:'next'},{...tenant,region:'next'}).length,0);
  const result=await dispatchAdmissionMessage(s.store,actor,tenant,{async deliver(){throw new Error('secret-must-not-be-stored');}},()=>now);assert.equal(result?.status,'PENDING');
  const next=claim(s,now+1000);const invalid={eventId:next.message.eventId,digest:next.digest,receiver:'wrong-receiver',protocolMode:'SHADOW' as const,outcome:'RECORDED' as const};
  assert.equal(s.store.finishMessage(actor,tenant,next.message.eventId,next.attempts,invalid,now+1000).lastError,'INVALID_RECEIPT');
  const json=s.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM admission_outbox WHERE id = ?','outbox').toArray()[0].payload_json;assert.equal(json.includes('secret-must-not-be-stored'),false);
 }finally{s.db.close();}
});
test('disabled tenants cannot acquire capacity while cancellation and query delivery remain possible',()=>{
 const s=open();try{
  const r=prepare(s),disabled={...tenant,status:'DISABLED' as const};
  assert.equal(s.store.claimMessage(actor,disabled,now),null);assert.equal(s.store.listMessages(actor,disabled)[0].lastError,'TENANT_DISABLED');
  transition(s,r,'CANCELLED');assert.equal(s.store.claimMessage(actor,disabled,now)?.message.kind,'RELEASE_CAPACITY');
 }finally{s.db.close();}
});
test('sender acknowledgement write failure retains lease and receiver deduplication after retry',async()=>{
 const sender=open(),receiver=open();let at=now;
 try{
  prepare(sender);const original=sender.storage.sql.exec.bind(sender.storage.sql);let first=true;
  const transport={async deliver(message:Parameters<AdmissionInbox['receive']>[0]){
   const receipt=receiver.inbox.receive(message,tenant.id,'capacity');
   if(first){first=false;sender.storage.sql.exec=(query,...bindings)=>{if(query.startsWith('INSERT OR REPLACE INTO admission_outbox'))throw new Error('ack disk failed');return original(query,...bindings);};}
   return receipt;
  }};
  await assert.rejects(dispatchAdmissionMessage(sender.store,actor,tenant,transport,()=>at),/ack disk failed/);
  sender.storage.sql.exec=original;assert.equal(sender.store.listMessages(actor,tenant)[0].status,'IN_FLIGHT');
  at+=10000;assert.equal((await dispatchAdmissionMessage(sender.store,actor,tenant,transport,()=>at))?.status,'ACKNOWLEDGED');
  const state=JSON.parse(receiver.storage.sql.exec<{payload_json:string}>('SELECT payload_json FROM admission_inbox WHERE id = ?','inbox').toArray()[0].payload_json);assert.equal(state.receipts.length,1);assert.equal(state.streams.length,1);
 }finally{sender.db.close();receiver.db.close();}
});
test('a repeatedly failing delivery does not starve older ready messages behind it',()=>{
 const s=open();try{
  const r=prepare(s);transition(s,r,'CANCELLED');
  s.store.prepare(actor,tenant,{...input,reservationId:'r2',attemptId:'attempt-2'},'second',now);
  const failed=claim(s);assert.equal(failed.message.kind,'RELEASE_CAPACITY');
  s.store.finishMessage(actor,tenant,failed.message.eventId,failed.attempts,null,now);
  const next=claim(s,now+2000);assert.equal(next.message.reservationId,'r2');assert.equal(next.message.kind,'RESERVE_CAPACITY');
 }finally{s.db.close();}
});
