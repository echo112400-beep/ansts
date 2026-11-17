import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {BodyExecutionEvidenceLedger} from '../../src/runtime/body_execution_evidence_ledger.js';
import {deliverBodyObservations} from '../../src/runtime/body_observation_delivery.js';
import {bodyEvidenceDigest,signBodyEvidence,type BodyExecutionEvidence} from '../../src/control/body_execution_evidence.js';
import {receiveBodyObservation} from '../../src/control/body_observation_ingress.js';
import {bodyServiceToken} from '../../src/cloudflare/body_auth.js';
import type {CloudflareCelldEnv,DurableObjectNamespaceLike} from '../../src/cloudflare/env.js';
const rootKey='c'.repeat(64),host='host-1',key=bodyServiceToken(rootKey,host);
const begin:BodyExecutionEvidence={schemaVersion:1,purpose:'ansts-body-execution-observation-v1',hostId:host,tenantId:'tenant',taskId:'task',attemptId:'attempt',fencing:1,reservationId:'r1',generation:1,policyVersion:1,tenantVersion:1,requestDigest:'d'.repeat(64),executionInputDigest:'e'.repeat(64),resources:{concurrentAttempts:1,cpuMilliCores:250,memoryMiB:128,queuedTasks:0},executionObserved:false,sequence:1,phase:'UNCERTAIN',observedAt:1000};
function namespace(handler:(r:Request)=>Promise<Response>):DurableObjectNamespaceLike{return {idFromName:name=>({toString:()=>name}),get:()=>({id:{toString:()=>''},fetch:r=>handler(new Request(r))})};}
function ingress(){
 const calls:{actor:{sub:string;role:string;scopeTenant:string};tenantId:string;proof:string;reservationId:string}[]=[];
 let denied=false;
 const env={ANSTS_BODY_AUTH_SECRET:rootKey,ANSTS_DIRECTORY_SERVICE_TOKEN:'directory'.repeat(8),ANSTS_ADMISSION_SERVICE_TOKEN:'admission'.repeat(8),ANSTS_TENANT_DIRECTORY_CELL:namespace(async()=>denied?Response.json({code:'CONTROL_FORBIDDEN'},{status:403}):Response.json({id:'tenant'})),ANSTS_TENANT_CONTROL_CELL:namespace(async request=>{assert.equal(request.headers.get('authorization'),'Bearer '+'admission'.repeat(8));calls.push(await request.json());return Response.json({sourceVerified:true});})} as CloudflareCelldEnv;
 const request=(proof:string,token=key,headers:Record<string,string>={})=>new Request('https://fleet/internal/v1/body-execution-evidence',{method:'POST',headers:{authorization:'Body '+token,'x-ansts-body-id':host,...headers},body:JSON.stringify({proof})});
 return {env,calls,request,deny(){denied=true;}};
}
test('host ingress derives tenant-scoped actor and refuses impersonation before invoking authority',async()=>{
 const s=ingress(),proof=signBodyEvidence(begin,key);
 assert.equal((await receiveBodyObservation(s.request(proof,'f'.repeat(64)),s.env)).status,401);
 assert.equal((await receiveBodyObservation(s.request(signBodyEvidence({...begin,hostId:'host-2'},key)),s.env)).status,403);
 assert.equal((await receiveBodyObservation(s.request(signBodyEvidence(begin,bodyServiceToken(rootKey,'host-2'))),s.env)).status,401);
 assert.equal(s.calls.length,0);
 const response=await receiveBodyObservation(s.request(proof,key,{'x-ansts-operator-sub':'github:attacker'}),s.env);
 assert.equal(response.status,200);assert.deepEqual(await response.json(),{sourceVerified:true,digest:bodyEvidenceDigest(begin)});
 assert.deepEqual(s.calls[0].actor,{sub:'system:body:host-1',role:'OPERATOR',scopeTenant:'tenant',scopeRegion:'all'});
 s.deny();assert.equal((await receiveBodyObservation(s.request(proof),s.env)).status,403);assert.equal(s.calls.length,1);
});
test('automatic observation relay persists retries and safely replays a lost authority response after restart',async()=>{
 const directory=mkdtempSync(join(tmpdir(),'ansts-observation-relay-'));let ledger=new BodyExecutionEvidenceLedger(directory),now=2000;const authority=ingress();
 const local=async(route:string,value:unknown)=>{
  const body=value as BodyExecutionEvidence&{digest:string;limit:number};
  if(route.endsWith('/pending'))return {observations:ledger.pending(body.limit,now).map(fact=>({proof:signBodyEvidence(fact,key),digest:bodyEvidenceDigest(fact)}))};
  if(route.endsWith('/defer'))return {deferred:ledger.defer(body,body.digest,now)};
  return {acknowledged:ledger.acknowledge(body,body.digest)};
 };
 try {
  ledger.begin(begin);const final=ledger.advance(begin,'TERMINATED',1500);
  const publish=async(proof:string)=>{const response=await receiveBodyObservation(authority.request(proof),authority.env);assert.equal(response.status,200);return response.json();};
  await assert.rejects(()=>deliverBodyObservations(local,async proof=>{await publish(proof);throw new Error('lost response');},key),/lost response/);
  ledger=new BodyExecutionEvidenceLedger(directory);assert.deepEqual(ledger.pending(4,now),[]);assert.deepEqual(ledger.pending(4,now+2000),[final]);
  now+=2000;assert.equal(await deliverBodyObservations(local,publish,key),1);
  assert.equal(authority.calls.length,2);assert.equal(authority.calls[0].proof,authority.calls[1].proof);
  assert.deepEqual(new BodyExecutionEvidenceLedger(directory).pending(4,now),[]);
 }finally{rmSync(directory,{recursive:true,force:true});}
});
test('remote acknowledgement cannot clear a newer fact observed during delivery',async()=>{
 const directory=mkdtempSync(join(tmpdir(),'ansts-observation-race-')),ledger=new BodyExecutionEvidenceLedger(directory);let now=2000;
 try {
  ledger.begin(begin);const running=ledger.advance(begin,'RUNNING',1100);let terminal:BodyExecutionEvidence;
  const local=async(route:string,value:unknown)=>{const body=value as BodyExecutionEvidence&{digest:string};if(route.endsWith('/pending'))return {observations:[{proof:signBodyEvidence(running,key),digest:bodyEvidenceDigest(running)}]};if(route.endsWith('/defer'))return {deferred:ledger.defer(body,body.digest,now)};return {acknowledged:ledger.acknowledge(body,body.digest)};};
  await assert.rejects(()=>deliverBodyObservations(local,async()=>{terminal=ledger.advance(begin,'TERMINATED',1200);return {sourceVerified:true,digest:bodyEvidenceDigest(running)};},key),/EVIDENCE_ACK_PENDING/);
  assert.deepEqual(ledger.pending(4,now),[terminal!]);
 }finally{rmSync(directory,{recursive:true,force:true});}
});
test('invalid remote receipt leaves evidence pending with persisted backoff',async()=>{
 const directory=mkdtempSync(join(tmpdir(),'ansts-observation-receipt-')),ledger=new BodyExecutionEvidenceLedger(directory);
 try {
  ledger.begin(begin);
  const local=async(route:string,value:unknown)=>{const body=value as BodyExecutionEvidence&{digest:string};if(route.endsWith('/pending'))return {observations:[{proof:signBodyEvidence(begin,key),digest:bodyEvidenceDigest(begin)}]};assert.ok(route.endsWith('/defer'));return {deferred:ledger.defer(body,body.digest,2000)};};
  await assert.rejects(()=>deliverBodyObservations(local,async()=>({sourceVerified:true,digest:'a'.repeat(64)}),key),/INVALID_EVIDENCE_RECEIPT/);
  assert.deepEqual(new BodyExecutionEvidenceLedger(directory).pending(4,4000),[begin]);
 }finally{rmSync(directory,{recursive:true,force:true});}
});

test('Host binding ingress rejects caller-selected actor and uses authenticated Host target',async()=>{
 const {lookupBodyObservation}=await import('../../src/control/body_observation_ingress.js');
 const s=ingress();const input={tenantId:begin.tenantId,taskId:begin.taskId,attemptId:begin.attemptId,fencing:begin.fencing,executionInputDigest:begin.executionInputDigest,resources:begin.resources};
 let forwarded:Record<string,unknown>|undefined;
 s.env.ANSTS_TENANT_CONTROL_CELL=namespace(async request=>{forwarded=await request.json();return Response.json({binding:null,protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'});});
 const request=(body:unknown,auth=key)=>new Request('https://fleet/internal/v1/body-observation-binding',{method:'POST',headers:{authorization:'Body '+auth,'x-ansts-body-id':host},body:JSON.stringify(body)});
 assert.equal((await lookupBodyObservation(request(input,'f'.repeat(64)),s.env)).status,401);
 assert.equal((await lookupBodyObservation(request({...input,actor:{role:'SRE_ADMIN'}}),s.env)).status,400);
 assert.equal((await lookupBodyObservation(request({...input,target:'other'}),s.env)).status,400);
 assert.equal(forwarded,undefined);
 const response=await lookupBodyObservation(request(input),s.env);assert.equal(response.status,200);assert.equal((await response.json()).binding,null);
 assert.equal(forwarded!.target,host);assert.equal((forwarded!.actor as {scopeTenant:string}).scopeTenant,begin.tenantId);
});

test('binding client accepts only an explicit shadow envelope and canonical pinned input',async()=>{
 const {lookupBodyObservationBinding}=await import('../../src/runtime/body_observation_binding.js');
 const input={tenantId:begin.tenantId,taskId:begin.taskId,attemptId:begin.attemptId,fencing:begin.fencing,executionInputDigest:begin.executionInputDigest,resources:begin.resources};
 const binding={reservationId:begin.reservationId,generation:begin.generation,policyVersion:begin.policyVersion,tenantVersion:begin.tenantVersion,requestDigest:begin.requestDigest,executionInputDigest:begin.executionInputDigest};
 assert.deepEqual(await lookupBodyObservationBinding(input,host,async()=>({binding,protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'})),binding);
 assert.equal(await lookupBodyObservationBinding(input,host,async()=>({binding:null,protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'})),null);
 for(const response of [{binding},{binding,protocolMode:'ENFORCE',enforcement:'ENABLED'},{binding:{...binding,executionInputDigest:'a'.repeat(64)},protocolMode:'SHADOW',enforcement:'NOT_CONNECTED'}])await assert.rejects(()=>lookupBodyObservationBinding(input,host,async()=>response));
});
