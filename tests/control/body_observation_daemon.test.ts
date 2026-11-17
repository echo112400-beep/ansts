import {test} from 'node:test';
import assert from 'node:assert/strict';
import * as http from 'node:http';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {BodyDaemonServer} from '../../src/runtime/body_daemon_server.js';
import {taskInputDigest} from '../../src/cloudflare/execution_attempt.js';
import {verifyBodyEvidence,type BodyObservationBinding} from '../../src/control/body_execution_evidence.js';
const image='node@sha256:'+'f'.repeat(64),token='a'.repeat(64),hostId='evidence-test';
async function upstream(cancel=false){const server=http.createServer(async(request,response)=>{for await(const _ of request){}response.writeHead(200,{'content-type':'application/json'});response.end(JSON.stringify({cancel_requested:cancel,lease_expires_at:Date.now()/1000+30}));});await new Promise<void>(resolve=>server.listen(0,'127.0.0.1',resolve));return {url:'http://127.0.0.1:'+(server.address() as {port:number}).port,async close(){server.closeAllConnections();await new Promise<void>(resolve=>server.close(()=>resolve()));}};}
const execution={kind:'container',image,argv:['true'],cpu:.25,memoryMb:128,timeoutMs:10000};
function payload(){const inputDigest=taskInputDigest('tenant','task','work',execution);const binding:BodyObservationBinding={reservationId:'r1',generation:1,policyVersion:1,tenantVersion:1,requestDigest:'b'.repeat(64),executionInputDigest:inputDigest};return {tenantId:'tenant',taskId:'task',attemptId:'attempt',fencingToken:7,promptText:'work',execution,tenantAdmissionObservation:binding};}
async function call(base:string,path:string,body:unknown,auth=true){return fetch(base+path,{method:'POST',headers:{'content-type':'application/json',...(auth?{authorization:'Body '+token,'x-ansts-body-id':hostId}:{})},body:JSON.stringify(body)});}
test('daemon persists authenticated positive no-start evidence and unknown identity never implies no-start',async()=>{
 const control=await upstream(true),directory=mkdtempSync(join(tmpdir(),'ansts-body-facts-'));let daemon=new BodyDaemonServer({nodeId:hostId,token,bodyWorkerUrl:control.url,storageDir:directory,host:'127.0.0.1',port:0,sandboxImage:image,containerRuntime:{recoverOrphans:async()=>0,execute:async()=>{throw new Error('Engine must not start');}}});
 try{
  let {port}=await daemon.start();let base='http://127.0.0.1:'+port;const identity={tenantId:'tenant',taskId:'task',attemptId:'attempt'};
  assert.equal((await call(base,'/v1/daemon/execution-evidence',identity,false)).status,401);assert.equal((await call(base,'/v1/daemon/execution-evidence',identity)).status,404);
  assert.equal((await call(base,'/v1/sandboxes/execute',payload())).status,200);
  const proof=(await (await call(base,'/v1/daemon/execution-evidence',identity)).json()).proof;assert.equal(verifyBodyEvidence(proof,token).phase,'NOT_STARTED');
  assert.equal((await call(base,'/v1/daemon/execution-evidence/pending',{},false)).status,401);
  assert.equal((await call(base,'/v1/daemon/execution-evidence/pending',{limit:101})).status,400);
  const pending=await (await call(base,'/v1/daemon/execution-evidence/pending',{})).json();assert.equal(pending.observations.length,1);assert.equal(pending.observations[0].proof,proof);
  assert.equal((await call(base,'/v1/daemon/execution-evidence/ack',{...identity,digest:'0'.repeat(64)})).status,409);
  assert.equal((await call(base,'/v1/daemon/execution-evidence/ack',{...identity,digest:pending.observations[0].digest})).status,200);
  assert.equal((await (await call(base,'/v1/daemon/execution-evidence/pending',{})).json()).observations.length,0);
  await daemon.stop();daemon=new BodyDaemonServer({nodeId:hostId,token,bodyWorkerUrl:control.url,storageDir:directory,host:'127.0.0.1',port:0,sandboxImage:image,containerRuntime:{recoverOrphans:async()=>0,execute:async()=>{throw new Error('Engine must not start');}}});({port}=await daemon.start());base='http://127.0.0.1:'+port;
  assert.equal((await (await call(base,'/v1/daemon/execution-evidence',identity)).json()).proof,proof);
  assert.equal((await (await call(base,'/v1/daemon/execution-evidence/pending',{})).json()).observations.length,0);
 }finally{await daemon.stop();await control.close();rmSync(directory,{recursive:true,force:true});}
});
test('daemon keeps uncertainty when an Engine adapter cannot confirm termination',async()=>{
 const control=await upstream(),directory=mkdtempSync(join(tmpdir(),'ansts-body-uncertain-'));
 const daemon=new BodyDaemonServer({nodeId:hostId,token,bodyWorkerUrl:control.url,storageDir:directory,host:'127.0.0.1',port:0,sandboxImage:image,containerRuntime:{recoverOrphans:async()=>0,execute:async(_command,_signal,_output,lifecycle)=>{lifecycle?.started?.();throw new Error('BODY_STOP_UNCONFIRMED');}}});
 try{
  const {port}=await daemon.start(),base='http://127.0.0.1:'+port;
  assert.equal((await call(base,'/v1/sandboxes/execute',payload())).status,503);
  const response=await call(base,'/v1/daemon/execution-evidence',{tenantId:'tenant',taskId:'task',attemptId:'attempt'});const fact=verifyBodyEvidence((await response.json()).proof,token);assert.equal(fact.phase,'UNCERTAIN');assert.equal(fact.executionObserved,true);
  assert.equal((await (await fetch(base+'/health')).json()).recoveryRequired,true);
 }finally{await daemon.stop();await control.close();rmSync(directory,{recursive:true,force:true});}
});

for(const uncertain of [false,true])test('agent observation '+(uncertain?'retains uncertain Engine stop':'spans model and tool execution until termination'),async()=>{
 const control=await upstream(),directory=mkdtempSync(join(tmpdir(),'ansts-agent-facts-'));let calls=0,release!:()=>void,entered!:()=>void;
 const enteredModel=new Promise<void>(resolve=>{entered=resolve;}),releaseModel=new Promise<void>(resolve=>{release=resolve;});
 const agentExecution={kind:'agent',image,cpu:.25,memoryMb:128,timeoutMs:10000,maxTurns:4,maxTokens:16000,files:{}};
 const input={...payload(),execution:agentExecution};input.tenantAdmissionObservation={...input.tenantAdmissionObservation,executionInputDigest:taskInputDigest('tenant','task','work',agentExecution)};
 const daemon=new BodyDaemonServer({nodeId:hostId,token,bodyWorkerUrl:control.url,storageDir:directory,host:'127.0.0.1',port:0,sandboxImage:image,
  agentModel:{next:async()=>{calls++;if(calls===1){entered();await releaseModel;return {tokens:1,action:{tool:'test',argv:['true']}};}return {tokens:1,action:{tool:'finish'}};}},
  containerRuntime:{recoverOrphans:async()=>0,execute:async()=>{if(uncertain)throw new Error('BODY_STOP_UNCONFIRMED');return {exitCode:0,executionOutcome:'SUCCEEDED',logs:'tool passed',logsTruncated:false,logsTailLimited:false,artifacts:[],artifactsTruncated:false};}}});
 try{
  const {port}=await daemon.start(),base='http://127.0.0.1:'+port,identity={tenantId:'tenant',taskId:'task',attemptId:'attempt'};
  const executing=call(base,'/v1/sandboxes/execute',input);await enteredModel;
  const running=verifyBodyEvidence((await (await call(base,'/v1/daemon/execution-evidence',identity)).json()).proof,token);assert.equal(running.phase,'RUNNING');assert.equal(running.executionObserved,true);
  release();assert.equal((await executing).status,uncertain?503:200);
  const final=verifyBodyEvidence((await (await call(base,'/v1/daemon/execution-evidence',identity)).json()).proof,token);
  assert.equal(final.phase,uncertain?'UNCERTAIN':'TERMINATED');assert.equal(final.executionObserved,true);
  assert.equal((await (await fetch(base+'/health')).json()).recoveryRequired,uncertain);
 }finally{release();await daemon.stop();await control.close();rmSync(directory,{recursive:true,force:true});}
});
