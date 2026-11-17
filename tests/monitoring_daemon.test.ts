import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {BodyDaemonServer} from '../src/runtime/body_daemon_server.js';
test('monitor credential reads real daemon gauges but cannot drain execution',async()=>{
 const directory=mkdtempSync(join(tmpdir(),'ansts-monitoring-'));const token='a'.repeat(64),monitor='b'.repeat(64);
 const server=new BodyDaemonServer({nodeId:'monitor-test',storageDir:directory,bodyWorkerUrl:'http://127.0.0.1:1',token,monitoringToken:monitor,host:'127.0.0.1',port:0});
 const {port}=await server.start();const base='http://127.0.0.1:'+port;
 try {
  assert.equal((await fetch(base+'/metrics')).status,401);
  const response=await fetch(base+'/metrics',{headers:{authorization:'Bearer '+monitor}});assert.equal(response.status,200);
  const text=await response.text();assert.match(text,/ansts_body_slots_total 1/);assert.match(text,/ansts_body_result_outbox_pending 0/);assert.match(text,/ansts_body_oldest_resource_reservation_seconds 0/);
  assert.ok(!text.includes(token));assert.ok(!text.includes(monitor));
  assert.equal((await fetch(base+'/v1/daemon/drain',{method:'POST',headers:{authorization:'Bearer '+monitor,'content-type':'application/json'},body:'{"draining":true}'})).status,401);
  assert.equal((await(await fetch(base+'/health')).json()).draining,false);
  const bad=await fetch(base+'/v1/sandboxes/execute',{method:'POST',headers:{authorization:'Body '+token,'x-ansts-body-id':'monitor-test','content-type':'application/json'},body:'{}'});assert.equal(bad.status,400);
  const measured=await(await fetch(base+'/metrics',{headers:{authorization:'Bearer '+monitor}})).text();
  assert.match(measured,/ansts_body_admission_decisions_total\{reason="invalid_request"} 1/);
  assert.match(measured,/ansts_body_admission_decisions_total\{reason="accepted"} 0/);
 }finally{await server.stop();rmSync(directory,{recursive:true,force:true});}
});
test('LayerFS remote sync honors daemon cancellation for push and pull',async()=>{
 const {LayerFsWorkspaceDriver}=await import('../src/runtime/layerfs_workspace_driver.js');
 const driver=new LayerFsWorkspaceDriver(),original=globalThis.fetch;let calls=0;
 try {
  globalThis.fetch=async(_input,init)=>{
   calls++;const signal=init?.signal;assert.ok(signal);
   return await new Promise<Response>((_resolve,reject)=>signal.addEventListener('abort',()=>reject(new DOMException('cancelled','AbortError')),{once:true}));
  };
  const pushAbort=new AbortController();const push=driver.pushBranchToRemoteS3('main','http://127.0.0.1:1',pushAbort.signal);pushAbort.abort();assert.equal(await push,false);
  const pullAbort=new AbortController();const pull=driver.pullBranchFromRemoteS3('main','http://127.0.0.1:1',pullAbort.signal);pullAbort.abort();assert.equal(await pull,false);assert.equal(calls,2);
  assert.equal(await driver.pullBranchFromRemoteS3('main','http://127.0.0.1:1',pullAbort.signal),false);assert.equal(calls,2);
 } finally {globalThis.fetch=original;}
});
test('daemon admission metrics count durable admission, replay and grant rejection exactly once',async()=>{
 const http=await import('node:http');let rejectGrant=false;
 const upstream=http.createServer(async(req,res)=>{
  for await(const _chunk of req){}
  res.writeHead(rejectGrant&&req.url?.startsWith('/v1/body/verify-execution-token')?409:200,{'content-type':'application/json'});
  res.end(JSON.stringify({cancel_requested:true,lease_expires_at:Date.now()/1000+30}));
 });
 await new Promise<void>(resolve=>upstream.listen(0,'127.0.0.1',resolve));const address=upstream.address();assert.ok(address&&typeof address==='object');
 const directory=mkdtempSync(join(tmpdir(),'ansts-admission-'));const token='a'.repeat(64),monitor='b'.repeat(64);
 const server=new BodyDaemonServer({nodeId:'admission-test',storageDir:directory,bodyWorkerUrl:'http://127.0.0.1:'+address.port,token,monitoringToken:monitor,host:'127.0.0.1',port:0});
 const {port}=await server.start();const base='http://127.0.0.1:'+port;
 const execute=()=>fetch(base+'/v1/sandboxes/execute',{method:'POST',headers:{authorization:'Body '+token,'x-ansts-body-id':'admission-test','content-type':'application/json'},body:JSON.stringify({taskId:'task',tenantId:'tenant',attemptId:'attempt',fencingToken:1})});
 try {
  assert.equal((await execute()).status,200);assert.equal((await execute()).status,200);
  rejectGrant=true;assert.equal((await execute()).status,409);
  const text=await(await fetch(base+'/metrics',{headers:{authorization:'Bearer '+monitor}})).text();
  for(const reason of ['accepted','replayed','grant_rejected'])assert.ok(text.includes('ansts_body_admission_decisions_total{reason="'+reason+'"} 1'));
  assert.equal((await(await fetch(base+'/health')).json()).activeSandboxes,0);
 }finally{await server.stop();await new Promise<void>(resolve=>upstream.close(()=>resolve()));rmSync(directory,{recursive:true,force:true});}
});
