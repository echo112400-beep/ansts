/** Read-only proof endpoint deployment checks. Missing evidence never means a task did not start. */
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {randomUUID,createHmac} from 'node:crypto';
for(const [hostId,port] of [['body-host-01',19501],['body-host-02',19502]]){
 const token=readFileSync('.dev-secrets/'+hostId+'.token','utf8').trim();
 const root=readFileSync('.dev-secrets/body-signing.key','utf8').trim();assert.ok(createHmac('sha256',root).update('ansts-body-v1:'+hostId).digest('hex')===token,'Configured Body identity must derive from the deployed root');
 const identity={tenantId:'m0-verification-'+randomUUID().slice(0,8),taskId:'unknown-evidence-probe',attemptId:'unknown-attempt'};
 const call=async(headers)=>fetch(`http://127.0.0.1:${port}/v1/daemon/execution-evidence`,{method:'POST',headers:{'content-type':'application/json',...headers},body:JSON.stringify(identity),signal:AbortSignal.timeout(5000),redirect:'error'});
 assert.equal((await call({})).status,401);
 const response=await call({authorization:'Body '+token,'x-ansts-body-id':hostId});assert.equal(response.status,404);const data=await response.json();assert.equal(data.error,'EXECUTION_EVIDENCE_UNKNOWN');assert.equal(data.proof,undefined);
 const monitor=readFileSync('.dev-secrets/monitoring.key','utf8').trim();assert.equal((await call({authorization:'Bearer '+monitor})).status,401);
}
console.log('PASS: both deployed Body evidence endpoints require host identity; missing records remain UNKNOWN with no terminal proof; monitoring identity rejected. No task mutations.');

// New outbox queries are read-only here: never acknowledge or defer a user's observation.
for(const [hostId,port] of [['body-host-01',19501],['body-host-02',19502]]){
 const token=readFileSync('.dev-secrets/'+hostId+'.token','utf8').trim(),headers={authorization:'Body '+token,'x-ansts-body-id':hostId,'content-type':'application/json'};
 const pending=async extra=>fetch(`http://127.0.0.1:${port}/v1/daemon/execution-evidence/pending`,{method:'POST',headers:{'content-type':'application/json',...extra},body:JSON.stringify({limit:4}),signal:AbortSignal.timeout(5000),redirect:'error'});
 assert.equal((await pending({})).status,401);
 const response=await pending(headers);assert.equal(response.status,200);const data=await response.json();assert.ok(Array.isArray(data.observations)&&data.observations.length<=4);
 for(const item of data.observations)assert.match(item.digest,/^[a-f0-9]{64}$/);
 for(const fleetPort of [18082,18083,18084]){
  const origin=`http://127.0.0.1:${fleetPort}`;
  for(const route of ['/internal/v1/body-observation-binding','/internal/v1/body-execution-evidence']){
   const denied=await fetch(origin+route,{method:'POST',headers:{'content-type':'application/json'},body:'{}',signal:AbortSignal.timeout(5000)});assert.equal(denied.status,401);
   const method=await fetch(origin+route,{headers,signal:AbortSignal.timeout(5000)});assert.equal(method.status,405);
  }
  const lookup={tenantId:'m0-verification-'+randomUUID().slice(0,8),taskId:'unknown-bound-task',attemptId:'unknown-bound-attempt',fencing:1,executionInputDigest:'e'.repeat(64),resources:{concurrentAttempts:1,cpuMilliCores:100,memoryMiB:128,queuedTasks:0}};
  const unknown=await fetch(origin+'/internal/v1/body-observation-binding',{method:'POST',headers,body:JSON.stringify(lookup),signal:AbortSignal.timeout(5000)});assert.equal(unknown.status,404);
  const retained=JSON.parse(readFileSync('.dev-secrets/redeployment/admission-shadow-recovery.json','utf8'));
  const legacyLookup={...lookup,tenantId:retained.tenantId,taskId:retained.reservation.taskId,attemptId:retained.reservation.attemptId,fencing:retained.reservation.fencing,resources:retained.reservation.resources};
  const legacy=await fetch(origin+'/internal/v1/body-observation-binding',{method:'POST',headers,body:JSON.stringify(legacyLookup),signal:AbortSignal.timeout(5000)});assert.equal(legacy.status,200);const legacyData=await legacy.json();assert.equal(legacyData.binding,null);assert.equal(legacyData.protocolMode,'SHADOW');assert.equal(legacyData.enforcement,'NOT_CONNECTED');
  const forged=await fetch(origin+'/internal/v1/body-observation-binding',{method:'POST',headers,body:JSON.stringify({...lookup,target:'another-host',actor:{role:'SRE_ADMIN'}}),signal:AbortSignal.timeout(5000)});assert.equal(forged.status,400);
 }
}
console.log('PASS: both deployed Host observation outboxes are bounded/authenticated; all 3 Fleet ingress instances enforce Host identity, method, lookup allowlist, existing-tenant resolution and read-only UNBOUND for retained legacy reservations. No real execution or outbox mutations.');
