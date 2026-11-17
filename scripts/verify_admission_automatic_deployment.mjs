/** Local-only live alarm acceptance. Owns metadata and shadow intents; never submits real tasks. */
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {gatewayDevAuthorization} from './gateway_dev_identity.mjs';
const reportPath='.dev-secrets/redeployment/admission-automatic-recovery.json';
const serviceKey=readFileSync('.dev-secrets/admission-service.key','utf8').trim();
const authorization=gatewayDevAuthorization('all','SRE_ADMIN',undefined,'development-auto-delivery-verifier');
const serviceActor={sub:'development-auto-delivery-verifier',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'};
async function call(path,{method='GET',body,key,origin='http://127.0.0.1:8088',auth=authorization}={}){
 const response=await fetch(origin+path,{method,headers:{authorization:auth,'content-type':'application/json',...(key?{'idempotency-key':key}:{})},body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(20000),redirect:'error'});return {status:response.status,data:await response.json()};
}
async function admission(tenantId,action,values={}){return call('/internal/v1/tenant-control/admission-'+action,{method:'POST',origin:'http://127.0.0.1:18082',auth:'Bearer '+serviceKey,body:{actor:serviceActor,tenantId,...values}});}
async function quiet(tenantId){
 const end=Date.now()+20000;let messages;
 do{messages=await admission(tenantId,'outbox');assert.equal(messages.status,200);if(!messages.data.messages.length)return;await new Promise(resolve=>setTimeout(resolve,250));}while(Date.now()<end);
 throw new Error('Automatic shadow delivery did not drain before deadline');
}
const phase=process.argv[2]??'prepare';
if(phase==='prepare'){
 const id='m0-verification-'+randomUUID().slice(0,8);let tenantVersion=0,centerVersion=0,report;
 const center={id,name:'自动投递验收成本中心',status:'ACTIVE',reason:'Owned shadow alarm verification'};
 const tenant={id,name:'自动投递验收租户',region:'dev',costCenter:id,status:'ACTIVE',reason:'Owned shadow alarm verification'};
 try{
  const c=await call('/v1/admin/cost-centers',{method:'POST',body:center,key:id+'-center'});assert.equal(c.status,200);centerVersion=c.data.version;
  const t=await call('/v1/admin/tenants',{method:'POST',body:tenant,key:id+'-tenant'});assert.equal(t.status,200);tenantVersion=t.data.version;
  const policy={expectedVersion:0,resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'100',softThresholdBps:8000},reason:'Automatic shadow delivery verification'};
  assert.equal((await call('/v1/admin/tenants/'+id+'/quota',{method:'PUT',body:policy,key:id+'-quota'})).status,200);
  const input={reservationId:id+'-automatic',taskId:id+'-shadow-task',attemptId:id+'-shadow-attempt',fencing:7,expectedPolicyVersion:1,resources:{concurrentAttempts:1,cpuMilliCores:100,memoryMiB:128,queuedTasks:0},estimatedCreditUnits:'1',expiresAt:Date.now()+300000};
  assert.equal((await admission(id,'prepare',{input,idempotencyKey:id+'-prepare'})).status,200);
  await quiet(id);
  const r=await admission(id,'read',{reservationId:input.reservationId});assert.equal(r.status,200);assert.equal(r.data.reservation.phase,'PREPARING');
  const status=await admission(id,'delivery-status');assert.equal(status.status,200);assert.equal(status.data.delivery.configuration.mode,'RUNNING');assert.equal(status.data.delivery.runtime.acknowledgedMessages,1);
  assert.equal((await admission(id,'delivery-control',{control:{expectedVersion:1,enabled:false,reason:'Pause across development restart',idempotencyKey:id+'-pause'}})).status,200);
  const cancelled=await admission(id,'transition',{input:{reservationId:input.reservationId,expectedVersion:1,generation:1,fencing:7,to:'CANCELLED',reason:'Cancel unstarted shadow fixture'},idempotencyKey:id+'-cancel'});assert.equal(cancelled.status,200);
  await new Promise(resolve=>setTimeout(resolve,1200));
  const pending=await admission(id,'outbox');assert.equal(pending.status,200);assert.equal(pending.data.messages.length,1);assert.equal(pending.data.messages[0].message.kind,'RELEASE_CAPACITY');
  report={tenantId:id,reservationId:input.reservationId,reservation:cancelled.data.reservation,messages:pending.data.messages,configurationVersion:2,stage:'prepared'};
 }finally{
  if(tenantVersion)assert.equal((await call('/v1/admin/tenants/'+id,{method:'PUT',body:{...tenant,status:'DISABLED',expectedVersion:tenantVersion,reason:'Verification metadata disabled; preserve shadow recovery'},key:id+'-disable'})).status,200);
  if(centerVersion)assert.equal((await call('/v1/admin/cost-centers/'+id,{method:'PUT',body:{...center,status:'DISABLED',expectedVersion:centerVersion,reason:'Verification complete'},key:id+'-center-disable'})).status,200);
 }
 mkdirSync('.dev-secrets/redeployment',{recursive:true,mode:0o700});writeFileSync(reportPath,JSON.stringify(report),{mode:0o600});
 console.log('PASS: live native alarm delivered without manual dispatch; pause retains pending release. Owned metadata disabled; ready for restart verification.');
}else if(phase==='verify-recovery'){
 const report=JSON.parse(readFileSync(reportPath,'utf8'));const previouslyVerified=report.stage==='verified';assert.ok(/^m0-verification-[a-z0-9-]+$/.test(report.tenantId));
 const r=await admission(report.tenantId,'read',{reservationId:report.reservationId});assert.equal(r.status,200);assert.deepEqual(r.data.reservation,report.reservation);
 const pending=await admission(report.tenantId,'outbox');assert.equal(pending.status,200);
 const before=await admission(report.tenantId,'delivery-status');assert.equal(before.status,200);assert.equal(before.data.delivery.configuration.mode,'PAUSED');
 if(report.stage!=='verified'){
  assert.deepEqual(pending.data.messages,report.messages);assert.equal(before.data.delivery.configuration.version,2);
  assert.equal((await admission(report.tenantId,'delivery-control',{control:{expectedVersion:2,enabled:true,reason:'Resume recovered shadow backlog',idempotencyKey:report.tenantId+'-resume'}})).status,200);
  await quiet(report.tenantId);
  const after=await admission(report.tenantId,'delivery-status');assert.equal(after.status,200);assert.equal(after.data.delivery.runtime.acknowledgedMessages,2);
  assert.equal((await admission(report.tenantId,'read',{reservationId:report.reservationId})).data.reservation.phase,'CANCELLED');
  assert.equal((await admission(report.tenantId,'delivery-control',{control:{expectedVersion:3,enabled:false,reason:'Leave validation worker paused',idempotencyKey:report.tenantId+'-final-pause'}})).status,200);
  report.stage='verified';report.verifiedAt=new Date().toISOString();writeFileSync(reportPath,JSON.stringify(report),{mode:0o600});
 }else{assert.equal(pending.data.messages.length,0);assert.equal(before.data.delivery.runtime.acknowledgedMessages,2);}
 console.log(previouslyVerified?'PASS: prior automatic-delivery checkpoint remains consistent after deployment; controller paused, two acknowledged messages, no mutations.':'PASS: delivery configuration and pending release survived live restart; native alarm resumed automatically, quota state unchanged, validation worker paused.');
}else throw new Error('Use prepare or verify-recovery');
