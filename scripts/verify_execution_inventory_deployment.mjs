/** Read-only execution discovery acceptance. Uses existing owned evidence; creates no task or charge. */
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,openSync,closeSync,fsyncSync} from 'node:fs';
import {createHmac} from 'node:crypto';
import {gatewayDevAuthorization} from './gateway_dev_identity.mjs';
const mode=process.argv[2]??'capture',checkpoint='.dev-secrets/redeployment/execution-inventory-acceptance.json';
assert.ok(['capture','verify-recovery','check-live','check-period'].includes(mode),'Use capture, verify-recovery, check-live or check-period YYYY-MM');
const usage=JSON.parse(readFileSync('.dev-secrets/redeployment/usage-acceptance.json','utf8'));
assert.match(usage.tenantId,/^m0-usage-[a-z0-9-]+$/);
const actor={sub:'system:execution-inventory-verifier',role:'SRE_ADMIN',scopeTenant:usage.tenantId,scopeRegion:'all'};
const origins=['http://127.0.0.1:18082','http://127.0.0.1:18083','http://127.0.0.1:18084'];
async function call(origin,path,payload,authorization='Bearer '+token){const response=await fetch(origin+path,{method:payload?'POST':'GET',headers:{authorization,'content-type':'application/json'},body:payload?JSON.stringify(payload):undefined,redirect:'error',signal:AbortSignal.timeout(20000)});return {status:response.status,data:await response.json()};}
if(mode==='check-period'){
 const period=process.argv[3];
 assert.match(period??'',/^[0-9]{4}-(0[1-9]|1[0-2])$/,'Supply a UTC month YYYY-MM');
 const auth=gatewayDevAuthorization(usage.tenantId,'VIEWER');
 const path='/v1/admin/tenants/'+encodeURIComponent(usage.tenantId)+'/settlements/'+period+'/preview';
 const results=[];
 for(const port of [18080,18081]){
  const {status,data}=await call('http://127.0.0.1:'+port,path,undefined,auth);
  if(status===409&&data.code==='PERIOD_ALREADY_CLOSED'){results.push({port,status,code:data.code});continue;}
  assert.equal(status,200,'Gateway preview on port '+port);
  assert.equal(data.tenantId,usage.tenantId,'preview tenant');
  assert.equal(data.periodId,period,'preview month');
  assert.equal(data.calendar,'UTC_MONTH','preview calendar');
  results.push({port,status,complete:data.complete,coverageComplete:data.coverage.complete,
   nonEmpty:data.coverage.attemptCount>0,revision:data.revision,
   totalCreditUnits:data.totalCreditUnits,reasons:data.reasons,coverageDiagnostics:data.coverageDiagnostics});
 }
 const [first,second]=results;
 const sameRevision=typeof first.revision==='string'&&first.revision.length>0&&first.revision===second.revision;
 const readyForNonEmptyClose=sameRevision&&first.totalCreditUnits===second.totalCreditUnits&&results.every(
  result=>result.complete===true&&result.coverageComplete===true&&result.nonEmpty
   &&typeof result.totalCreditUnits==='string'&&/^-?\d+$/.test(result.totalCreditUnits)
   &&result.reasons.length===0&&result.coverageDiagnostics.length===0);
 console.log(JSON.stringify({period,readyForNonEmptyClose,sameRevision,gateways:results}));
 process.exit(0);
}
const token=readFileSync('.dev-secrets/directory-service.key','utf8').trim(),secret=readFileSync('.dev-secrets/body-signing.key','utf8').trim();
async function inventory(origin,source,taskId){const response=await call(origin,'/internal/v1/execution-inventory',{actor,tenantId:usage.tenantId,source,...taskId?{taskId}:{}});assert.equal(response.status,200,'private source must be available');assert.equal(response.data.complete,true);assert.equal(response.data.tenantId,usage.tenantId);return response.data;}
function fixed(value){return {watermark:value.watermark,entries:value.entries,coverageSince:value.coverageSince,count:value.count};}
const scheduler=await inventory(origins[0],'SCHEDULER'),taskId=usage.events[0].event.taskId,body=await inventory(origins[0],'BODY',taskId);
assert.ok(scheduler.entries.some(entry=>entry.observation.taskId===taskId));assert.ok(body.entries.some(entry=>entry.observation.taskId===taskId));
// Existing evidence predates the journal. Recording it now cannot prove its lost attempt history.
assert.ok(body.entries.some(entry=>entry.baseline));
for(const origin of origins){assert.deepEqual(fixed(await inventory(origin,'SCHEDULER')),fixed(scheduler));assert.deepEqual(fixed(await inventory(origin,'BODY',taskId)),fixed(body));}
const bodyAuthorization='BodyControl '+createHmac('sha256',secret).update('ansts-body-v1:body:control').digest('hex');
const proxy=await call('http://127.0.0.1:18085','/internal/v1/execution-inventory/body?tenantId='+encodeURIComponent(usage.tenantId)+'&taskId='+encodeURIComponent(taskId),undefined,bodyAuthorization);
assert.equal(proxy.status,200);assert.deepEqual(fixed(proxy.data),fixed(body));
const response=await call(origins[0],'/internal/v1/settlement-coverage',{actor,tenantId:usage.tenantId,periodId:new Date().toISOString().slice(0,7)});
assert.equal(response.status,200);assert.equal(response.data.coverage.complete,false);assert.ok(response.data.reasons.includes('HISTORICAL_COVERAGE_UNAVAILABLE'));assert.equal(response.data.final,false);
assert.equal((await call(origins[0],'/internal/v1/execution-inventory',{actor,tenantId:usage.tenantId,source:'SCHEDULER'},'Bearer invalid')).status,401);
assert.equal((await call(origins[0],'/v1/scheduler/execution-inventory?tenantId='+usage.tenantId)).status,403);
assert.equal((await call(origins[0],'/internal/v1/settlement-coverage',{actor,tenantId:usage.tenantId,periodId:'2026-05',complete:true})).status,422);
const value={tenantId:usage.tenantId,taskId,scheduler:fixed(scheduler),body:fixed(body)};
if(mode==='capture'){const fd=openSync(checkpoint,'w',0o600);try{writeFileSync(fd,JSON.stringify(value));fsyncSync(fd);}finally{closeSync(fd);}}
else if(mode==='verify-recovery')assert.deepEqual(value,JSON.parse(readFileSync(checkpoint,'utf8')));
else {
 const cutoff=new Date(scheduler.coverageSince);
 const period=new Date(Date.UTC(cutoff.getUTCFullYear(),cutoff.getUTCMonth()-1,1)).toISOString().slice(0,7);
 const path='/v1/admin/tenants/'+encodeURIComponent(usage.tenantId)+'/settlements/'+period+'/preview';
 const viewer=gatewayDevAuthorization(usage.tenantId,'VIEWER');
 for(const gateway of ['http://127.0.0.1:18080','http://127.0.0.1:18081']){
  assert.equal((await call(gateway,path,undefined,'')).status,401);
  const preview=await call(gateway,path,undefined,viewer);
  assert.equal(preview.status,200,'Gateway must serve the settlement preview');
  assert.equal(preview.data.coverage.complete,false);
  assert.equal(preview.data.totalCreditUnits,null);
  assert.ok(preview.data.coverageDiagnostics.includes('HISTORICAL_COVERAGE_UNAVAILABLE'));
 }
 const firstMonth=new Date(Date.UTC(cutoff.getUTCFullYear(),cutoff.getUTCMonth(),1));
 if(firstMonth.getTime()<cutoff.getTime())firstMonth.setUTCMonth(firstMonth.getUTCMonth()+1);
 const firstMonthEnd=new Date(firstMonth);
 firstMonthEnd.setUTCMonth(firstMonthEnd.getUTCMonth()+1);
 console.log(JSON.stringify({schedulerCoverageSince:cutoff.toISOString(),
  earliestFullSchedulerMonth:firstMonth.toISOString().slice(0,7),firstMonthEnd:firstMonthEnd.toISOString()}));
}
console.log('PASS: all three Fleet entries and Body proxy share exact retained inventory; existing pre-cutover evidence stays incomplete, forged complete/unauthenticated requests rejected, no execution or final bill created'+(mode==='verify-recovery'?'; source rows and watermarks survived restart.':'.'));
