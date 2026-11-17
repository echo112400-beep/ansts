/** Live management acceptance: creates clearly named validation metadata, then disables it. */
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {mkdirSync,writeFileSync,readFileSync} from 'node:fs';
import {gatewayDevAuthorization} from './gateway_dev_identity.mjs';
const base=process.env.ANSTS_CONSOLE_URL??'http://127.0.0.1:8088';
if(!['127.0.0.1','localhost'].includes(new URL(base).hostname))throw new Error('Live verifier is restricted to local development');
const id='m0-verification-'+randomUUID().slice(0,8);
const actor='development-directory-verifier';
const authorization=gatewayDevAuthorization('all','SRE_ADMIN',undefined,actor);
let tenantVersion=0,centerVersion=0,operationId=null;
async function call(path,{method='GET',body,key,auth=authorization,origin=base}={}) {
 const response=await fetch(origin+path,{method,headers:{authorization:auth,'content-type':'application/json',...(key?{'idempotency-key':key}:{})},body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(20000),redirect:'error'});
 const data=await response.json();return {status:response.status,data,operationId:response.headers.get('x-ansts-operation-id')};
}
const center={id,name:'M0验收成本中心（测试）',status:'ACTIVE',reason:'Isolated management deployment verification'};
const tenant={id,region:'dev',name:'M0验收租户（测试）',costCenter:id,status:'ACTIVE',reason:'Isolated management deployment verification'};
try {
 const first=await call('/v1/admin/cost-centers',{method:'POST',body:center,key:id+'-center'});assert.equal(first.status,200);centerVersion=first.data.version;
 const created=await call('/v1/admin/tenants',{method:'POST',body:tenant,key:id+'-create'});assert.equal(created.status,200);tenantVersion=created.data.version;assert.ok(created.operationId);operationId=created.operationId;
 const replay=await call('/v1/admin/tenants',{method:'POST',body:tenant,key:id+'-create',origin:'http://127.0.0.1:18081'});assert.equal(replay.status,200);assert.deepEqual(replay.data,created.data);assert.equal(replay.operationId,created.operationId);
 const updated=await call('/v1/admin/tenants/'+id,{method:'PUT',body:{...tenant,name:tenant.name+' 已核验',expectedVersion:1},key:id+'-update'});assert.equal(updated.status,200);tenantVersion=updated.data.version;
 assert.equal((await call('/v1/admin/tenants/'+id,{method:'PUT',body:{...tenant,expectedVersion:1},key:id+'-stale'})).status,409);
 const quotaPath='/v1/admin/tenants/'+id+'/quota';
 const quota={expectedVersion:0,resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:512,queuedTasks:10},budget:{period:'UTC_DAY',limitCreditUnits:'9007199254740993',softThresholdBps:8000},reason:'Live quota policy validation'};
 assert.equal((await call(quotaPath)).data.current,null);
 const saved=await call(quotaPath,{method:'PUT',body:quota,key:id+'-quota'});assert.equal(saved.status,200);assert.equal(saved.data.enforcement,'NOT_CONNECTED');
 const quotaReplay=await call(quotaPath,{method:'PUT',body:quota,key:id+'-quota',origin:'http://127.0.0.1:18081'});assert.deepEqual(quotaReplay.data,saved.data);
 assert.equal((await call(quotaPath,{method:'PUT',body:quota,key:id+'-quota-stale'})).status,409);
 if(process.env.ANSTS_VERIFY_ADMISSION==='1') {
  const coordinator=readFileSync('.dev-secrets/admission-service.key','utf8').trim();
  const directoryKey=readFileSync('.dev-secrets/directory-service.key','utf8').trim();
  const serviceActor={sub:actor,role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'};
  const origin='http://127.0.0.1:18082';
  async function admission(action,values={},auth='Bearer '+coordinator){return call('/internal/v1/tenant-control/admission-'+action,{method:'POST',origin,auth,body:{actor:serviceActor,tenantId:id,...values}});}
  assert.equal((await admission('read',{reservationId:id+'-shadow'},'Bearer '+directoryKey)).status,401);
  const paused=await admission('delivery-control',{control:{expectedVersion:0,enabled:false,reason:'Manual shadow delivery validation fixture',idempotencyKey:id+'-manual-delivery'}});assert.equal(paused.status,200);
  const input={reservationId:id+'-shadow',taskId:id+'-shadow-task',attemptId:id+'-shadow-attempt',fencing:7,expectedPolicyVersion:1,resources:{concurrentAttempts:1,cpuMilliCores:100,memoryMiB:128,queuedTasks:0},estimatedCreditUnits:'1',expiresAt:Date.now()+300000};
  const prepared=await admission('prepare',{idempotencyKey:id+'-prepare',input});assert.equal(prepared.status,200);assert.equal(prepared.data.protocolMode,'SHADOW');assert.equal(prepared.data.enforcement,'NOT_CONNECTED');
  assert.deepEqual((await admission('prepare',{idempotencyKey:id+'-prepare',input})).data,prepared.data);
  const capacityDelivery=await admission('dispatch');assert.equal(capacityDelivery.status,200);assert.equal(capacityDelivery.data.delivery.status,'ACKNOWLEDGED');
  assert.equal((await admission('read',{reservationId:input.reservationId})).data.reservation.phase,'PREPARING');
  let reservation=prepared.data.reservation;
  const duplicate=await admission('prepare',{idempotencyKey:id+'-over-quota',input:{...input,reservationId:id+'-excess',attemptId:id+'-excess-attempt'}});assert.equal(duplicate.status,409);assert.equal(duplicate.data.code,'RESOURCE_QUOTA_EXCEEDED');
  for(const to of ['RESERVED','START_AUTHORIZED']) {
   const transition={reservationId:reservation.id,expectedVersion:reservation.version,generation:reservation.generation,fencing:reservation.fencing,to,reason:'Live shadow protocol verification'};
   if(to==='RESERVED')Object.assign(transition,{target:'validation-shadow-body',capacityReceipt:'validation-shadow-capacity-unverified'});
   const result=await admission('transition',{idempotencyKey:id+'-'+to,input:transition});assert.equal(result.status,200);reservation=result.data.reservation;
  }
  const startDelivery=await admission('dispatch');assert.equal(startDelivery.status,200);assert.equal(startDelivery.data.delivery.status,'ACKNOWLEDGED');
  assert.equal((await admission('read',{reservationId:reservation.id})).data.reservation.phase,'START_AUTHORIZED');
  const issued=await admission('credential',{reservationId:reservation.id});assert.equal(issued.status,200);
  const verification={reservationId:reservation.id,credential:issued.data.credential,target:'validation-shadow-body'};
  assert.equal((await admission('verify',verification)).status,200);
  assert.equal((await admission('verify',{...verification,target:'wrong-body'})).status,409);
  assert.equal((await admission('transition',{idempotencyKey:id+'-no-adapter',input:{to:'RUNNING'}})).status,409);
  const uncertain=await admission('transition',{idempotencyKey:id+'-uncertain',input:{reservationId:reservation.id,expectedVersion:reservation.version,generation:reservation.generation,fencing:reservation.fencing,to:'UNCERTAIN',reason:'Verification disconnect; preserve shadow occupancy'}});assert.equal(uncertain.status,200);
  assert.equal((await admission('verify',verification)).status,409);
  const pending=await admission('outbox');assert.equal(pending.status,200);assert.equal(pending.data.messages.length,1);assert.equal(pending.data.messages[0].message.kind,'QUERY_EXECUTION');
  mkdirSync('.dev-secrets/redeployment',{recursive:true,mode:0o700});
  writeFileSync('.dev-secrets/redeployment/admission-shadow-recovery.json',JSON.stringify({tenantId:id,reservationId:reservation.id,reservation:uncertain.data.reservation,messages:pending.data.messages}),{mode:0o600});
 }
 const lowered=await call(quotaPath,{method:'PUT',body:{...quota,expectedVersion:1,resources:{...quota.resources,concurrentAttempts:0}},key:id+'-quota-lower'});assert.equal(lowered.status,200);
 const firstHistory=await call(quotaPath+'?limit=1');assert.equal(firstHistory.status,200);assert.equal(firstHistory.data.current.version,2);assert.equal(firstHistory.data.history[0].version,1);assert.ok(firstHistory.data.nextCursor);
 const nextHistory=await call(quotaPath+'?limit=1&cursor='+encodeURIComponent(firstHistory.data.nextCursor),{origin:'http://127.0.0.1:18081'});assert.equal(nextHistory.data.history[0].version,2);
 assert.equal((await call(quotaPath,{method:'PUT',body:quota,key:'operator-quota',auth:gatewayDevAuthorization(id,'OPERATOR')})).status,403);
 const scoped=gatewayDevAuthorization(id,'VIEWER',undefined,'directory-viewer-verifier');
 const listing=await call('/v1/admin/tenants',{auth:scoped});assert.equal(listing.status,200);assert.deepEqual(listing.data.items.map(t=>t.id),[id]);
 assert.equal((await call('/v1/admin/tenants',{method:'POST',body:tenant,key:'viewer-denied',auth:scoped})).status,403);
 const operation=await call('/v1/admin/operations/'+encodeURIComponent(created.operationId));assert.equal(operation.status,200);assert.equal(operation.data.resultVersion,1);
 const scopedAdmin=gatewayDevAuthorization(id,'SRE_ADMIN',undefined,actor);
 const audit=await call('/v1/admin/audit?domain=tenant',{auth:scopedAdmin});assert.equal(audit.status,200);assert.equal(audit.data.items.length,2);
 if(process.env.PLAYWRIGHT_MODULE&&process.env.CHROME_PATH) {
  const {pathToFileURL}=await import('node:url');const {chromium}=await import(pathToFileURL(process.env.PLAYWRIGHT_MODULE).href);
  const browser=await chromium.launch({executablePath:process.env.CHROME_PATH,headless:true,args:['--no-sandbox','--no-proxy-server']});
  try {const page=await browser.newPage();await page.goto(base+'/manage/tenants');await page.getByText('使用短期开发凭据').click();await page.locator('input[type=password]').fill(authorization.replace(/^Bearer /,''));await page.getByRole('button',{name:'连接工作区'}).click();await page.getByRole('row').filter({hasText:tenant.name+' 已核验'}).waitFor();await page.getByRole('row').filter({hasText:tenant.name+' 已核验'}).getByRole('button',{name:'查看配额策略'}).click();const quotaPanel=page.locator('[aria-labelledby=tenant-quota-title]');await quotaPanel.getByText(/当前 v2/).waitFor();await quotaPanel.getByRole('button',{name:'编辑额度策略',exact:true}).click();await quotaPanel.getByLabel('内部预算（credit整数单位）',{exact:true}).fill('9007199254740994');await quotaPanel.getByLabel('配额变更原因').fill('Live quota UI validation');await quotaPanel.getByRole('button',{name:'确认保存策略',exact:true}).click();await quotaPanel.getByText(/策略版本 3 已保存，尚未连接执行准入/).waitFor();await quotaPanel.getByRole('combobox',{name:'历史每页'}).selectOption('1');await quotaPanel.getByRole('button',{name:'下一页策略历史'}).waitFor();await quotaPanel.getByRole('button',{name:'下一页策略历史'}).click();await quotaPanel.locator('tbody tr').filter({hasText:'v2'}).waitFor();mkdirSync('docs/validation/admin-platform',{recursive:true});await page.screenshot({path:'docs/validation/admin-platform/live-directory.png'});}finally{await browser.close();}
 }
} finally {
 if(tenantVersion){const result=await call('/v1/admin/tenants/'+id,{method:'PUT',body:{...tenant,status:'DISABLED',expectedVersion:tenantVersion,reason:'Verification complete; retain audit only'},key:id+'-disable'});assert.equal(result.status,200);}
 if(centerVersion){const result=await call('/v1/admin/cost-centers/'+id,{method:'PUT',body:{...center,status:'DISABLED',expectedVersion:centerVersion,reason:'Verification complete; retain audit only'},key:id+'-center-disable'});assert.equal(result.status,200);}
}
const state=await call('/v1/admin/archives');assert.equal(state.status,200);
const archive=await call('/v1/admin/archives',{method:'POST',body:{expectedVersion:state.data.currentVersion,reason:'Verified M0 deployment checkpoint; validation metadata disabled'},key:id+'-archive'});assert.equal(archive.status,200);
const artifact=await call('/v1/admin/archives/'+archive.data.id);assert.equal(artifact.status,200);
assert.equal(createHash('sha256').update(JSON.stringify(artifact.data.snapshot)).digest('hex'),artifact.data.manifest.digest);
mkdirSync('.dev-secrets/backups',{recursive:true,mode:0o700});writeFileSync('.dev-secrets/backups/directory-'+archive.data.id+'.json',JSON.stringify(artifact.data,null,2),{mode:0o600});
if(process.env.PLAYWRIGHT_MODULE&&process.env.CHROME_PATH) {
 const {pathToFileURL}=await import('node:url');const {chromium}=await import(pathToFileURL(process.env.PLAYWRIGHT_MODULE).href);
 const browser=await chromium.launch({executablePath:process.env.CHROME_PATH,headless:true,args:['--no-sandbox','--no-proxy-server']});
 try {
  const page=await browser.newPage();await page.goto(base+'/manage/approvals');await page.getByText('使用短期开发凭据').click();await page.locator('input[type=password]').fill(authorization.replace(/^Bearer /,''));await page.getByRole('button',{name:'连接工作区'}).click();
  const auditPanel=page.locator('[aria-labelledby=management-audit-title]');await auditPanel.getByLabel('操作 ID',{exact:true}).fill(operationId);await auditPanel.getByRole('button',{name:'查询操作',exact:true}).click();await auditPanel.locator('[role=status]').filter({hasText:id}).waitFor();
  const archives=page.locator('[aria-labelledby=directory-archives-title]');const row=archives.getByRole('row').filter({hasText:'v'+archive.data.sourceVersion+' ·'});
  const awaited=page.waitForEvent('download');await row.getByRole('button',{name:'校验并下载'}).click();const download=await awaited;
  assert.deepEqual(JSON.parse(readFileSync(await download.path(),'utf8')),artifact.data);
  await page.screenshot({path:'docs/validation/admin-platform/live-audit-archive.png'});
 } finally {await browser.close();}
}
console.log('PASS: '+(process.env.ANSTS_VERIFY_ADMISSION==='1'?'live shadow admission cross-object delivery/outbox/credentials/revocation; ':'')+'live quota CRUD/replay/CAS/paging/UI and deployed directory CRUD/replay/CAS, viewer isolation, operation/audit, disabled validation metadata and verified host-file backup.');
