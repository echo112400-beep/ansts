/** Independent development backup/recovery. Never starts execution or restores live volumes. */
import assert from 'node:assert/strict';
import {mkdirSync,readFileSync,writeFileSync,openSync,closeSync,fsyncSync,renameSync,mkdtempSync} from 'node:fs';
import {join,dirname} from 'node:path';
import {execFileSync} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {gatewayDevAuthorization} from './gateway_dev_identity.mjs';
const base='http://127.0.0.1:8088',mode=process.argv[2]??'capture',checkpoint='.dev-secrets/redeployment/finops-backup-acceptance.json';
const authorization=gatewayDevAuthorization('all','SRE_ADMIN',undefined,'development-finops-backup-verifier');
const usage=JSON.parse(readFileSync('.dev-secrets/redeployment/usage-acceptance.json','utf8')),retained=JSON.parse(readFileSync('.dev-secrets/redeployment/admission-shadow-recovery.json','utf8'));
for(const id of [usage.tenantId,retained.tenantId])assert.match(id,/^m0-(usage|verification)-[a-z0-9-]+$/);
async function call(path,body,key,origin=base){const response=await fetch(origin+path,{method:body?'POST':'GET',headers:{authorization,'content-type':'application/json',...(key?{'idempotency-key':key}:{})},body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(20000),redirect:'error'});return {status:response.status,data:await response.json()};}
async function good(...args){const result=await call(...args);assert.equal(result.status,200,args[0]);return result.data;}
function durable(path,value){const fd=openSync(path,'wx',0o600);try{writeFileSync(fd,JSON.stringify(value));fsyncSync(fd);}finally{closeSync(fd);}const parent=openSync(dirname(path),'r');try{fsyncSync(parent);}finally{closeSync(parent);}}
if(mode==='capture'){
 mkdirSync('.dev-secrets/finops-backups',{recursive:true,mode:0o700});const folder=mkdtempSync('.dev-secrets/finops-backups/verification-'),records=[];
 for(const tenantId of [null,usage.tenantId,retained.tenantId]){
  const path=tenantId?'/v1/admin/tenants/'+tenantId+'/finops-archives':'/v1/admin/finops/archives',kind=tenantId?'TENANT':'DIRECTORY';let manifest;
  const key='independent-backup-'+randomUUID();for(let attempt=0;attempt<5;attempt++){const status=await good(path);const result=await call(path,{expectedRevision:status.sourceRevision,reason:'Independent accounting backup/recovery acceptance'},key);if(result.status===409)continue;assert.equal(result.status,200);manifest=result.data;break;}assert.ok(manifest,'bounded consistent archive capture');assert.match(manifest.id,/^[a-f0-9]{64}$/);
  const artifact=await good(path+'/'+manifest.id),artifactFile=join(folder,manifest.id+'.json'),databaseFile=join(folder,manifest.id+'.sqlite');durable(artifactFile,artifact);
  const restored=JSON.parse(execFileSync(process.execPath,['scripts/restore_finops_archive.mjs',artifactFile,'.dev-secrets/directory-service.key',databaseFile,kind,...(tenantId?[tenantId]:[])],{encoding:'utf8',stdio:['ignore','pipe','pipe']}));assert.equal(restored.sourceRevision,manifest.sourceRevision);assert.equal(restored.operationalResume,'DISABLED');
  if(tenantId){const costs=await good('/v1/admin/tenants/'+tenantId+'/costs');assert.deepEqual(restored.summary,costs.summary);}
  if(tenantId===retained.tenantId){const row=artifact.snapshot.tables.find(t=>t.name==='tenant_admission_state').rows.find(r=>r[0]==='admission');const state=JSON.parse(row[1]);assert.deepEqual(state.reservations.find(r=>r.id===retained.reservationId),retained.reservation);assert.ok(restored.summary.reasons.includes('UNSETTLED_ADMISSION_RESERVATIONS'));}
  records.push({tenantId,path,id:manifest.id,artifactFile,databaseFile,restored});
 }
 const pending=checkpoint+'.tmp-'+randomUUID();durable(pending,{records});renameSync(pending,checkpoint);const parent=openSync(dirname(checkpoint),'r');try{fsyncSync(parent);}finally{closeSync(parent);}
 console.log('PASS: directory rates/receipts and two real tenant authority snapshots downloaded to private host files and independently restored into new offline SQLite databases. Exact source revisions, pending costs, UNCERTAIN reservation and outbox retained; operational resume disabled.');
}else if(mode==='verify-recovery'){
 const {records}=JSON.parse(readFileSync(checkpoint,'utf8'));assert.equal(records.length,3);for(const record of records){const archived=JSON.parse(readFileSync(record.artifactFile,'utf8'));for(const origin of [base,'http://127.0.0.1:18080','http://127.0.0.1:18081'])assert.deepEqual(await good(record.path+'/'+record.id,undefined,undefined,origin),archived);}
 const audit=await good('/v1/admin/audit?domain=rate-card');assert.ok(audit.items.every(entry=>entry.domain==='rate-card'));
 const activations=await good('/v1/admin/audit?domain=rate-activation&tenantId='+usage.tenantId);assert.ok(activations.items.every(entry=>entry.domain==='rate-activation'));
 assert.equal((await call('/v1/admin/audit?domain=rate-activation')).status,422);
 console.log('PASS: all three immutable archive artifacts survived real restart and match independent host copies through both Gateways; unified template/tenant-activation audit scope verified.');
}else throw new Error('Use capture or verify-recovery');
if(process.env.PLAYWRIGHT_MODULE&&process.env.CHROME_PATH){
 const {pathToFileURL}=await import('node:url'),{chromium}=await import(pathToFileURL(process.env.PLAYWRIGHT_MODULE).href),browser=await chromium.launch({executablePath:process.env.CHROME_PATH,headless:true,args:['--no-sandbox','--no-proxy-server']});
 try{const page=await browser.newPage();await page.goto(base+'/manage/approvals');await page.getByText('使用短期开发凭据').click();await page.locator('input[type=password]').fill(authorization.replace(/^Bearer /,''));await page.getByRole('button',{name:'连接工作区'}).click();const panel=page.locator('[aria-labelledby=management-audit-title]');await panel.getByRole('combobox',{name:'审计对象'}).selectOption('rate-card');await panel.getByText('当前授权范围暂无此类管理变更。').waitFor();await panel.getByRole('combobox',{name:'审计对象'}).selectOption('rate-activation');await panel.getByLabel('租户 ID',{exact:true}).fill(usage.tenantId);await panel.getByRole('button',{name:'刷新审计首页',exact:true}).click();await panel.getByText('当前授权范围暂无此类管理变更。').waitFor();assert.equal(await panel.locator('[role=alert]').count(),0);await page.screenshot({path:'docs/validation/admin-platform/live-finops-audit.png'});}finally{await browser.close();}
 console.log('PASS: real browser uses unified template and tenant activation audit filters in the existing console layout.');
}
