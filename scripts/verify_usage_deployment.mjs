/** Owned local-development trial. Runs one real no-network container, never a model request. */
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,openSync,fsyncSync,closeSync,renameSync,existsSync,mkdirSync} from 'node:fs';
import {dirname} from 'node:path';
import {randomUUID} from 'node:crypto';
import {createRequire} from 'node:module';
import {gatewayDevAuthorization} from './gateway_dev_identity.mjs';
const file='.dev-secrets/redeployment/usage-acceptance.json',mode=process.argv[2]??'configure',base='http://127.0.0.1:8088';
const authorization=gatewayDevAuthorization('all','SRE_ADMIN',undefined,'development-usage-verifier');
const {taskStorageId}=createRequire(import.meta.url)('../dist/src/cloudflare/task_identity.js');
function atomic(path,text){const fd=openSync(path+'.tmp','w',0o600);try{writeFileSync(fd,text);fsyncSync(fd);}finally{closeSync(fd);}renameSync(path+'.tmp',path);const parent=openSync(dirname(path),'r');try{fsyncSync(parent);}finally{closeSync(parent);}}
function save(value){mkdirSync(dirname(file),{recursive:true,mode:0o700});atomic(file,JSON.stringify(value));}
async function call(path,body,key,method=body?'POST':'GET',auth=authorization){const response=await fetch(base+path,{method,headers:{authorization:auth,'content-type':'application/json',...(key?{'idempotency-key':key}:{})},body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(15000),redirect:'error'});return {status:response.status,data:await response.json()};}
async function good(...args){const result=await call(...args);assert.ok(result.status>=200&&result.status<300,'owned usage request must succeed: '+args[0]+' status '+result.status);return result.data;}
async function pages(tenant){let cursor=null,events=[];do{const page=await good('/v1/admin/tenants/'+tenant+'/usage?limit=1'+(cursor?'&cursor='+encodeURIComponent(cursor):''));assert.equal(page.pricing,'NOT_CONNECTED');events.push(...page.events);cursor=page.nextCursor;}while(cursor);return events;}
const checkpoint=existsSync(file)?JSON.parse(readFileSync(file,'utf8')):{tenantId:'m0-usage-'+randomUUID().slice(0,8)};
assert.match(checkpoint.tenantId,/^m0-usage-[a-z0-9-]+$/);const id=checkpoint.tenantId;
const center={id,name:'用量源验收成本中心',status:'ACTIVE',reason:'Owned real usage acceptance, not a charge'},tenant={id,name:'用量源验收租户',region:'dev',costCenter:id,status:'ACTIVE',reason:'Owned real usage acceptance'};
if(mode==='configure'){
 save(checkpoint);
 const c=await good('/v1/admin/cost-centers',center,id+'-center'),t=await good('/v1/admin/tenants',tenant,id+'-tenant');checkpoint.centerVersion=c.version;checkpoint.tenantVersion=t.version;
 for(const node of ['01','02']){
  const path='.dev-secrets/body-daemon-'+node+'-runtime.env',original=readFileSync(path,'utf8'),lines=original.split('\n');
  const value=name=>{const row=lines.find(line=>line.startsWith(name+'='));return row?.slice(name.length+1).trim().replace(/^['"]|['"]$/g,'');};
  const prior=value('ANSTS_USAGE_METERING');if(prior!==undefined&&prior!=='shadow')throw new Error('Preserving an explicit non-shadow usage configuration');
  const tenants=[...new Set([...(value('ANSTS_USAGE_TENANTS')??'').split(',').filter(Boolean),id])].sort();
  const retained=lines.filter(line=>!line.startsWith('ANSTS_USAGE_METERING=')&&!line.startsWith('ANSTS_USAGE_TENANTS='));while(retained.at(-1)==='')retained.pop();
  retained.push('ANSTS_USAGE_METERING=shadow','ANSTS_USAGE_TENANTS='+tenants.join(','),'');atomic(path,retained.join('\n'));
 }
 checkpoint.stage='configured';save(checkpoint);console.log('PASS: owned trial metadata registered; only its explicit metering allowlist was added, existing runtime secrets preserved. Rebuild/restart before execute.');
}else if(mode==='execute'){
 const publicId=id+'-container',canonical=taskStorageId(id,publicId),path='/v1/tasks/'+encodeURIComponent(publicId)+'?tenantId='+id;
 const before=await call(path);
 if(before.status===404){const result=await call('/v1/tasks',{tenant_id:id,task_id:publicId,prompt:'Owned source metering verification',timeout_seconds:30,resource_profile:{think_cpu_cores:.1,act_cpu_cores:.1,act_memory_mb:64,disk_mb:32},execution:{kind:'container',image:'node@sha256:fe4f6d74e20370acb04ad889077d14d0f64a9c8ea6a42bc60cddf1e844699914',argv:['node','-e',"console.log('usage-source-live')"],cpu:.1,memoryMb:64,timeoutMs:10000}},id+'-task');assert.ok([200,202].includes(result.status));}
 let events=[],completed=false;
 for(let retry=0;retry<120;retry++){
  const read=await call(path+'&includeResult=1');if(read.status===404){await new Promise(resolve=>setTimeout(resolve,500));continue;}assert.equal(read.status,200);const task=read.data;events=await pages(id);
  if(task.phase==='COMPLETED'&&events.some(row=>row.event.kind==='RESOURCE_CLOSE')){assert.equal(task.result.exitCode,0);assert.ok(task.result.logs.includes('usage-source-live'));completed=true;break;}
  await new Promise(resolve=>setTimeout(resolve,500));
 }
 assert.ok(completed,'owned actual container and automatic usage receipt must complete');
 assert.ok(events.every(row=>row.event.tenantId===id&&row.event.taskId===canonical&&row.context.tenantVersion===1&&row.context.rateCardVersion===null));
 const segments=events.filter(row=>row.event.kind==='RESOURCE_SEGMENT');assert.ok(segments.length);for(const {event} of segments){assert.equal(BigInt(event.allocation.cpuNanoCoreMilliseconds),BigInt(event.allocation.slotMilliseconds)*100000000n);assert.equal(BigInt(event.allocation.memoryMiBMilliseconds),BigInt(event.allocation.slotMilliseconds)*64n);}
 assert.ok(!events.some(row=>row.event.kind.startsWith('PROVIDER_')));
 const viewer=gatewayDevAuthorization(id,'VIEWER');assert.equal((await call('/v1/admin/tenants/'+id+'/usage',undefined,undefined,'GET',viewer)).status,200);
 const outsider=gatewayDevAuthorization('another-tenant','VIEWER');assert.ok([403,404].includes((await call('/v1/admin/tenants/'+id+'/usage',undefined,undefined,'GET',outsider)).status));
 if(!checkpoint.disabled){await good('/v1/admin/tenants/'+id,{...tenant,status:'DISABLED',expectedVersion:checkpoint.tenantVersion,reason:'Owned usage verification complete, retain real quantity evidence'},id+'-disable','PUT');await good('/v1/admin/cost-centers/'+id,{...center,status:'DISABLED',expectedVersion:checkpoint.centerVersion,reason:'Preserve source usage acceptance history'},id+'-center-disable','PUT');checkpoint.disabled=true;}
 checkpoint.stage='executed';checkpoint.taskId=publicId;checkpoint.events=events;save(checkpoint);console.log('PASS: one owned real container completed with automatic signed tenant usage, exact source quantities and cursor paging; Viewer isolation passed, metadata disabled, no provider call or charge.');
}else if(mode==='verify-recovery'){
 assert.equal(checkpoint.stage,'executed');const events=await pages(id);assert.deepEqual(events,checkpoint.events);assert.ok(checkpoint.disabled);
 for(const origin of ['http://127.0.0.1:18080','http://127.0.0.1:18081']){const response=await fetch(origin+'/v1/admin/tenants/'+id+'/usage',{headers:{authorization},signal:AbortSignal.timeout(10000)});assert.equal(response.status,200);assert.equal((await response.json()).events.length,events.length);}
 const rules=await(await fetch('http://127.0.0.1:9090/api/v1/rules')).json();assert.ok(rules.data.groups.some(group=>group.name==='ansts-usage-integrity'&&group.rules.length===4));
 console.log('PASS: real usage bytes, received timestamps, immutable context and page ordering survived actual restart; both Gateways share authority and all four metering alert rules are loaded.');
}else throw new Error('Use configure, execute or verify-recovery');
