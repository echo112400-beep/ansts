import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
import {join} from 'node:path';
import {handleBodyUsageIngress} from '../../src/control/body_usage_ingress.js';
import {handleDirectoryApi} from '../../src/control/admin_directory_api.js';
import {TenantControlCell} from '../../src/cloudflare/tenant_control_cell.js';
import {bodyServiceToken} from '../../src/cloudflare/body_auth.js';
import {signUsageEvent,usageEventId,type UsageIdentity,type UsageEvent} from '../../src/control/usage_event.js';
import type {DirectoryStorage} from '../../src/control/tenant_directory.js';
import type {CloudflareCelldEnv,DurableObjectNamespaceLike,DurableObjectStateLike} from '../../src/cloudflare/env.js';
const root='f'.repeat(64),directory='d'.repeat(64),admission='e'.repeat(64),key=bodyServiceToken(root,'body-1');
const identity:UsageIdentity={producerId:'body-1',tenantId:'tenant',taskId:'task',attemptId:'attempt',fencing:7,executionInputDigest:'a'.repeat(64)},resources={cpuNanoCores:250000000,cpuMilliCores:250,memoryMiB:128,slots:1};
function namespace(handler:(request:Request)=>Promise<Response>,name?:(name:string)=>void):DurableObjectNamespaceLike{return {idFromName:value=>{name?.(value);return {toString:()=>value};},get:()=>({id:{toString:()=>''},fetch:request=>handler(new Request(request))})};}
function fixture(){
 const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');const db=new DatabaseSync(':memory:');
 const storage:DirectoryStorage={sql:{exec<T>(q:string,...args:unknown[]){const rows=db.prepare(q).all(...args) as T[];return {toArray:()=>rows};}},transactionSync<T>(fn:()=>T){db.exec('BEGIN');try{const result=fn();db.exec('COMMIT');return result;}catch(e){db.exec('ROLLBACK');throw e;}}};
 let phase='ADMITTED',retired=false,bodyReads=0;
 const env={ANSTS_BODY_AUTH_SECRET:root,ANSTS_DIRECTORY_SERVICE_TOKEN:directory,ANSTS_ADMISSION_SERVICE_TOKEN:admission,ANSTS_ADMISSION_SIGNING_KEY:'c'.repeat(64),ANSTS_BODY_NAMESPACE_PREFIX:'body-ha-v1:',ANSTS_TENANT_DIRECTORY_CELL:namespace(async request=>{const body=await request.json() as {tenantId:string;actor:{scopeTenant:string}};assert.ok(body.actor.scopeTenant==='all'||body.actor.scopeTenant===body.tenantId);return Response.json({id:'tenant',name:'Tenant',version:1,region:'dev',costCenter:'eng',status:'ACTIVE'});}),ANSTS_BODY_CELL:namespace(async()=>{bodyReads++;if(retired)throw new Error('current Body evidence is retired');return Response.json({execution:{phase,node_id:'body-1',tenant_id:'tenant',task_id:'task',attempt_id:'attempt',fencing_token:7,input_digest:identity.executionInputDigest}});},name=>assert.equal(name,'body-ha-v1:execution:task'))} as CloudflareCelldEnv;
 const cell=new TenantControlCell({storage} as unknown as DurableObjectStateLike,env);env.ANSTS_TENANT_CONTROL_CELL=namespace(request=>cell.fetch(request),name=>assert.equal(name,'tenant-control-v1:tenant'));
 const request=(path:string,body:unknown,token=key)=>new Request('https://fleet/internal/v1/'+path,{method:'POST',headers:{authorization:'Body '+token,'x-ansts-body-id':'body-1'},body:JSON.stringify(body)});
 return {db,env,request,setPhase(value:string){phase=value;},retire(){retired=true;},reads(){return bodyReads;}};
}
test('meter ingress rejects forged identity and requires real Body authorization before durable binding',async()=>{
 const s=fixture();try{
  assert.equal((await handleBodyUsageIngress(s.request('body-usage-binding',{identity,resources},'0'.repeat(64)),s.env)).status,401);
  assert.equal((await handleBodyUsageIngress(s.request('body-usage-binding',{identity,resources,actor:{role:'SRE_ADMIN'}}),s.env)).status,422);
  assert.equal((await handleBodyUsageIngress(s.request('body-usage-binding',{identity,resources}),s.env)).status,409);
  s.setPhase('RUNNING');assert.equal((await handleBodyUsageIngress(s.request('body-usage-binding',{identity,resources}),s.env)).status,200);
  assert.equal((await handleBodyUsageIngress(s.request('body-usage-binding',{identity,resources:{...resources,memoryMiB:256}}),s.env)).status,409);
  const event:UsageEvent={schemaVersion:1,purpose:'ansts-usage-event-v1',...identity,resources,kind:'RESOURCE_OPEN',at:1000,sequence:1,eventId:usageEventId(identity,1),allocation:null,provider:null};
  const forged={...event,producerId:'other'};forged.eventId=usageEventId(forged,1);assert.equal((await handleBodyUsageIngress(s.request('body-usage',{proof:signUsageEvent(forged,key)}),s.env)).status,403);
  const reads=s.reads();s.retire();const response=await handleBodyUsageIngress(s.request('body-usage',{proof:signUsageEvent(event,key)}),s.env);assert.equal(response.status,200);assert.equal(s.reads(),reads);assert.equal((await response.json()).receipt.outcome,'RECORDED');
  const viewer={sub:'github:1',role:'VIEWER' as const,scopeTenant:'tenant',scopeRegion:'dev'};
  const page=await handleDirectoryApi(new Request('https://gateway/v1/admin/tenants/tenant/usage?limit=1'),s.env,viewer);assert.equal(page.status,200);const data=await page.json();assert.equal(data.events[0].context.costCenter,'eng');assert.equal(data.events[0].context.rateCardVersion,null);
  assert.equal((await handleDirectoryApi(new Request('https://gateway/v1/admin/tenants/tenant/usage'),s.env,{...viewer,scopeTenant:'other'})).status,404);
 }finally{s.db.close();}
});
