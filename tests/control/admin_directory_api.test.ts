import { test } from 'node:test';
import assert from 'node:assert/strict';
import { handleDirectoryApi } from '../../src/control/admin_directory_api.js';
import { TenantDirectoryCell } from '../../src/cloudflare/tenant_directory_cell.js';
import { MockDurableObjectState } from '../../src/cloudflare/env.js';
const admin={sub:'github:1',role:'SRE_ADMIN' as const,scopeTenant:'a',scopeRegion:'dev'};
const credential='test-only-directory-service-token-123456789';
test('API rejects readonly writes, operator administration, and missing bindings',async()=>{
  const req=()=>new Request('https://console/v1/admin/tenants',{method:'POST',body:'{}'});
  assert.equal((await handleDirectoryApi(req(),{},admin)).status,503);
  assert.equal((await handleDirectoryApi(req(),{},{...admin,role:'OPERATOR'})).status,403);
  assert.equal((await handleDirectoryApi(req(),{},{...admin,readOnly:true})).status,403);
});
test('API forwards verified identity and path ID, ignoring body impersonation',async()=>{
  let forwarded:Record<string,unknown>={};
  const response=await handleDirectoryApi(new Request('https://console/v1/admin/tenants/a',{
    method:'PUT',headers:{'idempotency-key':'key'},body:JSON.stringify({id:'other',actor:{sub:'evil'},region:'dev',name:'A',costCenter:'eng',status:'ACTIVE',expectedVersion:1,reason:'update'})
  }),{ANSTS_DIRECTORY_SERVICE_TOKEN:credential,ANSTS_TENANT_DIRECTORY_CELL:{
    idFromName(name){assert.equal(name,'tenant-directory-v1');return {toString:()=>name};},
    get(id){return {id,async fetch(input){const r=input as Request; assert.equal(r.headers.get('authorization'),'Bearer '+credential);forwarded=await r.json() as Record<string,unknown>;return Response.json({ok:true});}};}
  }},admin);
  assert.equal(response.status,200);
  assert.deepEqual(forwarded.actor,admin);
  assert.equal((forwarded.input as {id:string}).id,'a');
});
test('cell refuses missing service identity and nontransactional legacy storage',async()=>{
  // The real constructor detects the missing transaction capability at runtime.
  const cell=new TenantDirectoryCell(new MockDurableObjectState(),{ANSTS_DIRECTORY_SERVICE_TOKEN:credential});
  assert.equal((await cell.fetch(new Request('https://directory/list',{method:'POST'}))).status,401);
  assert.equal((await cell.fetch(new Request('https://directory/list',{method:'POST',headers:{authorization:'Bearer '+credential}}))).status,503);
});

test('real Gateway authenticates admin routes and fails closed without directory binding',async()=>{
  const {handleUnifiedGatewayRequest}=await import('../../src/cloudflare/gateway_cell.js');
  const {ConsoleRbacAuthGuard}=await import('../../src/dashboard/console_rbac_guard.js');
  const secret='test-only-user-signing-secret-1234567890123456';
  const token=new ConsoleRbacAuthGuard(secret).issueToken({sub:'github:1',role:'SRE_ADMIN'});
  const env={ANSTS_USER_AUTH_SECRET:secret};
  assert.equal((await handleUnifiedGatewayRequest(new Request('https://console/v1/admin/tenants'),env))?.status,401);
  assert.equal((await handleUnifiedGatewayRequest(new Request('https://console/v1/admin/tenants',{headers:{authorization:'Bearer '+token}}),env))?.status,503);
});

test('remote directory sends one authenticated write and never falls back locally',async()=>{
  const http=await import('node:http');
  let requests=0;let local=0;
  const server=http.createServer(async(req,res)=>{
    requests++;
    assert.equal(req.url,'/internal/v1/directory/mutate');
    assert.equal(req.headers.authorization,'Bearer '+credential);
    const chunks:Buffer[]=[];for await(const chunk of req) chunks.push(Buffer.from(chunk));
    const body=JSON.parse(Buffer.concat(chunks).toString());
    assert.equal(body.actor.sub,admin.sub);
    assert.equal(body.input.id,'a');
    res.writeHead(503,{'content-type':'application/json'});res.end(JSON.stringify({code:'DIRECTORY_OFFLINE'}));
  });
  await new Promise<void>(resolve=>server.listen(0,'127.0.0.1',resolve));
  try {
    const address=server.address();assert.ok(address&&typeof address==='object');
    const result=await handleDirectoryApi(new Request('https://console/v1/admin/tenants',{method:'POST',headers:{'idempotency-key':'remote'},body:JSON.stringify({id:'a',region:'dev',name:'A',costCenter:'eng',status:'ACTIVE',reason:'create'})}),{
      ANSTS_DIRECTORY_URL:'http://127.0.0.1:'+address.port,ANSTS_DIRECTORY_SERVICE_TOKEN:credential,
      ANSTS_TENANT_DIRECTORY_CELL:{idFromName(){local++;return {toString:()=>''};},get(){throw new Error('must not fall back');}}
    },admin);
    assert.equal(result.status,503);assert.equal(requests,1);assert.equal(local,0);
  } finally {await new Promise<void>((resolve,reject)=>server.close(e=>e?reject(e):resolve()));}
});
test('remote directory rejects unsafe origins before dispatch',async()=>{
  for(const origin of ['file:///etc/passwd','https://user:password@example.com','https://example.com/path','https://example.com/?token=value']) {
    const result=await handleDirectoryApi(new Request('https://console/v1/admin/tenants'),{ANSTS_DIRECTORY_URL:origin,ANSTS_DIRECTORY_SERVICE_TOKEN:credential},admin);
    assert.equal(result.status,503);assert.equal((await result.json()).code,'INVALID_DIRECTORY_ORIGIN');
  }
});
test('quota routes bind the path tenant, reject malformed IDs, and deny operator writes',async()=>{
 const path='https://console/v1/admin/tenants/a/quota';
 assert.equal((await handleDirectoryApi(new Request(path),{},{...admin,scopeTenant:'b'})).status,404);
 assert.equal((await handleDirectoryApi(new Request('https://console/v1/admin/tenants/%ZZ/quota'),{},admin)).status,422);
 assert.equal((await handleDirectoryApi(new Request(path,{method:'PUT',body:'{}'}),{},{...admin,role:'OPERATOR'})).status,403);
 let envelope:Record<string,unknown>={};
 const response=await handleDirectoryApi(new Request(path,{method:'PUT',headers:{'idempotency-key':'quota'},body:JSON.stringify({tenantId:'b',region:'untrusted',actor:{sub:'evil'},resources:{concurrentAttempts:1,cpuMilliCores:1000,memoryMiB:128,queuedTasks:1},budget:{period:'UTC_DAY',limitCreditUnits:'10',softThresholdBps:8000},expectedVersion:0,reason:'allocation'})}),{ANSTS_DIRECTORY_SERVICE_TOKEN:credential,ANSTS_TENANT_CONTROL_CELL:{idFromName(name){assert.equal(name,'tenant-control-v1:a');return {toString:()=>name};},get(id){return {id,async fetch(input){const r=input as Request;assert.equal(new URL(r.url).pathname,'/policy-write');envelope=await r.json() as Record<string,unknown>;return Response.json({enforcement:'NOT_CONNECTED'});}};}}},admin);
 assert.equal(response.status,200);assert.equal(envelope.tenantId,'a');assert.deepEqual(envelope.actor,admin);assert.equal((envelope.input as {region?:unknown}).region,undefined);
});
