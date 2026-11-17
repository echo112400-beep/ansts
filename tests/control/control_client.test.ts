import {test} from 'node:test';
import assert from 'node:assert/strict';
import {controlClient,ControlApiError} from '../../ui/console/lib/control.js';
test('control client preserves mutation data on conflict and does not retry',async()=>{
  let calls=0;
  const input={region:'dev',name:'Team',costCenter:'eng',status:'ACTIVE' as const,expectedVersion:1,reason:'review'};
  const before=JSON.stringify(input);
  const client=controlClient(async(path,options)=>{
    calls++;assert.equal(path,'/v1/admin/tenants/a%2Fb');
    assert.equal(options?.credentials,'same-origin');
    assert.equal(options?.body,before);
    assert.equal(new Headers(options?.headers).get('idempotency-key'),'stable-key');
    return Response.json({code:'VERSION_CONFLICT'},{status:409});
  });
  await assert.rejects(client.saveTenant('a/b',input,'stable-key'),(e:unknown)=>e instanceof ControlApiError&&e.conflict);
  assert.equal(JSON.stringify(input),before);assert.equal(calls,1);
});
test('control client surfaces unavailable backend and safely encodes cursor',async()=>{
  const client=controlClient(async(path)=>{assert.equal(path,'/v1/admin/tenants?cursor=a%2Bb&limit=10');return Response.json({code:'DIRECTORY_UNAVAILABLE'},{status:503});});
  await assert.rejects(client.tenants({cursor:'a+b',limit:10}),(e:unknown)=>e instanceof ControlApiError&&e.unavailable);
});
test('control client preserves abort and rejects malformed response',async()=>{
  await assert.rejects(controlClient(async()=>{throw new DOMException('cancelled','AbortError');}).tenants(),{name:'AbortError'});
  await assert.rejects(controlClient(async()=>new Response('bad gateway',{status:502})).tenants(),{code:'INVALID_API_RESPONSE'});
});
test('control client creates tenants with an explicit idempotency key',async()=>{
  const client=controlClient(async(path,options)=>{
    assert.equal(path,'/v1/admin/tenants');assert.equal(options?.method,'POST');
    assert.equal(new Headers(options?.headers).get('idempotency-key'),'create-key');
    const body=JSON.parse(String(options?.body));assert.equal(body.id,'team');assert.equal(body.name,'Team');
    return Response.json({...body,version:1});
  });
  assert.equal((await client.createTenant('team',{region:'dev',name:'Team',costCenter:'eng',status:'ACTIVE',reason:'register'},'create-key')).version,1);
});
test('management client retains operation receipt from successful response headers',async()=>{
 const id='tenant:'+ 'a'.repeat(64);
 const client=controlClient(async()=>Response.json({id:'team',version:1},{headers:{'x-ansts-operation-id':id}}));
 const saved=await client.createTenant('team',{region:'dev',name:'Team',costCenter:'eng',status:'ACTIVE',reason:'register'},'key');
 assert.equal(saved.operationId,id);
});
test('archive download verification rejects changed bytes, identity and format',async()=>{
 const {verifyDirectoryArchive}=await import('../../ui/console/lib/control.js');
 const {createHash}=await import('node:crypto');
 const snapshot={schemaVersion:1,audit:[],name:'中文😀',receipts:{a:'x'}};
 const payload=JSON.stringify(snapshot),id='a'.repeat(64);
 const manifest={id,schemaVersion:1 as const,sourceVersion:0,digest:createHash('sha256').update(payload).digest('hex'),bytes:Buffer.byteLength(payload),actor:'github:1',reason:'test',requestId:'test',createdAt:'2026-11-15T00:00:00Z',tenantAuditWatermark:0,costCenterAuditWatermark:0,receiptRetention:'ALL_RETAINED' as const,safeReplayWatermark:null};
 assert.equal(await verifyDirectoryArchive({manifest,snapshot},id),true);
 assert.equal(await verifyDirectoryArchive({manifest,snapshot:{...snapshot,receipts:{a:'y'}}},id),false);
 assert.equal(await verifyDirectoryArchive({manifest,snapshot},'b'.repeat(64)),false);
 assert.equal(await verifyDirectoryArchive({manifest,snapshot:{...snapshot,schemaVersion:2}},id),false);
});
