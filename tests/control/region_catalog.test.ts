import assert from 'node:assert/strict';
import {test} from 'node:test';
import {createHmac} from 'node:crypto';
import {handleUnifiedGatewayRequest} from '../../src/cloudflare/gateway_cell.js';
import {createRequire} from 'node:module';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {RegionCatalogCell} from '../../src/cloudflare/region_catalog_cell.js';
import type {DurableObjectStateLike} from '../../src/cloudflare/env.js';

const {DatabaseSync}=createRequire(join(process.cwd(),'package.json'))('node:sqlite');

test('region authority preserves catalog and trust boundaries after reopening SQLite',async()=>{
 const dir=mkdtempSync(join(tmpdir(),'ansts-regions-')),path=join(dir,'catalog.sqlite');
 let db=new DatabaseSync(path);
 const storage={
  sql:{exec<T>(query:string,...args:unknown[]){const rows=db.prepare(query).all(...args) as T[];return {toArray:()=>rows};}},
  transactionSync<T>(fn:()=>T){db.exec('BEGIN IMMEDIATE');try{const result=fn();db.exec('COMMIT');return result;}catch(error){db.exec('ROLLBACK');throw error;}},
 };
 const token='a'.repeat(64);
 let probeCalls=0,probeHealthy=true;
 const open=(profile?:string,trial?:string)=>new RegionCatalogCell({storage} as unknown as DurableObjectStateLike,{
  ANSTS_RUNTIME_PROFILE:profile,ANSTS_REGION_PLACEMENT_TRIAL:trial,
  ANSTS_DIRECTORY_SERVICE_TOKEN:token,ANSTS_REGION_PROBE_IDENTITY_REF:'issuer-a',ANSTS_REGION_PROBE_TOKEN:'fixture',
  ANSTS_REGION_PROBE_SERVICE:{async fetch(request){
   probeCalls++;
   assert.equal(request.url,'https://region.internal/readyz');
   assert.equal(request.headers.get('authorization'),'Bearer fixture');
   return new Response(null,{status:probeHealthy?200:503});
  }},
 });
 let cell=open();
 const actor={sub:'fixture',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'};
 const call=(action:string,input:object,authorized=true)=>cell.fetch(new Request('https://region.test/'+action,{
  method:'POST',headers:authorized?{authorization:'Bearer '+token}:{},body:JSON.stringify(input),
 }));
 const region={id:'region-dev',jurisdiction:'DE',failureDomain:'site-a',identityRef:'issuer-a',adapterRef:'adapter-a',
  models:['model-a'],dataTypes:['internal'],environment:'development',evidence:{ref:'probe-a',verifiedAt:1,expiresAt:2}};
 try{
  assert.equal((await call('list',{actor},false)).status,401);
  const key='b'.repeat(64);
  const encode=(value:unknown)=>Buffer.from(JSON.stringify(value)).toString('base64url');
  const session=(role:string)=>{
   const unsigned=encode({alg:'HS256',typ:'JWT'})+'.'+encode({...actor,role,iat:Math.floor(Date.now()/1000),exp:Math.floor(Date.now()/1000)+60});
   return 'Bearer '+unsigned+'.'+createHmac('sha256',key).update(unsigned).digest('base64url');
  };
  const gateway=(method:string,role?:string,input?:object,suffix='')=>handleUnifiedGatewayRequest(new Request(
   'https://gateway.test/v1/admin/regions'+suffix,{method,headers:role?{authorization:session(role)}:{},
   body:input?JSON.stringify(input):undefined}),{
   ANSTS_USER_AUTH_SECRET:key,ANSTS_DIRECTORY_SERVICE_TOKEN:token,
   ANSTS_REGION_CATALOG_CELL:{idFromName:name=>({toString:()=>name}),get:id=>({id,fetch:request=>cell.fetch(new Request(request))})},
  });
  assert.equal((await gateway('GET'))?.status,401);
  assert.equal((await gateway('GET','VIEWER'))?.status,200);
  assert.equal((await gateway('PUT','VIEWER',{actor,input:region,expectedVersion:0}))?.status,403);

  assert.deepEqual(await (await call('list',{actor})).json(),{regions:[]});
  assert.equal((await call('write',{actor:{...actor,role:'VIEWER'},input:region,expectedVersion:0})).status,403);
  const first=await gateway('PUT','SRE_ADMIN',{input:region,expectedVersion:0});
  assert.ok(first);
  assert.equal(first.status,200);
  assert.deepEqual(await first.json(),{region:{...region,version:1,trust:'DEVELOPMENT_ONLY'}});
  assert.equal((await call('write',{actor,input:region,expectedVersion:0})).status,409);
  assert.equal((await call('write',{actor,input:{...region,id:'prod',environment:'production'},expectedVersion:0})).status,200);
  db.close();db=new DatabaseSync(path);cell=open();
  const result=await call('list',{actor:{...actor,role:'VIEWER'}});
  assert.deepEqual(await result.json(),{regions:[
   {...region,id:'prod',environment:'production',version:1,trust:'EXPIRED'},
   {...region,version:1,trust:'DEVELOPMENT_ONLY'},
  ]});
  assert.equal((await call('list',{actor:{...actor,scopeRegion:'prod'}})).status,403);
  const now=Date.now();
  const evidence={ref:'fixture',verifiedAt:now-10,expiresAt:now+60000};
  for(const [id,jurisdiction] of [['slow','DE'],['fast','DE'],['foreign','US']]){
   assert.equal((await gateway('PUT','SRE_ADMIN',{input:{...region,id,jurisdiction,evidence,environment:'production'},expectedVersion:0}))?.status,200);
  }
  assert.equal((await gateway('PUT','SRE_ADMIN',{input:{...region,id:'probe-target',adapterRef:'service-probe'},expectedVersion:0}))?.status,200);
  const probe=(role:string,input:object)=>gateway('POST',role,{input},'/probe');
  assert.equal((await probe('OPERATOR',{regionId:'probe-target'}))?.status,403);
  assert.equal((await probe('SRE_ADMIN',{regionId:'probe-target',extra:true}))?.status,422);
  assert.equal((await probe('SRE_ADMIN',{regionId:'missing'}))?.status,404);
  assert.equal((await probe('SRE_ADMIN',{regionId:'prod'}))?.status,503);
  assert.equal(probeCalls,0);
  for(const healthy of [true,false]){
   probeHealthy=healthy;
   const response=await probe('SRE_ADMIN',{regionId:'probe-target'});
   assert.equal(response?.status,200);
   assert.equal((await response!.json()).healthy,healthy);
  }
  assert.equal(probeCalls,2);
  const before=await (await call('list',{actor})).json();
  const simulation=await gateway('POST','OPERATOR',{input:{jurisdictions:['DE'],model:'model-a',dataType:'internal',
   health:[['slow',100],['fast',20],['foreign',1],['prod',0]].map(([regionId,rttMs])=>({regionId,rttMs,healthy:true,expiresAt:now+60000})),
  }},'/simulate');
  assert.equal(simulation?.status,200);
  const decision=await simulation!.json();
  assert.equal(decision.healthSource,'HYPOTHETICAL');
  assert.deepEqual(decision.ranked,['fast','slow']);
  assert.deepEqual(decision.candidates.find((entry:{regionId:string})=>entry.regionId==='foreign').reasons,['JURISDICTION_NOT_ALLOWED']);
  assert.deepEqual(decision.candidates.find((entry:{regionId:string})=>entry.regionId==='prod').reasons,['EXPIRED']);
  assert.deepEqual(await (await call('list',{actor})).json(),before);
  const task={action:'submit',id:'original',model:'fixture-model',dataType:'fixture-data',jurisdiction:'fixture'};
  const trial=(input:object,role='SRE_ADMIN')=>gateway('POST',role,{input},'/placement-trial');
  assert.equal((await trial(task))?.status,409);
  cell=open('production','true');
  assert.equal((await trial(task))?.status,409);
  cell=open('development','true');
  assert.equal((await trial(task,'OPERATOR'))?.status,403);
  assert.equal((await (await trial(task))!.json()).regionId,'trial-a');
  assert.equal((await trial({action:'drain',id:'trial-a'}))?.status,200);
  assert.deepEqual(await (await trial(task))!.json(),{mode:'SIMULATION',regionId:'trial-a',replayed:true});
  assert.equal((await (await trial({...task,id:'next'}))!.json()).regionId,'trial-b');
  assert.equal((await trial({action:'drain',id:'trial-b'}))?.status,200);
  assert.equal((await (await trial({...task,id:'blocked'}))!.json()).regionId,null);
  assert.deepEqual(await (await call('list',{actor})).json(),before);

 }finally{db.close();rmSync(dir,{recursive:true,force:true});}
});
