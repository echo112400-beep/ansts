/** Local development inventory acceptance. Registers observed local hardware once; never grants capacity. */
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {createRequire} from 'node:module';
import {gatewayDevAuthorization} from './gateway_dev_identity.mjs';
const {verifyCapacityReport}=createRequire(import.meta.url)('../dist/src/control/capacity_report.js');
const authorization=gatewayDevAuthorization('all','SRE_ADMIN',undefined,'development-capacity-verifier');
const checkpoint='.dev-secrets/redeployment/capacity-inventory-recovery.json';
async function api(path,body,key,method=body?'POST':'GET',base='http://127.0.0.1:8088'){
 const response=await fetch(base+path,{method,headers:{authorization,'content-type':'application/json',...(key?{'idempotency-key':key}:{})},body:body?JSON.stringify(body):undefined,signal:AbortSignal.timeout(10000),redirect:'error'});assert.equal(response.status,200);return response.json();
}
const dimensions=['cpuMilliCores','memoryMiB','slots'];
const mode=process.argv[2]??'prepare';
if(mode==='prepare'){
 const reports=[];
 for(const [id,port] of [['body-host-01',19501],['body-host-02',19502]]){
  const token=readFileSync('.dev-secrets/'+id+'.token','utf8').trim();
  const response=await fetch('http://127.0.0.1:'+port+'/v1/daemon/capacity-report',{method:'POST',headers:{authorization:'Body '+token,'x-ansts-body-id':id,'content-type':'application/json'},body:'{}',signal:AbortSignal.timeout(5000)});assert.equal(response.status,200);
  const report=verifyCapacityReport((await response.json()).proof,token);assert.equal(report.daemonId,id);reports.push(report);
 }
 assert.equal(reports[0].hostId,reports[1].hostId,'local Daemons must share the mounted physical machine identity');
 const hostId=reports[0].hostId,daemonIds=reports.map(r=>r.daemonId).sort(),limit=Object.fromEntries(dimensions.map(d=>[d,Math.min(...reports.map(r=>r.total[d]))]));
 const before=await api('/v1/admin/capacity/hosts'),existing=before.hosts.find(h=>h.id===hostId),poolId=existing?.poolId??'dev-local';
 if(!before.pools.some(p=>p.id===poolId))await api('/v1/admin/capacity/pools',{id:poolId,name:'本机开发容量池',region:'dev',status:'ACTIVE',limit,expectedVersion:0,reason:'Register observed local capacity ceiling; allocation remains disabled'},'capacity-dev-pool-v1');
 if(!existing)await api('/v1/admin/capacity/hosts',{id:hostId,poolId,daemonIds,status:'ACTIVE',limit,expectedVersion:0,reason:'Register two local Daemons under their common physical identity; no grants'},'capacity-dev-host-'+hostId);
 else assert.deepEqual(existing.daemonIds,daemonIds,'existing physical ownership cannot be silently rewritten');
 let snapshot,host;
 for(let retry=0;retry<100;retry++){snapshot=await api('/v1/admin/capacity/hosts');host=snapshot.hosts.find(h=>h.id===hostId);if(host?.health==='HEALTHY')break;await new Promise(resolve=>setTimeout(resolve,250));}
 assert.equal(host.health,'HEALTHY');assert.equal(snapshot.allocation,'NOT_CONNECTED');
 for(const d of dimensions)assert.ok(host.lastKnownCapacity[d]<=limit[d]);
 const pool=snapshot.pools.find(p=>p.id===poolId);assert.equal(pool.hostCount,1);assert.deepEqual(pool.capacity,host.lastKnownCapacity);
 mkdirSync('.dev-secrets/redeployment',{recursive:true,mode:0o700});writeFileSync(checkpoint,JSON.stringify({hostId,poolId,hostVersion:host.version,poolVersion:pool.version,daemonIds,epochs:Object.fromEntries(host.reports.map(r=>[r.daemonId,r.epoch]))}),{mode:0o600});
 console.log('PASS: real Daemon-signed resource snapshots and common machine identity registered once; both automatic reporters fresh, physical capacity counted once, no grants or task mutations.');
}else if(mode==='verify-recovery'){
 const saved=JSON.parse(readFileSync(checkpoint,'utf8'));let snapshot,host;
 for(let retry=0;retry<100;retry++){snapshot=await api('/v1/admin/capacity/hosts');host=snapshot.hosts.find(h=>h.id===saved.hostId);if(host?.health==='HEALTHY'&&host.reports.every(r=>r.epoch>saved.epochs[r.daemonId]))break;await new Promise(resolve=>setTimeout(resolve,250));}
 assert.equal(host.health,'HEALTHY');assert.equal(host.version,saved.hostVersion);assert.deepEqual(host.daemonIds,saved.daemonIds);assert.ok(host.reports.every(r=>r.epoch>saved.epochs[r.daemonId]));
 assert.equal(snapshot.pools.find(p=>p.id===saved.poolId).version,saved.poolVersion);assert.equal(snapshot.allocation,'NOT_CONNECTED');
 for(const base of ['http://127.0.0.1:18080','http://127.0.0.1:18081'])assert.equal((await api('/v1/admin/capacity/hosts',undefined,undefined,'GET',base)).hosts.find(h=>h.id===saved.hostId).version,saved.hostVersion);
 const viewer=gatewayDevAuthorization('all','VIEWER');assert.equal((await fetch('http://127.0.0.1:8088/v1/admin/capacity/hosts',{headers:{authorization:viewer}})).status,403);
 console.log('PASS: actual Fleet/Daemon restart retained Host/Pool ownership and versions; both signed reporter epochs advanced and are fresh; dual Gateway reads share authority, Viewer inventory access denied, allocation still NOT_CONNECTED.');
}else throw new Error('Use prepare or verify-recovery');
