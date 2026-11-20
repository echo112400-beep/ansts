import {RegionPlacementSimulation} from '../../src/cluster/region_placement_simulation.js';
import type {RegionRecord} from '../../src/cluster/region_catalog.js';
import assert from 'node:assert/strict';
import {test} from 'node:test';
import {RegionProbeRegistry,type RegionProbeConfig} from '../../src/cluster/region_probe.js';
import {SimulatedRegionAdapter} from '../../src/cluster/region_adapter.js';

test('probe accepts only registered references and production disallows loopback configuration',async()=>{
 const loopback=new Map<string,RegionProbeConfig>([['local',{kind:'development-loopback',port:8080}]]);
 assert.throws(()=>new RegionProbeRegistry('production',loopback),{code:'INVALID_ADAPTER_CONFIG'});
 let calls=0;
 const registry=new RegionProbeRegistry('production',new Map<string,RegionProbeConfig>([
  ['primary',{kind:'service-binding',binding:{async fetch(request){
   calls++;
   assert.equal(request.url,'https://region.internal/readyz');
   assert.equal(request.redirect,'error');
   assert.equal(request.headers.get('authorization'),'Bearer fixture');
   return new Response(null,{status:200});
  }}}],
 ]));
 await assert.rejects(registry.probe('unknown','fixture'),{code:'REGION_ADAPTER_NOT_CONFIGURED'});
 assert.equal(calls,0);
 assert.equal((await registry.probe('primary','fixture')).healthy,true);
 assert.equal(calls,1);
});

test('simulation supports capability checks, credential rotation and idempotent drain',()=>{
 const adapter=new SimulatedRegionAdapter('old',['model'],['internal'],5);
 assert.deepEqual(adapter.capabilities(),{models:['model'],dataTypes:['internal']});
 const request={id:'task',model:'model',dataType:'internal'};
 assert.throws(()=>adapter.submit('old',{...request,dataType:'unknown'}),{code:'REGION_CAPABILITY_MISMATCH'});
 const result=adapter.submit('old',request);
 adapter.rotateIdentity('old','new');
 assert.throws(()=>adapter.probe('old'),{code:'REGION_IDENTITY_REJECTED'});
 assert.deepEqual(adapter.probe('new'),{healthy:true,rttMs:5});
 adapter.drain('new');
 assert.equal(adapter.probe('new').healthy,false);
 assert.deepEqual(adapter.submit('new',request),result);
 assert.throws(()=>adapter.submit('new',{...request,id:'next'}),{code:'REGION_DRAINING'});
});

test('placement simulation drains only new work and preserves accepted task bindings',()=>{
 const now=100;
 const regions:RegionRecord[]=['fast','slow'].map(id=>({
  id,jurisdiction:'DE',failureDomain:id,identityRef:id,adapterRef:id,models:['model'],
  dataTypes:['internal'],environment:'production',evidence:{ref:'fixture',verifiedAt:1,expiresAt:200},version:1,
 }));
 const first=new SimulatedRegionAdapter('first',['model'],['internal'],10);
 const second=new SimulatedRegionAdapter('second',['model'],['internal'],25);
 const simulation=new RegionPlacementSimulation(new Map([
  ['fast',{adapter:first,identity:'first'}],['slow',{adapter:second,identity:'second'}],
 ]));
 const policy={jurisdictions:['DE'],model:'model',dataType:'internal'};
 assert.equal(simulation.submit('existing',regions,policy,now).regionId,'fast');
 simulation.drain('fast');
 assert.deepEqual(simulation.submit('existing',regions,policy,now),{mode:'SIMULATION',regionId:'fast',replayed:true});
 assert.equal(simulation.submit('new',regions,policy,now).regionId,'slow');
 simulation.drain('slow');
 const denied=simulation.submit('blocked',regions,policy,now);
 assert.equal(denied.regionId,null);
 assert.deepEqual(denied.decision?.candidates.map(entry=>entry.reasons),[['UNHEALTHY'],['UNHEALTHY']]);
 assert.throws(()=>simulation.submit('existing',regions,{...policy,model:'other'},now),{code:'IDEMPOTENCY_CONFLICT'});
});
