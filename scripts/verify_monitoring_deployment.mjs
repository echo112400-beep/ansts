/** Live monitoring checks. Does not execute tasks or change drain state. */
import assert from 'node:assert/strict';
import {readFileSync,mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {execFileSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
const token=readFileSync(new URL('../.dev-secrets/monitoring.key',import.meta.url),'utf8').trim();
const endpoints=[['gateway-01','http://127.0.0.1:18080','ansts_gateway_requests_total'],['gateway-02','http://127.0.0.1:18081','ansts_gateway_requests_total'],['body-01','http://127.0.0.1:19501','ansts_body_slots_total'],['body-02','http://127.0.0.1:19502','ansts_body_slots_total']];
for(const [name,base,metric] of endpoints) {
 assert.equal((await fetch(base+'/metrics')).status,401,name);
 const response=await fetch(base+'/metrics',{headers:{authorization:'Bearer '+token}});assert.equal(response.status,200,name);
 assert.match(response.headers.get('content-type'),/text\/plain/);
 const body=await response.text();assert.ok(body.includes(metric),name);assert.ok(!body.includes(token));assert.ok(!body.includes('tenant_id='));assert.ok(!body.includes('task_id='));
 if(name.startsWith('body')) {
  const before=await(await fetch(base+'/health')).json();
  const denied=await fetch(base+'/v1/daemon/drain',{method:'POST',headers:{authorization:'Body '+token,'x-ansts-body-id':name==='body-01'?'body-host-01':'body-host-02','content-type':'application/json'},body:JSON.stringify({draining:!before.draining})});
  assert.equal(denied.status,401);assert.equal((await(await fetch(base+'/health')).json()).draining,before.draining);
  assert.match(body,/ansts_body_admission_decisions_total\{reason="accepted"}/);
  assert.match(body,/ansts_body_result_outbox_pending/);assert.match(body,/ansts_body_oldest_resource_reservation_seconds/);
 }else{
  assert.equal((await fetch(base+'/v1/tasks',{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:'{}'})).status,401);
 }
}
const deadline=Date.now()+45000;let targets=[];
while(Date.now()<deadline) {
 const result=await(await fetch('http://127.0.0.1:9090/api/v1/targets')).json();
 targets=result.data.activeTargets.filter(target=>['ansts-gateways','ansts-body-daemons'].includes(target.labels.job));
 if(targets.length===4&&targets.every(target=>target.health==='up'))break;
 await new Promise(resolve=>setTimeout(resolve,1000));
}
assert.equal(targets.length,4);assert.ok(targets.every(target=>target.health==='up'),JSON.stringify(targets.map(t=>({instance:t.labels.instance,health:t.health,error:t.lastError}))));
const measured=await(await fetch('http://127.0.0.1:9090/api/v1/query?query='+encodeURIComponent('sum(ansts_body_admission_decisions_total)'))).json();
assert.equal(measured.status,'success');assert.equal(measured.data.result.length,1);assert.ok(Number.isFinite(Number(measured.data.result[0].value[1])));
const scratch=mkdtempSync(join(tmpdir(),'ansts-publication-monitor-'));
try {
 const root=resolve(import.meta.dirname,'..'),bundle=join(scratch,'gate.mjs');
 execFileSync(join(root,'node_modules/.bin/esbuild'),[join(root,'src/control/publication_observability.ts'),'--bundle','--platform=node','--format=esm','--outfile='+bundle],{stdio:'pipe'});
 const {collectPublicationObservability,assertPublicationObservability}=await import(pathToFileURL(bundle).href);
 const ids=targets.map(target=>target.labels.job+'/'+target.labels.instance);
 const evidence=await collectPublicationObservability('http://127.0.0.1:9090',ids);
 assertPublicationObservability(evidence,ids);
 assert.throws(()=>assertPublicationObservability({...evidence,targets:evidence.targets.slice(1)},ids),{code:'PUBLICATION_MONITORING_REQUIRED'});
} finally {rmSync(scratch,{recursive:true,force:true});}
console.log('PASS: all 4 Prometheus targets up; private read-only credentials, finite-cardinality metrics, outbox/reservation gauges and business-action denial; live publication evidence requires every fresh target.');
