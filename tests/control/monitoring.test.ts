import {test} from 'node:test';
import assert from 'node:assert/strict';
import {monitoringAuthorized,gaugeMetrics,gatewayMetrics} from '../../src/control/monitoring.js';
import gateway from '../../src/cloudflare/gateway_worker.js';
const token='a'.repeat(64);
test('monitor credentials only expose metrics, not user management routes',async()=>{
 assert.equal(monitoringAuthorized('Bearer '+token,undefined),false);
 assert.equal(monitoringAuthorized('Bearer '+token,token),true);
 assert.equal(monitoringAuthorized('Body '+token,token),false);
 assert.equal((await gateway.fetch(new Request('https://gateway/metrics'),{ANSTS_MONITORING_TOKEN:token})).status,401);
 const response=await gateway.fetch(new Request('https://gateway/metrics',{headers:{authorization:'Bearer '+token}}),{ANSTS_MONITORING_TOKEN:token});
 assert.equal(response.status,200);assert.match(await response.text(),/ansts_gateway_requests_total/);
 assert.equal((await gateway.fetch(new Request('https://gateway/v1/admin/tenants',{headers:{authorization:'Bearer '+token}}),{ANSTS_MONITORING_TOKEN:token})).status,401);
 assert.ok(!gatewayMetrics().includes('tenant_id'));assert.ok(!gatewayMetrics().includes(token));
});
test('metric formatting rejects injected labels and non-finite values',()=>{
 assert.match(gaugeMetrics({ansts_body_slots_total:1}),/ansts_body_slots_total 1/);
 assert.throws(()=>gaugeMetrics({'bad\nmetric':1}));assert.throws(()=>gaugeMetrics({count:NaN}));
});
test('admission decisions use a fixed reason vocabulary, not error strings as labels',async()=>{
 const {AdmissionMetrics,admissionRejection}=await import('../../src/control/monitoring.js');
 const metrics=new AdmissionMetrics();metrics.record('accepted');metrics.record(admissionRejection(429,'BODY_RESOURCE_EXHAUSTED'));metrics.record(admissionRejection(503,'private-task-id-123'));
 assert.match(metrics.render(),/reason="accepted"} 1/);assert.match(metrics.render(),/reason="capacity_exhausted"} 1/);assert.match(metrics.render(),/reason="other_rejection"} 1/);assert.ok(!metrics.render().includes('private-task'));
});
