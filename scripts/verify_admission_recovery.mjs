/** Read-only recovery evidence for the live verifier's owned, disabled shadow tenant. */
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const record=JSON.parse(readFileSync('.dev-secrets/redeployment/admission-shadow-recovery.json','utf8'));
assert.ok(/^m0-verification-[a-z0-9-]+$/.test(record.tenantId));
assert.equal(record.reservationId,record.tenantId+'-shadow');
assert.equal(record.reservation.phase,'UNCERTAIN');
assert.ok(Array.isArray(record.messages),'Run the current live admission verifier before recovery verification');
const token=readFileSync('.dev-secrets/admission-service.key','utf8').trim();
for(const port of [18082,18083,18084]) {
 const response=await fetch(`http://127.0.0.1:${port}/internal/v1/tenant-control/admission-read`,{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify({actor:{sub:'development-directory-verifier',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},tenantId:record.tenantId,reservationId:record.reservationId}),signal:AbortSignal.timeout(20000),redirect:'error'});
 assert.equal(response.status,200);const data=await response.json();
 assert.equal(data.protocolMode,'SHADOW');assert.equal(data.enforcement,'NOT_CONNECTED');
 assert.deepEqual(data.reservation,record.reservation);
 if(record.messages){
  const messages=await fetch(`http://127.0.0.1:${port}/internal/v1/tenant-control/admission-outbox`,{method:'POST',headers:{authorization:'Bearer '+token,'content-type':'application/json'},body:JSON.stringify({actor:{sub:'development-directory-verifier',role:'SRE_ADMIN',scopeTenant:'all',scopeRegion:'all'},tenantId:record.tenantId}),signal:AbortSignal.timeout(20000),redirect:'error'});
  assert.equal(messages.status,200);assert.deepEqual((await messages.json()).messages,record.messages);
 }
}
console.log('PASS: all three live Fleet ingress points retain the exact UNCERTAIN shadow reservation and recorded pending outbox; no mutations or execution.');
