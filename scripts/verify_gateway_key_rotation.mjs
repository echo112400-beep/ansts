/** Live verification of development user-key rotation; never prints the captured credential. */
import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, unlinkSync } from 'node:fs';
import { gatewayDevAuthorization } from './gateway_dev_identity.mjs';
const captured = new URL('../.dev-secrets/user-rotation-smoke.jwt', import.meta.url);
if (process.argv[2] === 'prepare') {
  writeFileSync(captured, gatewayDevAuthorization('rotation-test', 'OPERATOR'), { mode: 0o600 });
  console.log('Captured a short-lived pre-rotation test credential (hidden).');
} else if (process.argv[2] === 'verify') {
  const gateway = 'http://127.0.0.1:18080';
  const previous = readFileSync(captured, 'utf8');
  try {
    assert.equal((await fetch(`${gateway}/v1beta1/console/gateway`, { headers: { authorization: previous } })).status, 200);
    const body = JSON.stringify({ taskId: `rotation-${Date.now()}`, tenantId: 'rotation-test', promptText: 'new credential only' });
    assert.equal((await fetch(`${gateway}/v1beta1/tasks/dispatch`, { method: 'POST', headers: { authorization: previous, 'content-type': 'application/json' }, body })).status, 401);
    assert.equal((await fetch(`${gateway}/v1beta1/tasks/dispatch`, { method: 'POST', headers: { authorization: gatewayDevAuthorization('rotation-test', 'OPERATOR'), 'content-type': 'application/json' }, body })).status, 202);
    console.log(JSON.stringify({ previousCredentialRead: 'allowed within grace', previousCredentialWrite: 'rejected', currentCredentialWrite: 'allowed' }));
  } finally { unlinkSync(captured); }
} else throw new Error('usage: prepare or verify');
