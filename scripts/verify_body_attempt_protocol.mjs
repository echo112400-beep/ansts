/** Verify the deployed Body attempt protocol without printing credentials. */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { gatewayDevAuthorization } from './gateway_dev_identity.mjs';
const gateway = process.env.ANSTS_SMOKE_GATEWAY ?? 'http://127.0.0.1:18080';
const brain = process.env.ANSTS_SMOKE_BRAIN ?? 'http://127.0.0.1:18082';
assert.equal((await fetch(`${brain}/v1/body/state`)).status, 401);
assert.equal((await fetch(`${process.env.ANSTS_SMOKE_DAEMON ?? 'http://127.0.0.1:19501'}/internal/v1beta1/body/execute`, {
  method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ taskId: 'anonymous-must-not-run', fencingToken: 1 }),
})).status, 401);
const taskId = `attempt-protocol-${Date.now()}`;
const response = await fetch(`${gateway}/v1beta1/tasks/dispatch`, {
  method: 'POST', headers: { 'content-type': 'application/json', authorization: gatewayDevAuthorization() },
  body: JSON.stringify({ tenantId: 'protocol-smoke', taskId, promptText: 'verify versioned execution attempt' }),
});
assert.equal(response.status, 202);
let task;
for (let i = 0; i < 30; i++) {
  task = await (await fetch(`${gateway}/v1/tasks/${taskId}`, { headers: { authorization: gatewayDevAuthorization() } })).json();
  if (task.state === 'COMPLETED') break;
  await new Promise(resolve => setTimeout(resolve, 1000));
}
assert.equal(task.state, 'COMPLETED');
const host = task.assigned_node_id;
const token = readFileSync(new URL(`../.dev-secrets/${host}.token`, import.meta.url), 'utf8').trim();
const result = await fetch(`${brain}/v1/body/state?taskId=${encodeURIComponent(task.executionTaskId ?? taskId)}`, {
  headers: { authorization: `Body ${token}`, 'x-ansts-body-id': host },
});
assert.equal(result.status, 200);
const { execution } = await result.json();
assert.equal(execution.phase, 'COMPLETED');
assert.equal(execution.version, 4);
assert.equal(execution.tenant_id, 'protocol-smoke');
assert.equal(task.gatewayLease.status, 'RELEASED');
console.log(JSON.stringify({ taskId, host, phase: execution.phase, version: execution.version, anonymousRequests: 'rejected' }));
