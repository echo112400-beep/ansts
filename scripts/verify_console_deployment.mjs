/** Live HTTP contract checks for the deployed console and identity endpoints. */
import assert from 'node:assert/strict';
import { gatewayDevAuthorization } from './gateway_dev_identity.mjs';
const base = process.env.ANSTS_SMOKE_CONSOLE ?? 'http://127.0.0.1:8088';
const page = await fetch(base + '/');
assert.equal(page.status, 200);
const html = await page.text();
assert.ok(html.includes('/console-auth.js'));
assert.ok(!html.includes('jwtToken:'));
for (const asset of ['/console-auth.js', '/console-auth.css', '/console-workspace.js']) assert.equal((await fetch(base + asset)).status, 200);
assert.equal((await fetch(base + '/v1/auth/me')).status, 401);
for (const role of ['VIEWER', 'OPERATOR']) {
  const authorization = gatewayDevAuthorization('console-check', role);
  const response = await fetch(base + '/v1/auth/me', { headers: { authorization } });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  const result = await response.json();
  assert.equal(result.user.scopeTenant, 'console-check');
  assert.equal(result.user.canWrite, role === 'OPERATOR');
  assert.equal(result.token, undefined);
  const tasks = await (await fetch(base + '/v1/console/tasks', { headers: { authorization } })).json();
  assert.ok(tasks.tasks.every(task => task.tenant_id === 'console-check'));
  assert.equal((await fetch(base + '/v1/console/tasks?tenantId=other', { headers: { authorization } })).status, 403);
}
console.log(JSON.stringify({ staticAssets: 'served', identity: 'server verified', viewer: 'read only', tenantScope: 'enforced' }));
