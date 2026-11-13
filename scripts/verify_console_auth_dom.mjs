/** DOM-level login/logout/expiry tests. Requires jsdom on NODE_PATH; no real credentials are used. */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import vm from 'node:vm';
const require = createRequire(import.meta.url);
const { JSDOM } = require('jsdom');
const { AnstsGatewayCell } = require('../dist/src/cloudflare/gateway_cell.js');
const { AnstsSchedulerCell } = require('../dist/src/cloudflare/scheduler_cell.js');
const { MockDurableObjectState } = require('../dist/src/cloudflare/env.js');
const { ConsoleRbacAuthGuard } = require('../dist/src/dashboard/console_rbac_guard.js');
const gatewayWorker = require('../dist/src/cloudflare/gateway_worker.js').default;
const html = readFileSync(new URL('../docs/index.html', import.meta.url), 'utf8');
for (const block of html.matchAll(/<script>([\s\S]*?)<\/script>/g)) new vm.Script(block[1]);
assert.equal(html.includes('jwtToken:'), false);
const secret = 'console-dom-test-signing-secret-at-least-32-chars';
const guard = new ConsoleRbacAuthGuard(secret);
const gateway = new AnstsGatewayCell(new MockDurableObjectState('gateway'));
const scheduler = new AnstsSchedulerCell(new MockDurableObjectState('scheduler'));
for (const [tenant, task] of [['a', 'a-visible'], ['b', 'b-private']]) await scheduler.fetch(new Request('https://scheduler/v1/scheduler/submit', { method: 'POST', body: JSON.stringify({ task_id: task, tenant_id: tenant, prompt: task }) }));
const env = { ANSTS_USER_AUTH_SECRET: secret, ANSTS_SCHEDULER_URL: 'https://scheduler', ANSTS_GATEWAY_CELL: { idFromName: name => ({ toString: () => name }), get: () => gateway } };
const originalFetch = globalThis.fetch;
globalThis.fetch = async (input, init) => scheduler.fetch(new Request(input, init));
const dom = new JSDOM(html, { url: 'https://console/', runScripts: 'outside-only' });
const { window } = dom;
await new Promise(resolve => window.document.readyState === 'complete' ? resolve() : window.addEventListener('load', resolve, { once: true }));
window.HTMLDialogElement.prototype.showModal = function () { this.open = true; };
window.HTMLDialogElement.prototype.close = function () { this.open = false; };
window.AbortController = AbortController; window.AbortSignal = AbortSignal;
let delayTasks = false, resolveDelayed, delayedResponse;
const calls = [];
window.fetch = async (path, init) => {
  calls.push(path);
  const response = await gatewayWorker.fetch(new Request(`https://console${path}`, init), env);
  if (delayTasks && path === '/v1/console/tasks') {
    delayedResponse = response;
    await new Promise(resolve => { resolveDelayed = resolve; });
  }
  return response;
};
const eventually = async predicate => {
  for (let i = 0; i < 100; i++) { if (predicate()) return; await new Promise(resolve => setTimeout(resolve, 10)); }
  throw new Error('DOM assertion timed out');
};
try {
  window.eval(readFileSync(new URL('../docs/console-auth.js', import.meta.url), 'utf8'));
  window.eval(readFileSync(new URL('../docs/console-workspace.js', import.meta.url), 'utf8'));
  for (const block of html.matchAll(/<script>([\s\S]*?)<\/script>/g)) window.eval(block[1]);
  const document = window.document;
  assert.equal(document.documentElement.dataset.authState, 'locked');
  assert.equal(calls.length, 0);
  const submitLogin = value => {
    document.querySelector('#console-login-token').value = value;
    document.querySelector('#console-login form').dispatchEvent(new window.Event('submit', { bubbles: true, cancelable: true }));
  };
  submitLogin('invalid');
  await eventually(() => document.querySelector('#console-login-error').textContent.includes('无效'));
  assert.equal(window.AnstsConsoleAuth.isAuthenticated(), false);
  const viewer = guard.issueToken({ sub: 'viewer-a', role: 'VIEWER', scopeTenant: 'a', ttlSec: 900 });
  submitLogin(viewer);
  await eventually(() => document.querySelector('#console-live-rows').textContent.includes('a-visible'));
  assert.equal(document.querySelector('#console-live-rows').textContent.includes('b-private'), false);
  assert.equal(document.querySelector('#console-live-create').hidden, true);
  assert.equal(document.querySelector('#console-login-token').value, '');
  assert.equal(document.documentElement.outerHTML.includes(viewer), false);
  assert.equal(window.localStorage.length, 0); assert.equal(window.sessionStorage.length, 0);
  delayTasks = true;
  document.querySelector('#console-live-refresh').click();
  await eventually(() => !!resolveDelayed);
  document.querySelector('.auth-session-bar button').click();
  resolveDelayed();
  await eventually(() => !window.AnstsConsoleAuth.isAuthenticated());
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(document.querySelector('#console-live-rows').textContent, '');
  assert.equal(document.documentElement.dataset.authState, 'locked');
  assert.equal(window.AnstsConsoleAuth.authorization(), '');
  delayTasks = false;
  const operator = guard.issueToken({ sub: 'operator-a', role: 'OPERATOR', scopeTenant: 'a', ttlSec: 900 });
  submitLogin(operator);
  await eventually(() => window.AnstsConsoleAuth.isAuthenticated());
  assert.equal(document.querySelector('#console-live-tenant').value, 'a');
  assert.equal(document.querySelector('#console-live-tenant').readOnly, true);
  document.querySelector('#console-live-task').value = 'created-from-console';
  document.querySelector('#console-live-prompt').value = 'console test';
  document.querySelector('#console-live-create').dispatchEvent(new window.Event('submit', { bubbles: true, cancelable: true }));
  await eventually(() => document.querySelector('#console-live-submit-status').textContent.includes('已提交'));
  window.Date.now = () => Date.now() + 901000;
  assert.equal(window.AnstsConsoleAuth.isAuthenticated(), false);
  assert.equal(document.querySelector('#console-live-rows').textContent, '');
  console.log(JSON.stringify({ login: 'server verified', tenantRows: 'filtered', viewerWrite: 'hidden and blocked', logout: 'cleared', delayedResponse: 'discarded', expiry: 'locked', credentialPersistence: 'none', operatorSubmit: 'verified' }));
} finally { window.close(); globalThis.fetch = originalFetch; }
