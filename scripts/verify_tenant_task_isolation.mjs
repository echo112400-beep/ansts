/** Verify same-name tenant tasks through the deployed Gateway, scheduler and Body. */
import assert from 'node:assert/strict';
const gateway = process.env.ANSTS_SMOKE_GATEWAY ?? 'http://127.0.0.1:18080';
const taskId = `shared-name-${Date.now()}`;
const identities = [];
for (const tenantId of ['isolation-a', 'isolation-b']) {
  const submitted = await fetch(`${gateway}/v1beta1/tasks/dispatch`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ taskId, tenantId, promptText: 'tenant isolation smoke' }) });
  assert.equal(submitted.status, 202);
  const body = await submitted.json();
  assert.equal(body.taskId, taskId);
  identities.push(body.executionTaskId);
}
assert.notEqual(identities[0], identities[1]);
assert.equal((await fetch(`${gateway}/v1/tasks/${taskId}`)).status, 400);
for (const [index, tenantId] of ['isolation-a', 'isolation-b'].entries()) {
  let task;
  for (let i = 0; i < 40; i++) {
    const response = await fetch(`${gateway}/v1/tasks/${taskId}?tenantId=${tenantId}`);
    assert.equal(response.status, 200);
    task = await response.json();
    if (task.state === 'COMPLETED') break;
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  assert.equal(task.tenant_id, tenantId);
  assert.equal(task.state, 'COMPLETED');
  assert.equal(task.executionTaskId, identities[index]);
}
console.log(JSON.stringify({ taskId, sameNameTenantTasks: 'independently completed', ambiguousRead: 'rejected' }));
