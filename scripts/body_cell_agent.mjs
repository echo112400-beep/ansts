/** Body execution agent: celld owns registration, placement and completion. */
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
const require = createRequire(import.meta.url);
const { NodeDaemon } = require('../dist/src/node_daemon.js');
const { executionDigest } = require('../dist/src/cloudflare/execution_attempt.js');
const nodeId = process.env.ANSTS_NODE_ID;
const daemon = new NodeDaemon(nodeId);
const base = process.env.ANSTS_BODY_CELL_URL;
const local = `http://127.0.0.1:${process.env.ANSTS_LISTEN_PORT}`;
const bodyToken = readFileSync(process.env.ANSTS_BODY_TOKEN_FILE, 'utf8').trim();
const serviceHeaders = { 'x-ansts-body-id': nodeId, authorization: `Body ${bodyToken}` };
async function rpc(path, body) {
  const response = await fetch(base + path, { method: body ? 'POST' : 'GET', headers: { 'content-type': 'application/json', ...serviceHeaders }, body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(10000) });
  if (!response.ok) throw new Error(`${path}: ${response.status}`);
  return response.json();
}
let stopping = false;
process.on('SIGTERM', () => { stopping = true; });
while (!stopping) {
  try {
    const hb = daemon.emitHeartbeat(0);
    await rpc(`/v1/body/heartbeat?hostId=${encodeURIComponent(nodeId)}`, hb);
    await rpc('/v1/scheduler/register-node', hb);
    await rpc('/v1/scheduler/schedule-tick', {});
    const { tasks } = await rpc('/v1/scheduler/tasks');
    for (const task of tasks) {
      if (task.assigned_node_id !== nodeId || task.state !== 'LEASED') continue;
      try {
      const lease = { task_id: task.task_id, tenant_id: task.tenant_id, attempt_id: `${task.task_id}:${task.lease_fencing_token}`, input_digest: executionDigest({ tenant_id: task.tenant_id, task_id: task.task_id, prompt: task.prompt }), node_id: nodeId, fencing_token: task.lease_fencing_token };
      await rpc('/v1/scheduler/lease-renew', lease);
      const controller = new AbortController();
      const executionPath = `/v1/body/acquire?taskId=${encodeURIComponent(task.task_id)}`;
      const admission = await rpc(executionPath, lease);
      if (!admission.acquired && admission.result) {
        await rpc('/v1/scheduler/complete', lease);
        if (admission.execution.node_id === nodeId && admission.execution.attempt_id === lease.attempt_id) {
          await rpc(`/v1/body/acknowledge?taskId=${encodeURIComponent(task.task_id)}`, { ...lease, expected_version: admission.execution.version });
        }
        continue;
      }
      const started = await rpc(`/v1/body/start?taskId=${encodeURIComponent(task.task_id)}`, { ...lease, expected_version: admission.execution.version });
      let renewing = false;
      const renewal = setInterval(async () => {
        if (renewing) return;
        renewing = true;
        try {
          await rpc('/v1/scheduler/lease-renew', lease);
          const activeHb = daemon.emitHeartbeat(1);
          await rpc('/v1/scheduler/register-node', activeHb);
          await rpc(`/v1/body/heartbeat?hostId=${encodeURIComponent(nodeId)}`, activeHb);
        } catch (error) { controller.abort(error); }
        finally { renewing = false; }
      }, 3000);
      let response;
      try {
        response = await fetch(local + '/internal/v1beta1/body/execute', { method: 'POST', headers: { 'content-type': 'application/json', ...serviceHeaders }, body: JSON.stringify({ taskId: task.task_id, tenantId: task.tenant_id, promptText: task.prompt, attemptId: lease.attempt_id, fencingToken: lease.fencing_token }), signal: AbortSignal.any([controller.signal, AbortSignal.timeout(Math.min(2147483647, Math.max(10000, Number(task.timeout_seconds ?? 1800) * 1000)))]) });
        if (!response.ok) throw new Error(`execute: ${response.status}`);
        response = await response.json();
      } catch (error) {
        await rpc(`/v1/body/uncertain?taskId=${encodeURIComponent(task.task_id)}`, { ...lease, expected_version: started.execution.version, reason: String(error) }).catch(() => {});
        throw error;
      } finally { clearInterval(renewal); }
      const result = response;
      const finished = await rpc(`/v1/body/finish?taskId=${encodeURIComponent(task.task_id)}`, { ...lease, expected_version: started.execution.version, result });
      await rpc(`/v1/body/receipt?hostId=${encodeURIComponent(nodeId)}`, { ...lease, result });
      await rpc('/v1/scheduler/complete', lease);
      await rpc(`/v1/body/acknowledge?taskId=${encodeURIComponent(task.task_id)}`, { ...lease, expected_version: finished.execution.version });
      console.log(JSON.stringify({ event: 'body-completed', ...lease }));
      } catch (error) {
        console.error(JSON.stringify({ event: 'body-task-blocked', taskId: task.task_id, error: String(error) }));
      }
    }
  } catch (error) { console.error(String(error)); }
  await new Promise(resolve => setTimeout(resolve, 2000));
}
