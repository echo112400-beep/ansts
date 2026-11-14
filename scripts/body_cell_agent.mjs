/** Body execution agent: celld owns registration, placement and completion. */
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import os from 'node:os';
const require = createRequire(import.meta.url);
const { NodeDaemon } = require('../dist/src/node_daemon.js');
const { taskInputDigest } = require('../dist/src/cloudflare/execution_attempt.js');
const { BodyResultOutbox } = require('../dist/src/runtime/body_result_outbox.js');
const nodeId = process.env.ANSTS_NODE_ID;
const { bodyResourceBudget, cgroupResourceLimits } = require('../dist/src/runtime/body_resource_budget.js');
const constrained = process.constrainedMemory?.();
const cgroup = cgroupResourceLimits(file => { try { return readFileSync(file, "utf8"); } catch (error) { if (["ENOENT", "ENOTDIR"].includes(error.code)) return undefined; throw error; } });
const memoryLimit = Math.min(os.totalmem(), constrained > 0 ? constrained : Infinity, cgroup.memoryBytes ?? Infinity) / 1048576;
const budget = bodyResourceBudget(Math.min(os.availableParallelism(), cgroup.cpu ?? Infinity), memoryLimit, process.env.ANSTS_BODY_CPU_CORES, process.env.ANSTS_BODY_MEMORY_MB);
const daemon = new NodeDaemon(nodeId, budget.cpu, budget.memoryMb, 0);
const executionSlots = Number(process.env.ANSTS_BODY_EXECUTION_SLOTS ?? 1);
if (!Number.isSafeInteger(executionSlots) || executionSlots !== 1) throw new Error("BODY_AGENT_REQUIRES_SINGLE_SLOT");
const heartbeat = async running => {
  const response = await fetch(local + '/health', { signal: AbortSignal.timeout(5000) });
  if (!response.ok) throw new Error('BODY_HEALTH_UNAVAILABLE');
  const health = await response.json();
  return { ...daemon.emitHeartbeat(running), execution_slots: executionSlots, warm_pool_ready: 0, healthy: !health.recoveryRequired };
};
const base = process.env.ANSTS_BODY_CELL_URL;
const local = `http://127.0.0.1:${process.env.ANSTS_LISTEN_PORT}`;
const bodyToken = readFileSync(process.env.ANSTS_BODY_TOKEN_FILE, 'utf8').trim();
const serviceHeaders = { 'x-ansts-body-id': nodeId, authorization: `Body ${bodyToken}` };
const outbox = new BodyResultOutbox(path.join(process.env.ANSTS_STORAGE_DIR, 'result-outbox'));
async function deliverResult(delivery) {
  const { lease, result, expected_version } = delivery;
  const finished = await rpc(`/v1/body/finish?taskId=${encodeURIComponent(lease.task_id)}`, { ...lease, expected_version, result });
  await rpc(`/v1/body/receipt?hostId=${encodeURIComponent(nodeId)}`, { ...lease, result });
  const { tasks } = await rpc('/v1/scheduler/tasks');
  const task = tasks.find(task => task.task_id === lease.task_id && task.tenant_id === lease.tenant_id);
  if (task?.state !== 'COMPLETED') await rpc('/v1/scheduler/complete', lease);
  await rpc(`/v1/body/acknowledge?taskId=${encodeURIComponent(lease.task_id)}`, { ...lease, expected_version: finished.execution.version });
  outbox.remove(lease);
}
async function flushResults() {
  for (const delivery of outbox.pending()) {
    if (delivery.next_retry_at > Date.now()) continue;
    try { await deliverResult(delivery); }
    catch (error) { outbox.defer(delivery); console.error(JSON.stringify({ event: 'body-result-deferred', taskId: delivery.lease.task_id, attempts: delivery.attempts, error: String(error) })); }
  }
}
async function rpc(path, body) {
  const response = await fetch(base + path, { method: body ? 'POST' : 'GET', headers: { 'content-type': 'application/json', ...serviceHeaders }, body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(10000) });
  if (!response.ok) {
    const error = new Error(`${path}: ${response.status}`);
    error.payload = await response.json().catch(() => ({}));
    throw error;
  }
  return response.json();
}
let stopping = false;
process.on('SIGTERM', () => { stopping = true; });
while (!stopping) {
  try {
    await flushResults();
    const hb = await heartbeat(0);
    await rpc(`/v1/body/heartbeat?hostId=${encodeURIComponent(nodeId)}`, hb);
    await rpc('/v1/scheduler/register-node', hb);
    if (!hb.healthy) { await new Promise(resolve => setTimeout(resolve, 2000)); continue; }
    await rpc('/v1/scheduler/schedule-tick', {});
    const { tasks } = await rpc('/v1/scheduler/tasks');
    for (const task of tasks) {
      if (task.assigned_node_id !== nodeId || task.state !== 'LEASED') continue;
      if (outbox.pending().some(delivery => delivery.lease.task_id === task.task_id && delivery.lease.tenant_id === task.tenant_id)) continue;
      try {
      const lease = { task_id: task.task_id, tenant_id: task.tenant_id, attempt_id: `${task.task_id}:${task.lease_fencing_token}`, input_digest: taskInputDigest(task.tenant_id, task.task_id, task.prompt, task.execution), node_id: nodeId, fencing_token: task.lease_fencing_token };
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
          const activeHb = await heartbeat(1);
          await rpc('/v1/scheduler/register-node', activeHb);
          await rpc(`/v1/body/heartbeat?hostId=${encodeURIComponent(nodeId)}`, activeHb);
        } catch (error) { controller.abort(error); }
        finally { renewing = false; }
      }, 3000);
      let response;
      try {
        response = await fetch(local + '/v1/sandboxes/execute', { method: 'POST', headers: { 'content-type': 'application/json', ...serviceHeaders }, body: JSON.stringify({ taskId: task.task_id, tenantId: task.tenant_id, promptText: task.prompt, execution: task.execution, attemptId: lease.attempt_id, fencingToken: lease.fencing_token, timeoutSeconds: task.timeout_seconds, executionToken: started.execution_token }), signal: AbortSignal.any([controller.signal, AbortSignal.timeout(Math.min(2147483647, Math.max(10000, Number(task.timeout_seconds ?? 1800) * 1000)))]) });
        if (!response.ok) throw new Error(`execute: ${response.status}`);
        response = await response.json();
        outbox.put({ lease, expected_version: started.execution.version, result: response, attempts: 0, next_retry_at: 0 });
      } catch (error) {
        clearInterval(renewal);
        await rpc(`/v1/body/uncertain?taskId=${encodeURIComponent(task.task_id)}`, { ...lease, expected_version: started.execution.version, reason: String(error) }).catch(() => {});
        throw error;
      }
      try { await flushResults(); }
      finally { clearInterval(renewal); }
      console.log(JSON.stringify({ event: 'body-result-recorded', ...lease }));
      } catch (error) {
        const execution = error.payload?.execution;
        if (execution?.phase === 'UNCERTAIN') {
          await rpc('/v1/scheduler/suspend-uncertain', { task_id: task.task_id, tenant_id: task.tenant_id, node_id: nodeId, fencing_token: task.lease_fencing_token }).catch(() => {});
        }
        console.error(JSON.stringify({ event: 'body-task-blocked', taskId: task.task_id, error: String(error) }));
      }
    }
  } catch (error) { console.error(String(error)); }
  await new Promise(resolve => setTimeout(resolve, 2000));
}
