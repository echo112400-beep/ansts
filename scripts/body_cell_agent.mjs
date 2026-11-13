/** Body execution agent: celld owns registration, placement and completion. */
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { NodeDaemon } = require('../dist/src/node_daemon.js');
const nodeId = process.env.ANSTS_NODE_ID;
const daemon = new NodeDaemon(nodeId);
const base = process.env.ANSTS_BODY_CELL_URL;
const local = `http://127.0.0.1:${process.env.ANSTS_LISTEN_PORT}`;
async function rpc(path, body) {
  const response = await fetch(base + path, { method: body ? 'POST' : 'GET', headers: { 'content-type': 'application/json' }, body: body ? JSON.stringify(body) : undefined, signal: AbortSignal.timeout(10000) });
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
      const lease = { task_id: task.task_id, node_id: nodeId, fencing_token: task.lease_fencing_token };
      await rpc('/v1/scheduler/lease-renew', lease);
      const state = await rpc(`/v1/body/state?hostId=${encodeURIComponent(nodeId)}`);
      if (state.receipts.some(row => row.task_id === task.task_id && row.fencing_token === lease.fencing_token)) {
        await rpc('/v1/scheduler/complete', lease);
        continue;
      }
      const controller = new AbortController();
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
        response = await fetch(local + '/internal/v1beta1/body/execute', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ taskId: task.task_id, tenantId: task.tenant_id, promptText: task.prompt, fencingToken: lease.fencing_token }), signal: AbortSignal.any([controller.signal, AbortSignal.timeout(Math.min(2147483647, Math.max(10000, Number(task.timeout_seconds ?? 1800) * 1000)))]) });
      } finally { clearInterval(renewal); }
      if (!response.ok) throw new Error(`execute: ${response.status}`);
      const result = await response.json();
      await rpc(`/v1/body/receipt?hostId=${encodeURIComponent(nodeId)}`, { ...lease, result });
      await rpc('/v1/scheduler/complete', lease);
      console.log(JSON.stringify({ event: 'body-completed', ...lease }));
    }
  } catch (error) { console.error(String(error)); }
  await new Promise(resolve => setTimeout(resolve, 2000));
}
