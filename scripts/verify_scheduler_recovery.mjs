/** Local celld process-restart smoke test; uses an isolated temporary project. */
import { strict as assert } from 'node:assert';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createServer } from 'node:net';
import { build } from 'esbuild';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const dir = await mkdtemp(path.join(tmpdir(), 'ansts-celld-recovery-'));
const listener = createServer();
await new Promise(resolve => listener.listen(0, '127.0.0.1', resolve));
const port = listener.address().port;
await new Promise(resolve => listener.close(resolve));
const base = `http://127.0.0.1:${port}`;
const log = createWriteStream(path.join(dir, 'celld.log'), { flags: 'a' });
let child;
async function start() {
  child = spawn('celld', ['dev', dir, '--port', String(port), '--no-watch', '--logs'], {
    env: { ...process.env, CELLD_ESBUILD: path.join(root, 'node_modules', '.bin', 'esbuild') },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout.pipe(log, { end: false });
  child.stderr.pipe(log, { end: false });
  let spawnError;
  child.on('error', error => { spawnError = error; });
  for (let i = 0; i < 200; i++) {
    if (spawnError) throw spawnError;
    if (child.exitCode !== null) throw new Error(`celld exited with ${child.exitCode}`);
    try {
      const response = await fetch(base, { signal: AbortSignal.timeout(300) });
      if (response.ok) return;
    } catch {}
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  throw new Error('celld readiness timeout');
}
async function stop() {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise(resolve => child.once('exit', resolve));
  // Abrupt process loss, preserving the temp project's persistent storage.
  child.kill('SIGKILL');
  await exited;
}
async function call(endpoint, body) {
  const response = await fetch(`${base}/v1beta1/${endpoint}`, {
    method: body === undefined ? 'GET' : 'POST',
    body: body === undefined ? undefined : JSON.stringify(body),
    headers: { 'content-type': 'application/json' }, signal: AbortSignal.timeout(20000),
  });
  assert.ok(response.ok, `${endpoint}: ${response.status} ${response.ok ? '' : await response.text()}`);
  return response.json();
}
try {
  await build({ entryPoints: [path.join(root, 'src/cloudflare/worker.ts')], bundle: true,
    platform: 'node', format: 'esm', external: ['crypto', 'fs'], outfile: path.join(dir, 'worker.js') });
  const config = JSON.parse(await readFile(path.join(root, 'wrangler.json'), 'utf8'));
  config.main = 'worker.js';
  delete config.triggers;
  delete config.d1_databases;
  delete config.kv_namespaces;
  delete config.r2_buckets;
  delete config.queues.consumers;
  await writeFile(path.join(dir, 'wrangler.json'), JSON.stringify(config));
  const spec = { apiVersion: 'scheduler.agent.io/v1beta1', kind: 'AgentTask',
    metadata: { task_id: 'restart-smoke', tenant_id: 'fin-enterprise', idempotency_key: 'restart-smoke' }, spec: {} };
  await start();
  await call('tasks', spec);
  await stop();
  await start();
  assert.equal((await call('cluster/top')).active_tasks, 1);
  assert.equal((await call('tasks', spec)).deduplicated, true);
  const tick = await call('scheduler/tick', {});
  assert.equal(tick.dispatched_count, 1);
  const assignment = tick.dispatched_tasks[0];
  await stop();
  await start();
  const top = await call('cluster/top');
  assert.equal(top.active_tasks, 1);
  assert.equal(top.celld_sqlite_leases_count, 1);
  assert.equal((await call('scheduler/tick', {})).dispatched_count, 0);
  assert.equal((await call('leases/validate', { task_id: assignment.task_id, node_id: assignment.node_id,
    epoch: assignment.fence.epoch, turn_seq: assignment.fence.turn_seq })).valid, true);
  console.log('celld restart recovery passed: queued task, idempotency, running lease and fence survived two SIGKILL restarts');
  await stop();
  log.end();
  await rm(dir, { recursive: true, force: true });
} catch (error) {
  await stop();
  log.end();
  console.error(await readFile(path.join(dir, 'celld.log'), 'utf8'));
  console.error(`Recovery smoke artifacts: ${dir}`);
  throw error;
}
