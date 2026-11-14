/** Quiesced migration from the legacy Brain binding to the independent Body Worker. */
import { readFileSync } from 'node:fs';
import { createHmac } from 'node:crypto';
const source = process.env.ANSTS_LEGACY_BODY_URL ?? 'http://celld-brain-01:8081';
const target = process.env.ANSTS_BODY_CELL_URL ?? 'http://body-celld:8080';
const secret = readFileSync(process.env.ANSTS_BODY_AUTH_SECRET_FILE, 'utf8').trim();
const sign = identity => createHmac('sha256', secret).update(`ansts-body-v1:${identity}`).digest('hex');
const operator = `Operator ${sign('operator:reconcile')}`;
async function request(url, authorization, body) {
  let last;
  for (let attempt = 0; attempt < 60; attempt++) {
    try {
      const response = await fetch(url, { method: body === undefined ? 'GET' : 'POST', headers: { authorization, 'content-type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(10000), redirect: 'error' });
      if (response.ok) return await response.json();
      if (![404, 503].includes(response.status)) throw new Error(`migration request failed: ${response.status}`);
      last = new Error(`migration waiting for deployment: ${response.status}`);
    } catch (error) { last = error; }
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  throw last;
}
const { tasks } = await request(`${source}/v1/scheduler/tasks`, `Gateway ${sign('gateway:control')}`);
const identities = [
  ...['body-host-01', 'body-host-02'].map(host => `hostId=${encodeURIComponent(host)}`),
  ...tasks.map(task => `taskId=${encodeURIComponent(task.task_id)}`),
];
let imported = 0, protectedExisting = 0, empty = 0;
for (const identity of new Set(identities)) {
  const snapshot = await request(`${source}/internal/v1/body-legacy/export?${identity}`, operator);
  if (snapshot.schema_version !== 1 || !Array.isArray(snapshot.rows)) throw new Error('INVALID_LEGACY_BODY_SNAPSHOT');
  if (!snapshot.rows.length) { empty++; continue; }
  const result = await request(`${target}/v1/body/import?${identity}`, operator, snapshot);
  if (result.imported) imported++;
  else if (result.already_initialized) protectedExisting++;
  else throw new Error('BODY_STATE_IMPORT_NOT_CONFIRMED');
}
console.log(JSON.stringify({ bodyStateMigration: 'complete', imported, protectedExisting, empty }));
