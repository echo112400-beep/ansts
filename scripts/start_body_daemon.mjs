/** Native-only Body host: sandbox lifecycle plus execution agent, with remote celld control. */
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import os from 'node:os';
const require = createRequire(import.meta.url);
const { physicalHostIdFromMachineId } = require('../dist/src/runtime/capacity_reporter.js');
const { BodyDaemonServer } = require('../dist/src/runtime/body_daemon_server.js');
const { DeepSeekModel } = require('../dist/src/runtime/deepseek_model.js');
const { bodyResourceBudget, cgroupResourceLimits } = require('../dist/src/runtime/body_resource_budget.js');
const limits = cgroupResourceLimits(file => { try { return readFileSync(file, 'utf8'); } catch (error) { if (['ENOENT', 'ENOTDIR'].includes(error.code)) return undefined; throw error; } });
const constrained = process.constrainedMemory?.();
const resources = bodyResourceBudget(Math.min(os.availableParallelism(), limits.cpu ?? Infinity), Math.min(os.totalmem(), constrained > 0 ? constrained : Infinity, limits.memoryBytes ?? Infinity) / 1048576, process.env.ANSTS_BODY_CPU_CORES, process.env.ANSTS_BODY_MEMORY_MB);
if(process.env.ANSTS_USAGE_METERING&&!['off','shadow'].includes(process.env.ANSTS_USAGE_METERING))throw new Error('UNSUPPORTED_USAGE_MODE');
const usageTenantIds = process.env.ANSTS_USAGE_TENANTS?.split(',').map(value=>value.trim()).filter(Boolean);
if(process.env.ANSTS_USAGE_METERING==='shadow'&&(!usageTenantIds?.length||usageTenantIds.some(id=>!/^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/.test(id))))throw new Error('EXPLICIT_USAGE_TENANTS_REQUIRED');
const server = new BodyDaemonServer({
  usageMetering: process.env.ANSTS_USAGE_METERING === 'shadow',
  usageTenantIds,
  physicalHostId: process.env.ANSTS_PHYSICAL_HOST_ID_FILE ? physicalHostIdFromMachineId(readFileSync(process.env.ANSTS_PHYSICAL_HOST_ID_FILE,"utf8")) : undefined,
  nodeId: process.env.ANSTS_NODE_ID,
  storageDir: process.env.ANSTS_STORAGE_DIR,
  bodyWorkerUrl: process.env.ANSTS_BODY_CELL_URL,
  bodyWorkerUrls: process.env.ANSTS_BODY_CELL_URLS,
  monitoringToken: process.env.ANSTS_MONITORING_TOKEN_FILE ? readFileSync(process.env.ANSTS_MONITORING_TOKEN_FILE,'utf8').trim() : undefined,
  token: readFileSync(process.env.ANSTS_BODY_TOKEN_FILE, 'utf8').trim(),
  host: process.env.ANSTS_HOST ?? '0.0.0.0', port: Number(process.env.ANSTS_LISTEN_PORT ?? 9501),
  s3Endpoint: process.env.S3_ENDPOINT,
  sandboxImage: process.env.ANSTS_SANDBOX_IMAGE,
  executionSlots: Number(process.env.ANSTS_BODY_EXECUTION_SLOTS ?? 1),
  resources,
  agentModel: process.env.ANSTS_DEEPSEEK_API_KEY_FILE ? new DeepSeekModel(readFileSync(process.env.ANSTS_DEEPSEEK_API_KEY_FILE, 'utf8').trim(), process.env.ANSTS_DEEPSEEK_MODEL ?? 'deepseek-flash') : undefined,
  executionDelayMs: Number(process.env.ANSTS_BODY_EXECUTION_DELAY_MS ?? 0),
});
const address = await server.start();
console.log(JSON.stringify({ runtime: 'native-body-daemon', nodeId: process.env.ANSTS_NODE_ID, apiPort: address.port, controlPlane: 'remote-celld-body', status: 'READY' }));
let stopping = false;
const stop = () => { if (stopping) return; stopping = true; server.stop().finally(() => process.exit(0)); };
process.on('SIGTERM', stop); process.on('SIGINT', stop);
await import('./body_cell_agent.mjs');
