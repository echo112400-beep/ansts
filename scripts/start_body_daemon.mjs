/** Native-only Body host: sandbox lifecycle plus execution agent, with remote celld control. */
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
const require = createRequire(import.meta.url);
const { BodyDaemonServer } = require('../dist/src/runtime/body_daemon_server.js');
const server = new BodyDaemonServer({
  nodeId: process.env.ANSTS_NODE_ID,
  storageDir: process.env.ANSTS_STORAGE_DIR,
  bodyWorkerUrl: process.env.ANSTS_BODY_CELL_URL,
  token: readFileSync(process.env.ANSTS_BODY_TOKEN_FILE, 'utf8').trim(),
  host: process.env.ANSTS_HOST ?? '0.0.0.0', port: Number(process.env.ANSTS_LISTEN_PORT ?? 9501),
  s3Endpoint: process.env.S3_ENDPOINT,
  executionDelayMs: Number(process.env.ANSTS_BODY_EXECUTION_DELAY_MS ?? 0),
});
const address = await server.start();
console.log(JSON.stringify({ runtime: 'native-body-daemon', nodeId: process.env.ANSTS_NODE_ID, apiPort: address.port, controlPlane: 'remote-celld-body', status: 'READY' }));
let stopping = false;
const stop = () => { if (stopping) return; stopping = true; server.stop().finally(() => process.exit(0)); };
process.on('SIGTERM', stop); process.on('SIGINT', stop);
await import('./body_cell_agent.mjs');
