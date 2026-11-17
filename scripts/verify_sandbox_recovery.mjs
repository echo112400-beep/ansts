/** Dedicated development recovery smoke; only the named fixture owner is eligible. */
import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { ContainerSandbox } = require('../dist/src/runtime/container_sandbox.js');
const runtime = new ContainerSandbox('node@sha256:fe4f6d74e20370acb04ad889077d14d0f64a9c8ea6a42bc60cddf1e844699914', '/var/run/docker.sock', 'recovery-smoke');
const count = await runtime.recoverOrphans();
if (count !== 1) throw new Error('Expected one recovery-smoke fixture; create it with the documented command first');
console.log(JSON.stringify({ orphanRecovery: 'confirmed', removed: count }));