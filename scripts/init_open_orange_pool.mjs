import { setTimeout } from "node:timers/promises";

const endpoint = process.env.RIVET_ENDPOINT;
const token = process.env.RIVET_TOKEN;
const namespace = process.env.RIVET_NAMESPACE ?? "default";
if (!endpoint || !token) throw new Error("RIVET_ENDPOINT and RIVET_TOKEN are required");

const pool = new URL("/pools/default", endpoint);
pool.searchParams.set("namespace", namespace);
const headers = { Authorization: "Bearer " + token, "Content-Type": "application/json" };

for (let attempt = 0; attempt < 30; attempt++) {
  let ready;
  try {
    ready = await fetch(pool, { headers, signal: AbortSignal.timeout(2500) });
  } catch {
    await setTimeout(1000);
    continue;
  }
  await ready.body?.cancel();
  if (ready.ok) process.exit(0);
  if (ready.status === 503) {
    await setTimeout(1000);
    continue;
  }
  if (ready.status !== 404) throw new Error(`Rivet pool lookup returned HTTP ${ready.status}`);
  const created = await fetch(pool, { method: "PUT", headers, body: "{}", signal: AbortSignal.timeout(5000) });
  await created.body?.cancel();
  if (created.ok) process.exit(0);
  throw new Error(`Rivet pool creation returned HTTP ${created.status}`);
}

throw new Error("Rivet Engine startup timed out");
