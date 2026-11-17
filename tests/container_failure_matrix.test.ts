import { it } from "node:test";
import * as assert from "node:assert/strict";
import * as http from "node:http";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { ContainerSandbox } from "../src/runtime/container_sandbox.js";
for (const cleanupFails of [false, true]) it(`cleans by generated identity after create response loss; cleanup failure=${cleanupFails}`, async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "engine-fault-")), socket = path.join(directory, "engine.sock");
  let created = "", deleted = "", calls = 0;
  const engine = http.createServer((request, response) => {
    calls++;
    if (request.method === "POST" && request.url?.startsWith("/v1.45/containers/create?name=")) { created = request.url.split("name=")[1]; request.resume(); request.on("end", () => response.destroy()); return; }
    if (request.method === "DELETE") { deleted = request.url ?? ""; response.writeHead(cleanupFails ? 500 : 204); response.end(); return; }
    response.writeHead(500); response.end();
  });
  await new Promise<void>(resolve => engine.listen(socket, resolve));
  try {
    const runtime = new ContainerSandbox("node@sha256:" + "a".repeat(64), socket);
    await assert.rejects(runtime.execute({ argv: ["true"], cpu: 0.5, memoryMb: 128, timeoutMs: 1000 }, new AbortController().signal), cleanupFails ? /BODY_STOP_UNCONFIRMED/ : /socket hang up/);
    assert.match(created, /^ansts-sandbox-/);
    assert.equal(deleted, `/v1.45/containers/${created}?force=true&v=true`);
    assert.equal(calls, 2);
  } finally { await new Promise<void>(resolve => engine.close(() => resolve())); fs.rmSync(directory, { recursive: true, force: true }); }
});
