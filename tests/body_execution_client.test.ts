import { it } from "node:test";
import * as assert from "node:assert/strict";
import * as http from "node:http";
import { requestBodyExecution } from "../src/runtime/body_execution_client.js";
it("waits for delayed execution headers, enforces the task deadline and rejects oversized results", async () => {
  const server = http.createServer((request, response) => {
    request.resume();
    if (request.url === "/large") { response.end(JSON.stringify({ logs: "x".repeat(65536) })); return; }
    const timer = setTimeout(() => response.end(JSON.stringify({ exitCode: 0 })), 100);
    response.on("close", () => clearTimeout(timer));
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const base = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  try {
    assert.equal((await requestBodyExecution(base, {}, {}, AbortSignal.timeout(1000))).exitCode, 0);
    await assert.rejects(requestBodyExecution(base, {}, {}, AbortSignal.timeout(10)), /abort/i);
    await assert.rejects(requestBodyExecution(base + "/large", {}, {}, AbortSignal.timeout(1000)), /BODY_RESULT_TOO_LARGE/);
  } finally { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); }
});
