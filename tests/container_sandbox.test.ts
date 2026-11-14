import { it } from "node:test";
import * as assert from "node:assert/strict";
import { ContainerSandbox } from "../src/runtime/container_sandbox.js";
const digest = process.env.ANSTS_SANDBOX_TEST_IMAGE;
it("requires an immutable image and validates commands before contacting Engine", async () => {
  assert.throws(() => new ContainerSandbox("node:latest"));
  const runtime = new ContainerSandbox("node@sha256:" + "a".repeat(64), "/missing-engine");
  await assert.rejects(runtime.execute({ argv: [], timeoutMs: 1000, cpu: 1, memoryMb: 64 }, new AbortController().signal), /INVALID_SANDBOX_COMMAND/);
});
it("runs isolated commands, records nonzero exits and confirms timeout cleanup", { skip: !digest && "ANSTS_SANDBOX_TEST_IMAGE required" }, async () => {
  const runtime = new ContainerSandbox(digest!);
  const base = { timeoutMs: 15000, cpu: 0.5, memoryMb: 128 };
  process.env.ANSTS_TEST_PARENT_SECRET = "must-not-reach-sandbox";
  try {
    const result = await runtime.execute({ ...base, argv: ["node", "-e", "const fs=require('fs');let readonly=false;try{fs.writeFileSync('/forbidden','x')}catch{readonly=true};fs.writeFileSync('/workspace/result','ok');console.log(JSON.stringify({uid:process.getuid(),secret:process.env.ANSTS_TEST_PARENT_SECRET,readonly,socket:fs.existsSync('/var/run/docker.sock'),network:Object.keys(require('os').networkInterfaces())}));"] }, new AbortController().signal);
    assert.equal(result.exitCode, 0);
    const output = JSON.parse(result.logs.trim());
    assert.deepEqual(output, { uid: 65534, readonly: true, socket: false, network: ["lo"] });
    const failed = await runtime.execute({ ...base, argv: ["node", "-e", "process.exit(7)"] }, new AbortController().signal);
    assert.equal(failed.exitCode, 7);
    await assert.rejects(runtime.execute({ ...base, timeoutMs: 1500, argv: ["node", "-e", "setInterval(()=>{},1000)"] }, new AbortController().signal));
  } finally { delete process.env.ANSTS_TEST_PARENT_SECRET; }
});