import { it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { AgentToolLoop } from "../src/runtime/agent_tool_loop.js";
import { AgentExecutionSpec, normalizeExecutionSpec } from "../src/execution_spec.js";
import { ContainerSandbox } from "../src/runtime/container_sandbox.js";
import { spawnSync } from "node:child_process";
const image = process.env.ANSTS_SANDBOX_TEST_IMAGE ?? "node@sha256:" + "a".repeat(64);
const spec: AgentExecutionSpec = { kind: "agent", image, timeoutMs: 30000, cpu: 0.5, memoryMb: 128, maxTurns: 8, maxTokens: 32768, files: {} };
it("validates Agent budgets and paths before admission", () => {
  assert.deepEqual(normalizeExecutionSpec(spec), spec);
  for (const patch of [{ maxTurns: 100 }, { files: { "../escape": "bad" } }, { maxTokens: 0 }, { endpoint: "https://attacker" }]) assert.throws(() => normalizeExecutionSpec({ ...spec, ...patch }));
});
it("requires a passing test and refuses tools outside the local workspace", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "agent-boundaries-"));
  try {
    await assert.rejects(new AgentToolLoop(directory, { next: async () => { throw new Error("must not call model"); } }).run("missing-checkpoint", 1, "work", spec, { execute: async () => { throw new Error("must not execute"); } }, new AbortController().signal, undefined, true), /AGENT_CHECKPOINT_MISSING/);
    for (const [identity, action] of [["finish", { tool: "finish" }], ["escape", { tool: "write", path: "../escape", content: "bad" }], ["external", { tool: "send_email" }]] as const) {
      const loop = new AgentToolLoop(directory, { next: async () => ({ action, tokens: 20 }) });
      const output = await loop.run(identity, 1, "work", spec, { execute: async () => { throw new Error("must not execute"); } }, new AbortController().signal);
      assert.equal(output.executionOutcome, "FAILED"); assert.equal(output.artifacts?.length, 0);
    }
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
});
it("retries a transient model request within persisted budgets without replaying any tools", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "agent-model-retry-"));
  try {
    for (const code of ["MODEL_TRANSPORT_FAILED", "MODEL_ACTION_JSON_INVALID"]) {
      let calls = 0;
      const model = { next: async () => { if (++calls === 1) throw new Error(code); return { action: { tool: "finish" }, tokens: 20 }; } };
      const output = await new AgentToolLoop(directory, model).run(code, 1, "work", spec, { execute: async () => { throw new Error("must not execute"); } }, new AbortController().signal);
      assert.equal(calls, 2); assert.equal(output.logs, "AGENT_PASSING_TEST_REQUIRED");
    }
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
});
it("runs a model-directed workspace edit with a real isolated test and immutable result recovery", { skip: !process.env.ANSTS_SANDBOX_TEST_IMAGE && "ANSTS_SANDBOX_TEST_IMAGE required" }, async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "agent-real-tools-"));
  let requests = 0;
  const actions = [
    { tool: "write", path: "sum.js", content: "module.exports=(a,b)=>a+b;" },
    { tool: "test", argv: ["node", "-e", "if(require('./sum')(2,3)!==5)process.exit(1);console.log('sum-test-passed')"] },
    { tool: "finish" },
  ];
  const model = { next: async () => ({ action: actions[requests++], tokens: 100 }) };
  try {
    const runtime = new ContainerSandbox(image);
    const output = await new AgentToolLoop(directory, model).run("tenant-task-input", 1, "Implement addition and test it", spec, runtime, new AbortController().signal);
    assert.equal(output.executionOutcome, "SUCCEEDED"); assert.match(output.logs, /sum-test-passed/);
    assert.equal(output.agentTurns?.length, 3);
    assert.equal(output.artifacts?.[0].path, "sum.js");
    const recovered = await new AgentToolLoop(directory, model).run("tenant-task-input", 2, "Implement addition and test it", spec, runtime, new AbortController().signal);
    assert.deepEqual(recovered, output); assert.equal(requests, 3);
    await assert.rejects(new AgentToolLoop(directory, model).run("tenant-task-input", 0, "work", spec, runtime, new AbortController().signal), /RECOVERY_REQUIRES_RECONCILIATION/);
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
});
it("resumes a committed Turn after abrupt process loss without replaying the committed write", { skip: !process.env.ANSTS_SANDBOX_TEST_IMAGE && "ANSTS_SANDBOX_TEST_IMAGE required" }, async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "agent-turn-restart-"));
  try {
    const program = `const {AgentToolLoop}=require(${JSON.stringify(require.resolve("../src/runtime/agent_tool_loop.js"))});let calls=0;new AgentToolLoop(${JSON.stringify(directory)},{next:async()=>{if(calls++)process.exit(87);return {action:{tool:'write',path:'sum.js',content:'module.exports=(a,b)=>a+b;'},tokens:100}}}).run('restart',1,'Add two numbers',${JSON.stringify(spec)},{execute:async()=>{throw Error('must not run')}},new AbortController().signal);`;
    assert.equal(spawnSync(process.execPath, ["-e", program]).status, 87);
    let calls = 0;
    const actions = [{ tool: "test", argv: ["node", "-e", "if(require('./sum')(2,3)!==5)process.exit(1)"] }, { tool: "finish" }];
    const result = await new AgentToolLoop(directory, { next: async () => ({ action: actions[calls++], tokens: 100 }) }).run("restart", 2, "Add two numbers", spec, new ContainerSandbox(image), new AbortController().signal);
    assert.equal(result.executionOutcome, "SUCCEEDED");
    assert.deepEqual(result.agentTurns?.map(turn => turn.tool), ["write", "test", "finish"]);
    assert.equal(calls, 2);
  } finally { fs.rmSync(directory, { recursive: true, force: true }); }
});
