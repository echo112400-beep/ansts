import { it } from "node:test";
import * as assert from "node:assert/strict";
import { TaskSpecValidator } from "../src/gateway/validator.js";
import { normalizeExecutionSpec } from "../src/execution_spec.js";
import { executionDigest, taskInputDigest } from "../src/cloudflare/execution_attempt.js";
import { CoreScheduler5D } from "../src/scheduler.js";
const execution = { kind: "container", image: "node@sha256:" + "a".repeat(64), argv: ["node", "-e", "console.log(1)"], cpu: 0.25, memoryMb: 128, timeoutMs: 5000 };
it("binds every execution field and preserves legacy trace digests", () => {
  assert.equal(taskInputDigest("tenant", "task", "work"), executionDigest({ tenant_id: "tenant", task_id: "task", prompt: "work" }));
  const digest = taskInputDigest("tenant", "task", "work", execution);
  for (const patch of [{ argv: ["false"] }, { image: "node@sha256:" + "b".repeat(64) }, { cpu: 0.5 }, { memoryMb: 256 }, { timeoutMs: 10000 }]) assert.notEqual(taskInputDigest("tenant", "task", "work", { ...execution, ...patch }), digest);
});
it("rejects ambient options and execution outside the declared task budget", () => {
  assert.throws(() => normalizeExecutionSpec({ ...execution, mounts: ["/etc"] }));
  const task = TaskSpecValidator.validateAndNormalize({ tenant_id: "tenant", task_id: "task", execution });
  assert.deepEqual(task.execution, execution);
  assert.throws(() => TaskSpecValidator.validateAndNormalize({ tenant_id: "tenant", task_id: "task", execution: { ...execution, cpu: 32 } }));
  assert.throws(() => TaskSpecValidator.validateAndNormalize({ tenant_id: "tenant", task_id: "task", timeout_seconds: 5, execution: { ...execution, timeoutMs: 6000 } }));
  const changed = TaskSpecValidator.validateAndNormalize({ tenant_id: "tenant", task_id: "other", execution: { ...execution, argv: ["false"] } });
  assert.notEqual(CoreScheduler5D.computeTaskContentFingerprint(task), CoreScheduler5D.computeTaskContentFingerprint(changed));
});