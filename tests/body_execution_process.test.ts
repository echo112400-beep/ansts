/** @fileoverview Disposable native execution and cancellation without publishing a partial commit. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import { LayerFsWorkspaceDriver } from "../src/runtime/layerfs_workspace_driver.js";
import { runBodyExecution } from "../src/runtime/body_execution_process.js";

it("executes user prompt as data and returns an importable committed branch", async () => {
  const driver = new LayerFsWorkspaceDriver();
  driver.forkBranch("main", "body-test");
  const output = await runBodyExecution({ bundle: driver.exportBranchBundle("body-test"), branchName: "body-test", tenantId: "tenant", stepId: "step", prompt: "$(exit 42) `exit 43` 'quoted'" }, new AbortController().signal);
  assert.equal(output.result.unmountedCleanly, true);
  driver.importBranchBundle(output.bundle);
  assert.equal(driver.getBranchHeadCommit("body-test").commitId, output.result.commitId);
  assert.equal(driver.getBranchHeadCommit("body-test").filesTree.get(".ansts_task_trace.txt")?.content, "$(exit 42) `exit 43` 'quoted'\n");
});
it("waits for the worker to exit on cancellation and leaves the parent's branch untouched", async () => {
  const driver = new LayerFsWorkspaceDriver();
  driver.forkBranch("main", "cancel-test");
  const head = driver.getBranchHeadCommit("cancel-test");
  const controller = new AbortController();
  const execution = runBodyExecution({ bundle: driver.exportBranchBundle("cancel-test"), branchName: "cancel-test", tenantId: "tenant", stepId: "step", prompt: "never commit", delayMs: 30000 }, controller.signal);
  const timer = setTimeout(() => controller.abort(), 200);
  try { await assert.rejects(execution, /BODY_EXECUTION_CANCELLED/); }
  finally { clearTimeout(timer); }
  assert.equal(driver.getBranchHeadCommit("cancel-test"), head);
});
