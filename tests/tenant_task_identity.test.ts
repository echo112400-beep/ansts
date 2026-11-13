/** @fileoverview Same-name tenant tasks remain distinct through Gateway and scheduler control paths. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import { taskStorageId, publicTaskId } from "../src/cloudflare/task_identity.js";
import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell.js";
import { AnstsGatewayCell, handleUnifiedGatewayRequest } from "../src/cloudflare/gateway_cell.js";
import { MockDurableObjectState } from "../src/cloudflare/env.js";

it("uses unambiguous tenant identities and requires tenant selection for ambiguous public IDs", async () => {
  assert.notEqual(taskStorageId("a:b", "c"), taskStorageId("a", "b:c"));
  assert.deepEqual(publicTaskId(taskStorageId("租户", "任务")), { tenantId: "租户", taskId: "任务" });
  const scheduler = new AnstsSchedulerCell(new MockDurableObjectState("scheduler"));
  const gateway = new AnstsGatewayCell(new MockDurableObjectState("gateway"));
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init) => scheduler.fetch(new Request(input, init));
  try {
    const env = { ANSTS_SCHEDULER_URL: "https://scheduler", ANSTS_GATEWAY_CELL: { idFromName: (name: string) => ({ toString: () => name }), get: () => gateway } } as never;
    for (const tenant of ["tenant-a", "tenant-b"]) {
      const response = await handleUnifiedGatewayRequest(new Request("https://gateway/v1/tasks", { method: "POST", body: JSON.stringify({ task_id: "same-name", tenant_id: tenant, prompt: `work for ${tenant}` }) }), env);
      assert.equal(response?.status, 202);
    }
    assert.equal((await handleUnifiedGatewayRequest(new Request("https://gateway/v1/tasks/same-name"), env))?.status, 400);
    for (const tenant of ["tenant-a", "tenant-b"]) {
      const response = await handleUnifiedGatewayRequest(new Request(`https://gateway/v1/tasks/same-name?tenantId=${tenant}`), env);
      assert.equal(response?.status, 200);
      const task = await response!.json();
      assert.equal(task.tenant_id, tenant);
      assert.equal(task.task_id, "same-name");
      assert.equal(task.executionTaskId, taskStorageId(tenant, "same-name"));
    }
  } finally { globalThis.fetch = originalFetch; }
});
