/** @fileoverview Durable result replay, retry backoff and conflicting-result rejection. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { BodyResultOutbox } from "../src/runtime/body_result_outbox.js";
it("retains results and retry state across restarts until explicitly confirmed", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "body-outbox-"));
  try {
    const outbox = new BodyResultOutbox(directory);
    const delivery = { lease: { tenant_id: "tenant", task_id: "task", attempt_id: "a1" }, expected_version: 2, result: { status: "COMPLETED" }, attempts: 0, next_retry_at: 0 };
    outbox.put(delivery);
    const restored = new BodyResultOutbox(directory);
    assert.deepEqual(restored.pending(), [delivery]);
    restored.defer(delivery);
    assert.equal(restored.pending()[0].attempts, 1);
    assert.ok(restored.pending()[0].next_retry_at > Date.now());
    assert.throws(() => restored.put({ ...delivery, result: { status: "CHANGED" } }), /OUTBOX_RESULT_CONFLICT/);
    restored.remove(delivery.lease);
    assert.equal(restored.pending().length, 0);
  } finally { fs.rmSync(directory, { recursive: true }); }
});
