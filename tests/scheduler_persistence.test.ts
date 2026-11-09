/**
 * @fileoverview feat/celld-v25 Single-Write-Path Scheduler Persistence & Queue Deduplication Suite (`b566a53`).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  InMemoryCellSqlStorage,
  SchedulerV25SingleWritePathStore,
  TaskPriority,
  TaskSpec,
} from "../src/index.js";

describe("feat/celld-v25 Single-Write-Path Scheduler Persistence Suite", () => {
  it("Commits tasks via single write path and deduplicates repeated idempotency keys", () => {
    const sql = new InMemoryCellSqlStorage();
    const store = new SchedulerV25SingleWritePathStore(sql);

    const task = new TaskSpec({
      task_id: "v25-task-001",
      tenant_id: "tenant-alpha",
      priority: TaskPriority.P1_HIGH,
      idempotency_key: "idem-v25-001",
      prompt: "Verify v2.5 single write path",
    });

    const first = store.commitTaskSingleWritePath(task);
    const second = store.commitTaskSingleWritePath(task);
    assert.equal(first.deduplicated, false);
    assert.equal(second.deduplicated, true);
    assert.equal(store.listPersistedTasks().length, 1);
  });
});
