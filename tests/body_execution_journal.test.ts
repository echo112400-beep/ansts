/** @fileoverview Execution restart and concurrent admission safety. */
import { it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { BodyExecutionJournal } from "../src/runtime/body_execution_journal.js";
it("blocks uncertain replay after restart and returns durable completed results", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "body-journal-"));
  try {
    const journal = new BodyExecutionJournal(directory);
    assert.equal(journal.admit("task").admitted, true);
    const restored = new BodyExecutionJournal(directory);
    assert.deepEqual(restored.admit("task"), { admitted: false, result: undefined });
    restored.complete("task", { status: "COMPLETED" });
    assert.deepEqual(new BodyExecutionJournal(directory).admit("task"), { admitted: false, result: { status: "COMPLETED" } });
  } finally { fs.rmSync(directory, { recursive: true }); }
});
