import { it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { BodyResourceLedger } from "../src/runtime/body_resource_ledger.js";
it("persists fractional CPU and memory reservations, refuses oversell and releases once", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "resources-"));
  try {
    const budget = { cpu: 1, memoryMb: 256 }, key = "a".repeat(64), other = "b".repeat(64);
    let ledger = new BodyResourceLedger(directory, budget);
    assert.equal(ledger.reserve(key, { cpu: 0.75, memoryMb: 128 }), true);
    ledger = new BodyResourceLedger(directory, budget);
    assert.deepEqual(ledger.snapshot().reserved, { cpu: 0.75, memoryMb: 128 });
    assert.equal(ledger.reserve(other, { cpu: 0.5, memoryMb: 64 }), false);
    assert.equal(ledger.reserve(other, { cpu: 0.25, memoryMb: 129 }), false);
    assert.equal(ledger.reserve(other, { cpu: 0.25, memoryMb: 128 }), true);
    ledger.release(key); ledger.release(key);
    assert.equal(ledger.snapshot().reservations, 1);
    assert.throws(() => ledger.reserve("../escape", { cpu: 1, memoryMb: 1 }));
    fs.writeFileSync(path.join(directory, key + ".json"), '{"schema_version":1,"resources":{"cpu":-1,"memoryMb":1}}');
    assert.throws(() => new BodyResourceLedger(directory, budget));
  } finally { fs.rmSync(directory, { force: true, recursive: true }); }
});
