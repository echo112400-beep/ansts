import { it } from "node:test";
import * as assert from "node:assert/strict";
import { SandboxLogs } from "../src/runtime/sandbox_output.js";
it("decodes fragmented Docker logs and bounds retained output independently of streamed bytes", () => {
  const logs = new SandboxLogs(), frame = Buffer.alloc(9008); frame[0] = 1; frame.writeUInt32BE(9000, 4); frame.fill(120, 8);
  logs.push(frame.subarray(0, 5)); logs.push(frame.subarray(5)); logs.finish();
  assert.equal(logs.snapshot().logs.length, 8192);
  assert.equal(logs.snapshot().logsTruncated, true);
});
