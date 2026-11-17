import { it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { BodyCleanupLedger } from "../src/runtime/body_cleanup_ledger.js";
import { BodyDaemonServer } from "../src/runtime/body_daemon_server.js";
it("retains unresolved cleanup across reconstruction and prevents startup without Engine recovery", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "body-cleanup-"));
  try {
    const directory = path.join(root, "cleanup-ledger");
    const key = "a".repeat(64);
    new BodyCleanupLedger(directory).reserve(key);
    const recovered = new BodyCleanupLedger(directory);
    assert.deepEqual(recovered.pending(), [key]);
    assert.throws(() => recovered.reserve(key));
    assert.throws(() => recovered.confirmRemoved("../escape"));
    const daemon = new BodyDaemonServer({ nodeId: "host", storageDir: root, token: "a".repeat(64), bodyWorkerUrl: "http://127.0.0.1:1", port: 0 });
    await assert.rejects(daemon.start(), /BODY_RECOVERY_REQUIRES_ENGINE/);
    assert.deepEqual(recovered.pending(), [key]);
    recovered.confirmAllRemoved();
    assert.deepEqual(new BodyCleanupLedger(directory).pending(), []);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});
import { BodyDrainState } from "../src/runtime/body_drain_state.js";
it("persists both drain and explicit resume and rejects corrupt state", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "body-drain-"));
  try {
    const file = path.join(root, "drain.json");
    const state = new BodyDrainState(file);
    assert.equal(state.draining, false);
    state.set(true); assert.equal(new BodyDrainState(file).draining, true);
    state.set(false); assert.equal(new BodyDrainState(file).draining, false);
    fs.writeFileSync(file, '{"schema_version":1,"draining":"false"}');
    assert.throws(() => new BodyDrainState(file), /INVALID_DRAIN_STATE/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});