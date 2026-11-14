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