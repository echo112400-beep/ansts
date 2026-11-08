"use strict";


var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var os = _interopRequireWildcard(require("node:os"));
var path = _interopRequireWildcard(require("node:path"));
var net = _interopRequireWildcard(require("node:net"));
var _index = require("../src/index.js");var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M11 Real Linux Runtime Primitives Suite (4 tests).
 */





(0, _nodeTest.describe)("M11 Linux Runtime Primitives Suite", () => {
  (0, _nodeTest.it)("1. LinuxCgroupV2AndPsiDriver writes cpu.max, memory.high, cgroup.freeze and parses /proc/pressure/memory", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-cgroup-"));
    const driver = new _index.LinuxCgroupV2AndPsiDriver(tmpDir);
    const limits = driver.writePhaseLimits("task-cg-1", "THINK", 0.25, 256, 512);
    assert.equal(limits.cpuMaxContent, "25000 100000");
    assert.equal(driver.setFreezeState("task-cg-1", true), "1");

    const psi = _index.LinuxCgroupV2AndPsiDriver.parseProcPressureMemory(
      "some avg10=12.45 avg60=5.00 avg300=1.00 total=100\nfull avg10=3.20 avg60=1.00 avg300=0.00 total=20\n"
    );
    assert.equal(psi.someAvg10, 12.45);
    assert.equal(psi.fullAvg10, 3.2);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  (0, _nodeTest.it)("2. CompanionContainerAndOverlayDriver creates OverlayFS mount directories and captures upperdir diff", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-ovl-"));
    const driver = new _index.CompanionContainerAndOverlayDriver(tmpDir);
    const layout = driver.createOverlayMountLayout("task-ovl-1");
    fs.writeFileSync(path.join(layout.upperDir, "result.txt"), "hello overlay", "utf-8");
    const snap = driver.captureUpperdirDeltaSnapshot("task-ovl-1", 1);
    assert.deepEqual(snap.changedFiles, ["result.txt"]);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  (0, _nodeTest.it)("3. ZeroTrustVsockHostDaemon frames binary RPC over Unix socket and enforces egress allowlist", async () => {
    const sockPath = path.join(os.tmpdir(), `ansts-vsock-${Date.now()}.sock`);
    const daemon = new _index.ZeroTrustVsockHostDaemon(["api.openai.com"]);
    await daemon.startUnixSocketServer(sockPath);

    const response = await new Promise((resolve, reject) => {
      const client = net.createConnection(sockPath, () => {
        client.write(
          _index.ZeroTrustVsockHostDaemon.encodeFrame({
            requestId: "req-1",
            type: "EGRESS_CHECK",
            hostname: "untrusted.evil.org"
          })
        );
      });
      client.on("data", (chunk) => {
        resolve(_index.ZeroTrustVsockHostDaemon.decodeFrame(chunk));
        client.end();
      });
      client.on("error", reject);
    });

    assert.equal(response.allowed, false);
    assert.equal(response.status, 403);
    await daemon.stop();
  });

  (0, _nodeTest.it)("4. EphemeralHeavyExecutorMigrator migrates upperdir snapshot across nodes via RustFS/R2", async () => {
    const bucket = new _index.InMemoryR2Bucket();
    const migrator = new _index.EphemeralHeavyExecutorMigrator(bucket);
    const res = await migrator.migrateHeavyTaskAcrossNodes({
      taskId: "t-mig-1",
      turnIndex: 3,
      sourceNodeId: "node-1",
      targetNodeId: "node-2",
      newFencingToken: 4,
      upperdirSnapshotBytes: new Uint8Array([1, 2, 3, 4, 5])
    });
    assert.equal(res.targetRestored, true);
  });
});