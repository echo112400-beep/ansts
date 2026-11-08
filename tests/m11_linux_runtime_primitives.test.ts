/**
 * @fileoverview M11 Real Linux Runtime Primitives Suite (4 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as net from "node:net";
import {
  LinuxCgroupV2AndPsiDriver,
  CompanionContainerAndOverlayDriver,
  ZeroTrustVsockHostDaemon,
  EphemeralHeavyExecutorMigrator,
  InMemoryR2Bucket,
} from "../src/index.js";

describe("M11 Linux Runtime Primitives Suite", () => {
  it("1. LinuxCgroupV2AndPsiDriver writes cpu.max, memory.high, cgroup.freeze and parses /proc/pressure/memory", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-cgroup-"));
    const driver = new LinuxCgroupV2AndPsiDriver(tmpDir);
    const limits = driver.writePhaseLimits("task-cg-1", "THINK", 0.25, 256, 512);
    assert.equal(limits.cpuMaxContent, "25000 100000");
    assert.equal(driver.setFreezeState("task-cg-1", true), "1");

    const psi = LinuxCgroupV2AndPsiDriver.parseProcPressureMemory(
      "some avg10=12.45 avg60=5.00 avg300=1.00 total=100\nfull avg10=3.20 avg60=1.00 avg300=0.00 total=20\n"
    );
    assert.equal(psi.someAvg10, 12.45);
    assert.equal(psi.fullAvg10, 3.2);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("2. CompanionContainerAndOverlayDriver creates OverlayFS mount directories and captures upperdir diff", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-ovl-"));
    const driver = new CompanionContainerAndOverlayDriver(tmpDir);
    const layout = driver.createOverlayMountLayout("task-ovl-1");
    fs.writeFileSync(path.join(layout.upperDir, "result.txt"), "hello overlay", "utf-8");
    const snap = driver.captureUpperdirDeltaSnapshot("task-ovl-1", 1);
    assert.deepEqual(snap.changedFiles, ["result.txt"]);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("3. ZeroTrustVsockHostDaemon frames binary RPC over Unix socket and enforces egress allowlist", async () => {
    const sockPath = path.join(os.tmpdir(), `ansts-vsock-${Date.now()}.sock`);
    const daemon = new ZeroTrustVsockHostDaemon(["api.openai.com"]);
    await daemon.startUnixSocketServer(sockPath);

    const response = await new Promise<Record<string, unknown>>((resolve, reject) => {
      const client = net.createConnection(sockPath, () => {
        client.write(
          ZeroTrustVsockHostDaemon.encodeFrame({
            requestId: "req-1",
            type: "EGRESS_CHECK",
            hostname: "untrusted.evil.org",
          })
        );
      });
      client.on("data", (chunk: Buffer) => {
        resolve(ZeroTrustVsockHostDaemon.decodeFrame(chunk));
        client.end();
      });
      client.on("error", reject);
    });

    assert.equal(response.allowed, false);
    assert.equal(response.status, 403);
    await daemon.stop();
  });

  it("4. EphemeralHeavyExecutorMigrator migrates upperdir snapshot across nodes via RustFS/R2", async () => {
    const bucket = new InMemoryR2Bucket();
    const migrator = new EphemeralHeavyExecutorMigrator(bucket);
    const res = await migrator.migrateHeavyTaskAcrossNodes({
      taskId: "t-mig-1",
      turnIndex: 3,
      sourceNodeId: "node-1",
      targetNodeId: "node-2",
      newFencingToken: 4,
      upperdirSnapshotBytes: new Uint8Array([1, 2, 3, 4, 5]),
    });
    assert.equal(res.targetRestored, true);
  });
});
