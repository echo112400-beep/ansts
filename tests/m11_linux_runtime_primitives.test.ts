/**
 * M11 Integration Test Suite (TASK-53 ~ TASK-56):
 * - TASK-53: Real Linux Cgroups v2 (`/sys/fs/cgroup`) & PSI (`/proc/pressure/*`) Driver
 * - TASK-54: Companion Container Lifecycle & OverlayFS `upperdir` CoW Snapshot/Rollback Driver
 * - TASK-55: Host `AF_VSOCK` / Socket Zero-Trust Bidirectional Proxy Daemon (Length-Prefixed + CRC32 + Fence + Allowlist)
 * - TASK-56: Ephemeral Heavy Executor Cross-Host Live Turn Migration (`<50ms` Handover)
 */

import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { test } from "node:test";

import {
  CompanionContainerAndOverlayDriver,
  EphemeralHeavyExecutorMigrator,
  FenceToken,
  LeaseCoordinator,
  LinuxCgroupV2AndPsiDriver,
  ZeroTrustVsockHostDaemon,
} from "../src/index";

test("M11 (TASK-53): LinuxCgroupV2AndPsiDriver writes exact cpu.max (0.1C <-> 8.0C), memory.high, pids.max, cgroup.freeze & parses PSI", () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m11-cgroup-"));
  const cgroupRoot = path.join(tmpDir, "cgroup");
  const procPressureRoot = path.join(tmpDir, "pressure");
  fs.mkdirSync(procPressureRoot, { recursive: true });

  try {
    const driver = new LinuxCgroupV2AndPsiDriver({ cgroupRoot, procPressureRoot });
    const sandboxId = "sbx-linux-01";

    // 1. Initialize baseline Cognitive Phase cgroup (0.1C = "10000 100000", 256MB memory.high, 256 pids.max)
    driver.initSandboxCgroup({
      sandboxId,
      cogCpuCores: 0.1,
      cogMemHighMb: 256,
      memMaxMb: 4096,
      pidsMax: 256,
    });
    const initState = driver.readSandboxCgroupState(sandboxId);
    assert.equal(initState.cpuMax, "10000 100000");
    assert.equal(initState.memoryHigh, 256 * 1024 * 1024);
    assert.equal(initState.pidsMax, 256);
    assert.equal(initState.frozen, false);

    // 2. Burst to Tool Execution phase (8.0C = "800000 100000", 4096MB memory.high)
    const burst = driver.applyPhaseLimits({
      sandboxId,
      phase: "TOOL_BURST",
      burstCpuCores: 8.0,
      burstMemHighMb: 4096,
    });
    assert.equal(burst.cpuMax, "800000 100000");
    assert.equal(driver.readSandboxCgroupState(sandboxId).cpuMax, "800000 100000");

    // 3. Freeze and thaw cgroup via `cgroup.freeze`
    driver.setCgroupFreeze(sandboxId, true);
    assert.equal(driver.readSandboxCgroupState(sandboxId).frozen, true);
    driver.setCgroupFreeze(sandboxId, false);
    assert.equal(driver.readSandboxCgroupState(sandboxId).frozen, false);

    // 4. Write simulated kernel `/proc/pressure/cpu` & `/proc/pressure/memory` and verify PSI parser
    fs.writeFileSync(
      path.join(procPressureRoot, "cpu"),
      "some avg10=18.50 avg60=9.20 avg300=3.10 total=450120\n",
      "utf8",
    );
    fs.writeFileSync(
      path.join(procPressureRoot, "memory"),
      "some avg10=44.80 avg60=20.10 avg300=5.00 total=891230\nfull avg10=16.40 avg60=6.00 avg300=1.10 total=210900\n",
      "utf8",
    );

    const psi = driver.sampleHostPsiPressure({ memorySomeAvg10Max: 40.0, memoryFullAvg10Max: 15.0 });
    assert.equal(psi.cpuSome.avg10, 18.5);
    assert.equal(psi.memorySome.avg10, 44.8);
    assert.equal(psi.memoryFull.avg10, 16.4);
    assert.equal(psi.emergencyHibernateRequired, true);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test("M11 (TASK-54): CompanionContainerAndOverlayDriver captures OverlayFS upperdir CoW snapshots and rolls back cleanly", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m11-overlay-"));
  try {
    const driver = new CompanionContainerAndOverlayDriver(tmpDir);
    const sandboxId = "sbx-overlay-01";

    const layout = driver.initOverlayWorkspace(sandboxId, {
      "src/app.ts": "export const version = 1;\n",
      "README.md": "# V1\n",
    });

    let containerStarted = false;
    await driver.startCompanionContainer(sandboxId, {
      start: async () => {
        containerStarted = true;
      },
      stop: async () => {
        containerStarted = false;
      },
    });
    assert.equal(containerStarted, true);

    // Turn 1 snapshot
    const snap1 = driver.captureUpperDirSnapshot(sandboxId, 1);
    assert.ok(snap1.merkleHash.startsWith("merkle_"));

    // Turn 2 modifies src/app.ts and adds a broken file
    fs.writeFileSync(path.join(layout.upperDir, "src/app.ts"), "export const version = 2_BROKEN;\n", "utf8");
    fs.writeFileSync(path.join(layout.upperDir, "src/bad.ts"), "syntax error !!!\n", "utf8");
    const snap2 = driver.captureUpperDirSnapshot(sandboxId, 2);
    assert.notEqual(snap1.merkleHash, snap2.merkleHash);

    // Velocity breaker triggers backtrack to Turn 1 -> restores exact upperdir files and deletes src/bad.ts
    const restored = driver.restoreUpperDirSnapshot(sandboxId, 1);
    assert.equal(restored.merkleHash, snap1.merkleHash);
    assert.equal(fs.readFileSync(path.join(layout.upperDir, "src/app.ts"), "utf8"), "export const version = 1;\n");
    assert.equal(fs.existsSync(path.join(layout.upperDir, "src/bad.ts")), false);

    await driver.stopCompanionContainer(sandboxId);
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});

test("M11 (TASK-55): ZeroTrustVsockHostDaemon enforces binary CRC32 framing, X-Agent-Fence, domain allowlist & credential injection", async () => {
  const leaseCoord = new LeaseCoordinator();
  const taskId = "tsk-vsock-01";
  leaseCoord.acquireOrIncrementEpoch(taskId, "host-01", "DeepSeek"); // epoch = 1, turnSeq = 0

  const daemon = new ZeroTrustVsockHostDaemon(leaseCoord);
  const port = await daemon.listenTcpLoopback(0);

  try {
    // 1. Valid request to allowed domain `api.deepseek.com` at (epoch=1, turnSeq=1) -> injects host vault key
    const res1 = await ZeroTrustVsockHostDaemon.sendFrameOverSocket(port, {
      requestId: "req-01",
      taskId,
      epoch: 1,
      turnSeq: 1,
      targetHost: "api.deepseek.com",
      targetPath: "/v1/chat/completions",
      secretKeyAlias: "DEEPSEEK_API_KEY",
      payload: { prompt: "Hello" },
    });
    assert.equal(res1.statusCode, 200);
    assert.equal(res1.fenceHeader, "1:1");
    assert.ok(res1.injectedCredentialFingerprint?.startsWith("injected:sk-host-vaul"));

    // 2. Unauthorized outbound domain `evil-exfil.example.com` -> blocked with 403 DomainBlockedByZeroTrust
    const res2 = await ZeroTrustVsockHostDaemon.sendFrameOverSocket(port, {
      requestId: "req-02",
      taskId,
      epoch: 1,
      turnSeq: 2,
      targetHost: "evil-exfil.example.com",
      targetPath: "/steal",
      payload: {},
    });
    assert.equal(res2.statusCode, 403);
    assert.ok(res2.error?.includes("DomainBlockedByZeroTrust"));

    // 3. Advance epoch to 2 on LeaseCoordinator -> stale epoch=1 frame from zombie sandbox gets 409 StaleFenceRejected
    leaseCoord.acquireOrIncrementEpoch(taskId, "host-02", "DeepSeek"); // epoch = 2
    const res3 = await ZeroTrustVsockHostDaemon.sendFrameOverSocket(port, {
      requestId: "req-03",
      taskId,
      epoch: 1,
      turnSeq: 3,
      targetHost: "api.deepseek.com",
      targetPath: "/v1/chat/completions",
      payload: {},
    });
    assert.equal(res3.statusCode, 409);
    assert.ok(res3.error?.includes("StaleFenceRejected"));
  } finally {
    await daemon.close();
  }
});

test("M11 (TASK-56): EphemeralHeavyExecutorMigrator streams OverlayFS + TurnWAL bundle across hosts in <50ms", async () => {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-m11-mig-"));
  try {
    const overlayDriver = new CompanionContainerAndOverlayDriver(tmpDir);
    const sandboxId = "sbx-mig-01";
    overlayDriver.initOverlayWorkspace(sandboxId, {
      "Cargo.toml": '[package]\nname = "ansts"\n',
      "src/lib.rs": "pub fn add(a: i32, b: i32) -> i32 { a + b }\n",
    });

    const sourceMigrator = new EphemeralHeavyExecutorMigrator("host-source-01", overlayDriver);
    const remoteMigrator = new EphemeralHeavyExecutorMigrator("heavy-executor-gpu-02", overlayDriver);

    const remoteUrl = await remoteMigrator.startRemoteExecutorServer((bundle) => ({
      modifiedFiles: {
        "target/release/libansts.rlib": `compiled_artifact_epoch_${bundle.fence.epoch}_turn_${bundle.fence.turnSeq}`,
      },
      stdout: "Finished release [optimized] target(s) in 0.42s",
    }));

    try {
      const migResult = await sourceMigrator.migrateAndExecuteTurnBurst({
        targetExecutorBaseUrl: remoteUrl,
        taskId: "tsk-mig-01",
        sandboxId,
        fence: new FenceToken(1, 3, "tsk-mig-01"),
        toolName: "cargo_build_release",
        commandPayload: "cargo build --release",
      });

      assert.equal(migResult.executedOnHostId, "heavy-executor-gpu-02");
      assert.ok(migResult.handoverLatencyMs < 50, `Expected <50ms handover, got ${migResult.handoverLatencyMs}ms`);
      assert.ok("target/release/libansts.rlib" in migResult.updatedFilesBase64);
      assert.ok(migResult.newMerkleHash.startsWith("merkle_"));
      assert.ok(migResult.stdout.includes("Finished release"));
    } finally {
      await remoteMigrator.stopRemoteExecutorServer();
    }
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
});
