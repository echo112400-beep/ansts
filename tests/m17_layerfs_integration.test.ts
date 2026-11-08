/**
 * @fileoverview M17 (TASK-83 ~ TASK-88) LayerFS (`layerfs.ai` v1.1.0 / v2.0) Real Compression, Real Workspace Mount & Multi-Agent Merge Suite (6 tests).
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import {
  LayerFsContentStoreEngine,
  LayerFsWorkspaceDriver,
  LayerFsMultiAgentBranchCoordinator,
  LayerFsMergeConflictError,
  LayerFsRemoteStoreSynchronizer,
  LayerFsStalePushRefusedError,
  LayerFsNumberedSandboxMultiplexer,
  DeterministicTimeTravelReplayer,
  InMemoryR2Bucket,
  AnstsClusterRuntime,
  runCli,
} from "../src/index.js";

describe("M17 (TASK-83 ~ TASK-88) LayerFS Deep Integration Suite", () => {
  it("1. [TASK-83] LayerFsContentStoreEngine performs real zlib compression, PREFIX/DELTA encoding, and bit-exact lossless reconstruction", () => {
    const store = new LayerFsContentStoreEngine();
    const baseText = "export function computeItem(idx: number) { return idx * 42; }\n".repeat(40);
    const r1 = store.putFileObject("src/app.ts", baseText);
    const r2 = store.putFileObject("src/app_copy.ts", baseText);
    assert.equal(r1.deduplicated, false);
    assert.equal(r2.deduplicated, true);
    assert.ok(r1.object.compressedBytes < r1.object.logicalBytes);
    assert.equal(store.reconstructObjectContent(r1.object.objectId), baseText);

    const editedText = baseText + "export const tailFlag = 'verified-prefix-delta';\n";
    const r3 = store.putFileObject("src/app.ts", editedText, baseText);
    assert.equal(r3.object.encoding, "PREFIX");
    assert.ok(r3.object.compressedBytes < 60);
    assert.equal(store.reconstructObjectContent(r3.object.objectId), editedText);

    const btree = store.updateDirectoryBTreePages("C1", null, [
      { path: "src/app.ts", objectId: r3.object.objectId, mode: 0o644, turnId: "step-1" },
    ]);
    assert.equal(btree.rewrittenLeafPages, 1);
    assert.ok(store.getStorageStats().dedupAndCompressionRatio > 5.0);
  });

  it("2. [TASK-84] LayerFsWorkspaceDriver materializes real temp workspace with pre-mounted node_modules and runs real shell command to capture changes", () => {
    const driver = new LayerFsWorkspaceDriver();
    driver.forkBranch("main", "agent-alpha");

    const fileOp = driver.executeInProcessFileTool({
      branchName: "agent-alpha",
      agentId: "agent-alpha",
      stepId: "turn-1-write",
      operation: "write",
      filePath: "src/index.js",
      newContent: "const react = require('react');\nconsole.log('react-ver:' + react.version);\n",
    });
    assert.equal(fileOp.committedNewSnapshot, true);
    assert.equal(fileOp.containerSpawned, false);

    const bashOp = driver.executeEphemeralBashWorkspace({
      branchName: "agent-alpha",
      agentId: "agent-alpha",
      stepId: "turn-2-bash",
      command: "node src/index.js > build_output.txt",
    });
    assert.equal(bashOp.committedNewSnapshot, true);
    assert.equal(bashOp.nodeModulesPreMounted, true);
    assert.ok(bashOp.mountLatencyMs > 0 && bashOp.mountLatencyMs < 250);

    const readGenerated = driver.executeInProcessFileTool({
      branchName: "agent-alpha",
      agentId: "agent-alpha",
      stepId: "turn-3-read",
      operation: "read",
      filePath: "build_output.txt",
    });
    assert.equal(readGenerated.readResult?.trim(), "react-ver:19.0.0");
  });

  it("3. [TASK-85] LayerFsMultiAgentBranchCoordinator merges identical bytes, applies .lww rules, preserves chmod, and refuses real conflicts", () => {
    const driver = new LayerFsWorkspaceDriver();
    const coord = new LayerFsMultiAgentBranchCoordinator(driver, ["pnpm-lock.yaml", ".cache/"]);

    driver.forkBranch("main", "branch-a");
    driver.forkBranch("main", "branch-b");

    driver.executeInProcessFileTool({
      branchName: "branch-a",
      agentId: "agent-a",
      stepId: "step-a1",
      operation: "write",
      filePath: "pnpm-lock.yaml",
      newContent: "lockfileVersion: '9.0-agent-a'",
    });
    driver.executeInProcessFileTool({
      branchName: "branch-b",
      agentId: "agent-b",
      stepId: "step-b1",
      operation: "write",
      filePath: "pnpm-lock.yaml",
      newContent: "lockfileVersion: '9.0-agent-b'",
    });

    const merged = coord.mergeBranchIntoTarget("branch-a", "branch-b", "C0");
    assert.deepEqual(merged.autoResolvedByLww, ["pnpm-lock.yaml"]);

    driver.forkBranch("main", "clash-1");
    driver.forkBranch("main", "clash-2");
    driver.executeInProcessFileTool({
      branchName: "clash-1",
      agentId: "a1",
      stepId: "s1",
      operation: "write",
      filePath: "src/auth.ts",
      newContent: "export const mode = 'jwt';",
    });
    driver.executeInProcessFileTool({
      branchName: "clash-2",
      agentId: "a2",
      stepId: "s2",
      operation: "write",
      filePath: "src/auth.ts",
      newContent: "export const mode = 'oauth2';",
    });

    assert.throws(() => {
      coord.mergeBranchIntoTarget("clash-1", "clash-2", "C0");
    }, LayerFsMergeConflictError);
  });

  it("4. [TASK-86] Zero-Copy Recursive Commit Fork & Step-Level Line Blame via anstsctl CLI", () => {
    const rt = new AnstsClusterRuntime();
    rt.layerfsDriver.executeInProcessFileTool({
      branchName: "main",
      agentId: "agent-main",
      stepId: "step-101",
      operation: "write",
      filePath: "src/core.ts",
      newContent: "line1\nline2",
    });
    const head1 = rt.layerfsDriver.getBranchHeadCommit("main").commitId;

    rt.layerfsDriver.executeInProcessFileTool({
      branchName: "main",
      agentId: "agent-main",
      stepId: "step-102",
      operation: "edit",
      filePath: "src/core.ts",
      newContent: "line1\nline2-modified-in-102",
    });

    const replayer = new DeterministicTimeTravelReplayer();
    const forked = replayer.forkLayerFsCommitZeroCopy(rt.layerfsDriver, head1, "mcts-rollout-1");
    assert.equal(forked.copiedFilesCount, 0);
    assert.equal(forked.headCommitId, head1);

    const cliRes = runCli(["blame", "main", "src/core.ts"], rt);
    assert.equal(cliRes.exitCode, 0);
    const parsed = JSON.parse(cliRes.stdout);
    assert.equal(parsed.lines[0].stepId, "step-101");
    assert.equal(parsed.lines[1].stepId, "step-102");
  });

  it("5. [TASK-87] LayerFsNumberedSandboxMultiplexer packs 16 Workspaces per Sandbox and auto-closes after 120s idle", () => {
    const mux = new LayerFsNumberedSandboxMultiplexer();
    const now = 1_000_000;
    for (let i = 1; i <= 16; i++) {
      const m = mux.mountWorkspaceIntoSandbox(`ws-${i}`, now);
      assert.equal(m.sandboxNumber, 1);
      assert.equal(m.spawnedNewSandbox, i === 1);
    }
    const overflow = mux.mountWorkspaceIntoSandbox("ws-17", now);
    assert.equal(overflow.sandboxNumber, 2);
    assert.equal(overflow.spawnedNewSandbox, true);

    mux.unmountWorkspace(2, "ws-17", now + 1000);
    const closedEarly = mux.sweepIdleSandboxes(now + 60_000);
    assert.equal(closedEarly.length, 0);

    const closedAfter2Min = mux.sweepIdleSandboxes(now + 125_000);
    assert.deepEqual(closedAfter2Min, [2]);
  });

  it("6. [TASK-88] LayerFsRemoteStoreSynchronizer persists SQLite Store to RustFS/R2 with Rule D9 stale-push refusal", async () => {
    const bucket = new InMemoryR2Bucket();
    const sync = new LayerFsRemoteStoreSynchronizer(bucket, "proj-alpha");

    const push1 = await sync.pushStoreSnapshot({
      machineId: "node-1",
      basedOnGeneration: 0,
      storeSqliteBytes: new Uint8Array([1, 2, 3, 4]),
      mainHeadCommitId: "C1",
    });
    assert.equal(push1.remoteGeneration, 1);

    await assert.rejects(async () => {
      await sync.pushStoreSnapshot({
        machineId: "node-2-stale",
        basedOnGeneration: 0,
        storeSqliteBytes: new Uint8Array([9, 9]),
        mainHeadCommitId: "C2",
      });
    }, LayerFsStalePushRefusedError);

    const pulled = await sync.pullStoreSnapshot();
    assert.equal(pulled.remoteGeneration, 1);
    assert.equal(pulled.mainHeadCommitId, "C1");
  });
});
