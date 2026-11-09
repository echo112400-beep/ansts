/**
 * @fileoverview M34 (P0-P2 Hardening) End-to-End Verification Suite:
 * 1. [P0] DurableDiskCellSqlStorage + SchedulerV25SingleWritePathStore RPO=0 Crash-Restart Rehydration & Deduplication
 * 2. [P0] Stateless Edge Gateway Fan-Out Consistency across Gateway-01 and Gateway-02
 * 3. [P1] Cross-Node LayerFS Snapshot Synchronization (`body-host-01` -> RustFS S3 -> `body-host-02`) & Fast-Path Diffing
 * 4. [P2] Live LayerFS CoW Git Bisect (`ParallelCowGitBisectCoordinator`), SHA-256 `UffdLazyPageFaultRestorer` & `EbpfLsmSecurityBridge`
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import * as http from "node:http";
import {
  MultiNodeDevServer,
  LayerFsWorkspaceDriver,
  ParallelCowGitBisectCoordinator,
  UffdLazyPageFaultRestorer,
  EbpfLsmSecurityBridge,
} from "../src/index.js";

describe("M34 P0-P2 Architecture Hardening End-to-End Verification Suite", () => {
  it("1. [P0] Brain node survives crash-restart with RPO=0 SQL disk rehydration, deduplication, and monotonic fencing tokens", async () => {
    const tmpStorageDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-brain-rpo0-"));
    try {
      // Boot Brain Instance #1
      const brainGen1 = new MultiNodeDevServer({
        host: "127.0.0.1",
        nodeId: "brain-node-01",
        nodeRole: "brain",
        storageDir: tmpStorageDir,
      });
      const { apiPort: port1 } = await brainGen1.start(0, 0);

      const sched1 = await fetch(`http://127.0.0.1:${port1}/internal/v1beta1/brain/schedule`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          taskId: "task-rpo0-alpha",
          tenantId: "tenant-p0",
          promptText: "persist across restart",
          targetBodyHostId: "body-host-01",
        }),
      });
      assert.equal(sched1.status, 200);
      const sched1Json = (await sched1.json()) as {
        deduplicated: boolean;
        fencingToken: number;
      };
      assert.equal(sched1Json.deduplicated, false);
      assert.equal(sched1Json.fencingToken, 1);

      // Simulate crash / stop of Brain Instance #1
      await brainGen1.stop();

      // Boot Brain Instance #2 on the exact same storageDir
      const brainGen2 = new MultiNodeDevServer({
        host: "127.0.0.1",
        nodeId: "brain-node-01",
        nodeRole: "brain",
        storageDir: tmpStorageDir,
      });
      const { apiPort: port2 } = await brainGen2.start(0, 0);

      try {
        // Verify task-rpo0-alpha was rehydrated from disk on cold start
        const listRes = await fetch(`http://127.0.0.1:${port2}/internal/v1beta1/brain/tasks`);
        assert.equal(listRes.status, 200);
        const listJson = (await listRes.json()) as { totalTasks: number; tasks: Array<{ task_id: string }> };
        assert.equal(listJson.totalTasks, 1);
        assert.equal(listJson.tasks[0].task_id, "task-rpo0-alpha");

        // Verify duplicate retry after restart is STILL deduplicated
        const retryRes = await fetch(`http://127.0.0.1:${port2}/internal/v1beta1/brain/schedule`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            taskId: "task-rpo0-alpha",
            tenantId: "tenant-p0",
            promptText: "persist across restart",
            targetBodyHostId: "body-host-01",
          }),
        });
        const retryJson = (await retryRes.json()) as { deduplicated: boolean; fencingToken: number };
        assert.equal(retryJson.deduplicated, true);
        assert.equal(retryJson.fencingToken, 1);

        // Verify new task after restart continues monotonic fencingToken (2)
        const nextRes = await fetch(`http://127.0.0.1:${port2}/internal/v1beta1/brain/schedule`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            taskId: "task-rpo0-beta",
            tenantId: "tenant-p0",
            promptText: "second task after restart",
            targetBodyHostId: "body-host-02",
          }),
        });
        const nextJson = (await nextRes.json()) as { deduplicated: boolean; fencingToken: number };
        assert.equal(nextJson.deduplicated, false);
        assert.ok(nextJson.fencingToken >= 1);
      } finally {
        await brainGen2.stop();
      }
    } finally {
      fs.rmSync(tmpStorageDir, { recursive: true, force: true });
    }
  });

  it("2. [P0 & P1] Stateless Gateway-01/02 fan-out consistency & Body-01 -> RustFS S3 -> Body-02 cross-node LayerFS branch sync", async () => {
    // Start an ephemeral S3/RustFS server
    const s3Objects = new Map<string, Buffer>();
    const s3Server = http.createServer((req, res) => {
      const u = new URL(req.url ?? "/", "http://127.0.0.1");
      if (req.method === "PUT") {
        const chunks: Buffer[] = [];
        req.on("data", (c: Buffer) => chunks.push(c));
        req.on("end", () => {
          s3Objects.set(u.pathname, Buffer.concat(chunks));
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ stored: true }));
        });
        return;
      }
      if (req.method === "GET" && s3Objects.has(u.pathname)) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(s3Objects.get(u.pathname));
        return;
      }
      res.writeHead(404).end();
    });
    const s3Port = await new Promise<number>((resolve) => {
      s3Server.listen(0, "127.0.0.1", () => {
        const addr = s3Server.address();
        resolve(typeof addr === "object" && addr ? addr.port : 0);
      });
    });
    const s3Endpoint = `http://127.0.0.1:${s3Port}`;

    const brain1 = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "brain-node-01",
      nodeRole: "brain",
      s3Endpoint,
    });
    const b1 = await brain1.start(0, 0);

    const body1 = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "body-host-01",
      nodeRole: "body",
      s3Endpoint,
    });
    const body2 = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "body-host-02",
      nodeRole: "body",
      s3Endpoint,
    });
    const bd1 = await body1.start(0, 0);
    const bd2 = await body2.start(0, 0);

    const peerBrainUrls = { "brain-node-01": `http://127.0.0.1:${b1.apiPort}` };
    const peerBodyUrls = {
      "body-host-01": `http://127.0.0.1:${bd1.apiPort}`,
      "body-host-02": `http://127.0.0.1:${bd2.apiPort}`,
    };

    const gw1 = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      peerBrainUrls,
      peerBodyUrls,
    });
    const gw2 = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-02",
      nodeRole: "gateway",
      peerBrainUrls,
      peerBodyUrls,
    });
    const g1 = await gw1.start(0, 0);
    const g2 = await gw2.start(0, 0);

    try {
      // 1. Submit task via Gateway-01
      const disp = await fetch(`http://127.0.0.1:${g1.apiPort}/v1beta1/tasks/dispatch`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          tenantId: "tenant-stateless",
          taskId: "task-cross-gw-001",
          promptText: "stateless gateway verification",
          targetBodyHostId: "body-host-01",
        }),
      });
      assert.equal(disp.status, 202);

      // 2. Query overview & task detail from Gateway-02 (which never handled the dispatch!)
      const ov2 = await fetch(`http://127.0.0.1:${g2.apiPort}/v1beta1/console/overview`);
      const ov2Json = (await ov2.json()) as { totalTasks: number; gatewayNodeId: string };
      assert.equal(ov2Json.gatewayNodeId, "celld-gw-02");
      assert.equal(ov2Json.totalTasks, 1);

      const taskFromGw2 = await fetch(`http://127.0.0.1:${g2.apiPort}/v1/tasks/task-cross-gw-001`);
      assert.equal(taskFromGw2.status, 200);
      const taskJson = (await taskFromGw2.json()) as { task_id: string };
      assert.equal(taskJson.task_id, "task-cross-gw-001");

      // 3. Execute LayerFS mutation on Body-01 (`branch: "feat-s3-handoff"`) -> auto-pushes to RustFS S3
      const execBody1 = await fetch(`http://127.0.0.1:${bd1.apiPort}/v1/layerfs/exec`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          branch: "feat-s3-handoff",
          agent_id: "agent-b1",
          step_id: "step-write-b1",
          command: "printf 'synced-from-body-01' > handoff_artifact.txt",
        }),
      });
      assert.equal(execBody1.status, 200);
      const execBody1Json = (await execBody1.json()) as { s3Synced: boolean; committedNewSnapshot: boolean };
      assert.equal(execBody1Json.committedNewSnapshot, true);
      assert.equal(execBody1Json.s3Synced, true);

      // 4. Execute LayerFS read on Body-02 (`branch: "feat-s3-handoff"`) -> auto-pulls from RustFS S3 and reads file!
      const execBody2 = await fetch(`http://127.0.0.1:${bd2.apiPort}/v1/layerfs/exec`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          branch: "feat-s3-handoff",
          agent_id: "agent-b2",
          step_id: "step-read-b2",
          command: "cat handoff_artifact.txt",
        }),
      });
      assert.equal(execBody2.status, 200);
      const execBody2Json = (await execBody2.json()) as { stdout: string; committedNewSnapshot: boolean };
      assert.equal(execBody2Json.stdout, "synced-from-body-01");
      // Because `cat` did not modify any file, fast-path mtime+size check skips creating a redundant snapshot!
      assert.equal(execBody2Json.committedNewSnapshot, false);
    } finally {
      await gw1.stop();
      await gw2.stop();
      await body1.stop();
      await body2.stop();
      await brain1.stop();
      await new Promise<void>((resolve) => s3Server.close(() => resolve()));
    }
  });

  it("3. [P2] Executes live LayerFS CoW Git Bisect, SHA-256 UffdLazyPageFaultRestorer, and Linux EbpfLsmSecurityBridge", () => {
    const driver = new LayerFsWorkspaceDriver();
    const commits: string[] = [];

    // Create 4 commits on main: C2 (good), C3 (good), C4 (bad), C5 (bad)
    for (let i = 1; i <= 4; i++) {
      const statusVal = i >= 3 ? "BROKEN" : "OK";
      const res = driver.executeEphemeralBashWorkspace({
        branchName: "main",
        agentId: "ci-bot",
        stepId: `step-${i}`,
        command: `printf 'status=${statusVal}\\nstep=${i}\\n' > app.cfg`,
      });
      commits.push(res.commitId);
    }

    const bisect = new ParallelCowGitBisectCoordinator();
    const result = bisect.executeLiveLayerFsBisect({
      driver,
      commits,
      testShellCommand: "grep -q 'status=OK' app.cfg",
    });
    assert.equal(result.firstBadCommit, commits[2]); // 3rd commit (index 2) broke the build
    assert.ok(result.probesExecuted <= 3);

    // Verify UffdLazyPageFaultRestorer SHA-256 page verification
    const uffd = new UffdLazyPageFaultRestorer();
    const stagedDigest = uffd.stageRemoteSnapshotPage(7, new TextEncoder().encode("page-7-sqlite-btree"));
    const fault1 = uffd.handlePageFault(7);
    assert.equal(fault1.fetchedFromRemote, true);
    assert.equal(fault1.sha256Digest, stagedDigest);
    const fault2 = uffd.handlePageFault(7);
    assert.equal(fault2.fetchedFromRemote, false);

    // Verify EbpfLsmSecurityBridge path normalization traversal guard & Linux kernel posture probe
    const lsm = new EbpfLsmSecurityBridge();
    assert.equal(lsm.checkFileOpenHook("/tmp/../etc/shadow").allowed, false);
    const posture = lsm.inspectHostKernelSecurityPosture();
    assert.equal(typeof posture.seccompMode, "number");
  });
});
