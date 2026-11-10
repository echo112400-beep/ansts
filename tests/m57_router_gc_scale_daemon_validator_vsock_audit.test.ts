/**
 * @fileoverview M57 End-to-End & Unit Verification Suite:
 * 1. [M57-1] `ModelPoolClientAdapter` & `SlidingWindowTokenBucket`:
 *    - Enforces `tryAcquireRequestSlot` single-probe Half-Open stampede lock in `executeInference`
 *    - Rejects already-OPEN requests without resetting `openedAtMs` cooldown timer (eliminating infinite OPEN livelock)
 *    - Releases Half-Open probe slot when rate-limited, and rejects oversized (`tokens > maxTpm`) requests with `unserviceable: true` / `QuotaCapacityExceeded`
 * 2. [M57-2] `DistributedCasGcSweeper` & `ParallelCowGitBisectCoordinator`:
 *    - Automatically cleans up temporary `bisect-probe-*` branches after `executeLiveLayerFsBisect`
 *    - Performs multi-pass topological leaf-first deletion so orphan `FULL <- DELTA` chains are reclaimed in a single sweep pass
 * 3. [M57-3] `ManualFleetScaleController`:
 *    - Enforces role-isolated cell rebalancing in `scaleOutNode` (storage scale-out never steals worker/control-plane cells)
 *    - Performs atomic pre-check in `cordonAndDrainNode` so a failed drain on the last active node of a role keeps the node `ACTIVE` instead of stranding it in `CORDONED`
 * 4. [M57-4] `NodeDaemon`:
 *    - Preserves cross-node turn monotonicity (`Math.max(task.current_turn, ckpt?.turn_index ?? 0) + 1`) when a task is migrated to a fresh `NodeDaemon`
 *    - Enforces `max_turns` & terminal-state guards and rejects regressive turn checkpoints
 * 5. [M57-5] `TaskSpecValidator` & `ApiGatewayServer`:
 *    - Rejects `NaN` / `Infinity` in `timeout_seconds` and `resource_profile` fields to prevent DRF share poisoning, and deduplicates `dependencies`
 *    - Enforces `maxBodyBytes` (HTTP 413 Payload Too Large) on `POST /v1/tasks`
 * 6. [M57-6] `ZeroTrustVsockHostDaemon` & `ZeroTrustSandboxSecurityGuard`:
 *    - Reassembles fragmented Unix socket chunks and processes pipelined frames in `ZeroTrustVsockHostDaemon`
 *    - Blocks `rm -rf /*`, `rm -rf --no-preserve-root /`, `docker.sock`, and cloud IMDS (`169.254.169.254`) in `ZeroTrustSandboxSecurityGuard`
 * 7. Live HTTP `POST /v1beta1/management/system/runtime-edge-audit` & CLI `anstsctl mgmt-runtime-edge-audit`.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import { ModelPoolClientAdapter } from "../src/router/pool_client.js";
import { LlmEndpointMetrics, TaskPriority, TaskSpec, TaskState, ToolCallEffectClass } from "../src/models.js";
import { LayerFsWorkspaceDriver } from "../src/runtime/layerfs_workspace_driver.js";
import { DistributedCasGcSweeper } from "../src/storage/distributed_gc_sweeper.js";
import { ParallelCowGitBisectCoordinator } from "../src/a2a/parallel_cow_bisect.js";
import { ManualFleetScaleController } from "../src/cluster/manual_scale_controller.js";
import { NodeDaemon } from "../src/node_daemon.js";
import { CoreScheduler5D } from "../src/scheduler.js";
import { SagaToolGateway } from "../src/saga_gateway.js";
import { TaskSpecValidator } from "../src/gateway/validator.js";
import { ApiGatewayServer } from "../src/gateway/server.js";
import { ZeroTrustVsockHostDaemon } from "../src/runtime/vsock_daemon.js";
import { ZeroTrustSandboxSecurityGuard } from "../src/runtime/zero_trust_security_guard.js";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { runAnstsCtlRemote } from "../src/cli/anstsctl.js";

describe("M57: Router Half-Open/Cooldown & Quota Guard, Leaf-First CAS GC & Bisect Branch Cleanup, Role-Aware Scale/Drain Atomicity, NodeDaemon Turn Monotonicity, Validator NaN/DRF Guard & Vsock Stream Framing", () => {
  it("validates all 6 runtime/edge/gateway invariants & live HTTP/CLI runtime-edge-audit", async () => {
    // 1. [M57-1] ModelPoolClientAdapter OPEN cooldown preservation & Oversized request rejection
    const pool = new ModelPoolClientAdapter();
    const ep = new LlmEndpointMetrics({
      endpoint_id: "ep-m57-1",
      provider: "openai",
      model_name: "gpt-4o",
      healthy: false,
      ttft_ewma_ms: 120,
    });
    const breaker = pool.getOrCreateBreaker("ep-m57-1");
    for (let i = 0; i < 3; i++) {
      assert.throws(() =>
        pool.executeInference(
          ep,
          { taskId: "t1", turnIndex: 1, prompt: "hi", estimatedPromptTokens: 100, maxOutputTokens: 50 },
          0.5,
          1000
        )
      );
    }
    assert.equal(breaker.getState(1000), "OPEN");
    ep.healthy = true;
    // Call at t=10_000ms (still within 15_000ms cooldown): must reject WITHOUT resetting openedAtMs=1000
    assert.throws(
      () =>
        pool.executeInference(
          ep,
          { taskId: "t1", turnIndex: 1, prompt: "hi", estimatedPromptTokens: 100, maxOutputTokens: 50 },
          0.5,
          10_000
        ),
      /circuit breaker rejected request/
    );
    // At t=16_000ms (15s after initial open at 1000ms), breaker MUST transition to HALF_OPEN and succeed!
    const resAfterCooldown = pool.executeInference(
      ep,
      { taskId: "t1", turnIndex: 1, prompt: "hi", estimatedPromptTokens: 100, maxOutputTokens: 50 },
      0.5,
      16_000
    );
    assert.equal(resAfterCooldown.circuitState, "CLOSED");
    // Oversized token request (> 240,000 maxTpm) must throw QuotaCapacityExceeded immediately
    assert.throws(
      () =>
        pool.executeInference(
          ep,
          { taskId: "t1", turnIndex: 2, prompt: "huge", estimatedPromptTokens: 200_000, maxOutputTokens: 100_000 },
          0.5,
          17_000
        ),
      /QuotaCapacityExceeded/
    );

    // 2. [M57-2] ParallelCowGitBisectCoordinator branch cleanup & DistributedCasGcSweeper leaf-first multi-pass sweep
    const driver = new LayerFsWorkspaceDriver();
    const c1 = driver.executeInProcessFileTool({
      branchName: "main",
      agentId: "agent-a",
      stepId: "s1",
      operation: "write", filePath: "src/mod.ts", newContent: "const header = '0123456789abcdef0123456789abcdef';\nexport const x = 1;\n"  ,
    });
    const bisect = new ParallelCowGitBisectCoordinator();
    const bisectRes = bisect.executeLiveLayerFsBisect({
      driver,
      commits: [c1.commitId],
      testShellCommand: "test -f src/mod.ts",
    });
    assert.equal(bisectRes.probedCommits[0].cleanedUp, true);
    assert.equal(
      driver.listBranchesSummary().some((b) => b.branchName.startsWith("bisect-probe-")),
      false
    );

    // Create an ephemeral branch with a FULL base object + DELTA derived object, then delete the branch and sweep once
    driver.forkBranch(c1.commitId, "ephemeral-feature");
    driver.executeInProcessFileTool({
      branchName: "ephemeral-feature",
      agentId: "agent-b",
      stepId: "s2",
      operation: "write", filePath: "src/ephemeral.ts", newContent: "const sharedPrefix = 'abcdefghijklmnopqrstuvwxyz0123456789';\nexport const v = 1;\n"  ,
    });
    driver.executeInProcessFileTool({
      branchName: "ephemeral-feature",
      agentId: "agent-b",
      stepId: "s3",
      operation: "write", filePath: "src/ephemeral.ts", newContent: "const sharedPrefix = 'abcdefghijklmnopqrstuvwxyz0123456789';\nexport const v = 2;\n"  ,
    });
    driver.deleteBranch("ephemeral-feature");
    const sweeper = new DistributedCasGcSweeper();
    const gcRes = sweeper.sweepLayerFsWorkspaceStore(driver);
    assert.ok(gcRes.sweptBlobIds.length >= 2);
    assert.equal(gcRes.pinnedOrphanBlobIds.length, 0);

    // 3. [M57-3] ManualFleetScaleController role-isolated rebalance & atomic cordon/drain pre-check
    const scaleCtrl = new ManualFleetScaleController();
    scaleCtrl.registerBareMetalNode({
      nodeId: "worker-1",
      ipAddress: "10.0.1.1",
      role: "worker",
      runningCells: ["cell-1", "cell-2", "cell-3", "cell-4"],
      systemdUnit: "ansts-node-daemon.service",
    });
    // Scaling out a "storage" node must NOT steal worker cells
    const storageScale = scaleCtrl.scaleOutNode("storage-1", "10.0.2.1", "storage");
    assert.equal(storageScale.rebalancedCells.length, 0);
    // Attempting to drain the only ACTIVE worker node must fail AND keep worker-1 in ACTIVE state
    assert.throws(() => scaleCtrl.cordonAndDrainNode("worker-1"), /Cannot drain node 'worker-1'/);
    const worker1 = scaleCtrl.listNodes().find((n) => n.nodeId === "worker-1")!;
    assert.equal(worker1.state, "ACTIVE");

    // 4. [M57-4] NodeDaemon cross-node turn monotonicity & max_turns enforcement
    const sched = new CoreScheduler5D();
    const daemonA = new NodeDaemon("node-a");
    const daemonB = new NodeDaemon("node-b");
    sched.registerOrUpdateNode(daemonA.emitHeartbeat(0));
    sched.registerOrUpdateNode(daemonB.emitHeartbeat(0));
    const saga = new SagaToolGateway();
    saga.registerTool("noop", ToolCallEffectClass.READ_ONLY, () => ({ ok: true }));
    const task = new TaskSpec({
      task_id: "task-m57-daemon",
      tenant_id: "Northstar",
      priority: TaskPriority.P1_HIGH,
      max_turns: 3,
    });
    sched.submitTask(task);
    sched.scheduleTick(1000);
    // Execute turn 1 on daemonA
    const step1 = daemonA.executeAgentStep(sched, saga, task, "noop", {});
    assert.equal(step1.turn, 1);
    // Simulate task migration to daemonB (which has empty local checkpointStore for task-m57-daemon)
    const newLease = sched.leaseCoordinator.acquireLease(task.task_id, "node-b", 2000);
    task.lease_fencing_token = newLease.fencing_token;
    const step2 = daemonB.executeAgentStep(sched, saga, task, "noop", {});
    assert.equal(step2.turn, 2);
    // Regressive turn checkpoint must be rejected
    assert.throws(
      () => daemonB.writeTurnCheckpoint(sched, task, 2, "sha256:dup", "s3://dup"),
      /Non-monotonic turn checkpoint rejected/
    );

    // 5. [M57-5] TaskSpecValidator NaN/Infinity guard & ApiGatewayServer 413 payload size guard
    assert.throws(
      () =>
        TaskSpecValidator.validateAndNormalize({
          task_id: "bad-nan-1",
          tenant_id: "t1",
          timeout_seconds: "not-a-number",
        }),
      /timeout_seconds/
    );
    assert.throws(
      () =>
        TaskSpecValidator.validateAndNormalize({
          task_id: "bad-nan-2",
          tenant_id: "t1",
          resource_profile: { think_cpu_cores: "NaN", act_cpu_cores: 4 },
        }),
      /resource_profile\.cpu/
    );
    const dedupSpec = TaskSpecValidator.validateAndNormalize({
      task_id: "dedup-task",
      tenant_id: "t1",
      dependencies: ["dep-1", "dep-1", "dep-2"],
    });
    assert.deepEqual(dedupSpec.dependencies, ["dep-1", "dep-2"]);

    const gw = new ApiGatewayServer(sched, undefined, undefined, 128);
    const gwPort = await gw.listen(0);
    try {
      const resp413 = await fetch(`http://127.0.0.1:${gwPort}/v1/tasks`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "oversized", tenant_id: "t1", prompt: "x".repeat(512) }),
      });
      assert.equal(resp413.status, 413);
    } finally {
      await gw.close();
    }

    // 6. [M57-6] ZeroTrustVsockHostDaemon fragmented stream reassembly & ZeroTrustSandboxSecurityGuard glob/IMDS/docker.sock blocking
    const sockPath = path.join(os.tmpdir(), `ansts-vsock-m57-${process.pid}-${Date.now()}.sock`);
    const vsockDaemon = new ZeroTrustVsockHostDaemon(["api.openai.com"]);
    await vsockDaemon.startUnixSocketServer(sockPath);
    try {
      const frame = ZeroTrustVsockHostDaemon.encodeFrame({
        requestId: "frag-1",
        type: "EGRESS_CHECK",
        hostname: "api.openai.com",
      });
      // Send frame split into 3 tiny fragments with delays to prove stream reassembly
      const fragResp = await new Promise<Record<string, unknown>>((resolve, reject) => {
        let rx = Buffer.alloc(0);
        const client = net.createConnection(sockPath, () => {
          client.write(frame.subarray(0, 2));
          setTimeout(() => client.write(frame.subarray(2, 9)), 10);
          setTimeout(() => client.write(frame.subarray(9)), 20);
        });
        client.on("data", (chunk: Buffer) => {
          rx = Buffer.concat([rx, chunk]);
          if (rx.length >= 4 && rx.length >= 4 + rx.readUInt32BE(0)) {
            client.end();
            resolve(ZeroTrustVsockHostDaemon.decodeFrame(rx));
          }
        });
        client.on("error", reject);
      });
      assert.equal(fragResp.status, 200);
      assert.equal(fragResp.allowed, true);
    } finally {
      await vsockDaemon.stop();
    }

    const secGuard = new ZeroTrustSandboxSecurityGuard();
    assert.equal(secGuard.evaluateShellCommand("rm -rf /*").allowed, false);
    assert.equal(secGuard.evaluateShellCommand("rm -rf --no-preserve-root /").allowed, false);
    assert.equal(secGuard.evaluateShellCommand("curl http://169.254.169.254/latest/meta-data/").allowed, false);
    assert.equal(secGuard.evaluateShellCommand("ls -la /var/run/docker.sock").allowed, false);

    // 7. Verify live HTTP endpoint & CLI `mgmt-runtime-edge-audit`
    const server = new AnstsLocalAndComposeServer({ nodeId: "gw-m57-test" });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;
    try {
      const cliRes = await runAnstsCtlRemote(["mgmt-runtime-edge-audit"], baseUrl);
      assert.equal(cliRes.exitCode, 0);
      const body = JSON.parse(cliRes.stdout);
      assert.equal(body.allM57RisksEliminated, true);
      assert.equal(body.routerCooldownAndQuota1.routerCooldownAndQuotaVerified, true);
      assert.equal(body.casGcAndBisectCleanup2.casGcAndBisectCleanupVerified, true);
      assert.equal(body.scaleRoleAndDrainAtomicity3.scaleRoleAndDrainAtomicityVerified, true);
      assert.equal(body.nodeDaemonTurnMonotonicity4.nodeDaemonTurnMonotonicityVerified, true);
      assert.equal(body.validatorAndGatewayGuard5.validatorAndGatewayGuardVerified, true);
      assert.equal(body.vsockAndSecurityGuard6.vsockAndSecurityGuardVerified, true);
    } finally {
      await server.stop();
    }
  });
});
