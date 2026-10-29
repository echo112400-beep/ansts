/**
 * Full M1 ~ M6 Verification & 5 Production Chaos Benchmarks in TypeScript (TASK-01 ~ TASK-32).
 */

import { strict as assert } from "assert";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { test } from "node:test";
import * as yaml from "js-yaml";

import {
  AgentTask,
  AnstsClusterRuntime,
  BayesianTaskProfiler,
  BidirectionalBackpressureController,
  CgroupPhaseController,
  CloudEventsBus,
  DashboardBridgeService,
  DeterministicTimeTravelReplayer,
  EgressPolicyViolationError,
  FenceToken,
  IngressBackpressureController,
  LeaseCoordinator,
  LinuxPsiMemoryGuard,
  ModelPoolClientAdapter,
  runCli,
  SlidingWindowTokenBucket,
  SseEventStreamGateway,
  StagingCommitAndApprovalGate,
  StaleFenceError,
  SyntacticAndVelocityBreaker,
  TaskSpecValidationError,
  TaskSpecValidator,
  TaskState,
  ThreeStateCircuitBreaker,
  TransactionalOutboxDispatcher,
  TurnWalEngine,
  WalCompactorAndPagedRestorer,
  WarmSandboxPoolManager,
  ZeroTrustVsockBridge,
} from "../src/index";

test("M1 (TASK-01..05): Gateway Validator, Idempotency, L3 429, SSE Credit Coalescing & Bayesian Persistence", () => {
  const cluster = new AnstsClusterRuntime();
  const specPath = path.join(__dirname, "..", "..", "specs", "taskspec_v1beta1_example.yaml");
  const sampleSpec = yaml.load(fs.readFileSync(specPath, "utf8")) as Record<string, any>;

  const validator = new TaskSpecValidator();
  const norm = validator.validateAndNormalize(sampleSpec);
  assert.equal(norm.spec.cognitive_policy.primary_model, "DeepSeek");

  assert.throws(
    () =>
      validator.validateAndNormalize({
        apiVersion: "scheduler.agent.io/v1beta1",
        kind: "AgentTask",
        metadata: { task_id: "t1", tenant_id: "tn" },
        spec: { cognitive_policy: { primary_model: "InvalidModel" } },
      }),
    TaskSpecValidationError,
  );

  for (let i = 0; i < 10; i++) {
    const res = cluster.apiGateway.submitTaskSpec(sampleSpec, 1.0);
    assert.ok(res.statusCode === 200 || res.statusCode === 202);
    if (i > 0) assert.equal(res.body.deduplicated, true);
  }
  assert.equal(cluster.scheduler.tasks.size, 1);

  const bp = new IngressBackpressureController(2);
  const dec = bp.evaluateAdmission("fin-enterprise", 2);
  assert.equal(dec.allowed, false);
  assert.equal(dec.statusCode, 429);
  assert.ok("Retry-After" in dec.headers);

  const sse = new SseEventStreamGateway();
  const sub = sse.registerClient("cli_1", 100);
  sub.availableCredits = 15;
  assert.equal(sse.publishEvent("cli_1", "agent.harness.thinking.delta", "tsk_1", "Step 1 ").length, 0);
  const f2 = sse.publishEvent("cli_1", "agent.harness.thinking.delta", "tsk_1", "done.");
  assert.equal(f2.length, 1);
  assert.equal(f2[0].payload, "Step 1 done.");

  const profiler = new BayesianTaskProfiler(3);
  const tProf = new AgentTask({ taskId: "t_prof", tenantId: "fin", maxTotalTokens: 500000, usedTokens: 120000 });
  for (let i = 0; i < 5; i++) profiler.recordCompletion(tProf, 0.80);

  const tmpFile = path.join(os.tmpdir(), `ansts_prof_${Date.now()}.json`);
  profiler.saveToFile(tmpFile);
  const loaded = new BayesianTaskProfiler(3);
  loaded.loadFromFile(tmpFile);
  const soft = loaded.assignInitialP50Reservation(tProf);
  assert.ok(soft >= 100000 && soft <= 150000);
  fs.unlinkSync(tmpFile);
});

test("M2 (TASK-06..11): Aging+EDF Zero Starvation, PreemptToHibernate & Concurrent CAS Leases", () => {
  const cluster = new AnstsClusterRuntime();
  const p4Task = new AgentTask({
    taskId: "tsk_p4",
    tenantId: "fin-enterprise",
    priorityClass: "P4",
    baseScore: 200,
    enqueueTs: 0.0,
    deadlineTs: 265.0,
  });
  const p1Task = new AgentTask({
    taskId: "tsk_p1",
    tenantId: "fin-enterprise",
    priorityClass: "P1",
    baseScore: 750,
    enqueueTs: 250.0,
    deadlineTs: 850.0,
  });
  cluster.queueMgr.enqueue(p4Task);
  cluster.queueMgr.enqueue(p1Task);
  const popped = cluster.queueMgr.popBestForTenant("fin-enterprise", 260.0)!;
  assert.equal(popped.taskId, "tsk_p4");

  const node1 = cluster.scheduler.nodes.get("worker-us-west-01")!;
  node1.cpuCap = 2.0;
  cluster.scheduler.nodes.get("worker-us-west-02")!.alive = false;
  cluster.scheduler.nodes.get("worker-us-west-03")!.alive = false;

  const p4Run = new AgentTask({
    taskId: "tsk_p4_run",
    tenantId: "fin-enterprise",
    priorityClass: "P4",
    baseScore: 180,
    execCpu: 2.0,
  });
  cluster.scheduler.submitTask(p4Run);
  cluster.scheduler.scheduleTick(1.0);
  assert.equal(p4Run.state, TaskState.RUNNING);

  const p0Urgent = new AgentTask({
    taskId: "tsk_p0_urgent",
    tenantId: "fin-enterprise",
    priorityClass: "P0",
    baseScore: 960,
    execCpu: 2.0,
  });
  cluster.scheduler.submitTask(p0Urgent);
  cluster.scheduler.scheduleTick(2.0);
  assert.equal(p4Run.state, TaskState.HIBERNATED);
  assert.equal(p0Urgent.state, TaskState.RUNNING);

  const lc = new LeaseCoordinator();
  const winners = Array.from({ length: 10 }, (_, i) =>
    lc.tryCasAcquire("tsk_cas_race", `node_${i}`, "DeepSeek", 0),
  ).filter(Boolean);
  assert.equal(winners.length, 1);
  assert.equal(winners[0]!.epoch, 1);
});

test("M3 (TASK-12..17): Pool Client Adapter, Token Bucket Pacing, 3-State Breaker & Approval Gate", () => {
  const adapter = new ModelPoolClientAdapter();
  const { chunks, summary } = adapter.streamCompletion("DeepSeek", "high", 500);
  assert.equal(chunks.length, 2);
  assert.ok(summary.thinkingTokens > 0);

  const bucket = new SlidingWindowTokenBucket(100, 200);
  assert.equal(bucket.consumeWithPacing(150, 1.0), 0.0);
  assert.ok(bucket.consumeWithPacing(150, 1.0) > 0.0);

  const breaker = new ThreeStateCircuitBreaker("DeepSeek", 0.25, 15.0, 5.0);
  for (let i = 0; i < 4; i++) breaker.recordOutcome(false, 1.2, 10.0);
  assert.equal(breaker.state, "OPEN");
  assert.equal(breaker.allowRequest(12.0), false);
  assert.equal(breaker.allowRequest(16.0), true);
  assert.equal(breaker.state, "HALF_OPEN");
  breaker.recordOutcome(true, 0.4, 16.5);
  assert.equal(breaker.state, "CLOSED");

  const gate = new StagingCommitAndApprovalGate();
  const task = new AgentTask({ taskId: "tsk_app", tenantId: "fin-enterprise", state: TaskState.RUNNING });
  const fence = new FenceToken(1, 3, "tsk_app");
  gate.stageWrite(task, fence, "settlement_ledger_post", "amt=9000", true);
  assert.equal(task.state, TaskState.HIBERNATED);
  assert.equal(gate.atomicFlush("tsk_app"), 0);
  gate.approveAndResume(task);
  assert.equal(task.state, TaskState.RUNNING);
  assert.equal(gate.atomicFlush("tsk_app"), 1);
});

test("M4 (TASK-18..23): Warm Pool, 20-Sandbox Burst Semaphore, Linux PSI Guard, VSOCK & AST Loop Breaker", () => {
  const wp = new WarmSandboxPoolManager(3);
  const inst = wp.acquireWarmSandbox("pool-py312-ds", "firecracker");
  assert.ok(inst.bootLatencyMs <= 65);
  assert.equal(wp.teardownAndZeroBlocks(inst), true);

  const pc = new CgroupPhaseController(32.0, 4);
  const tasks = Array.from({ length: 20 }, (_, i) =>
    new AgentTask({ taskId: `tsk_b_${i}`, tenantId: "codegen-pro", execCpu: 1.0, burstCpu: 8.0 }),
  );
  tasks.forEach((t) => pc.enterExecutionPhase(t));
  assert.equal(pc.activeBurstTasks.size, 4);
  const microQueued = tasks.filter((t) => pc.appliedLimits.get(t.taskId)!.microQueuedMs > 0);
  assert.equal(microQueued.length, 16);

  tasks.slice(0, 3).forEach((t) => {
    t.state = TaskState.RUNNING;
    pc.enterCognitivePhase(t);
  });
  const psiGuard = new LinuxPsiMemoryGuard(40.0);
  const hib = psiGuard.evaluateAndHibernate(52.0, tasks.slice(0, 3));
  assert.equal(hib.length, 1);
  assert.equal(hib[0].state, TaskState.HIBERNATED);

  const lc = new LeaseCoordinator();
  const f = lc.acquireOrIncrementEpoch("tsk_v", "node_1", "DeepSeek");
  const bridge = new ZeroTrustVsockBridge(lc);
  const headers = bridge.proxyOutboundRequest(f, "api.github.com", "DeepSeek");
  assert.equal(headers["X-Agent-Fence"], "1:1");
  assert.throws(
    () => bridge.proxyOutboundRequest(f, "evil.example.com", "DeepSeek"),
    EgressPolicyViolationError,
  );

  const astBreaker = new SyntacticAndVelocityBreaker(3);
  assert.equal(astBreaker.recordAndCheckToolCall("tsk_v", "bash", "{'cmd': 'ls -la'}"), "OK");
  assert.equal(astBreaker.recordAndCheckToolCall("tsk_v", "bash", "{ 'cmd' : 'ls -la' }"), "OK");
  assert.equal(astBreaker.recordAndCheckToolCall("tsk_v", "bash", "{'cmd':'ls -la'}"), "REFLEXION_INTERVENTION");
  assert.equal(astBreaker.recordAndCheckToolCall("tsk_v", "bash", "{'cmd': 'ls -la'}"), "CIRCUIT_BROKEN");
});

test("M5 & M6 (TASK-24..32): TurnWAL, Time-Travel Replay, Outbox, 4-Stage Backpressure & CLI", () => {
  const wal = new TurnWalEngine();
  for (let seq = 1; seq <= 8; seq++) {
    wal.writeWalFrameIfNoneMatch({
      taskId: "tsk_wal_1",
      fence: new FenceToken(2, seq, "tsk_wal_1"),
      routedModel: "DeepSeek",
      effectiveEffort: "high",
      tokensUsed: 1000,
      workspaceMerkleHash: `merkle_${seq}`,
      passRate: 0.1 * seq,
      errorCount: Math.max(0, 8 - seq),
      diffEntropy: 0.4,
      contextMessages: [`msg_turn_${seq}`],
    });
  }
  assert.throws(
    () =>
      wal.writeWalFrameIfNoneMatch({
        taskId: "tsk_wal_1",
        fence: new FenceToken(1, 9, "tsk_wal_1"),
        routedModel: "DeepSeek",
        effectiveEffort: "high",
        tokensUsed: 500,
        workspaceMerkleHash: "stale",
        passRate: 0.0,
        errorCount: 5,
        diffEntropy: 0.0,
        contextMessages: [],
      }),
    StaleFenceError,
  );

  const frames = wal.listTaskFrames("tsk_wal_1");
  const compactor = new WalCompactorAndPagedRestorer();
  const restored = compactor.pagedRestore("tsk_wal_1", frames);
  assert.equal(restored.restoredToTurn, 8);
  assert.equal(restored.hotPagesReady, true);

  const replayer = new DeterministicTimeTravelReplayer();
  const rep = replayer.replayToTurn("tsk_wal_1", frames, 5);
  assert.equal(rep.replayedUpToTurn, 5);
  assert.equal(rep.tokensBilledDuringReplay, 0);
  assert.equal(rep.restoredWorkspaceHash, "merkle_5");

  const bus = new CloudEventsBus();
  const ev = bus.emit("agent.task.completed", "tsk_wal_1", "fin-enterprise", { turns: 8 });
  const outbox = new TransactionalOutboxDispatcher("whsec_test", 5);
  outbox.stageInTransaction(ev);

  let calls = 0;
  const { deliveredCount, dlqCount } = outbox.dispatchPending(() => {
    calls += 1;
    return calls <= 3 ? 503 : 200;
  });
  assert.equal(deliveredCount, 1);
  assert.equal(dlqCount, 0);
  assert.equal(calls, 4);

  const bp = new BidirectionalBackpressureController();
  const task = new AgentTask({
    taskId: "tsk_bp",
    tenantId: "fin-enterprise",
    state: TaskState.RUNNING,
    effort: "max",
    activeSessions: 4,
  });
  assert.equal(bp.evaluate({ modelTtftP95Sec: 7.5 }, [task]), "STAGE1_SOFT_PACING");
  assert.equal(task.effort, "medium");
  assert.equal(bp.evaluate({ clientSseCreditRatio: 0.15 }, [task]), "STAGE4_UPSTREAM_THROTTLE");
  assert.equal(task.activeSessions, 1);
  assert.equal(bp.evaluate({ nodePsiMemoryPct: 48.0 }, [task]), "STAGE2_POOL_FAILOVER_HIBERNATE");
  assert.equal(bp.evaluate({ availableComputeRatio: 0.02 }, [task]), "STAGE3_QUEUE_FREEZE");
  assert.equal(bp.evaluate({}, [task]), "NORMAL");

  const topOut = runCli(["top"]);
  assert.ok("DeepSeek" in topOut.model_pools);
  const chaosOut = runCli(["chaos", "--scenario", "deepseek-429"]);
  assert.equal(chaosOut.breaker, "OPEN");

  const bridge = new DashboardBridgeService();
  assert.ok(bridge.getClusterStateJson().includes("tenants_dominant_share"));
});
