/**
 * Verification Suite for `celld.dev/docs` Persistent, Recoverable & High-Availability Scheduling Engine.
 */

import { strict as assert } from "assert";
import { test } from "node:test";

import {
  DurableWorkflowContext,
  FenceToken,
  FleetHaAndAutoscalerController,
  RpoZeroDurabilityEngine,
  SafeEpochGarbageCollector,
  ShardedQueueBrokerPool,
  StaleFenceError,
} from "../src/index";

test("celld HA #1: RPO=0 Dual Proof (Fleet Ensemble Fsync & Session Sealing vs Bucket Re-Read Proof)", () => {
  const engine = new RpoZeroDurabilityEngine("fleet", 10000);
  const lease1 = engine.claimCellOwnershipCas("Agent:tsk_901", "node-a", 1000);
  assert.equal(lease1.epoch, 1);

  // Node A commits TX #1 in fleet mode -> replicated to peer followers node-b & node-c
  const proof1 = engine.commitAndProveRpoZero({
    cellScope: "Agent:tsk_901",
    nodeId: "node-a",
    fence: new FenceToken(1, 1, "Agent:tsk_901"),
    ltxPayload: "SQLITE_PAGE_DIFF_TX1",
    peerFollowerIds: ["node-b", "node-c"],
    writeCursorConsumed: true,
  });
  assert.equal(proof1.acknowledged, true);
  assert.equal(proof1.proofType, "fleet");

  // Unfinished SQLite write cursor is strictly rejected before output
  assert.throws(
    () =>
      engine.commitAndProveRpoZero({
        cellScope: "Agent:tsk_901",
        nodeId: "node-a",
        fence: new FenceToken(1, 2, "Agent:tsk_901"),
        ltxPayload: "BAD",
        peerFollowerIds: ["node-b"],
        writeCursorConsumed: false,
      }),
    /Unfinished SQL write cursor/,
  );

  // Node B takes over after lease expiry -> seals Node A's epoch 1 session!
  const lease2 = engine.claimCellOwnershipCas("Agent:tsk_901", "node-b", 12000);
  assert.equal(lease2.epoch, 2);

  // Partitioned Node A attempts to complete another fleet proof on epoch 1 -> rejected by sealed session!
  assert.throws(
    () =>
      engine.commitAndProveRpoZero({
        cellScope: "Agent:tsk_901",
        nodeId: "node-a",
        fence: new FenceToken(1, 2, "Agent:tsk_901"),
        ltxPayload: "ZOMBIE_TX",
        peerFollowerIds: ["node-c"],
      }),
    StaleFenceError,
  );
});

test("celld HA #2: Durable WorkflowEntrypoint Replay, Early Event Buffering & Isolate Hibernation", async () => {
  const wf = new DurableWorkflowContext("wf_task_01", 3600 * 1000);
  let sideEffectExecutions = 0;

  const step1Out = await wf.do("provision-sandbox", () => {
    sideEffectExecutions += 1;
    return { sandboxId: "sbx_01", bootMs: 62 };
  });
  assert.equal(step1Out.sandboxId, "sbx_01");
  assert.equal(sideEffectExecutions, 1);

  // Replay of finished step returns stored result without re-running callback
  const step1Replay = await wf.do("provision-sandbox", () => {
    sideEffectExecutions += 1;
    return { sandboxId: "sbx_duplicate", bootMs: 999 };
  });
  assert.equal(step1Replay.sandboxId, "sbx_01");
  assert.equal(sideEffectExecutions, 1);

  // Long sleep (>1h CELLD_ALARM_RESIDENT_MS) hibernates the cell isolate
  await wf.sleep("wait-human-audit", 4 * 3600 * 1000, 1000);
  assert.equal(wf.status, "waiting");
  assert.equal(wf.isolateHibernated, true);

  // Early approval event arrives and wakes workflow step
  wf.sendEvent("human-approval", { approvedBy: "sec-ops" });
  const evt = await wf.waitForEvent<{ approvedBy: string }>("human-approval");
  assert.equal(evt?.approvedBy, "sec-ops");
  assert.equal(wf.isolateHibernated, false);
});

test("celld HA #3: Sharded Queue Producer (256 Cap Protection) & Exponential Backoff Consumer", () => {
  const qPool = new ShardedQueueBrokerPool("ansts-events", 4, 3);
  // Saturate shard 0 to 256 active producers -> router automatically overflows to next healthy shard
  qPool.activeProducersByShard.set(0, 256);
  const routed = qPool.produceMessage("msg_idemp_01", { type: "task.completed" });
  assert.ok(routed.shardIndex >= 0 && routed.shardIndex < 4);

  const msg = { id: "msg_idemp_01", body: {}, attempts: 1 };
  const r1 = qPool.settleOrRetryMessage(msg, false);
  assert.equal(r1.action, "RETRY_SCHEDULED");
  assert.equal(r1.delaySeconds, 4);

  const r2 = qPool.settleOrRetryMessage(msg, false);
  assert.equal(r2.action, "RETRY_SCHEDULED");
  assert.equal(r2.delaySeconds, 8);

  // 3rd failure reaches maxRetries=3 -> moves to dead_letter_queue
  const r3 = qPool.settleOrRetryMessage(msg, false);
  assert.equal(r3.action, "MOVED_TO_DLQ");
  assert.equal(qPool.deadLetterQueue.length, 1);
});

test("celld HA #4: Safe Epoch Garbage Collection (CELLD_LTX_RETENTION_SECS & retired.json)", () => {
  const gc = new SafeEpochGarbageCollector();
  const res = gc.runEpochGc({
    cellScope: "Agent:tsk_901",
    currentOwnerNodeId: "node-b",
    ownershipRecord: {
      cellScope: "Agent:tsk_901",
      ownerNodeId: "node-b",
      epoch: 4,
      leaseExpiresAtMs: 999999,
    },
    epochs: [
      { epoch: 1, createdAtSec: 100, byteSize: 4096 },
      { epoch: 2, createdAtSec: 200, byteSize: 8192 },
      { epoch: 3, createdAtSec: 950, byteSize: 4096 },
      { epoch: 4, createdAtSec: 990, byteSize: 2048 },
    ],
    retentionSecs: 300,
    nowSec: 1000,
    pagedRestoreComplete: true,
  });

  // Keeps current epoch (4), previous epoch (3), and deletes expired epochs (1, 2) older than 300s
  assert.deepEqual(res.deletedEpochs, [1, 2]);
  assert.deepEqual(res.keptEpochs, [3, 4]);
  assert.equal(res.retiredJsonWritten, true);
});

test("celld HA #5: Fleet Autoscaler Telemetry, 32-Cell Rebalancer & Preserve Handoff Shutdown", () => {
  const fleet = new FleetHaAndAutoscalerController();
  fleet.registerOrUpdateNodeLoad({
    node_id: "node-a",
    sampled_ms: 1000,
    owned_cells: 600,
    placement_weight: 8,
    resident_cells: 250,
    host_websockets: 40,
    rss_bytes: 500_000_000,
    in_use_bytes: 400_000_000,
    cpu_percent_x100: 4500,
    open_fds: 1024,
    pressured: false,
    memory_headroom: true,
    shed_cells: 0,
    restoring: 0,
    draining: false,
  });
  fleet.registerOrUpdateNodeLoad({
    node_id: "node-b",
    sampled_ms: 1000,
    owned_cells: 200,
    placement_weight: 8,
    resident_cells: 90,
    host_websockets: 12,
    rss_bytes: 200_000_000,
    in_use_bytes: 150_000_000,
    cpu_percent_x100: 1800,
    open_fds: 512,
    pressured: false,
    memory_headroom: true,
    shed_cells: 0,
    restoring: 0,
    draining: false,
  });

  // Rebalancer moves at most 32 cells per interval from node-a (600) to node-b (200)
  const batch = fleet.computeRebalanceBatch();
  assert.ok(batch !== null);
  assert.equal(batch!.fromNodeId, "node-a");
  assert.equal(batch!.toNodeId, "node-b");
  assert.equal(batch!.movedCells, 32);

  // Same-node preserve handoff (`POST /shutdown?handoff=preserve`) sets draining=true & health=503
  const handoff = fleet.prepareShutdownHandoff("node-a", true);
  assert.equal(handoff.status, "HANDOFF_PRESERVE_READY");
  assert.equal(handoff.preservedOwnershipRecords, 568);
  assert.equal(fleet.checkNodePublicHealth("node-a").statusCode, 503);
});
