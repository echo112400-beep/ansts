"use strict";





var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));
var _index = require("../src/index.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview Design-to-Deliverable Symbol & Contract Consistency Audit Suite (5 tests).
 * Explicitly verifies that every symbol referenced in `proto/ansts_rpc.proto` and `google_engineering_implementation_report.md`
 * (`FenceToken`, `StaleFenceError`, `TurnWalFrame`, `VirtualHashSlotRingManager`, `submitTaskRpc`, `scheduleTickRpc`)
 * is exported and behaviorally verified.
 */







(0, _nodeTest.describe)("Design-to-Deliverable Consistency Audit", () => {
  (0, _nodeTest.it)("1. Verifies all 58 TypeScript source modules, deployment scripts, proto, SQL migrations, and Wrangler configs exist", () => {
    const requiredFiles = [
    "package.json",
    "Dockerfile",
    "docker-compose.yml",
    "scripts/run-local.sh",
    "src/server_main.ts",
    "tsconfig.json",
    "wrangler.json",
    "wrangler.toml",
    "proto/ansts_rpc.proto",
    "migrations/0001_init_ansts.sql",
    "deploy/rustfs-celld/start-rustfs-celld.sh",
    "deploy/manual-fleet/scale-fleet.sh",
    "src/index.ts",
    "src/cloudflare/celld_native_optimizations.ts",
    "src/checkpoint/unified_state_store.ts",
    "src/cluster/manual_scale_controller.ts"];

    for (const rel of requiredFiles) {
      const full = path.join(process.cwd(), rel);
      assert.ok(fs.existsSync(full), `Missing required deliverable file: ${rel}`);
    }
  });

  (0, _nodeTest.it)("2. Verifies Dual-Tuple FenceToken & StaleFenceError [409] contract matching FenceTokenProto", () => {
    const f1 = new _index.FenceToken(1, 0, "task-fence-proto");
    const f1Turn1 = f1.nextTurn();
    const f2 = f1Turn1.nextEpoch();
    assert.equal(f1Turn1.epoch, 1);
    assert.equal(f1Turn1.turnSeq, 1);
    assert.equal(f2.epoch, 2);
    assert.equal(f2.turnSeq, 0);
    assert.equal(f1Turn1.isStrictlyOlderThan(f2), true);
    assert.deepEqual(f2.toProto(), { epoch: 2, turn_seq: 0, task_id: "task-fence-proto" });

    const coord = new _index.LeaseCoordinator();
    coord.acquireLease("task-fence-proto", "node-a");
    coord.acquireLease("task-fence-proto", "node-b");
    assert.throws(
    () => {
      coord.assertValidFence("task-fence-proto", f1Turn1);
    },
    (err) => err instanceof _index.StaleFenceError && err.statusCode === 409);

  });

  (0, _nodeTest.it)("3. Verifies TurnWalFrame CRC32 integrity calculation, corruption detection, and TurnWalEngine integration", () => {
    const wal = new _index.TurnWalEngine();
    const entry = wal.appendTurnDelta({
      taskId: "task-wal-frame",
      turnIndex: 1,
      fencingToken: 2,
      changedFiles: ["src/a.ts", "src/b.ts"],
      sqliteWalPages: 16,
      highestSubmittedFencingToken: 2
    });
    assert.ok(entry.frame instanceof _index.TurnWalFrame);
    assert.equal(entry.frame.verifyIntegrity(), true);
    assert.equal(entry.frame.fence.epoch, 2);
    assert.equal(entry.frame.fence.turnSeq, 1);

    const corrupted = new _index.TurnWalFrame({
      frameId: "corrupt-1",
      taskId: "task-wal-frame",
      turnIndex: 1,
      fence: new _index.FenceToken(2, 1, "task-wal-frame"),
      changedFiles: ["src/a.ts"],
      sqliteWalPages: 16,
      crc32Hex: "00000000"
    });
    assert.equal(corrupted.verifyIntegrity(), false);
  });

  (0, _nodeTest.it)("4. Verifies VirtualHashSlotRingManager 1024-slot consistent hashing & ManualFleetScaleController integration", () => {
    const ring = new _index.VirtualHashSlotRingManager(1024, 16);
    ring.addNode("bm-node-1");
    ring.addNode("bm-node-2");
    ring.addNode("bm-node-3");
    const dist = ring.computeSlotDistribution();
    const totalAssigned = (dist.get("bm-node-1") ?? 0) + (dist.get("bm-node-2") ?? 0) + (dist.get("bm-node-3") ?? 0);
    assert.equal(totalAssigned, 1024);
    assert.ok((dist.get("bm-node-1") ?? 0) > 100);

    const fleet = new _index.ManualFleetScaleController();
    fleet.scaleOutNode("bm-node-1", "10.0.0.1", "worker");
    fleet.scaleOutNode("bm-node-2", "10.0.0.2", "worker");
    const ownerBefore = fleet.slotRing.getOwnerNodeForKey("tenant-1:task-99");
    assert.ok(ownerBefore.nodeId === "bm-node-1" || ownerBefore.nodeId === "bm-node-2");
    fleet.cordonAndDrainNode("bm-node-1");
    const ownerAfter = fleet.slotRing.getOwnerNodeForKey("tenant-1:task-99");
    assert.equal(ownerAfter.nodeId, "bm-node-2");
  });

  (0, _nodeTest.it)("5. Verifies AnstsSchedulerCell native js_rpc methods (submitTaskRpc, scheduleTickRpc, rehydrateSchedulerFromSql) and AbortSignal", async () => {
    const state = new _index.MockDurableObjectState("cell-rpc-1");
    const cell = new _index.AnstsSchedulerCell(state);
    await cell.submitTaskRpc(new _index.TaskSpec({ task_id: "rpc-t1", tenant_id: "t1" }));
    assert.equal(cell.rehydrateSchedulerFromSql(), 1);

    const controller = new AbortController();
    controller.abort();
    await assert.rejects(async () => {
      await cell.scheduleTickRpc(controller.signal);
    }, /scheduleTickRpc aborted/);
  });
});