"use strict";





var _nodeTest = require("node:test");
var assert = _interopRequireWildcard(require("node:assert/strict"));
var fs = _interopRequireWildcard(require("node:fs"));
var _server_main = require("../src/server_main.js");
var _anstsctl = require("../src/cli/anstsctl.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M48 End-to-End Verification: Unified 8-Table Full-Cluster State Snapshot & Cold-Start Rehydration,
 * Native `denoland/celld` `ctx.container` (`kata` OCI + `celld-fence` IMDSv2/RFC1918 Firewall) + `DurableWorkflowContext`,
 * Speculative Deterministic Multi-Branch CoW Execution (`CoWSandboxBranchForker` + `DeterministicExecutionEnvelope`),
 * and Closed-Loop Linux `/proc/pressure/memory` PSI Guard + Dynamic Queue/Container Auto-Scaler.
 */(0, _nodeTest.describe)("M48 Unified 8-Table Full-Cluster Persistence, Native ctx.container + celld-fence Bridge, Speculative Deterministic Branching & Closed-Loop PSI/Queue Auto-Scaler", () => {(0, _nodeTest.it)("executes 8-table cluster snapshot & cold-start rehydration, native kata ctx.container + celld-fence IMDS block + DurableWorkflowContext, deterministic speculative CoW branches, and closed-loop PSI/Queue auto-scaling", async () => {const tmpDir = `/tmp/ansts-m48-${process.pid}-${Date.now()}`;fs.mkdirSync(tmpDir, { recursive: true });

    const server = new _server_main.AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: `${tmpDir}/gw-01`
    });
    const { apiPort } = await server.start(0, 0);
    const baseUrl = `http://127.0.0.1:${apiPort}`;

    try {
      // 1. [M48-1] Unified 8-Table Full-Cluster State Snapshot & Cold-Start Rehydration (`POST /v1beta1/management/persistence/snapshot`)
      const snapCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-snapshot", "verify_cold_start_rehydration"], baseUrl);
      assert.equal(snapCli.exitCode, 0);
      const snapData = JSON.parse(snapCli.stdout);







      assert.ok(snapData.restoredTasksCount >= 4);
      assert.equal(snapData.restoredFinopsCount, 4);
      assert.ok(snapData.restoredApprovalsCount >= 3);
      assert.ok(snapData.restoredMcpCount >= 1);
      assert.equal(snapData.rpoZeroVerified, true);
      assert.ok(snapData.tableCounts.persisted_tasks >= 4);
      assert.ok(snapData.tableCounts.persisted_leases >= 4);

      // 2. [M48-2] Native `denoland/celld` `ctx.container` (`kata`) + `celld-fence` IMDSv2 Block & `DurableWorkflowContext`
      const contCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-container", "start_and_probe_container"], baseUrl);
      assert.equal(contCli.exitCode, 0);
      const contData = JSON.parse(contCli.stdout);





      assert.equal(contData.started.running, true);
      assert.equal(contData.started.ociRuntime, "kata");
      assert.deepEqual(contData.started.resolvConf, ["1.1.1.1", "1.0.0.1"]);
      assert.equal(contData.execResult.exitCode, 0);
      assert.ok(contData.execResult.rawStdout.includes("nameserver 1.1.1.1"));
      assert.equal(contData.webSocketProbe.status, 101);
      assert.equal(contData.webSocketProbe.webSocketUpgraded, true);
      assert.equal(contData.celldFenceImdsBlocked, true);

      const wfCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-container", "run_durable_workflow"], baseUrl);
      assert.equal(wfCli.exitCode, 0);
      const wfData = JSON.parse(wfCli.stdout);



      assert.equal(wfData.firstExecution.replayedFromJournal, false);
      assert.equal(wfData.crashReplayExecution.replayedFromJournal, true);
      assert.equal(wfData.firstExecution.result.sha256, wfData.crashReplayExecution.result.sha256);

      // 3. [M48-3] Speculative Deterministic Multi-Branch CoW Execution & Winning Branch Merge (`POST /v1beta1/management/layerfs/speculative`)
      const specCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-speculative", "feat-ns-auth"], baseUrl);
      assert.equal(specCli.exitCode, 0);
      const specData = JSON.parse(specCli.stdout);




      assert.equal(specData.deterministicReproducible, true);
      assert.equal(specData.branches.length, 2);
      assert.ok(specData.mergeResult.mergedCommitId);

      // 4. [M48-4] Closed-Loop Linux `/proc/pressure/memory` PSI Guard, Bidirectional Backpressure & Dynamic Queue/Container Auto-Scaler
      const autoCli = await (0, _anstsctl.runAnstsCtlRemote)(["mgmt-autoscale", "simulate_psi_spike"], baseUrl);
      assert.equal(autoCli.exitCode, 0);
      const autoData = JSON.parse(autoCli.stdout);






      assert.ok(autoData.liveKernelPsi.freeMemMb > 0);
      assert.equal(autoData.psiDefense.level, "FREEZE_AND_CHECKPOINT");
      assert.equal(autoData.backpressure.mode, "RED_SHEDDING");
      assert.equal(autoData.queueSharding.newShards, 32);
      assert.ok(autoData.poolScaling.totalContainerPoolSize >= 4);
    } finally {
      await server.stop();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});