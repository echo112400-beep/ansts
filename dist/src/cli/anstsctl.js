"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.AnstsClusterRuntime = void 0;exports.runCli = runCli;



var _scheduler = require("../scheduler.js");
var _models = require("../models.js");
var _manual_scale_controller = require("../cluster/manual_scale_controller.js");
var _layerfs_workspace_driver = require("../runtime/layerfs_workspace_driver.js"); /**
 * @fileoverview M12 TASK-46 & M16 TASK-82: `anstsctl` Operational CLI
 * (Submit, Status, Scale-Out, `diagnose` Fleet Lease Probe & `gc-audit` Safe Epoch GC Dry-Run).
 */

















class AnstsClusterRuntime {
  scheduler = new _scheduler.CoreScheduler5D();
  fleetController = new _manual_scale_controller.ManualFleetScaleController();
  layerfsDriver = new _layerfs_workspace_driver.LayerFsWorkspaceDriver();
  peerProbes = [];
  epochCandidates = [];
}exports.AnstsClusterRuntime = AnstsClusterRuntime;

function runCli(argv, runtime = new AnstsClusterRuntime()) {
  const [cmd, ...rest] = argv;
  if (cmd === "submit") {
    const taskId = rest[0] ?? `task-${Date.now()}`;
    const tenantId = rest[1] ?? "tenant-default";
    const spec = new _models.TaskSpec({
      task_id: taskId,
      tenant_id: tenantId,
      priority: _models.TaskPriority.P1_HIGH,
      prompt: rest.slice(2).join(" ") || "CLI submitted task"
    });
    const t = runtime.scheduler.submitTask(spec);
    return { exitCode: 0, stdout: JSON.stringify({ task_id: t.task_id, state: t.state }) };
  }
  if (cmd === "status") {
    return {
      exitCode: 0,
      stdout: JSON.stringify({
        queuedTasks: runtime.scheduler.queueManager.totalQueued(),
        totalTasks: runtime.scheduler.allTasks.size,
        fleetNodes: runtime.fleetController.listNodes().length
      })
    };
  }
  if (cmd === "scale-out") {
    const nodeId = rest[0] ?? "node-worker-new";
    const ip = rest[1] ?? "10.20.1.99";
    const res = runtime.fleetController.scaleOutNode(nodeId, ip, "worker");
    return { exitCode: 0, stdout: JSON.stringify(res) };
  }
  if (cmd === "diagnose") {
    const expectedProtocol = rest[0] ?? "v0.6.3";
    const expiredLeases = runtime.peerProbes.filter((p) => p.leaseExpired).map((p) => p.nodeId);
    const unreachablePeers = runtime.peerProbes.filter((p) => !p.reachable).map((p) => p.nodeId);
    const protocolMismatches = runtime.peerProbes.
    filter((p) => p.protocolVersion !== expectedProtocol).
    map((p) => p.nodeId);
    const totalRestoring = runtime.peerProbes.reduce((acc, p) => acc + p.restoringCount, 0);
    const rollingUpgradeSafe =
    expiredLeases.length === 0 &&
    unreachablePeers.length === 0 &&
    protocolMismatches.length === 0 &&
    totalRestoring === 0;
    return {
      exitCode: rollingUpgradeSafe ? 0 : 2,
      stdout: JSON.stringify({
        expiredLeases,
        unreachablePeers,
        protocolMismatches,
        totalRestoring,
        rollingUpgradeSafe
      })
    };
  }
  if (cmd === "gc-audit") {
    const graceSecs = Number(rest[0] ?? 3600);
    const deletableEpochs = [];
    for (const c of runtime.epochCandidates) {
      if (c.isFacetStream) continue;
      if (c.isPagedCell && c.hydrateMbps === 0) continue;
      if (c.epochId >= c.ownerCurrentEpoch - 1) continue;
      if (c.ageSeconds < graceSecs) continue;
      if (deletableEpochs.length < 64) {
        deletableEpochs.push(c);
      }
    }
    const reclaimableBytes = deletableEpochs.reduce((acc, c) => acc + c.bytes, 0);
    return {
      exitCode: 0,
      stdout: JSON.stringify({
        dryRun: true,
        graceSecs,
        deletableEpochCount: deletableEpochs.length,
        reclaimableBytes,
        deletableScopes: deletableEpochs.map((e) => `${e.cellScope}@epoch-${e.epochId}`)
      })
    };
  }
  if (cmd === "blame") {
    const branchOrCommit = rest[0] ?? "main";
    const filePath = rest[1] ?? "package.json";
    const blameLines = runtime.layerfsDriver.blameFile(branchOrCommit, filePath);
    return {
      exitCode: 0,
      stdout: JSON.stringify({ branchOrCommit, filePath, lines: blameLines })
    };
  }
  return { exitCode: 1, stdout: JSON.stringify({ error: `Unknown command: ${cmd ?? ""}` }) };
}