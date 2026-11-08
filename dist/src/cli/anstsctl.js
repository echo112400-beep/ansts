"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.AnstsClusterRuntime = void 0;exports.runCli = runCli;


var _scheduler = require("../scheduler.js");
var _models = require("../models.js");
var _manual_scale_controller = require("../cluster/manual_scale_controller.js"); /**
 * @fileoverview M12 TASK-46: `anstsctl` Operational CLI (Submit, Inspect, Time-Travel Fork, Approval & Manual Scale).
 */class AnstsClusterRuntime {
  scheduler = new _scheduler.CoreScheduler5D();
  fleetController = new _manual_scale_controller.ManualFleetScaleController();
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
  return { exitCode: 1, stdout: JSON.stringify({ error: `Unknown command: ${cmd ?? ""}` }) };
}