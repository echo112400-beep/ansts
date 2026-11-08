/**
 * @fileoverview M12 TASK-46: `anstsctl` Operational CLI (Submit, Inspect, Time-Travel Fork, Approval & Manual Scale).
 */
import { CoreScheduler5D } from "../scheduler.js";
import { TaskPriority, TaskSpec } from "../models.js";
import { ManualFleetScaleController } from "../cluster/manual_scale_controller.js";

export class AnstsClusterRuntime {
  public readonly scheduler = new CoreScheduler5D();
  public readonly fleetController = new ManualFleetScaleController();
}

export function runCli(argv: string[], runtime = new AnstsClusterRuntime()): { exitCode: number; stdout: string } {
  const [cmd, ...rest] = argv;
  if (cmd === "submit") {
    const taskId = rest[0] ?? `task-${Date.now()}`;
    const tenantId = rest[1] ?? "tenant-default";
    const spec = new TaskSpec({
      task_id: taskId,
      tenant_id: tenantId,
      priority: TaskPriority.P1_HIGH,
      prompt: rest.slice(2).join(" ") || "CLI submitted task",
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
        fleetNodes: runtime.fleetController.listNodes().length,
      }),
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
