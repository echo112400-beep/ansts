"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.NodeDaemon = void 0;


var _models = require("./models.js");
var _lease_coordinator = require("./lease_coordinator.js"); /**
 * @fileoverview M4 TASK-18: NodeDaemon Runtime Agent with Turn-Level WAL Checkpointing.
 */

class NodeDaemon {
  nodeId;
  totalCpu;
  totalMemMb;
  warmPoolSize;
  checkpointStore = new Map();
  psiMemorySomeAvg10 = 2.5;

  constructor(nodeId, totalCpu = 32.0, totalMemMb = 65536, warmPoolSize = 8) {
    this.nodeId = nodeId;
    this.totalCpu = totalCpu;
    this.totalMemMb = totalMemMb;
    this.warmPoolSize = warmPoolSize;
  }

  emitHeartbeat(runningTasks = 0) {
    return new _models.NodeHeartbeat({
      node_id: this.nodeId,
      total_cpu_cores: this.totalCpu,
      free_cpu_cores: Math.max(0.0, this.totalCpu - runningTasks * 0.5),
      total_memory_mb: this.totalMemMb,
      free_memory_mb: Math.max(0, this.totalMemMb - runningTasks * 1024),
      warm_pool_ready: this.warmPoolSize,
      psi_memory_some_avg10: this.psiMemorySomeAvg10,
      running_tasks: runningTasks
    });
  }

  writeTurnCheckpoint(
  scheduler,
  task,
  turnIndex,
  stateHash,
  overlayfsDiffUri)
  {
    if (!scheduler.leaseCoordinator.validateFencingToken(task.task_id, task.lease_fencing_token)) {
      throw new _lease_coordinator.StaleFenceError(
        task.task_id,
        task.lease_fencing_token,
        scheduler.leaseCoordinator.getLease(task.task_id)?.fencing_token ?? -1
      );
    }
    task.current_turn = turnIndex;
    const record = {
      checkpoint_id: `ckpt-${task.task_id}-t${turnIndex}-f${task.lease_fencing_token}`,
      task_id: task.task_id,
      turn_index: turnIndex,
      fencing_token: task.lease_fencing_token,
      state_hash: stateHash,
      overlayfs_diff_uri: overlayfsDiffUri,
      created_at: Date.now() / 1000
    };
    const list = this.checkpointStore.get(task.task_id) ?? [];
    list.push(record);
    this.checkpointStore.set(task.task_id, list);
    return record;
  }

  latestCheckpoint(taskId) {
    const records = this.checkpointStore.get(taskId);
    if (!records || records.length === 0) return null;
    return records[records.length - 1];
  }

  executeAgentStep(
  scheduler,
  sagaGateway,
  task,
  toolName,
  toolArgs)
  {
    if (!scheduler.leaseCoordinator.renewLease(task.task_id, this.nodeId, task.lease_fencing_token)) {
      throw new Error(`Lease renewal failed for task ${task.task_id} on node ${this.nodeId}`);
    }
    const ckpt = this.latestCheckpoint(task.task_id);
    const nextTurn = ckpt ? ckpt.turn_index + 1 : 1;

    task.state = _models.TaskState.RUNNING_THINK;
    task.state = _models.TaskState.RUNNING_ACT;
    const [result, deduplicated] = sagaGateway.invokeTool(task.task_id, nextTurn, toolName, toolArgs);

    this.writeTurnCheckpoint(
      scheduler,
      task,
      nextTurn,
      `sha256:turn-${nextTurn}`,
      `s3://checkpoints/${task.task_id}/turn-${nextTurn}.tar.zst`
    );
    return { turn: nextTurn, result, deduplicated };
  }
}exports.NodeDaemon = NodeDaemon;