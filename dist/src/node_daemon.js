"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.NodeDaemon = void 0;



var _models = require("./models.js");
var _lease_coordinator = require("./lease_coordinator.js"); /**
 * @fileoverview M4 TASK-18, M57-4 & M60-4: NodeDaemon Runtime Agent with Turn-Level WAL Checkpointing,
 * Cross-Node Turn Monotonicity Guard, Max-Turns Enforcement, Failure State Rollback & Deterministic Breakpoint Crash-Recovery.
 */

const TERMINAL_STATES = new Set([
_models.TaskState.SUCCEEDED,
_models.TaskState.FAILED,
_models.TaskState.DEAD_LETTER,
_models.TaskState.CANCELLED]);


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

  /**
   * [M60-4] Recovers a task on this `NodeDaemon` from its committed `TurnWalEngine` breakpoint after
   * a crash, lease expiration, or cross-node preemption:
   * - Reconstructs the local `checkpointStore` from the canonical WAL entries up to `restoredTurnIndex`,
   * - Synchronizes `task.current_turn = restoredTurnIndex`,
   * - Automatically rolls back and invalidates any uncommitted mid-turn Saga side-effects at `turn >= restoredTurnIndex + 1`,
   * - Ensures the next `executeAgentStep` continues deterministically from `restoredTurnIndex + 1`.
   */
  recoverTaskFromBreakpoint(
  scheduler,
  task,
  walEngine,
  sagaGateway)







  {
    if (!scheduler.leaseCoordinator.validateFencingToken(task.task_id, task.lease_fencing_token)) {
      throw new _lease_coordinator.StaleFenceError(
      task.task_id,
      task.lease_fencing_token,
      scheduler.leaseCoordinator.getLease(task.task_id)?.fencing_token ?? -1);

    }

    const breakpoint = walEngine.recoverTaskBreakpoint(task.task_id, task.lease_fencing_token);
    const checkpoints = breakpoint.canonicalEntries.map((e) => ({
      ...e.checkpoint,
      fencing_token: e.fencingToken,
      turn_index: e.turnIndex
    }));
    this.checkpointStore.set(task.task_id, checkpoints);
    task.current_turn = breakpoint.restoredTurnIndex;

    let rolledBackUncommittedSteps = 0;
    if (sagaGateway) {
      const rb = sagaGateway.rollbackAndInvalidateTask(
      task.task_id,
      breakpoint.restoredTurnIndex + 1);

      rolledBackUncommittedSteps = rb.rolledBackSteps.length + rb.invalidatedIdemKeys.length;
    }

    return {
      restoredTurnIndex: breakpoint.restoredTurnIndex,
      restoredFencingToken: breakpoint.restoredFencingToken,
      rehydratedCheckpoints: checkpoints.length,
      rolledBackUncommittedSteps,
      discardedWalFrames: breakpoint.discardedFrameIds.length,
      nextTurnToExecute: breakpoint.restoredTurnIndex + 1
    };
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
      scheduler.leaseCoordinator.getLease(task.task_id)?.fencing_token ?? -1);

    }
    const latest = this.latestCheckpoint(task.task_id);
    if (latest && latest.fencing_token === task.lease_fencing_token && turnIndex <= latest.turn_index) {
      throw new Error(
      `Non-monotonic turn checkpoint rejected for task '${task.task_id}': turnIndex=${turnIndex} <= latest=${latest.turn_index}`);

    }
    task.current_turn = Math.max(task.current_turn, turnIndex);
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
    if (TERMINAL_STATES.has(task.state)) {
      throw new Error(`Cannot execute agent step for task '${task.task_id}' in terminal state '${task.state}'`);
    }
    if (!scheduler.leaseCoordinator.renewLease(task.task_id, this.nodeId, task.lease_fencing_token)) {
      throw new Error(`Lease renewal failed for task ${task.task_id} on node ${this.nodeId}`);
    }
    const ckpt = this.latestCheckpoint(task.task_id);
    // [M57-4] Respect task.current_turn across cross-node migrations where local checkpointStore is empty
    const baseTurn = Math.max(task.current_turn ?? 0, ckpt ? ckpt.turn_index : 0);
    if (baseTurn >= task.max_turns) {
      task.state = _models.TaskState.DEAD_LETTER;
      throw new Error(`Task '${task.task_id}' reached max_turns (${task.max_turns})`);
    }
    const nextTurn = baseTurn + 1;
    const prevState = task.state;

    try {
      task.state = _models.TaskState.RUNNING_THINK;
      task.state = _models.TaskState.RUNNING_ACT;
      const [result, deduplicated] = sagaGateway.invokeTool(task.task_id, nextTurn, toolName, toolArgs);

      this.writeTurnCheckpoint(
      scheduler,
      task,
      nextTurn,
      `sha256:turn-${nextTurn}`,
      `s3://checkpoints/${task.task_id}/turn-${nextTurn}.tar.zst`);

      return { turn: nextTurn, result, deduplicated };
    } catch (err) {
      task.state = prevState;
      throw err;
    }
  }
}exports.NodeDaemon = NodeDaemon;