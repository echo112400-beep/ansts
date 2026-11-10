/**
 * @fileoverview M4 TASK-18 & M57-4: NodeDaemon Runtime Agent with Turn-Level WAL Checkpointing,
 * Cross-Node Turn Monotonicity Guard, Max-Turns Enforcement & Failure State Rollback.
 */
import { CheckpointRecord, NodeHeartbeat, TaskSpec, TaskState } from "./models.js";
const TERMINAL_STATES = new Set<TaskState>([TaskState.SUCCEEDED, TaskState.FAILED, TaskState.DEAD_LETTER, TaskState.CANCELLED]);
import { StaleFenceError } from "./lease_coordinator.js";
import { SagaToolGateway } from "./saga_gateway.js";
import { CoreScheduler5D } from "./scheduler.js";

export class NodeDaemon {
  public readonly nodeId: string;
  public readonly totalCpu: number;
  public readonly totalMemMb: number;
  public readonly warmPoolSize: number;
  private readonly checkpointStore: Map<string, CheckpointRecord[]> = new Map();
  public psiMemorySomeAvg10 = 2.5;

  constructor(nodeId: string, totalCpu = 32.0, totalMemMb = 65536, warmPoolSize = 8) {
    this.nodeId = nodeId;
    this.totalCpu = totalCpu;
    this.totalMemMb = totalMemMb;
    this.warmPoolSize = warmPoolSize;
  }

  public emitHeartbeat(runningTasks = 0): NodeHeartbeat {
    return new NodeHeartbeat({
      node_id: this.nodeId,
      total_cpu_cores: this.totalCpu,
      free_cpu_cores: Math.max(0.0, this.totalCpu - runningTasks * 0.5),
      total_memory_mb: this.totalMemMb,
      free_memory_mb: Math.max(0, this.totalMemMb - runningTasks * 1024),
      warm_pool_ready: this.warmPoolSize,
      psi_memory_some_avg10: this.psiMemorySomeAvg10,
      running_tasks: runningTasks,
    });
  }

  public writeTurnCheckpoint(
    scheduler: CoreScheduler5D,
    task: TaskSpec,
    turnIndex: number,
    stateHash: string,
    overlayfsDiffUri: string
  ): CheckpointRecord {
    if (!scheduler.leaseCoordinator.validateFencingToken(task.task_id, task.lease_fencing_token)) {
      throw new StaleFenceError(
        task.task_id,
        task.lease_fencing_token,
        scheduler.leaseCoordinator.getLease(task.task_id)?.fencing_token ?? -1
      );
    }
    const latest = this.latestCheckpoint(task.task_id);
    if (latest && latest.fencing_token === task.lease_fencing_token && turnIndex <= latest.turn_index) {
      throw new Error(
        `Non-monotonic turn checkpoint rejected for task '${task.task_id}': turnIndex=${turnIndex} <= latest=${latest.turn_index}`
      );
    }
    task.current_turn = Math.max(task.current_turn, turnIndex);
    const record: CheckpointRecord = {
      checkpoint_id: `ckpt-${task.task_id}-t${turnIndex}-f${task.lease_fencing_token}`,
      task_id: task.task_id,
      turn_index: turnIndex,
      fencing_token: task.lease_fencing_token,
      state_hash: stateHash,
      overlayfs_diff_uri: overlayfsDiffUri,
      created_at: Date.now() / 1000,
    };
    const list = this.checkpointStore.get(task.task_id) ?? [];
    list.push(record);
    this.checkpointStore.set(task.task_id, list);
    return record;
  }

  public latestCheckpoint(taskId: string): CheckpointRecord | null {
    const records = this.checkpointStore.get(taskId);
    if (!records || records.length === 0) return null;
    return records[records.length - 1];
  }

  public executeAgentStep(
    scheduler: CoreScheduler5D,
    sagaGateway: SagaToolGateway,
    task: TaskSpec,
    toolName: string,
    toolArgs: Record<string, unknown>
  ): Record<string, unknown> {
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
      task.state = TaskState.DEAD_LETTER;
      throw new Error(`Task '${task.task_id}' reached max_turns (${task.max_turns})`);
    }
    const nextTurn = baseTurn + 1;
    const prevState = task.state;

    try {
      task.state = TaskState.RUNNING_THINK;
      task.state = TaskState.RUNNING_ACT;
      const [result, deduplicated] = sagaGateway.invokeTool(task.task_id, nextTurn, toolName, toolArgs);

      this.writeTurnCheckpoint(
        scheduler,
        task,
        nextTurn,
        `sha256:turn-${nextTurn}`,
        `s3://checkpoints/${task.task_id}/turn-${nextTurn}.tar.zst`
      );
      return { turn: nextTurn, result, deduplicated };
    } catch (err) {
      task.state = prevState;
      throw err;
    }
  }
}
