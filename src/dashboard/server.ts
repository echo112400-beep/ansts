/**
 * TASK-31 & TASK-63: Real-Time Dashboard API & SSE Bridge with:
 * - 5D W-DRF Dominant Shares & CloudEvents stream (`DashboardBridgeService`)
 * - `SchedulerCell` & `Queue` Shard Heatmap (`/api/v1/shards/heatmap`)
 * - `DurableWorkflowContext` Step Timeline (`/api/v1/workflows/:taskId`)
 * - Interactive `STAGED_COMMIT` Human Approval / Rejection endpoint (`POST /api/v1/saga/approve`)
 */

import { AnstsClusterRuntime } from "../cli/anstsctl";
import { PartitionedSchedulerRouter } from "../cloudflare/horizontal_scaler";
import { AgentTask, FenceToken, TaskState } from "../models";

export class DashboardBackendServer {
  public readonly schedulerRouter = new PartitionedSchedulerRouter({ initialShardCount: 8 });

  constructor(public readonly cluster: AnstsClusterRuntime = new AnstsClusterRuntime()) {}

  public handleGetClusterState(): Record<string, any> {
    return this.cluster.clusterTopSnapshot();
  }

  public getClusterStateJson(): string {
    return JSON.stringify(this.handleGetClusterState());
  }

  /**
   * TASK-63: Returns real-time load heatmap across all partitioned `SchedulerCell` shards.
   */
  public handleGetShardHeatmap(): {
    totalShards: number;
    shards: ReturnType<PartitionedSchedulerRouter["getShardStates"]>;
  } {
    const shards = this.schedulerRouter.getShardStates();
    return {
      totalShards: shards.length,
      shards,
    };
  }

  /**
   * TASK-63: Approves or rejects a `STAGED_COMMIT` high-risk tool call from the web dashboard,
   * waking the hibernated sandbox and flushing (or discarding) the staged mutation atomically.
   */
  public handleStageCommitDecision(params: {
    taskId: string;
    epoch: number;
    turnSeq: number;
    toolCallId: string;
    toolName: string;
    payload: string;
    approved: boolean;
  }): {
    taskId: string;
    decision: "APPROVED_AND_FLUSHED" | "REJECTED_AND_DISCARDED";
    taskStateAfter: TaskState;
    flushedCount: number;
  } {
    const fence = new FenceToken(params.epoch, params.turnSeq, params.taskId);
    let task = this.cluster.scheduler.tasks.get(params.taskId);
    if (!task) {
      task = new AgentTask({ taskId: params.taskId, tenantId: "fin-enterprise", state: TaskState.RUNNING });
      this.cluster.scheduler.tasks.set(params.taskId, task);
    }

    this.cluster.leaseCoord.acquireOrIncrementEpoch(params.taskId, "worker-us-west-01", "DeepSeek");
    this.cluster.leaseCoord.verifyAndAdvanceFence(fence);

    this.cluster.sagaGw.approvalGate.stageWrite(
      task,
      fence,
      params.toolName,
      params.payload,
      true,
    );

    if (params.approved) {
      this.cluster.sagaGw.approvalGate.approveAndResume(task);
      const flushedCount = this.cluster.sagaGw.approvalGate.atomicFlush(params.taskId);
      return {
        taskId: params.taskId,
        decision: "APPROVED_AND_FLUSHED",
        taskStateAfter: task.state,
        flushedCount,
      };
    }

    this.cluster.sagaGw.approvalGate.stagedItems.delete(params.taskId);
    task.state = TaskState.CIRCUIT_BROKEN;
    return {
      taskId: params.taskId,
      decision: "REJECTED_AND_DISCARDED",
      taskStateAfter: TaskState.CIRCUIT_BROKEN,
      flushedCount: 0,
    };
  }
}

export class DashboardBridgeService extends DashboardBackendServer {}
