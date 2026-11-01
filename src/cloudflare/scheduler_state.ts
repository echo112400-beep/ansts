/** Versioned scheduler checkpoint. References are task IDs, never duplicated objects. */
import { createHash } from "crypto";
import { AnstsClusterRuntime } from "../cli/anstsctl";
import { AgentTask, FenceToken, ResourceVector5D } from "../models";
import { SubAgentDagGroup } from "../queue_manager";
import { DurableObjectStorage } from "./env";

export interface SchedulerEvent {
  event_id: string;
  event_type: string;
  task_id: string;
  tenant_id: string;
  epoch: number;
  turn_seq: number;
  payload: Record<string, any>;
}

export function snapshotRuntime(c: AnstsClusterRuntime) {
  // Include children waiting in the fast queue and DAG members not yet in scheduler.tasks.
  const tasks = new Map(c.scheduler.tasks);
  for (const dag of c.queueMgr.activeDags.values()) {
    for (const task of [dag.parentTask, ...dag.children]) tasks.set(task.taskId, task);
  }
  for (const item of c.queueMgr.escrowFastQueue) tasks.set(item.child.taskId, item.child);
  return {
    tasks: Array.from(tasks.values()),
    scheduledTaskIds: Array.from(c.scheduler.tasks.keys()),
    tenants: Array.from(c.scheduler.tenants), nodes: Array.from(c.scheduler.nodes),
    capacity: c.scheduler.clusterCapacity,
    quotas: Array.from(c.queueMgr.tenantQuotas),
    queues: Array.from(c.queueMgr.tenantQueues, ([id, q]) => [id, q.map(t => t.taskId)] as [string, string[]]),
    expired: c.queueMgr.expiredTasks.map(t => t.taskId),
    fastQueue: c.queueMgr.escrowFastQueue.map(item => ({ childId: item.child.taskId, voucher: item.voucher })),
    vouchers: Array.from(c.queueMgr.issuedVouchers.values()),
    dags: Array.from(c.queueMgr.activeDags.values(), d => ({
      id: d.dagId, parentId: d.parentTask.taskId, childIds: d.children.map(t => t.taskId),
      race: d.speculativeRaceMode, ratio: d.stragglerTriggerRatio, boost: d.boostTpsMultiplier,
    })),
    leases: c.leaseCoord.snapshot(), profiles: c.profiler.snapshot(),
    idempotency: c.apiGateway.snapshot(), pools: c.cogRouter.pools,
    pressureStage: c.bpController.currentStage, admissionStage: c.apiGateway.bpController.clusterStage,
  };
}

type RuntimeSnapshot = ReturnType<typeof snapshotRuntime>;
export function restoreRuntime(s: RuntimeSnapshot): AnstsClusterRuntime {
  const c = new AnstsClusterRuntime();
  const tasks = new Map(s.tasks.map(raw => {
    const task = Object.assign(new AgentTask(raw), raw);
    task.fence = new FenceToken(raw.fence.epoch, raw.fence.turnSeq, raw.taskId);
    return [task.taskId, task] as const;
  }));
  const task = (id: string): AgentTask => {
    const found = tasks.get(id);
    if (!found) throw new Error(`Invalid checkpoint task reference: ${id}`);
    return found;
  };
  c.scheduler.tasks = new Map(s.scheduledTaskIds.map(id => [id, task(id)]));
  c.scheduler.tenants = new Map(s.tenants);
  c.scheduler.nodes = new Map(s.nodes);
  const v = s.capacity;
  c.scheduler.clusterCapacity = new ResourceVector5D(v.steadyCpu, v.burstCpu, v.steadyMemMb, v.sessions, v.effectiveTps);
  c.queueMgr.tenantQuotas = new Map(s.quotas);
  c.queueMgr.tenantQueues = new Map(s.queues.map(([id, ids]) => [id, ids.map(task)]));
  c.queueMgr.expiredTasks = s.expired.map(task);
  c.queueMgr.escrowFastQueue = s.fastQueue.map(item => ({
    child: task(item.childId), voucher: {
      ...item.voucher, parentFence: new FenceToken(item.voucher.parentFence.epoch,
        item.voucher.parentFence.turnSeq, item.voucher.parentTaskId),
    },
  }));
  c.queueMgr.issuedVouchers = new Map(s.vouchers.map(v => [v.childTaskId, {
    ...v, parentFence: new FenceToken(v.parentFence.epoch, v.parentFence.turnSeq, v.parentTaskId),
  }]));
  c.queueMgr.activeDags = new Map(s.dags.map(d => {
    const dag = new SubAgentDagGroup(d.id, task(d.parentId), d.childIds.map(task), d.race);
    dag.stragglerTriggerRatio = d.ratio;
    dag.boostTpsMultiplier = d.boost;
    return [d.id, dag];
  }));
  c.leaseCoord.restore(s.leases);
  c.profiler.restore(s.profiles);
  c.apiGateway.restore(s.idempotency);
  c.cogRouter.pools = s.pools;
  c.bpController.currentStage = s.pressureStage;
  c.apiGateway.bpController.clusterStage = s.admissionStage;
  return c;
}

export interface SchedulerCheckpoint {
  version: 1;
  revision: number;
  runtime: RuntimeSnapshot;
  outbox: SchedulerEvent[];
}

export class SchedulerStateStore {
  constructor(private readonly storage: DurableObjectStorage) {
    storage.sql.exec("CREATE TABLE IF NOT EXISTS scheduler_checkpoint (id INTEGER PRIMARY KEY CHECK (id = 1), payload TEXT NOT NULL, checksum TEXT NOT NULL)");
  }

  public load(): SchedulerCheckpoint | undefined {
    const rows = this.storage.sql.exec<{ payload: string; checksum: string }>("SELECT * FROM scheduler_checkpoint").toArray();
    if (!rows.length) return undefined;
    if (rows.length !== 1) throw new Error("Ambiguous scheduler checkpoint");
    const row = rows[0];
    if (createHash("sha256").update(row.payload).digest("hex") !== row.checksum) {
      throw new Error("Scheduler checkpoint checksum mismatch");
    }
    const state = JSON.parse(row.payload) as SchedulerCheckpoint;
    if (state.version !== 1 || !Number.isSafeInteger(state.revision) || state.revision < 0 || !Array.isArray(state.outbox)) {
      throw new Error("Unsupported scheduler checkpoint");
    }
    return state;
  }

  public save(state: SchedulerCheckpoint): void {
    const payload = JSON.stringify(state);
    // Fail closed before exceeding the operational bound of this snapshot implementation.
    if (new TextEncoder().encode(payload).length > 1024 * 1024) throw new Error("Scheduler checkpoint exceeds 1 MiB; split the cluster");
    const checksum = createHash("sha256").update(payload).digest("hex");
    this.storage.transactionSync(() => {
      this.storage.sql.exec("DELETE FROM scheduler_checkpoint");
      this.storage.sql.exec("INSERT INTO scheduler_checkpoint (id, payload, checksum) VALUES (?, ?, ?)", 1, payload, checksum);
      // Rebuild legacy diagnostic projections atomically; these tables are not recovery sources.
      this.storage.sql.exec("DELETE FROM cell_leases");
      for (const lease of state.runtime.leases.leases) {
        this.storage.sql.exec("INSERT INTO cell_leases (task_id, node_id, model_pool, epoch, turn_seq) VALUES (?, ?, ?, ?, ?)",
          lease.taskId, lease.nodeId, lease.modelPool, lease.fence.epoch, lease.fence.turnSeq);
      }
      this.storage.sql.exec("DELETE FROM cell_dag_vouchers");
      for (const voucher of state.runtime.vouchers) {
        this.storage.sql.exec("INSERT INTO cell_dag_vouchers (child_task_id, parent_task_id, delegated_tps, delegated_tokens) VALUES (?, ?, ?, ?)",
          voucher.childTaskId, voucher.parentTaskId, voucher.delegatedTps, voucher.delegatedTokens);
      }
    });
  }
}
