import { AnstsSchedulerCell } from "../src/cloudflare/scheduler_cell.ts";
import { ExecutionInventory } from "../src/control/execution_inventory.ts";
import { NodeHeartbeat, TaskSpec } from "../src/models.ts";

export class SchedulerProbe {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.cell = new AnstsSchedulerCell(ctx, {ANSTS_ADMISSION_SERVICE_TOKEN:env.FIXTURE_TOKEN});
  }

  alarm() { this.alarmCount = (this.alarmCount ?? 0) + 1; this.firstAlarmAt ??= Date.now(); return this.cell.alarm(); }
  view() {
    const { scheduler } = this.cell;
    const task = scheduler.allTasks.get("orange-lease-fixture");
    const node = scheduler.nodes.get("orange-body-fixture");
    const inventory = new ExecutionInventory(this.ctx.storage, "SCHEDULER", this.ctx.id.toString(), () => []);
    return {
      task: { id:task.task_id, state:task.state, fencing:task.lease_fencing_token, node:task.assigned_node_id },
      lease: scheduler.leaseCoordinator.getLease(task.task_id),
      node: { slots:node.running_tasks, cpu:node.free_cpu_cores, memory:node.free_memory_mb },
      history: inventory.snapshot("isolated"),
    };
  }

  async fetch(request) {
    const action = new URL(request.url).pathname;
    const { storage } = this.ctx;
    if (action === "/alarm-state") return Response.json({at:await storage.getAlarm(),fired:this.alarmCount ?? 0,firstFiredAt:this.firstAlarmAt ?? null});
    if (action === "/expiry-status") return this.cell.fetch(new Request("https://scheduler.internal/pilot-expiry-status",{method:"POST",headers:{authorization:"Bearer "+this.env.FIXTURE_TOKEN},body:await request.text()}));
    if (action === "/claim") return this.cell.fetch(new Request("https://scheduler.internal/claim",{method:"POST",body:await request.text()}));
    if (action === "/capture") {
      this.cell.registerNode(new NodeHeartbeat({node_id:"orange-body-fixture",total_cpu_cores:8,free_cpu_cores:8,total_memory_mb:8192,free_memory_mb:8192,execution_slots:1}));
      await this.cell.submitTaskRpc(new TaskSpec({task_id:"orange-lease-fixture",tenant_id:"isolated",prompt:"lease migration fixture"}));
      const tick = await this.cell.scheduleTickRpc();
      if (tick.placements.length !== 1) throw new Error("fixture was not placed");
      this.cell.registerNode(this.cell.scheduler.nodes.get("orange-body-fixture"));
      await this.cell.fetch(new Request("https://scheduler.internal/claim",{method:"POST",body:JSON.stringify({task_id:"orange-lease-fixture",holder_id:"fixture-owner"})}));
      storage.sql.exec("INSERT INTO ansts_pilot_submissions VALUES (?, ?, ?, ?)","expired","expired-task","expired-digest",Date.now()-100);
      storage.sql.exec("INSERT INTO ansts_pilot_submissions VALUES (?, ?, ?, ?)","accepted","purged-task","accepted-digest",Date.now()-100);
      storage.sql.exec("INSERT INTO ansts_pilot_submission_accepted VALUES (?)","accepted");
      await storage.setAlarm(Date.now()+8000);
      const tables = ["ansts_tasks","ansts_nodes","execution_inventory_heads","execution_inventory_events","ansts_claims","ansts_pilot_submissions","ansts_pilot_submission_accepted"];
      return Response.json({source:this.view(),alarmAt:await storage.getAlarm(),rows:Object.fromEntries(tables.map(table=>[table,storage.sql.exec(`SELECT * FROM ${table}`).toArray()]))});
    }
    if (action === "/checkpoint") {
      await this.cell.submitTaskRpc(new TaskSpec({task_id:"orange-target-write",tenant_id:"isolated",prompt:"Rivet target-only write"}));
      await storage.setAlarm(Date.now()+8000);
      const tables = ["ansts_tasks","ansts_nodes","execution_inventory_heads","execution_inventory_events","ansts_claims","ansts_pilot_submissions","ansts_pilot_submission_accepted"];
      return Response.json({source:this.view(),alarmAt:await storage.getAlarm(),rows:Object.fromEntries(tables.map(table=>[table,storage.sql.exec(`SELECT * FROM ${table}`).toArray()]))});
    }
    if (action === "/target-write") {
      const task = this.cell.scheduler.allTasks.get("orange-target-write");
      return Response.json({id:task?.task_id ?? null,state:task?.state ?? null});
    }
    if (action === "/restore") {
      const backup = await request.json();
      storage.transactionSync(() => {
        if (storage.sql.exec("SELECT task_id FROM ansts_tasks LIMIT 1").toArray().length) throw new Error("target is not empty");
        const insert = (table, columns) => {
          for (const row of backup.rows[table]) {
            storage.sql.exec(`INSERT INTO ${table} (${columns.join(",")}) VALUES (${columns.map(()=>"?").join(",")})`,...columns.map(column=>row[column]));
          }
        };
        insert("ansts_tasks",["task_id","tenant_id","priority","state","fencing_token","assigned_node_id","payload_json"]);
        insert("ansts_nodes",["node_id","heartbeat_json","updated_at"]);
        insert("ansts_claims",["task_id","holder_id","claimed_at"]);
        insert("ansts_pilot_submissions",["reservation_id","task_id","input_digest","expires_at"]);
        insert("ansts_pilot_submission_accepted",["reservation_id"]);
        insert("execution_inventory_heads",["tenant_id","payload_json"]);
        insert("execution_inventory_events",["tenant_id","ordinal","task_id","payload_json"]);
        const history = new ExecutionInventory(storage,"SCHEDULER",this.ctx.id.toString(),()=>[]).snapshot("isolated");
        if (!history.complete || history.headDigest !== backup.source.history.headDigest ||
          JSON.stringify(history.entries) !== JSON.stringify(backup.source.history.entries)) throw new Error("history differs");
      });
      this.cell = new AnstsSchedulerCell(this.ctx,{ANSTS_ADMISSION_SERVICE_TOKEN:this.env.FIXTURE_TOKEN});
      await storage.setAlarm(backup.alarmAt);
      return Response.json(this.view());
    }
    if (action === "/expire") {
      const lease = this.cell.scheduler.leaseCoordinator.getLease("orange-lease-fixture");
      const early = this.cell.scheduler.reconcileOrphanedLeases(lease.expires_at - 0.001);
      const expired = this.cell.scheduler.reconcileOrphanedLeases(lease.expires_at);
      return Response.json({early,expired,state:this.view()});
    }
    return Response.json(this.view());
  }
}

export default {
  fetch(request, env) {
    if (request.headers.get("authorization") !== "Bearer " + env.FIXTURE_TOKEN)
      return Response.json({code:"UNAUTHORIZED"},{status:401});
    return env.SCHEDULER.get(env.SCHEDULER.idFromName("isolated-migration-fixture")).fetch(request);
  },
};
