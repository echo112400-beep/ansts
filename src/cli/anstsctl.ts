/**
 * @fileoverview M12 TASK-46 & M16 TASK-82: `anstsctl` Operational CLI
 * (Submit, Status, Scale-Out, `diagnose` Fleet Lease Probe & `gc-audit` Safe Epoch GC Dry-Run).
 */
import { CoreScheduler5D } from "../scheduler.js";
import { TaskPriority, TaskSpec } from "../models.js";
import { ManualFleetScaleController } from "../cluster/manual_scale_controller.js";
import { LayerFsWorkspaceDriver } from "../runtime/layerfs_workspace_driver.js";

export interface FleetPeerProbeInput {
  nodeId: string;
  leaseExpired: boolean;
  reachable: boolean;
  protocolVersion: string;
  restoringCount: number;
}

export interface CellEpochGcCandidate {
  cellScope: string;
  epochId: number;
  ownerCurrentEpoch: number;
  ageSeconds: number;
  isFacetStream: boolean;
  isPagedCell: boolean;
  hydrateMbps: number;
  bytes: number;
}

export class AnstsClusterRuntime {
  public readonly scheduler = new CoreScheduler5D();
  public readonly fleetController = new ManualFleetScaleController();
  public readonly layerfsDriver = new LayerFsWorkspaceDriver();
  public peerProbes: FleetPeerProbeInput[] = [];
  public epochCandidates: CellEpochGcCandidate[] = [];
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
  if (cmd === "diagnose") {
    const expectedProtocol = rest[0] ?? "v0.6.3";
    const expiredLeases = runtime.peerProbes.filter((p) => p.leaseExpired).map((p) => p.nodeId);
    const unreachablePeers = runtime.peerProbes.filter((p) => !p.reachable).map((p) => p.nodeId);
    const protocolMismatches = runtime.peerProbes
      .filter((p) => p.protocolVersion !== expectedProtocol)
      .map((p) => p.nodeId);
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
        rollingUpgradeSafe,
      }),
    };
  }
  if (cmd === "gc-audit") {
    const graceSecs = Number(rest[0] ?? 3600);
    const deletableEpochs: CellEpochGcCandidate[] = [];
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
        deletableScopes: deletableEpochs.map((e) => `${e.cellScope}@epoch-${e.epochId}`),
      }),
    };
  }
  if (cmd === "blame") {
    const branchOrCommit = rest[0] ?? "main";
    const filePath = rest[1] ?? "package.json";
    const blameLines = runtime.layerfsDriver.blameFile(branchOrCommit, filePath);
    return {
      exitCode: 0,
      stdout: JSON.stringify({ branchOrCommit, filePath, lines: blameLines }),
    };
  }
  return { exitCode: 1, stdout: JSON.stringify({ error: `Unknown command: ${cmd ?? ""}` }) };
}

/**
 * [M39] Live Remote Dev-Cluster CLI Runner (`runAnstsCtlRemote`).
 * Connects directly to a running ANSTS Gateway / Body node over HTTP.
 */
export async function runAnstsCtlRemote(
  argv: string[],
  gatewayUrl = process.env.ANSTS_GATEWAY_URL ?? "http://127.0.0.1:18080"
): Promise<CliCommandOutput> {
  const baseUrl = gatewayUrl.replace(/\/$/, "");
  const [cmd, ...rest] = argv;

  if (cmd === "cluster-overview") {
    const [ovResp, shResp] = await Promise.all([
      fetch(`${baseUrl}/v1beta1/console/overview`),
      fetch(`${baseUrl}/v1beta1/console/shards`),
    ]);
    if (!ovResp.ok || !shResp.ok) {
      return { exitCode: 1, stdout: JSON.stringify({ error: "GATEWAY_UNREACHABLE" }) };
    }
    const overview = await ovResp.json();
    const shards = await shResp.json();
    return { exitCode: 0, stdout: JSON.stringify({ overview, shards }) };
  }

  if (cmd === "dispatch") {
    const tenantId = rest[0] ?? "tenant-cli";
    const taskId = rest[1] ?? `task-cli-${Date.now()}`;
    const promptText = rest[2] ?? "cli dispatched task";
    const resp = await fetch(`${baseUrl}/v1beta1/tasks/dispatch`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ tenantId, taskId, promptText }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "attest") {
    const branchOrCommit = rest[0] ?? "main";
    const resp = await fetch(`${baseUrl}/v1beta1/security/attest`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        branchOrCommit,
        spiffeId: "spiffe://ansts.io/ns/fleet/sa/cli",
      }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-state") {
    const region = rest[0] ?? "all";
    const resp = await fetch(`${baseUrl}/v1beta1/management/state?region=${encodeURIComponent(region)}`);
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-quota") {
    const tenantId = rest[0] ?? "Northstar";
    const maxSlots = Number(rest[1] ?? 160);
    const dailyBudgetUsd = Number(rest[2] ?? 1500);
    const expectedVersion = Number(rest[3] ?? 14);
    const actor = rest[4] ?? "chen";
    const resp = await fetch(`${baseUrl}/v1beta1/management/tenants/config`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ tenantId, maxSlots, dailyBudgetUsd, expectedVersion, actor, role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-drain") {
    const nodeId = rest[0] ?? "body-host-02";
    const forceMigrateLeases = rest.includes("--force-migrate");
    const resp = await fetch(`${baseUrl}/v1beta1/management/nodes/batch`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ operation: "drain", nodeIds: [nodeId], forceMigrateLeases, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.status === 200 ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-approve") {
    const approvalId = rest[0] ?? "APR-2026-091";
    const approver = rest[1] ?? "zhou";
    const decision = rest[2] ?? "approve";
    const resp = await fetch(`${baseUrl}/v1beta1/management/approvals/decide`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ approvalId, approver, decision, role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-turns") {
    const taskId = rest[0] ?? "task-ns-8841";
    const resp = await fetch(`${baseUrl}/v1beta1/management/tasks/${encodeURIComponent(taskId)}/turns`);
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "celld-isolate") {
    const taskId = rest[0] ?? "task-cli-iso";
    const jsSourceCode = rest[1] ?? "globalThis.__result = { isolateOk: true, val: (inputPayload.x || 7) * 6 };";
    const resp = await fetch(`${baseUrl}/v1beta1/celld/isolate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ taskId, turnIndex: 1, jsSourceCode, allowInternetEgress: false, inputPayload: { x: 7 } }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "ltx-compact") {
    const taskId = rest[0] ?? "task-ns-8841";
    const resp = await fetch(`${baseUrl}/v1beta1/celld/ltx-compaction`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ taskId, l0ObjectCount: 320, txidDistance: 512, totalL0Megabytes: 48 }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "layerfs-store-sync") {
    const action = rest[0] ?? "push";
    const basedOnGeneration = Number(rest[1] ?? 0);
    const resp = await fetch(`${baseUrl}/v1beta1/layerfs/store-sync`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, basedOnGeneration }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-snapshot") {
    const action = rest[0] ?? "verify_cold_start_rehydration";
    const resp = await fetch(`${baseUrl}/v1beta1/management/persistence/snapshot`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-container") {
    const action = rest[0] ?? "start_and_probe_container";
    const resp = await fetch(`${baseUrl}/v1beta1/management/celld/container-workflow`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-speculative") {
    const sourceBranch = rest[0] ?? "feat-ns-auth";
    const resp = await fetch(`${baseUrl}/v1beta1/management/layerfs/speculative`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ sourceBranch, mergeWinningBranch: true, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-autoscale") {
    const action = rest[0] ?? "probe_and_autoscale";
    const resp = await fetch(`${baseUrl}/v1beta1/management/cluster/autoscale-psi`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-sandbox") {
    const action = rest[0] ?? "allocate_and_mount";
    const taskId = rest[1] ?? "task-ns-8841";
    const resp = await fetch(`${baseUrl}/v1beta1/management/runtime/sandbox`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, taskId, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-mcp") {
    const action = rest[0] ?? "invoke_mcp_tool";
    const toolName = rest[1] ?? "mcp_pg_migration_apply";
    const taskId = rest[2] ?? "task-ns-8841";
    const resp = await fetch(`${baseUrl}/v1beta1/management/mcp`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, toolName, taskId, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-a2a") {
    const action = rest[0] ?? "put_blackboard_cas";
    const key = rest[1] ?? "arch/zero-trust-policy";
    const resp = await fetch(`${baseUrl}/v1beta1/management/a2a`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, key, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-migrate") {
    const taskId = rest[0] ?? "task-ns-8841";
    const targetNodeId = rest[1] ?? "body-host-03";
    const resp = await fetch(`${baseUrl}/v1beta1/management/tasks/migrate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ taskId, targetNodeId, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-gc-sweep") {
    const action = rest[0] ?? "sweep_cas";
    const resp = await fetch(`${baseUrl}/v1beta1/management/storage/gc-sweep`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-deep-risk-phase2-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/deep-risk-phase2-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-potential-risks-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/potential-risks-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-consistency-phase2-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/deep-consistency-phase2-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-consistency-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/implementation-consistency-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-dfx-deep-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/dfx-deep-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-dfx-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/dfx-hardening-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-subsystem-idempotency-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/subsystem-persistence-idempotency-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-persistence-idempotency-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/persistence-idempotency-recovery-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-sql-kv-uffd-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/sql-kv-sse-uffd-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-container-security-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/container-security-telemetry-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-runtime-edge-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/runtime-edge-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-kernel-storage-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/kernel-storage-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-storage-ha-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/storage-ha-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-runtime-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/deep-runtime-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-risk-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/risk-elimination-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-rectify-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/system/deep-rectification-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-scheduler-audit") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/scheduler/kernel-audit`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-design") {
    const resp = await fetch(`${baseUrl}/v1beta1/management/design/blueprint`);
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-config") {
    const action = rest[0] ?? "inspect_active_config";
    const arg1 = rest[1];
    const bodyObj: Record<string, unknown> = { action, actor: "chen", role: "SRE_ADMIN" };
    if (action === "validate_and_apply_config") {
      bodyObj.runtimeOverrides = { maxCellConcurrentRequests: Number(arg1 ?? 96) };
    } else if (action === "rollback_revision") {
      bodyObj.targetRevision = Number(arg1 ?? 1);
    }
    const resp = await fetch(`${baseUrl}/v1beta1/management/config/center`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(bodyObj),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-runtime-engine") {
    const action = rest[0] ?? "scheduler_cell_rpc_and_alarm";
    const taskId = rest[1] ?? "task-ns-8841";
    const resp = await fetch(`${baseUrl}/v1beta1/management/runtime/celld-engine`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, taskId, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-ops") {
    const action = rest[0] ?? "probe_slo_traces_and_metrics";
    const runbookId = rest[1] ?? "RB-SLO-BURN-AND-DLQ-HEAL";
    const resp = await fetch(`${baseUrl}/v1beta1/management/ops/observability`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, runbookId, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-router-pipeline") {
    const action = rest[0] ?? "execute_sse_inference";
    const tenantId = rest[1] ?? "Northstar";
    const resp = await fetch(`${baseUrl}/v1beta1/management/router/inference-pipeline`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, tenantId, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-security") {
    const action = rest[0] ?? "vsock_unix_daemon_probe";
    const taskId = rest[1] ?? "task-ns-8841";
    const resp = await fetch(`${baseUrl}/v1beta1/management/security/kernel-attestation`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, taskId, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-chaos") {
    const action = rest[0] ?? "run_partition_and_split_brain_drill";
    const targetHost = rest[1] ?? "body-us-west-02";
    const resp = await fetch(`${baseUrl}/v1beta1/management/chaos/drill`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, targetHost, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-federation") {
    const action = rest[0] ?? "tunnel_command_and_failover";
    const sovereigntyZone = rest[1] ?? "EU";
    const resp = await fetch(`${baseUrl}/v1beta1/management/federation/tunnel-harness`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ action, sovereigntyZone, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "mgmt-diagnose") {
    const expectedProtocol = rest[0] ?? "v0.6.3";
    const resp = await fetch(`${baseUrl}/v1beta1/management/diagnostics/probe`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ expectedProtocol, enqueueWebhookTest: true, actor: "chen", role: "SRE_ADMIN" }),
    });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  if (cmd === "gc") {
    const resp = await fetch(`${baseUrl}/v1beta1/storage/gc`, { method: "POST" });
    const data = await resp.json();
    return { exitCode: resp.ok ? 0 : 1, stdout: JSON.stringify(data) };
  }

  return { exitCode: 1, stdout: JSON.stringify({ error: `Unknown remote command: ${cmd ?? ""}` }) };
}
