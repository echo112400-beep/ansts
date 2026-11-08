/**
 * @fileoverview ANSTS v2.5 TanStack Control Plane Management Console UI, BFF Client & Headless Table Engine
 * (`src/dashboard/tanstack_console_ui.ts`).
 *
 * Implements:
 * 1. `TanStackConsoleBffClient`: Real `fetch`-based BFF client binding all 5 Console Prototype tabs
 *    (`overview`, `fleet`, `harness`, `tunnels`, `wal`) + Canary Rollout & Make-Before-Break Drain mutations.
 * 2. `ConsoleDataTableEngine<T>`: Headless TanStack Table state engine supporting filtering, sorting,
 *    and pagination over `ShardMatrixRow` and `BodyFleetRow` datasets.
 */

export interface ShardMatrixRow {
  shardId: string;
  hostBrainNodeId: string;
  assignedSlots: string;
  inFlightRequests: number;
  ewmaLatencyMs: number;
  status: "STEADY" | "DRAINING" | "SNAPSHOT_TRANSFERRED" | "CUTOVER";
  epoch: number;
}

export interface BodyFleetRow {
  hostId: string;
  ipAddress: string;
  primaryGateway: string;
  secondaryGateway: string;
  connGeneration: number;
  epoch?: number;
  status?: string;
  cgroupBoostActive?: boolean;
}

/**
 * Headless data table state engine (TanStack Table pattern).
 */
export class ConsoleDataTableEngine<T> {
  constructor(public data: T[], public filterText: string = "") {}

  public getFilteredData(filterFn: (row: T, text: string) => boolean): T[] {
    if (!this.filterText.trim()) return this.data;
    return this.data.filter((row) => filterFn(row, this.filterText.toLowerCase()));
  }

  public getSortedData(compareFn: (a: T, b: T) => number): T[] {
    return [...this.data].sort(compareFn);
  }
}

/**
 * Typed TanStack Query / Mutation BFF Client for the ANSTS v2.5 Enterprise Console.
 * Connects the 5 Prototype Tabs (`overview`, `fleet`, `harness`, `tunnels`, `wal`) to live backend endpoints.
 */
export class TanStackConsoleBffClient {
  constructor(public readonly baseUrl: string) {}

  public async fetchOverview(region = "us-west-2"): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/overview?region=${encodeURIComponent(region)}`);
    if (!res.ok) throw new Error(`BFF fetchOverview failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async fetchFleet(): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/fleet`);
    if (!res.ok) throw new Error(`BFF fetchFleet failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async fetchHarnessAndShards(): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/harness`);
    if (!res.ok) throw new Error(`BFF fetchHarnessAndShards failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async fetchTunnels(): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/tunnels`);
    if (!res.ok) throw new Error(`BFF fetchTunnels failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateDrainGatewayOrHost(targetId: string, gracePeriodMs = 15000): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/tunnels/drain`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ gatewayId: targetId, gracePeriodMs }),
    });
    if (!res.ok) throw new Error(`BFF mutateDrainGatewayOrHost failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateWalCatchupReplay(params: {
    clientId?: string;
    taskId: string;
    lastReceivedTurnSeq: number;
  }): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/wal/catchup`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`BFF mutateWalCatchupReplay failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateCanaryAction(params: {
    action: "advance" | "rollback";
    targetPhase?: string;
    canaryGatewayId?: string;
  }): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/canary/action`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`BFF mutateCanaryAction failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateDispatchTask(params: {
    tenantId: string;
    taskId: string;
    repoId?: string;
    promptText?: string;
    targetBodyHostId?: string;
  }): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/tasks/dispatch`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`BFF mutateDispatchTask failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async fetchShadowLab(): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/shadow-lab`);
    if (!res.ok) throw new Error(`BFF fetchShadowLab failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateRunShadowEval(params: {
    sourceTaskId: string;
    baselinePool: string;
    candidatePool: string;
    shadowTrafficSamplePct?: number;
  }): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/shadow-lab/run`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`BFF mutateRunShadowEval failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async fetchSlotsHeatmap(): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/slots-heatmap`);
    if (!res.ok) throw new Error(`BFF fetchSlotsHeatmap failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateMigrateSlotOnline(slotIndex: number, targetShardId: string): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/slots/migrate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ slotIndex, targetShardId }),
    });
    if (!res.ok) throw new Error(`BFF mutateMigrateSlotOnline failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateCaptureV8FlameGraph(isolateId = "celld-brain-01"): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/profiler/capture`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ isolateId }),
    });
    if (!res.ok) throw new Error(`BFF mutateCaptureV8FlameGraph failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateRunChaosDrills(): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/chaos/run`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    if (!res.ok) throw new Error(`BFF mutateRunChaosDrills failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateEvacuateSpotHost(spotHostId = "body-host-edge-02"): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/spot/evacuate`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ spotHostId }),
    });
    if (!res.ok) throw new Error(`BFF mutateEvacuateSpotHost failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async fetchHitlAndTimeTravel(taskId = "task-prod-101"): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/hitl-timetravel?taskId=${encodeURIComponent(taskId)}`);
    if (!res.ok) throw new Error(`BFF fetchHitlAndTimeTravel failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateHitlDecision(params: {
    taskId: string;
    decision: "approve" | "reject" | "stage_new";
    toolName?: string;
    payload?: string;
  }): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/hitl/decide`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`BFF mutateHitlDecision failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateTimeTravelFork(taskId: string, targetTurnSeq: number): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/timetravel/fork`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ taskId, targetTurnSeq }),
    });
    if (!res.ok) throw new Error(`BFF mutateTimeTravelFork failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async fetchCowAndAstMerger(): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/cow-ast`);
    if (!res.ok) throw new Error(`BFF fetchCowAndAstMerger failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateForkAndAttestBranch(params: {
    sandboxId?: string;
    patchFile?: string;
    patchContent?: string;
  }): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/cow/fork-attest`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`BFF mutateForkAndAttestBranch failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async fetchFinOpsAndWdrfRadar(): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/finops-wdrf`);
    if (!res.ok) throw new Error(`BFF fetchFinOpsAndWdrfRadar failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateIssueRbacToken(operatorId: string, role: "VIEWER" | "OPERATOR" | "SRE_ADMIN"): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/rbac/token`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ operatorId, role }),
    });
    if (!res.ok) throw new Error(`BFF mutateIssueRbacToken failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }

  public async mutateVerifyRbacPermission(token: string, permission: string): Promise<{ status: number; body: Record<string, any> }> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/rbac/verify`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ token, permission }),
    });
    const body = (await res.json()) as Record<string, any>;
    return { status: res.status, body };
  }

  public async mutateRunV27DeepTechProbe(): Promise<Record<string, any>> {
    const res = await fetch(`${this.baseUrl}/v1beta1/console/v27/deep-tech-probe`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({}),
    });
    if (!res.ok) throw new Error(`BFF mutateRunV27DeepTechProbe failed: ${res.status}`);
    return (await res.json()) as Record<string, any>;
  }
}
