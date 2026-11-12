/**
 * @fileoverview M-FLEET-1: denoland/celld Fleet HTTP Client — `fleet/capacity-v1.json` shared capacity view,
 * live `/state` node telemetry, `cells/<CLASS>:<cellId>/own.json` ownership lease CAS and brain-schedule
 * dispatch onto celld public listeners.
 *
 * Every route and object schema below was verified against a live `ghcr.io/denoland/celld:0.6.3` node
 * backed by rustfs (S3):
 *   - `GET  <internal>/state`                    → 200 (public listener serves 404 for `/state`)
 *   - `GET  <public>/.well-known/celld/health`   → `{"ok":true}`, 503 while draining
 *   - `fleet/capacity-v1.json`                   → `{ready:{started_ms,leases:[{node,expires_ms,addr,load{...}}]}}`
 *                                                  republished by every node every `CELLD_REBALANCE_INTERVAL_MS`
 *   - `cells/<CLASS>:<cellId>/own.json`          → `{"node":"celld-node-01","epoch":1}`
 *   - `nodes/<node>.json`, `deploy/current.json` → node lease and deployment pointer
 *
 * Ownership CAS writes need an R2 binding, so they are exercised from the Worker side; the Node gateway
 * only reads. celld itself owns leases for cells it runs.
 */
import { R2BucketLike } from "../cloudflare/env.js";

export interface CelldNodeEndpoint {
  id: string;
  publicUrl: string;
  internalUrl: string;
}

export interface CelldNodeState {
  nodeId: string;
  ready: boolean;
  /** Leader deployment generation per node; nodes retarget in place, so they converge over a few seconds. */
  deploymentGeneration: number | null;
  capacityWaiting: number;
  activationWaiting: number;
  activating: number;
  restoring: number;
  ownedCells: number;
  residentCells: number;
  workingSetBytes: number;
  rssBytes: number;
  draining: boolean;
  pressured: boolean;
  placementWeight: number;
  stateAgeMs: number;
}

/**
 * Outcome of claiming a placed task: this gateway won it, a sibling gateway already holds it, or the fleet could
 * not answer at all. Callers must treat `TAKEN` as "stay out of the way" and `UNREACHABLE` as "proceed anyway".
 */
export type CelldClaimOutcome = "CLAIMED" | "TAKEN" | "UNREACHABLE";

export interface CelldOwnershipRecord {
  className: string;
  cellId: string;
  nodeId: string;
  epoch: number;
  etag: string;
}

export interface CelldBrainScheduleRequest {
  taskId: string;
  tenantId: string;
  promptText: string;
  targetBodyHostId?: string;
  slot?: number;
  failoverFromBrainId?: string;
}

export interface CelldBrainScheduleResult {
  scheduledByBrainId: string;
  state: string;
  fencingToken: number;
  deduplicated: boolean;
  placements: Array<[string, string, number]>;
  celldNodeId: string;
}

export class CelldFleetUnavailableError extends Error {
  public readonly nodeId: string;
  constructor(nodeId: string, detail: string) {
    super(`celld node '${nodeId}' is unavailable: ${detail}`);
    this.name = "CelldFleetUnavailableError";
    this.nodeId = nodeId;
  }
}

/**
 * Thrown when the cell refuses a purge request (for example an in-flight task state). Distinct from
 * `CelldFleetUnavailableError` because the fleet answered fine — the request itself was wrong.
 */
export class CelldPurgeRejectedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CelldPurgeRejectedError";
  }
}

export class CelldOwnershipCasError extends Error {
  public readonly cellId: string;
  public readonly expectedEpoch: number;
  constructor(cellId: string, expectedEpoch: number) {
    super(`S3 CAS rejected ownership write for cell '${cellId}' at epoch ${expectedEpoch}`);
    this.name = "CelldOwnershipCasError";
    this.cellId = cellId;
    this.expectedEpoch = expectedEpoch;
  }
}

const SAFE_ID = /^[a-zA-Z0-9._:-]+$/;

function assertSafeId(value: string, label: string): string {
  const trimmed = value.trim();
  if (!trimmed || !SAFE_ID.test(trimmed) || trimmed.includes("..")) {
    throw new Error(`Invalid celld ${label} '${value}': must match ${SAFE_ID}`);
  }
  return trimmed;
}

function num(source: unknown, key: string): number {
  if (source === null || typeof source !== "object") return 0;
  const raw = (source as Record<string, unknown>)[key];
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  if (typeof raw === "string" && raw.trim() !== "" && Number.isFinite(Number(raw))) return Number(raw);
  return 0;
}

function emptyState(nodeId: string): CelldNodeState {
  return {
    nodeId,
    ready: false,
    deploymentGeneration: null,
    capacityWaiting: 0,
    activationWaiting: 0,
    activating: 0,
    restoring: 0,
    ownedCells: 0,
    residentCells: 0,
    workingSetBytes: 0,
    rssBytes: 0,
    draining: false,
    pressured: false,
    placementWeight: 0,
    stateAgeMs: 0,
  };
}

export class CelldFleetClient {
  private readonly nodes: CelldNodeEndpoint[];
  private readonly bucket: R2BucketLike | null;
  private readonly leaseTtlMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly now: () => number;

  constructor(options: {
    nodes: CelldNodeEndpoint[];
    bucket?: R2BucketLike | null;
    leaseTtlMs?: number;
    fetchImpl?: typeof fetch;
    now?: () => number;
  }) {
    this.nodes = options.nodes;
    this.bucket = options.bucket ?? null;
    this.leaseTtlMs = options.leaseTtlMs ?? 10_000;
    this.fetchImpl = options.fetchImpl ?? fetch;
    this.now = options.now ?? (() => Date.now());
  }

  /** Parses `ANSTS_CELLD_FLEET_URLS` (public) and `ANSTS_CELLD_INTERNAL_URLS` (internal listener) maps. */
  public static fromEnv(env: Record<string, string | undefined>, bucket?: R2BucketLike | null): CelldFleetClient {
    const publicUrls = CelldFleetClient.parseUrlMap(env.ANSTS_CELLD_FLEET_URLS);
    const internalUrls = CelldFleetClient.parseUrlMap(env.ANSTS_CELLD_INTERNAL_URLS);
    const ids = Array.from(new Set([...Object.keys(publicUrls), ...Object.keys(internalUrls)])).sort();
    const nodes: CelldNodeEndpoint[] = ids
      .filter((id) => Boolean(publicUrls[id]))
      .map((id) => ({
        id,
        publicUrl: publicUrls[id].replace(/\/+$/, ""),
        internalUrl: (internalUrls[id] ?? publicUrls[id]).replace(/\/+$/, ""),
      }));
    return new CelldFleetClient({ nodes, bucket: bucket ?? null });
  }

  private static parseUrlMap(raw: string | undefined): Record<string, string> {
    if (!raw || raw.trim() === "") return {};
    const parsed = JSON.parse(raw) as unknown;
    if (parsed === null || typeof parsed !== "object") {
      throw new Error("celld URL env vars must be a JSON object of node id → url");
    }
    const out: Record<string, string> = {};
    for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
      if (typeof value !== "string" || value.trim() === "") continue;
      out[key] = value;
    }
    return out;
  }

  public listNodes(): CelldNodeEndpoint[] {
    return [...this.nodes];
  }

  /** `GET <internal>/state` + public `/.well-known/celld/health`; verified on celld 0.6.3. */
  public async probeNode(node: CelldNodeEndpoint): Promise<CelldNodeState> {
    const startedAt = this.now();
    const [stateResp, healthResp] = await Promise.all([
      this.fetchImpl(`${node.internalUrl}/state`, { signal: AbortSignal.timeout(5_000) }),
      this.fetchImpl(`${node.publicUrl}/.well-known/celld/health`, { signal: AbortSignal.timeout(5_000) }),
    ]);
    if (!stateResp.ok) {
      throw new CelldFleetUnavailableError(node.id, `GET /state returned HTTP ${stateResp.status}`);
    }
    const healthText = healthResp.ok ? await healthResp.text() : "";
    const body = (await stateResp.json()) as Record<string, unknown>;
    const deployment = (body.deployment ?? {}) as Record<string, unknown>;
    const generation = deployment.generation;
    const residents = body.residents;
    return {
      nodeId: node.id,
      ready: healthResp.ok && healthText.includes('"ok":true'),
      deploymentGeneration:
        typeof generation === "number" && Number.isFinite(generation) ? generation : null,
      capacityWaiting: num(body, "capacity_waiting"),
      activationWaiting: num(body, "activation_waiting"),
      activating: num(body, "activating"),
      restoring: num(body, "restoring"),
      ownedCells: num(body, "owned_cells"),
      residentCells: Array.isArray(residents) ? residents.length : num(body, "resident_cells"),
      workingSetBytes: num(body, "cgroup_working_set_bytes"),
      rssBytes: num(body, "rss_bytes"),
      draining: false,
      pressured: false,
      placementWeight: 0,
      stateAgeMs: this.now() - startedAt,
    };
  }

  /** Probes every configured node; unreachable nodes are reported as not-ready instead of throwing. */
  public async probeFleet(): Promise<CelldNodeState[]> {
    return Promise.all(
      this.nodes.map(async (node) => {
        try {
          return await this.probeNode(node);
        } catch {
          return emptyState(node.id);
        }
      })
    );
  }

  /**
   * Parses the fleet capacity view every celld node republishes (`fleet/capacity-v1.json`).
   * Shape: `{ready:{started_ms,leases:[{node,expires_ms,addr,load:{...}}]}}`.
   */
  public parseCapacityView(view: Record<string, unknown> | null): CelldNodeState[] {
    const ready = (view?.ready ?? null) as Record<string, unknown> | null;
    const leases = Array.isArray(ready?.leases) ? (ready?.leases as Record<string, unknown>[]) : [];
    return leases.map((lease) => {
      const load = (lease.load ?? {}) as Record<string, unknown>;
      const expiresMs = num(lease, "expires_ms");
      return {
        ...emptyState(String(lease.node ?? "")),
        ready: expiresMs === 0 || expiresMs > this.now(),
        ownedCells: num(load, "owned_cells"),
        residentCells: num(load, "resident_cells"),
        restoring: num(load, "restoring"),
        workingSetBytes: num(load, "cgroup_working_set_bytes"),
        rssBytes: num(load, "rss_bytes"),
        draining: Boolean(load.draining),
        pressured: Boolean(load.pressured),
        placementWeight: num(load, "placement_weight"),
      } satisfies CelldNodeState;
    });
  }

  public async loadCapacityView(): Promise<Record<string, unknown> | null> {
    if (!this.bucket) return null;
    const object = await this.bucket.get("fleet/capacity-v1.json");
    if (!object) return null;
    const text = await object.text();
    if (!text) return null;
    try {
      return JSON.parse(text) as Record<string, unknown>;
    } catch {
      return null;
    }
  }

  /**
   * Least-loaded ready node. celld forwards an ingress request to the cell owner, so any ready node is a
   * valid entry point; picking the emptiest one keeps the entry hop off the busiest node.
   */
  public async pickNode(): Promise<CelldNodeEndpoint> {
    const capacity = this.parseCapacityView(await this.loadCapacityView());
    const states =
      capacity.length > 0
        ? capacity
        : await this.probeFleet();
    const byId = new Map(states.map((state) => [state.nodeId, state]));
    const ready = this.nodes.filter((node) => {
      const state = byId.get(node.id);
      return state ? state.ready && !state.draining : false;
    });
    if (ready.length === 0) {
      throw new CelldFleetUnavailableError(
        this.nodes.map((node) => node.id).join(",") || "<none>",
        "no celld node reports readiness"
      );
    }
    ready.sort((a, b) => {
      const sa = byId.get(a.id) as CelldNodeState;
      const sb = byId.get(b.id) as CelldNodeState;
      const backlog =
        sa.capacityWaiting + sa.activationWaiting + sa.activating - (sb.capacityWaiting + sb.activationWaiting + sb.activating);
      if (backlog !== 0) return backlog;
      return sa.workingSetBytes - sb.workingSetBytes;
    });
    return ready[0];
  }

  /**
   * Renews the leases this gateway holds through celld. celld expires a lease unless the holder renews it,
   * so without this every celld-placed task is reconciled as orphaned and eventually dead-lettered.
   * Returns the task ids whose lease is gone (task unknown to the cell), so callers can stop renewing them.
   */
  public async renewLeases(
    leases: Array<{ taskId: string; nodeId: string; fencingToken: number; celldNodeId: string }>
  ): Promise<string[]> {
    const dropped: string[] = [];
    await Promise.all(
      leases.map(async (lease) => {
        const node = this.nodes.find((candidate) => candidate.id === lease.celldNodeId);
        if (!node) return;
        try {
          const resp = await this.fetchImpl(`${node.publicUrl}/v1/scheduler/lease-renew`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ task_id: lease.taskId, node_id: lease.nodeId, fencing_token: lease.fencingToken }),
            signal: AbortSignal.timeout(5_000),
          });
          if (resp.status === 404) dropped.push(lease.taskId);
        } catch {
          // A transient node failure is not a reason to forget the lease; the next tick retries.
        }
      })
    );
    return dropped;
  }

  /** Tells the cell that a placed task finished, which releases the lease and the body capacity. */
  public async completeTask(
    taskId: string,
    celldNodeId: string,
    nodeId: string,
    fencingToken: number
  ): Promise<boolean> {
    const node = this.nodes.find((candidate) => candidate.id === celldNodeId);
    if (!node) return false;
    const resp = await this.fetchImpl(`${node.publicUrl}/v1/scheduler/complete`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ task_id: taskId, node_id: nodeId, fencing_token: fencingToken }),
      signal: AbortSignal.timeout(5_000),
    });
    return resp.ok;
  }

  /**
   * Claims a placed task for this gateway. The cell arbitrates claim conflicts, so exactly one gateway runs a
   * task even when several of them poll the same cell.
   *
   * The three outcomes are deliberately distinct: `TAKEN` means a sibling gateway already owns the task and this
   * one must stay out of the way, while `UNREACHABLE` means the fleet could not answer — there the caller keeps
   * executing, because not running the task at all is worse than a rare duplicate while the fleet is down.
   */
  public async claimTask(taskId: string, holderId: string): Promise<CelldClaimOutcome> {
    for (const node of this.nodes) {
      try {
        const resp = await this.fetchImpl(`${node.publicUrl}/v1/scheduler/claim`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ task_id: taskId, holder_id: holderId }),
          signal: AbortSignal.timeout(5_000),
        });
        if (resp.ok) {
          const body = (await resp.json()) as Record<string, unknown>;
          return body.claimed ? "CLAIMED" : "TAKEN";
        }
        // 409 = held by a sibling gateway; 404 = gone from the cell. Both mean "do not execute".
        if (resp.status === 409 || resp.status === 404) return "TAKEN";
      } catch {
        // Try the next celld node; the cell is forwarded to its owner node either way.
      }
    }
    return "UNREACHABLE";
  }

  /**
   * Purges finished tasks from the cell. Only terminal tasks can be purged; the cell rejects any in-flight
   * state, so a cleanup request can never drop a task a body is about to execute. `dryRun` reports what would
   * go without deleting it.
   */
  public async purgeTerminalTasks(
    options: { states?: string[]; olderThanMs?: number; dryRun?: boolean } = {}
  ): Promise<{ purged: number; taskIds: string[]; remaining: number }> {
    let lastError: unknown = null;
    for (const node of this.nodes) {
      try {
        const resp = await this.fetchImpl(`${node.publicUrl}/v1/scheduler/purge`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            states: options.states,
            older_than_ms: options.olderThanMs,
            dry_run: options.dryRun === true,
          }),
          signal: AbortSignal.timeout(10_000),
        });
        const body = (await resp.json()) as Record<string, unknown>;
        if (!resp.ok) {
          // A rejected purge is a caller error, not an unreachable fleet: surface it as-is and stop probing,
          // otherwise ops reads "node unavailable" for what is really "you asked to purge a running task".
          throw new CelldPurgeRejectedError(String(body.message ?? `purge failed with HTTP ${resp.status}`));
        }
        return {
          purged: Number(body.purged ?? 0),
          taskIds: Array.isArray(body.task_ids) ? body.task_ids.map((id) => String(id)) : [],
          remaining: Number(body.remaining ?? 0),
        };
      } catch (err) {
        if (err instanceof CelldPurgeRejectedError) throw err;
        lastError = err;
      }
    }
    throw new CelldFleetUnavailableError(
      `celld purge unreachable: ${lastError instanceof Error ? lastError.message : "no node answered"}`
    );
  }

  /**
   * Reads the tasks the cell holds. Used to adopt placed tasks this gateway did not schedule itself, so a
   * gateway restart (or a submission that landed on a sibling gateway) cannot strand a task in `LEASED`.
   */
  public async listTasks(): Promise<{ celldNodeId: string; tasks: Array<Record<string, unknown>> }> {
    for (const node of this.nodes) {
      try {
        const resp = await this.fetchImpl(`${node.publicUrl}/internal/v1beta1/brain/tasks`, {
          method: "GET",
          signal: AbortSignal.timeout(5_000),
        });
        if (!resp.ok) continue;
        const body = (await resp.json()) as { tasks?: Array<Record<string, unknown>> };
        return { celldNodeId: node.id, tasks: Array.isArray(body.tasks) ? body.tasks : [] };
      } catch {
        // Try the next celld node.
      }
    }
    return { celldNodeId: "", tasks: [] };
  }

  /**
   * Heartbeats every body host into the scheduler cell on EVERY celld node. celld keeps the body registry
   * per node process, while brain-schedule requests are load-balanced across nodes, so a registration that
   * only reached one node leaves the other nodes' cells with no body capacity.
   */
  public async registerNodes(
    heartbeats: Array<{
      node_id: string;
      total_cpu_cores: number;
      free_cpu_cores: number;
      total_memory_mb: number;
      free_memory_mb: number;
      warm_pool_ready?: number;
      running_tasks?: number;
      healthy?: boolean;
    }>
  ): Promise<number> {
    if (heartbeats.length === 0 || this.nodes.length === 0) return 0;
    let registered = 0;
    for (const node of this.nodes) {
      const results = await Promise.all(
        heartbeats.map(async (hb) => {
          try {
            const resp = await this.fetchImpl(`${node.publicUrl}/v1/scheduler/register-node`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(hb),
              signal: AbortSignal.timeout(5_000),
            });
            return resp.ok;
          } catch {
            return false;
          }
        })
      );
      registered += results.filter(Boolean).length;
    }
    return registered;
  }

  /** Dispatches one brain-schedule call onto a ready celld public listener. */
  public async scheduleTask(
    request: CelldBrainScheduleRequest,
    nodeId?: string
  ): Promise<CelldBrainScheduleResult> {
    const node = nodeId
      ? this.nodes.find((candidate) => candidate.id === nodeId)
      : await this.pickNode();
    if (!node) {
      throw new CelldFleetUnavailableError(nodeId ?? "<auto>", "node is not configured in the fleet");
    }
    const resp = await this.fetchImpl(`${node.publicUrl}/internal/v1beta1/brain/schedule`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(request),
      signal: AbortSignal.timeout(15_000),
    });
    if (!resp.ok) {
      throw new CelldFleetUnavailableError(node.id, `brain schedule returned HTTP ${resp.status}`);
    }
    const body = (await resp.json()) as Record<string, unknown>;
    const placements = Array.isArray(body.placements)
      ? (body.placements as Array<[string, string, number]>)
      : [];
    return {
      scheduledByBrainId: String(body.scheduledByBrainId ?? node.id),
      state: String(body.state ?? "LEASED"),
      fencingToken: Number(body.fencingToken ?? 1),
      deduplicated: Boolean(body.deduplicated),
      placements,
      celldNodeId: node.id,
    };
  }

  /** `cells/<CLASS>:<cellId>/own.json` — the exact object celld writes when it leases a cell. */
  private ownershipKey(className: string, cellId: string): string {
    return `cells/${assertSafeId(className, "className")}:${assertSafeId(cellId, "cellId")}/own.json`;
  }

  public async readOwnership(className: string, cellId: string): Promise<CelldOwnershipRecord | null> {
    if (!this.bucket) return null;
    const key = this.ownershipKey(className, cellId);
    const object = await this.bucket.get(key);
    if (!object) return null;
    const text = await object.text();
    if (!text) return null;
    const parsed = JSON.parse(text) as Record<string, unknown>;
    return {
      className,
      cellId,
      nodeId: String(parsed.node ?? ""),
      epoch: num(parsed, "epoch"),
      etag: object.etag,
    };
  }

  /**
   * Claims or takes over a cell lease with the conditional write celld uses:
   * `If-None-Match: *` for the first claim, `If-Match: <etag>` to bump an epoch. Requires a bucket binding.
   */
  public async claimOwnership(className: string, cellId: string, nodeId: string): Promise<CelldOwnershipRecord> {
    if (!this.bucket) {
      throw new Error("CelldFleetClient.claimOwnership requires a bucket binding for lease CAS");
    }
    const safeNodeId = assertSafeId(nodeId, "nodeId");
    const key = this.ownershipKey(className, cellId);
    const existing = await this.readOwnership(className, cellId);
    const epoch = existing ? existing.epoch + 1 : 1;
    const written = await this.bucket.put(key, JSON.stringify({ node: safeNodeId, epoch }), {
      onlyIf: existing ? { etagMatches: existing.etag } : { etagDoesNotMatch: "*" },
      customMetadata: { cellClass: className, cellId, node: safeNodeId, epoch: String(epoch) },
    });
    if (!written) {
      throw new CelldOwnershipCasError(cellId, epoch);
    }
    return { className, cellId, nodeId: safeNodeId, epoch, etag: written.etag };
  }

  /** Re-writes `own.json` with the same epoch and node to refresh the node lease (celld's renewal path). */
  public async renewOwnership(record: CelldOwnershipRecord): Promise<CelldOwnershipRecord> {
    if (!this.bucket) {
      throw new Error("CelldFleetClient.renewOwnership requires a bucket binding for lease CAS");
    }
    const key = this.ownershipKey(record.className, record.cellId);
    const written = await this.bucket.put(
      key,
      JSON.stringify({ node: record.nodeId, epoch: record.epoch }),
      {
        onlyIf: { etagMatches: record.etag },
        customMetadata: {
          cellClass: record.className,
          cellId: record.cellId,
          node: record.nodeId,
          epoch: String(record.epoch),
        },
      }
    );
    if (!written) {
      throw new CelldOwnershipCasError(record.cellId, record.epoch);
    }
    return { ...record, etag: written.etag };
  }

  /** Lease TTL the client assumes when it schedules its own renewals (celld default `CELLD_TTL_MS`). */
  public get leaseTtl(): number {
    return this.leaseTtlMs;
  }
}
