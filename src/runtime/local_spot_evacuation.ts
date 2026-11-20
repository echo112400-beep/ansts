import { closeSync, existsSync, fsyncSync, mkdirSync, mkdtempSync, openSync, readFileSync, realpathSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { FenceToken } from "../models.js";

interface SandboxSnapshot {
  taskId: string;
  sandboxId: string;
  fence: FenceToken;
  initialFiles: Record<string, string>;
}

interface SpotHost {
  hostId: string;
  tier: "ON_DEMAND" | "SPOT_PREEMPTIBLE";
  hourlyCostUsd: number;
  healthy: boolean;
  cordoned: boolean;
  activeSandboxes: Map<string, SandboxSnapshot>;
}

export interface SpotEvacuationReport {
  evictedSpotHostId: string;
  migratedSandboxesCount: number;
  targetOnDemandHostIds: string[];
  newFencesByTask: Record<string, { epoch: number; turnSeq: number }>;
  totalEvacuationLatencyMs: number;
  zeroStateLossVerified: boolean;
}

function safeSegment(value: string): boolean {
  return /^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$/.test(value);
}

export class LocalSpotEvacuation {
  public readonly bodyHosts = new Map<string, SpotHost>();
  private readonly root: string;

  constructor(migrationStorageRoot: string) {
    const root = resolve(migrationStorageRoot);
    mkdirSync(root, { recursive: true, mode: 0o700 });
    this.root = realpathSync(root);
  }

  public registerBodyHost(host: Omit<SpotHost, "activeSandboxes">): void {
    if (!safeSegment(host.hostId) || !Number.isFinite(host.hourlyCostUsd) || host.hourlyCostUsd < 0) {
      throw new Error("INVALID_SPOT_HOST");
    }
    const existing = this.bodyHosts.get(host.hostId);
    this.bodyHosts.set(host.hostId, { ...host, activeSandboxes: existing?.activeSandboxes ?? new Map() });
  }

  private availableHosts(allowSpot: boolean): SpotHost[] {
    return [...this.bodyHosts.values()]
      .filter(host => host.healthy && !host.cordoned && (allowSpot || host.tier === "ON_DEMAND"))
      .sort((left, right) => left.hourlyCostUsd - right.hourlyCostUsd || left.hostId.localeCompare(right.hostId));
  }

  public placeSandboxOnCostOptimalHost(params: SandboxSnapshot & { allowSpotInstance: boolean }) {
    if (!safeSegment(params.sandboxId) || params.fence.taskId !== params.taskId ||
      Object.keys(params.initialFiles).some(file => !file.split("/").every(safeSegment))) {
      throw new Error("INVALID_SPOT_SNAPSHOT");
    }
    if ([...this.bodyHosts.values()].some(host => host.activeSandboxes.has(params.sandboxId))) throw new Error("SPOT_SANDBOX_ALREADY_PLACED");
    const host = this.availableHosts(params.allowSpotInstance)[0];
    if (!host) throw new Error("NO_AVAILABLE_SPOT_HOST");
    const { allowSpotInstance: _allowSpot, ...snapshot } = params;
    host.activeSandboxes.set(snapshot.sandboxId, { ...snapshot, initialFiles: { ...snapshot.initialFiles } });
    return { assignedHostId: host.hostId, tier: host.tier, hourlyCostUsd: host.hourlyCostUsd };
  }

  public evacuateSpotHostOnTerminationNotice(hostId: string): SpotEvacuationReport {
    const started = performance.now();
    const source = this.bodyHosts.get(hostId);
    if (source?.tier !== "SPOT_PREEMPTIBLE") throw new Error("SPOT_HOST_NOT_FOUND");
    source.cordoned = true;
    const destinations = this.availableHosts(false);
    if (source.activeSandboxes.size && !destinations.length) throw new Error("NO_SPOT_EVACUATION_TARGET");
    const workDir = mkdtempSync(join(this.root, ".evacuation-"));
    const published: string[] = [];
    try {
      const transfers = [...source.activeSandboxes.values()].map((sandbox, index) => {
        const host = destinations[index % destinations.length];
        const hostDir = join(this.root, host.hostId);
        mkdirSync(hostDir, { recursive: true, mode: 0o700 });
        if (realpathSync(hostDir) !== hostDir) throw new Error("INVALID_SPOT_TARGET_DIRECTORY");
        const destination = join(hostDir, sandbox.sandboxId);
        if (existsSync(destination)) throw new Error("SPOT_SNAPSHOT_ALREADY_EXISTS");
        const stage = join(workDir, String(index));
        mkdirSync(stage, { mode: 0o700 });
        for (const [file, value] of Object.entries(sandbox.initialFiles)) {
          const target = join(stage, file);
          mkdirSync(dirname(target), { recursive: true, mode: 0o700 });
          const fileHandle = openSync(target, "wx", 0o600);
          try {
            writeFileSync(fileHandle, value, "utf8");
            fsyncSync(fileHandle);
          } finally {
            closeSync(fileHandle);
          }
          if (readFileSync(target, "utf8") !== value) throw new Error("SPOT_SNAPSHOT_VERIFICATION_FAILED");
        }
        const fence = new FenceToken(sandbox.fence.epoch + 1, sandbox.fence.turnSeq, sandbox.taskId);
        return { host, stage, destination, sandbox: { ...sandbox, fence } };
      });
      for (const transfer of transfers) {
        renameSync(transfer.stage, transfer.destination);
        published.push(transfer.destination);
      }
      for (const { host, sandbox } of transfers) host.activeSandboxes.set(sandbox.sandboxId, sandbox);
      source.activeSandboxes.clear();
      source.healthy = false;
      return {
        evictedSpotHostId: hostId, migratedSandboxesCount: transfers.length,
        targetOnDemandHostIds: [...new Set(transfers.map(transfer => transfer.host.hostId))],
        newFencesByTask: Object.fromEntries(transfers.map(({ sandbox: { taskId, fence } }) => [taskId, { epoch: fence.epoch, turnSeq: fence.turnSeq }])),
        totalEvacuationLatencyMs: performance.now() - started, zeroStateLossVerified: true,
      };
    } catch (error) {
      for (const directory of published) rmSync(directory, { recursive: true, force: true });
      throw error;
    } finally {
      rmSync(workDir, { recursive: true, force: true });
    }
  }
}
