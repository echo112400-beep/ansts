/**
 * @fileoverview M16 TASK-75, M48-2, M58-1 & M67-1: denoland/celld Native `ctx.container` Companion Sandbox Bridge,
 * `runsc`/`kata` OCI Runtime Selector, Full RFC1918/CGNAT/Loopback/ULA/Hex-IP/Metadata `celld-fence` Blocker,
 * Start-Time `max_instances` Cap Guard, Subprocess Exit-Code Propagation & Ephemeral WorkDir Cleanup.
 */
import * as crypto from "node:crypto";
import * as childProcess from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

export type CelldContainerInstanceType =
  | "lite"
  | "dev"
  | "basic"
  | "standard"
  | "standard-1"
  | "standard-2"
  | "standard-3"
  | "standard-4";

export type CelldOciRuntime = "runc" | "runsc" | "kata";

export interface CelldContainerStartOptions {
  entrypoint?: string[];
  env?: Record<string, string>;
  enableInternet?: boolean;
  labels?: Record<string, string>;
  hardTimeout?: number;
}

export interface CelldContainerClassConfig {
  class_name: string;
  image: string;
  name?: string;
  instance_type?: CelldContainerInstanceType;
  max_instances?: number;
  runtime?: CelldOciRuntime;
}

export const INSTANCE_TYPE_SPECS: Record<CelldContainerInstanceType, { cpuCores: number; memoryMb: number }> = {
  lite: { cpuCores: 0.0625, memoryMb: 256 },
  dev: { cpuCores: 0.0625, memoryMb: 256 },
  basic: { cpuCores: 0.25, memoryMb: 1024 },
  standard: { cpuCores: 0.5, memoryMb: 4096 },
  "standard-1": { cpuCores: 0.5, memoryMb: 4096 },
  "standard-2": { cpuCores: 1.0, memoryMb: 6144 },
  "standard-3": { cpuCores: 2.0, memoryMb: 8192 },
  "standard-4": { cpuCores: 4.0, memoryMb: 12288 },
};

export class OperationInterruptedError extends Error {
  public readonly cellId: string;
  constructor(cellId: string, reason = "Container stopped due to cell migration or node restart") {
    super(`OperationInterruptedError [${cellId}]: ${reason}`);
    this.name = "OperationInterruptedError";
    this.cellId = cellId;
  }
}

export class CelldContainerHandle {
  public readonly cellId: string;
  public readonly config: CelldContainerClassConfig;
  public running = false;
  public inactivityTimeoutMs = 600_000;
  public resolvConfServers: string[] = [];
  public enableInternet = false;
  public readonly containerWorkDir: string;
  private migratedOrDestroyed = false;
  private readonly onBeforeStart?: (handle: CelldContainerHandle) => void;

  constructor(
    cellId: string,
    config: CelldContainerClassConfig,
    onBeforeStart?: (handle: CelldContainerHandle) => void
  ) {
    this.cellId = cellId;
    this.config = config;
    this.onBeforeStart = onBeforeStart;
    const safeId = cellId.replace(/[^a-zA-Z0-9._-]/g, "_");
    this.containerWorkDir = path.join(os.tmpdir(), `celld-container-${safeId}`);
  }

  public start(options: CelldContainerStartOptions = {}): {
    running: boolean;
    ociRuntime: CelldOciRuntime;
    resolvConf: string[];
    committedMemoryMb: number;
    bucketImageTarKey: string;
    containerWorkDir: string;
  } {
    if (options.hardTimeout !== undefined && (!Number.isFinite(options.hardTimeout) || options.hardTimeout <= 0)) {
      throw new Error("ctx.container.start(): hardTimeout must be > 0");
    }
    if (!this.running && this.onBeforeStart) {
      this.onBeforeStart(this);
    }
    this.running = true;
    this.migratedOrDestroyed = false;
    this.enableInternet = options.enableInternet ?? false;
    const runtime: CelldOciRuntime = this.config.runtime ?? "runc";
    this.resolvConfServers = runtime === "runsc" || runtime === "kata" ? ["1.1.1.1", "1.0.0.1"] : ["127.0.0.11"];
    const spec = INSTANCE_TYPE_SPECS[this.config.instance_type ?? "dev"];
    const imageHash = crypto.createHash("sha256").update(this.config.image, "utf-8").digest("hex").slice(0, 24);
    try {
      fs.mkdirSync(this.containerWorkDir, { recursive: true });
      fs.writeFileSync(
        path.join(this.containerWorkDir, "resolv.conf"),
        this.resolvConfServers.map((s) => `nameserver ${s}`).join("\n") + "\n",
        "utf-8"
      );
    } catch {
      // ignore if read-only
    }
    return {
      running: this.running,
      ociRuntime: runtime,
      resolvConf: [...this.resolvConfServers],
      committedMemoryMb: spec.memoryMb,
      bucketImageTarKey: `deploy/images/${imageHash}.tar`,
      containerWorkDir: this.containerWorkDir,
    };
  }

  public setInactivityTimeout(timeoutMs: number): void {
    const safe = Number.isFinite(timeoutMs) ? timeoutMs : 1000;
    this.inactivityTimeoutMs = Math.max(1000, safe);
  }

  private cleanupWorkDir(): void {
    try {
      if (fs.existsSync(this.containerWorkDir)) {
        fs.rmSync(this.containerWorkDir, { recursive: true, force: true });
      }
    } catch {
      // ignore cleanup error
    }
  }

  public simulateCellMoveDestroy(): void {
    this.running = false;
    this.migratedOrDestroyed = true;
    this.cleanupWorkDir();
  }

  public getTcpPort(port: number): {
    fetch: (url: string, init?: { headers?: Record<string, string> }) => Promise<{
      status: number;
      webSocketUpgraded: boolean;
      port: number;
      body: Record<string, unknown>;
    }>;
  } {
    return {
      fetch: async (url: string, init?: { headers?: Record<string, string> }) => {
        if (this.migratedOrDestroyed || !this.running) {
          throw new OperationInterruptedError(this.cellId);
        }
        // Validate raw hostname before URL normalization to catch hex/octal IPv4 notations too
        const rawHostMatch = url.match(/^[a-zA-Z]+:\/\/([^/:?#]+)/);
        if (rawHostMatch) {
          CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget(
            rawHostMatch[1],
            this.enableInternet
          );
        }
        const parsed = new URL(url);
        CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget(parsed.hostname, this.enableInternet);
        const isWs = init?.headers?.["Upgrade"]?.toLowerCase() === "websocket";
        return {
          status: isWs ? 101 : 200,
          webSocketUpgraded: isWs,
          port,
          body: { ok: true, cellId: this.cellId, target: parsed.hostname },
        };
      },
    };
  }

  public async exec(cmd: string[]): Promise<{ exitCode: number; stdout: string; rawStdout?: string; workDir: string }> {
    if (this.migratedOrDestroyed || !this.running) {
      throw new OperationInterruptedError(this.cellId);
    }
    fs.mkdirSync(this.containerWorkDir, { recursive: true });
    let rawStdout = "";
    let exitCode = 0;
    if (cmd.length > 0) {
      try {
        rawStdout = childProcess
          .execFileSync(cmd[0], cmd.slice(1), { cwd: this.containerWorkDir, stdio: "pipe", timeout: 5_000 })
          .toString("utf-8")
          .trim();
      } catch (err) {
        const e = err as { status?: number; code?: string; stdout?: Buffer };
        if (typeof e.status === "number") {
          exitCode = e.status;
        }
        rawStdout = e.stdout ? e.stdout.toString("utf-8").trim() : "";
      }
    }
    return {
      exitCode,
      stdout: rawStdout
        ? `${rawStdout} (Executed [${cmd.join(" ")}] in container for ${this.cellId})`
        : `Executed [${cmd.join(" ")}] in container for ${this.cellId}`,
      rawStdout,
      workDir: this.containerWorkDir,
    };
  }

  public async destroy(): Promise<void> {
    this.running = false;
    this.cleanupWorkDir();
  }
}

export class CelldNativeContainerSandboxManager {
  private readonly handles: Map<string, CelldContainerHandle> = new Map();
  private readonly maxInstancesByClass: Map<string, number> = new Map();

  public registerContainerClass(config: CelldContainerClassConfig): void {
    const allowedKeys = new Set(["class_name", "image", "name", "instance_type", "max_instances", "runtime"]);
    for (const k of Object.keys(config)) {
      if (!allowedKeys.has(k)) {
        throw new Error(`Unsupported celld containers configuration key: '${k}'`);
      }
    }
    const maxInst =
      config.max_instances !== undefined && Number.isFinite(config.max_instances) && config.max_instances > 0
        ? Math.floor(config.max_instances)
        : 64;
    this.maxInstancesByClass.set(config.class_name, maxInst);
  }

  private assertClassCapacityAvailable(className: string, fallbackMax?: number, excludeCellId?: string): void {
    const runningCount = Array.from(this.handles.values()).filter(
      (h) => h.cellId !== excludeCellId && h.config.class_name === className && h.running
    ).length;
    const cap = this.maxInstancesByClass.get(className) ?? fallbackMax ?? 64;
    if (runningCount >= cap) {
      throw new Error(`container_start_failed: fleet running count (${runningCount}) reached max_instances (${cap})`);
    }
  }

  public getOrCreateContainerHandle(cellId: string, config: CelldContainerClassConfig): CelldContainerHandle {
    let handle = this.handles.get(cellId);
    if (!handle) {
      this.assertClassCapacityAvailable(config.class_name, config.max_instances);
      handle = new CelldContainerHandle(cellId, config, (h) => {
        this.assertClassCapacityAvailable(h.config.class_name, h.config.max_instances, h.cellId);
      });
      this.handles.set(cellId, handle);
    }
    return handle;
  }

  public async destroyContainerHandle(cellId: string): Promise<boolean> {
    const handle = this.handles.get(cellId);
    if (!handle) return false;
    await handle.destroy();
    this.handles.delete(cellId);
    return true;
  }

  public getHandleCount(): number {
    return this.handles.size;
  }

  /**
   * [M58-1 & M67-1] Enforces `celld-fence` outbound network policy:
   * - Blocks all external traffic when `enableInternet === false` (allowing only container loopback bridge).
   * - Blocks RFC1918 (`10/8`, `172.16/12`, `192.168/16`), CGNAT (`100.64/10`), Link-Local/IMDS (`169.254/16`),
   *   non-bridge loopback (`127.0.0.0/8` except `127.0.0.1`), IPv6 ULA (`fc00::/7`), IPv6 Link-Local (`fe80::/10`),
   *   IPv4-mapped/compatible IPv6 (`::ffff:*`, `0:0:0:0:0:ffff:*`), hex/octal/integer IP encodings, and cloud metadata DNS names.
   */
  public static assertCelldFenceAllowsTarget(hostname: string, enableInternet: boolean): void {
    const cleanHost = hostname
      .trim()
      .replace(/^\[|\]$/g, "")
      .replace(/\.+$/, "")
      .toLowerCase();
    if (cleanHost === "localhost" || cleanHost === "127.0.0.1" || cleanHost === "container.bridge.local") {
      return;
    }
    if (!enableInternet) {
      throw new Error(`celld-fence blocked outbound traffic to '${hostname}' (enableInternet: false)`);
    }
    // Block cloud metadata DNS aliases and wildcard DNS rebinding domains
    if (
      cleanHost === "metadata.google.internal" ||
      cleanHost === "metadata.goog" ||
      cleanHost === "instance-data" ||
      cleanHost.endsWith(".nip.io") ||
      cleanHost.endsWith(".sslip.io")
    ) {
      throw new Error(`celld-fence nftables blocked metadata/rebinding hostname '${hostname}'`);
    }
    // Block hex/octal/integer IPv4 obfuscations (e.g. 0xa9fea9fe, 2852039166, 0177.0.0.1)
    if (
      /^0x[0-9a-f]+$/i.test(cleanHost) ||
      /^\d+$/.test(cleanHost) ||
      cleanHost.split(".").some((part) => /^0x[0-9a-f]+$/i.test(part) || (/^0\d+$/.test(part) && part !== "0"))
    ) {
      throw new Error(`celld-fence nftables blocked obfuscated IP literal '${hostname}'`);
    }
    // Check IPv6 link-local (fe80::/10 -> fe80..febf), ULA (fc00::/7 -> fc00..fdff), unspecified (::), loopback (::1), or IPv4-mapped IPv6
    if (
      cleanHost === "::" ||
      cleanHost === "::1" ||
      cleanHost === "0.0.0.0" ||
      /^fe[89ab][0-9a-f]:/.test(cleanHost) ||
      /^f[cd][0-9a-f]{2}:/.test(cleanHost) ||
      cleanHost.startsWith("::ffff:") ||
      cleanHost.startsWith("0:0:0:0:0:ffff:")
    ) {
      throw new Error(`celld-fence nftables blocked private/link-local range access to '${hostname}'`);
    }
    // Check IPv4 private/reserved/loopback ranges: 0.0.0.0/8, 10.0.0.0/8, 127.0.0.0/8, 192.168.0.0/16, 169.254.0.0/16 (IMDS), 172.16.0.0/12, 100.64.0.0/10
    const ipv4Match = cleanHost.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
    if (ipv4Match) {
      const octets = [
        Number(ipv4Match[1]),
        Number(ipv4Match[2]),
        Number(ipv4Match[3]),
        Number(ipv4Match[4]),
      ];
      if (octets.some((o) => o > 255)) {
        throw new Error(`celld-fence nftables blocked invalid IPv4 address '${hostname}'`);
      }
      const [o1, o2] = octets;
      const isPrivateOrReserved =
        o1 === 0 ||
        o1 === 10 ||
        o1 === 127 ||
        (o1 === 192 && o2 === 168) ||
        (o1 === 169 && o2 === 254) ||
        (o1 === 172 && o2 >= 16 && o2 <= 31) ||
        (o1 === 100 && o2 >= 64 && o2 <= 127);
      if (isPrivateOrReserved) {
        throw new Error(`celld-fence nftables blocked private/link-local range access to '${hostname}'`);
      }
    }
  }
}
