"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.OperationInterruptedError = exports.INSTANCE_TYPE_SPECS = exports.CelldNativeContainerSandboxManager = exports.CelldContainerHandle = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M16 TASK-75: denoland/celld Native `ctx.container` Companion Sandbox Bridge,
 * `runsc`/`kata` OCI Runtime Selector, `celld-fence` nftables Private-Range Blocker & Bucket Layer Cache.
 */



























const INSTANCE_TYPE_SPECS = {
  lite: { cpuCores: 0.0625, memoryMb: 256 },
  dev: { cpuCores: 0.0625, memoryMb: 256 },
  basic: { cpuCores: 0.25, memoryMb: 1024 },
  standard: { cpuCores: 0.5, memoryMb: 4096 },
  "standard-1": { cpuCores: 0.5, memoryMb: 4096 },
  "standard-2": { cpuCores: 1.0, memoryMb: 6144 },
  "standard-3": { cpuCores: 2.0, memoryMb: 8192 },
  "standard-4": { cpuCores: 4.0, memoryMb: 12288 }
};exports.INSTANCE_TYPE_SPECS = INSTANCE_TYPE_SPECS;

class OperationInterruptedError extends Error {
  cellId;
  constructor(cellId, reason = "Container stopped due to cell migration or node restart") {
    super(`OperationInterruptedError [${cellId}]: ${reason}`);
    this.name = "OperationInterruptedError";
    this.cellId = cellId;
  }
}exports.OperationInterruptedError = OperationInterruptedError;

class CelldContainerHandle {
  cellId;
  config;
  running = false;
  inactivityTimeoutMs = 600_000;
  resolvConfServers = [];
  enableInternet = false;
  migratedOrDestroyed = false;

  constructor(cellId, config) {
    this.cellId = cellId;
    this.config = config;
  }

  start(options = {})





  {
    if (options.hardTimeout !== undefined && options.hardTimeout <= 0) {
      throw new Error("ctx.container.start(): hardTimeout must be > 0");
    }
    this.running = true;
    this.migratedOrDestroyed = false;
    this.enableInternet = options.enableInternet ?? false;
    const runtime = this.config.runtime ?? "runc";
    this.resolvConfServers = runtime === "runsc" || runtime === "kata" ? ["1.1.1.1", "1.0.0.1"] : ["127.0.0.11"];
    const spec = INSTANCE_TYPE_SPECS[this.config.instance_type ?? "dev"];
    const imageHash = crypto.createHash("sha256").update(this.config.image, "utf-8").digest("hex").slice(0, 24);
    return {
      running: this.running,
      ociRuntime: runtime,
      resolvConf: [...this.resolvConfServers],
      committedMemoryMb: spec.memoryMb,
      bucketImageTarKey: `deploy/images/${imageHash}.tar`
    };
  }

  setInactivityTimeout(timeoutMs) {
    this.inactivityTimeoutMs = Math.max(1000, timeoutMs);
  }

  simulateCellMoveDestroy() {
    this.running = false;
    this.migratedOrDestroyed = true;
  }

  getTcpPort(port)






  {
    return {
      fetch: async (url, init) => {
        if (this.migratedOrDestroyed || !this.running) {
          throw new OperationInterruptedError(this.cellId);
        }
        const parsed = new URL(url);
        CelldNativeContainerSandboxManager.assertCelldFenceAllowsTarget(parsed.hostname, this.enableInternet);
        const isWs = init?.headers?.["Upgrade"]?.toLowerCase() === "websocket";
        return {
          status: isWs ? 101 : 200,
          webSocketUpgraded: isWs,
          port,
          body: { ok: true, cellId: this.cellId, target: parsed.hostname }
        };
      }
    };
  }

  async exec(cmd) {
    if (this.migratedOrDestroyed || !this.running) {
      throw new OperationInterruptedError(this.cellId);
    }
    return { exitCode: 0, stdout: `Executed [${cmd.join(" ")}] in container for ${this.cellId}` };
  }

  async destroy() {
    this.running = false;
  }
}exports.CelldContainerHandle = CelldContainerHandle;

class CelldNativeContainerSandboxManager {
  handles = new Map();
  maxInstancesByClass = new Map();

  registerContainerClass(config) {
    const allowedKeys = new Set(["class_name", "image", "name", "instance_type", "max_instances", "runtime"]);
    for (const k of Object.keys(config)) {
      if (!allowedKeys.has(k)) {
        throw new Error(`Unsupported celld containers configuration key: '${k}'`);
      }
    }
    this.maxInstancesByClass.set(config.class_name, config.max_instances ?? 64);
  }

  getOrCreateContainerHandle(cellId, config) {
    let handle = this.handles.get(cellId);
    if (!handle) {
      const runningCount = Array.from(this.handles.values()).filter(
      (h) => h.config.class_name === config.class_name && h.running).
      length;
      const cap = this.maxInstancesByClass.get(config.class_name) ?? config.max_instances ?? 64;
      if (runningCount >= cap) {
        throw new Error(`container_start_failed: fleet running count (${runningCount}) reached max_instances (${cap})`);
      }
      handle = new CelldContainerHandle(cellId, config);
      this.handles.set(cellId, handle);
    }
    return handle;
  }

  static assertCelldFenceAllowsTarget(hostname, enableInternet) {
    if (hostname === "localhost" || hostname === "127.0.0.1" || hostname === "container.bridge.local") {
      return;
    }
    if (!enableInternet) {
      throw new Error(`celld-fence blocked outbound traffic to '${hostname}' (enableInternet: false)`);
    }
    const blockedPrefixes = ["10.", "192.168.", "169.254.", "100.64.", "172.16.", "172.17.", "172.18.", "fe80:", "fc00:", "fd00:"];
    if (blockedPrefixes.some((p) => hostname.toLowerCase().startsWith(p))) {
      throw new Error(`celld-fence nftables blocked private/link-local range access to '${hostname}'`);
    }
  }
}exports.CelldNativeContainerSandboxManager = CelldNativeContainerSandboxManager;