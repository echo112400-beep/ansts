/** Bounded, read-only infrastructure observation. Only called after SRE authorization. */
export type SystemStatus =
  "healthy" | "degraded" | "unavailable" | "unverified";
export interface TopologyMetric {
  key: string;
  label: string;
  value: number | string | boolean | null;
  unit?: string;
}
interface ServiceConfig {
  id: string;
  label: string;
  kind: string;
  plane: string;
  description: string;
  url?: string;
  healthPath?: string;
  telemetryUrl?: string;
  telemetryPath?: string;
  critical?: boolean;
}
export interface TopologyEdge {
  source: string;
  target: string;
  label: string;
  kind: string;
}
interface TopologyConfig {
  schemaVersion: number;
  nodes: ServiceConfig[];
  edges: TopologyEdge[];
}
export interface TopologyNode extends ServiceConfig {
  status: SystemStatus;
  summary: string;
  endpoint: string | null;
  checkedAt: number | null;
  latencyMs: number | null;
  httpStatus: number | null;
  metrics: TopologyMetric[];
  checks: {
    label: string;
    status: SystemStatus;
    httpStatus: number | null;
    latencyMs: number;
  }[];
  targets?: {
    job: string;
    endpoint: string;
    healthy: boolean;
    error: string | null;
    lastScrape: string | null;
  }[];
}
export interface SystemSnapshot {
  schemaVersion: 1;
  observedAt: number;
  startedAt: number;
  refreshIntervalMs: number;
  status: SystemStatus;
  nodes: TopologyNode[];
  edges: TopologyEdge[];
  summary: {
    healthy: number;
    degraded: number;
    unavailable: number;
    unverified: number;
    probed: number;
    total: number;
    slots: number | null;
    availableSlots: number | null;
    capacityReporting: number;
    executors: number;
    fleetReady: number;
    fleetTotal: number;
  };
  logicalServices: {
    name: string;
    responsibility: string;
    authority: string;
  }[];
  boundaries: string[];
}
export interface TopologyEnvironment {
  ANSTS_SYSTEM_TOPOLOGY_JSON?: string;
  ANSTS_AGENT_ENABLED?: string;
  ANSTS_SANDBOX_IMAGE?: string;
}
interface Probe {
  ok: boolean;
  httpStatus: number | null;
  latencyMs: number;
  data: Record<string, any>;
  error?: string;
}
const number = (value: unknown): number | null =>
  typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : null;
const metric = (
  key: string,
  label: string,
  value: TopologyMetric["value"],
  unit?: string,
): TopologyMetric => ({ key, label, value, ...(unit ? { unit } : {}) });
function base(value: unknown): string {
  if (typeof value !== "string") throw new Error("TOPOLOGY_CONFIG_INVALID");
  const url = new URL(value);
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/"
  )
    throw new Error("TOPOLOGY_CONFIG_INVALID");
  return url.origin;
}
export function parseTopologyConfig(source?: string): TopologyConfig {
  if (!source) throw new Error("TOPOLOGY_UNCONFIGURED");
  if (source.length > 32768) throw new Error("TOPOLOGY_CONFIG_INVALID");
  const raw = JSON.parse(source) as TopologyConfig;
  const ids = new Set<string>();
  if (
    raw.schemaVersion !== 1 ||
    !Array.isArray(raw.nodes) ||
    !raw.nodes.length ||
    raw.nodes.length > 24 ||
    !Array.isArray(raw.edges) ||
    raw.edges.length > 80
  )
    throw new Error("TOPOLOGY_CONFIG_INVALID");
  for (const node of raw.nodes) {
    if (
      !node ||
      !/^[a-z0-9-]{1,64}$/.test(node.id) ||
      ids.has(node.id) ||
      typeof node.label !== "string" ||
      node.label.length > 100 ||
      typeof node.description !== "string" ||
      node.description.length > 512 ||
      ![
        "http",
        "gateway",
        "fleet",
        "daemon",
        "storage",
        "prometheus",
        "grafana",
        "logical",
        "external",
      ].includes(node.kind) ||
      ![
        "ingress",
        "gateway",
        "control",
        "execution",
        "storage",
        "observability",
        "external",
      ].includes(node.plane)
    )
      throw new Error("TOPOLOGY_CONFIG_INVALID");
    ids.add(node.id);
    for (const field of ["url", "telemetryUrl"] as const)
      if (node[field]) base(node[field]);
    for (const field of ["healthPath", "telemetryPath"] as const)
      if (
        node[field] !== undefined &&
        (typeof node[field] !== "string" ||
          !/^\/[a-zA-Z0-9/_.-]*$/.test(node[field]!))
      )
        throw new Error("TOPOLOGY_CONFIG_INVALID");
    if (node.telemetryUrl && !node.telemetryPath)
      throw new Error("TOPOLOGY_CONFIG_INVALID");
  }
  for (const edge of raw.edges)
    if (
      !ids.has(edge.source) ||
      !ids.has(edge.target) ||
      typeof edge.label !== "string" ||
      edge.label.length > 100 ||
      ![
        "request",
        "state",
        "execution",
        "storage",
        "telemetry",
        "provider",
        "compatibility",
      ].includes(edge.kind)
    )
      throw new Error("TOPOLOGY_CONFIG_INVALID");
  return {
    schemaVersion: 1,
    nodes: raw.nodes.map((n) => ({
      id: n.id,
      label: n.label,
      kind: n.kind,
      plane: n.plane,
      description: n.description,
      ...(n.url ? { url: n.url, healthPath: n.healthPath } : {}),
      ...(n.telemetryUrl
        ? { telemetryUrl: n.telemetryUrl, telemetryPath: n.telemetryPath }
        : {}),
      ...(typeof n.critical === "boolean" ? { critical: n.critical } : {}),
    })),
    edges: raw.edges.map((e) => ({
      source: e.source,
      target: e.target,
      label: e.label,
      kind: e.kind,
    })),
  };
}
async function probe(
  origin: string,
  route: string,
  json = true,
): Promise<Probe> {
  const start = Date.now();
  let responseStatus: number | null = null;
  try {
    const response = await fetch(new URL(route, base(origin)), {
      redirect: "error",
      signal: AbortSignal.timeout(2500),
      headers: { accept: json ? "application/json" : "text/html" },
    });
    responseStatus = response.status;
    let data: Record<string, any> = {};
    if (json && response.body) {
      const reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let total = 0;
      try {
        for (;;) {
          const part = await reader.read();
          if (part.done) break;
          total += part.value.length;
          if (total > 262144) {
            await reader.cancel();
            throw new Error("PROBE_BODY_LIMIT");
          }
          chunks.push(part.value);
        }
      } finally {
        reader.releaseLock();
      }
      data = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      if (!data || typeof data !== "object" || Array.isArray(data))
        throw new Error("PROBE_SCHEMA_INVALID");
    } else await response.body?.cancel();
    return {
      ok: response.ok,
      httpStatus: response.status,
      latencyMs: Date.now() - start,
      data,
    };
  } catch (error) {
    return {
      ok: false,
      httpStatus: responseStatus,
      latencyMs: Date.now() - start,
      data: {},
      error:
        (error as Error).name === "TimeoutError"
          ? "PROBE_TIMEOUT"
          : "PROBE_FAILED",
    };
  }
}
async function observe(
  config: ServiceConfig,
  env: TopologyEnvironment,
  githubEnabled: boolean,
): Promise<TopologyNode> {
  const node: TopologyNode = {
    ...config,
    endpoint: config.url
      ? new URL(config.healthPath ?? "/healthz", base(config.url)).toString()
      : null,
    status: "unverified",
    summary: "未主动探测",
    checkedAt: null,
    latencyMs: null,
    httpStatus: null,
    metrics: [],
    checks: [],
  };
  if (!config.url || ["logical", "external"].includes(config.kind)) {
    const enabled =
      config.id === "github"
        ? githubEnabled
        : config.id === "deepseek"
          ? env.ANSTS_AGENT_ENABLED === "true"
          : !!env.ANSTS_SANDBOX_IMAGE;
    node.summary =
      config.kind === "logical"
        ? "本地 Engine · 未独立探测"
        : enabled
          ? "已配置 · 未主动探测"
          : "未启用 · 未主动探测";
    node.metrics = [metric("configured", "已配置", enabled)];
    if (config.id === "engine")
      node.metrics.push(
        metric("image", "允许的沙箱镜像", env.ANSTS_SANDBOX_IMAGE ?? null),
        metric(
          "boundary",
          "执行边界",
          "非 root / 禁网 / 只读根 / CPU、内存、PID 限额",
        ),
      );
    return node;
  }
  const [health, telemetry] = await Promise.all([
    probe(
      config.url,
      config.healthPath ?? "/healthz",
      config.kind !== "http" || config.healthPath !== "/",
    ),
    config.telemetryUrl
      ? probe(config.telemetryUrl, config.telemetryPath!)
      : Promise.resolve(null),
  ]);
  node.checkedAt = Date.now();
  node.latencyMs = health.latencyMs;
  node.httpStatus = health.httpStatus;
  node.status = health.ok
    ? "healthy"
    : health.httpStatus === null
      ? "unavailable"
      : "degraded";
  node.summary = health.ok
    ? "探测通过"
    : health.httpStatus
      ? `HTTP ${health.httpStatus}`
      : health.error === "PROBE_TIMEOUT"
        ? "探测超时"
        : "无法连接或响应无效";
  node.checks.push({
    label: "就绪 / 健康端点",
    status: node.status,
    httpStatus: health.httpStatus,
    latencyMs: health.latencyMs,
  });
  const d = health.data;
  if (config.kind === "gateway" && health.ok) {
    if (d.controlPlane !== "celld-gateway" || d.healthy !== true) {
      node.status = "degraded";
      node.summary = "Gateway 依赖未就绪";
    }
    node.metrics.push(
      metric(
        "runtime",
        "运行时",
        typeof d.runtime === "string" ? d.runtime.slice(0, 100) : null,
      ),
      metric(
        "schedulerReachable",
        "Scheduler 可达",
        typeof d.scheduler?.reachable === "boolean"
          ? d.scheduler.reachable
          : null,
      ),
      metric(
        "sharedState",
        "共享控制状态",
        typeof d.gateway?.cellId === "string"
          ? d.gateway.cellId.slice(0, 100)
          : null,
      ),
    );
  }
  if (config.kind === "fleet") {
    if (
      health.ok &&
      (d.controlPlane !== "celld-scheduler" || d.healthy !== true)
    ) {
      node.status = "degraded";
      node.summary = "Worker 未就绪";
    }
    if (telemetry) {
      node.checks.push({
        label: "celld 内部遥测",
        status: telemetry.ok ? "healthy" : "unavailable",
        httpStatus: telemetry.httpStatus,
        latencyMs: telemetry.latencyMs,
      });
      if (!telemetry.ok && health.ok) {
        node.status = "degraded";
        node.summary = "Worker 可达 · 遥测不可用";
      }
      const t = telemetry.data;
      node.metrics.push(
        metric("ownedCells", "拥有对象", number(t.owned_cells)),
        metric(
          "residentCells",
          "驻留对象",
          Array.isArray(t.residents)
            ? t.residents.length
            : number(t.resident_cells),
        ),
        metric(
          "workingSetBytes",
          "内存工作集",
          number(t.cgroup_working_set_bytes),
          "bytes",
        ),
        metric("rssBytes", "RSS", number(t.rss_bytes), "bytes"),
        metric("generation", "部署代次", number(t.deployment?.generation)),
        metric("capacityWaiting", "等待容量", number(t.capacity_waiting)),
        metric("activationWaiting", "等待激活", number(t.activation_waiting)),
        metric("restoring", "恢复中", number(t.restoring)),
        metric("activating", "激活中", number(t.activating)),
      );
    }
    node.metrics.push(
      metric(
        "bodyAuthority",
        "Body 状态权威",
        ["fleet", "remote-worker"].includes(d.bodyAuthority)
          ? d.bodyAuthority
          : null,
      ),
      metric(
        "bodyNamespace",
        "Body 命名空间",
        typeof d.bodyNamespace === "string"
          ? d.bodyNamespace.slice(0, 100)
          : null,
      ),
    );
  }
  if (config.kind === "daemon") {
    if (
      d.draining === true ||
      d.recoveryRequired === true ||
      d.healthy === false
    )
      node.status = "degraded";
    node.summary =
      d.recoveryRequired === true
        ? "恢复待确认 · 保留资源"
        : d.draining === true
          ? "已 Drain · 停止新准入"
          : d.healthy === true && health.ok
            ? "可接受新执行"
            : node.summary;
    node.metrics.push(
      metric("slots", "执行槽位", number(d.capacity?.slots)),
      metric("availableSlots", "可用槽位", number(d.capacity?.available)),
      metric("reservedSlots", "预留槽位", number(d.capacity?.reserved)),
      metric("activeSandboxes", "活动沙箱", number(d.activeSandboxes)),
      metric(
        "cpuTotal",
        "可分配 CPU",
        number(d.resources?.total?.cpu),
        "cores",
      ),
      metric(
        "cpuReserved",
        "预留 CPU",
        number(d.resources?.reserved?.cpu),
        "cores",
      ),
      metric(
        "memoryTotal",
        "可分配内存",
        number(d.resources?.total?.memoryMb),
        "MiB",
      ),
      metric(
        "memoryReserved",
        "预留内存",
        number(d.resources?.reserved?.memoryMb),
        "MiB",
      ),
      metric(
        "draining",
        "Drain",
        typeof d.draining === "boolean" ? d.draining : null,
      ),
      metric(
        "recoveryRequired",
        "需要恢复确认",
        typeof d.recoveryRequired === "boolean" ? d.recoveryRequired : null,
      ),
    );
  }
  if (config.kind === "storage" && health.ok) {
    if (d.ready !== true) {
      node.status = "degraded";
      node.summary = "对象存储未就绪";
    }
    node.metrics.push(
      metric(
        "ready",
        "存储就绪",
        typeof d.ready === "boolean" ? d.ready : null,
      ),
      metric(
        "version",
        "版本",
        typeof d.version === "string" ? d.version.slice(0, 80) : null,
      ),
    );
  }
  if (config.kind === "grafana" && health.ok) {
    if (d.database !== "ok") {
      node.status = "degraded";
      node.summary = "Grafana 数据库异常";
    }
    node.metrics.push(
      metric(
        "version",
        "版本",
        typeof d.version === "string" ? d.version.slice(0, 80) : null,
      ),
      metric(
        "database",
        "数据库",
        typeof d.database === "string" ? d.database.slice(0, 40) : null,
      ),
    );
  }
  if (config.kind === "prometheus" && health.ok) {
    const targets = d.data?.activeTargets;
    if (
      d.status !== "success" ||
      !Array.isArray(targets) ||
      targets.length > 200
    ) {
      node.status = "degraded";
      node.summary = "采集目标响应无效";
    } else {
      node.targets = targets.map((t: any) => {
        let endpoint = "未知目标";
        try {
          const u = new URL(t.scrapeUrl);
          endpoint = u.host + u.pathname;
        } catch {}
        const code =
          typeof t.lastError === "string"
            ? /HTTP status (\d{3})/.exec(t.lastError)?.[1]
            : null;
        return {
          job:
            typeof t.labels?.job === "string"
              ? t.labels.job.slice(0, 80)
              : "未命名",
          endpoint: endpoint.slice(0, 180),
          healthy: t.health === "up",
          error: t.health === "up" ? null : code ? `HTTP ${code}` : "抓取失败",
          lastScrape:
            typeof t.lastScrape === "string" &&
            Number.isFinite(Date.parse(t.lastScrape))
              ? t.lastScrape
              : null,
        };
      });
      const up = node.targets.filter((t) => t.healthy).length;
      node.status =
        up === targets.length && targets.length > 0 ? "healthy" : "degraded";
      node.summary = `服务可达 · ${up}/${targets.length} 个目标采集正常`;
      node.metrics.push(
        metric("targetsUp", "采集正常", up),
        metric("targetsTotal", "采集目标", targets.length),
      );
    }
  }
  return node;
}
let cached:
  { key: string; expires: number; snapshot: SystemSnapshot } | undefined;
let pending: { key: string; promise: Promise<SystemSnapshot> } | undefined;
export async function collectSystemTopology(
  env: TopologyEnvironment,
  githubEnabled = false,
): Promise<SystemSnapshot> {
  const config = parseTopologyConfig(env.ANSTS_SYSTEM_TOPOLOGY_JSON);
  const key = JSON.stringify([
    env.ANSTS_SYSTEM_TOPOLOGY_JSON,
    githubEnabled,
    env.ANSTS_AGENT_ENABLED,
    env.ANSTS_SANDBOX_IMAGE,
  ]);
  if (cached?.key === key && cached.expires > Date.now())
    return cached.snapshot;
  if (pending?.key === key) return pending.promise;
  const promise = (async () => {
    const startedAt = Date.now();
    const nodes = await Promise.all(
      config.nodes.map((node) => observe(node, env, githubEnabled)),
    );
    const count = (status: SystemStatus) =>
      nodes.filter((n) => n.status === status).length;
    const value = (n: TopologyNode, k: string) =>
      n.metrics.find((m) => m.key === k)?.value;
    const fleet = nodes.filter((n) => n.kind === "fleet"),
      bodies = nodes.filter((n) => n.kind === "daemon"),
      capacity = bodies.filter(
        (n) =>
          typeof value(n, "slots") === "number" &&
          typeof value(n, "availableSlots") === "number",
      );
    const failedPlane = ["gateway", "control", "execution", "storage"].some(
      (plane) => {
        const members = nodes.filter(
          (n) =>
            n.plane === plane && n.critical !== false && n.kind !== "logical",
        );
        return (
          members.length > 0 && members.every((n) => n.status === "unavailable")
        );
      },
    );
    const snapshot: SystemSnapshot = {
      schemaVersion: 1,
      observedAt: Date.now(),
      startedAt,
      refreshIntervalMs: 15000,
      status: failedPlane
        ? "unavailable"
        : count("degraded") + count("unavailable") > 0
          ? "degraded"
          : "healthy",
      nodes,
      edges: config.edges,
      summary: {
        healthy: count("healthy"),
        degraded: count("degraded"),
        unavailable: count("unavailable"),
        unverified: count("unverified"),
        probed: nodes.filter((n) => n.checkedAt !== null).length,
        total: nodes.length,
        slots: capacity.length
          ? capacity.reduce((s, n) => s + Number(value(n, "slots")), 0)
          : null,
        availableSlots: capacity.length
          ? capacity.reduce(
              (s, n) =>
                s +
                (n.status === "healthy"
                  ? Number(value(n, "availableSlots"))
                  : 0),
              0,
            )
          : null,
        capacityReporting: capacity.length,
        executors: bodies.length,
        fleetReady: fleet.filter((n) => n.status === "healthy").length,
        fleetTotal: fleet.length,
      },
      logicalServices: [
        {
          name: "Gateway Cell",
          responsibility: "共享派发、租约与入口状态",
          authority: "gateway-control-plane",
        },
        {
          name: "Scheduler Cell",
          responsibility: "任务状态、队列、节点与单次认领",
          authority: "global-shard-0",
        },
        {
          name: "Body Cell",
          responsibility: "执行凭证、结果、日志与核对审计",
          authority:
            [
              ...new Set(
                fleet
                  .map(
                    (n) =>
                      n.metrics.find((m) => m.key === "bodyNamespace")?.value,
                  )
                  .filter((v): v is string => typeof v === "string" && !!v),
              ),
            ].join(" / ") || "命名空间未提供",
        },
        {
          name: "Harness / Tunnel",
          responsibility: "会话编排与通道对象",
          authority: "已部署绑定，未逐对象探测",
        },
      ],
      boundaries: [
        "连线表示配置中的依赖关系，不代表每条链路已被逐跳验证。",
        "CPU / 内存与槽位来自执行器预留账本，不是宿主实时使用率。",
        "逻辑对象承载于 celld Fleet，未逐对象执行读写探测；不宣称副本数或 RPO/RTO。",
        "外部身份/模型及 Docker Engine 未独立探测；不主动触发登录、执行或模型请求。",
        "存储就绪不代表 EC:4 或容灾演练通过。",
      ],
    };
    cached = { key, expires: Date.now() + 5000, snapshot };
    return snapshot;
  })();
  pending = { key, promise };
  try {
    return await promise;
  } finally {
    if (pending?.promise === promise) pending = undefined;
  }
}
