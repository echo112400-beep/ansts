export type SystemState = "healthy" | "degraded" | "unavailable" | "unverified";
export type Metric = {
  key: string;
  label: string;
  value: number | string | boolean | null;
  unit?: string;
};
export type SystemNode = {
  id: string;
  label: string;
  kind: string;
  plane: string;
  description: string;
  critical?: boolean;
  status: SystemState;
  summary: string;
  endpoint: string | null;
  telemetryUrl?: string;
  telemetryPath?: string;
  checkedAt: number | null;
  latencyMs: number | null;
  httpStatus: number | null;
  metrics: Metric[];
  checks: {
    label: string;
    status: SystemState;
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
};
export type SystemEdge = {
  source: string;
  target: string;
  label: string;
  kind: string;
};
export type SystemSnapshot = {
  schemaVersion: 1;
  observedAt: number;
  startedAt: number;
  refreshIntervalMs: number;
  status: SystemState;
  nodes: SystemNode[];
  edges: SystemEdge[];
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
};
export const stateLabel: Record<SystemState, string> = {
  healthy: "探测正常",
  degraded: "需要关注",
  unavailable: "无法验证可用",
  unverified: "未探测",
};
export const planeLabel: Record<string, string> = {
  ingress: "入口与控制台",
  gateway: "接入与鉴权",
  control: "状态与调度",
  execution: "执行与沙箱",
  storage: "持久化存储",
  observability: "可观测性",
  external: "外部依赖",
};
export const time = (v: number | null) =>
  v ? new Date(v).toLocaleTimeString("zh-CN", { hour12: false }) : "未探测";
export function metricValue(m: Metric) {
  if (m.value === null) return "未提供";
  if (typeof m.value === "boolean") return m.value ? "是" : "否";
  if (typeof m.value === "number") {
    if (m.unit === "bytes")
      return m.value >= 1024 ** 3
        ? `${(m.value / 1024 ** 3).toFixed(2)} GiB`
        : `${(m.value / 1024 ** 2).toFixed(1)} MiB`;
    if (m.unit === "MiB")
      return `${Math.round(m.value).toLocaleString("zh-CN")} MiB`;
    return `${m.value.toLocaleString("zh-CN")}${m.unit === "cores" ? " 核" : ""}`;
  }
  return m.value;
}
export const numeric = (node: SystemNode, key: string) => {
  const v = node.metrics.find((m) => m.key === key)?.value;
  return typeof v === "number" ? v : null;
};
