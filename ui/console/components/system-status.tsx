"use client";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { RefObject } from "react";
import {
  Activity,
  AlertTriangle,
  ArrowRight,
  ArrowUpRight,
  Box,
  Check,
  ChevronRight,
  Clock,
  Cloud,
  Database,
  GitBranch,
  Globe,
  HardDrive,
  Layers,
  LockKeyhole,
  Network,
  Pause,
  Play,
  RefreshCw,
  Search,
  Server,
  ShieldCheck,
  Terminal,
  X,
} from "lucide-react";
import type { User } from "../lib/console";
import {
  metricValue,
  numeric,
  planeLabel,
  stateLabel,
  time,
  type SystemSnapshot,
  type SystemNode,
  type SystemEdge,
} from "../lib/system";
type Api = (path: string) => Promise<any>;
const kindName: Record<string, string> = {
  http: "HTTP 服务",
  gateway: "Gateway Worker",
  fleet: "celld Fleet",
  daemon: "Body Daemon",
  storage: "S3 存储",
  prometheus: "指标采集",
  grafana: "观测看板",
  logical: "本地执行依赖",
  external: "外部服务",
};
function NodeIcon({ kind, size = 16 }: { kind: string; size?: number }) {
  const Icon =
    (
      {
        http: Globe,
        gateway: ShieldCheck,
        fleet: Layers,
        daemon: Terminal,
        storage: Database,
        prometheus: Activity,
        grafana: GitBranch,
        logical: Box,
        external: Cloud,
      } as Record<string, typeof Activity>
    )[kind] ?? Server;
  return <Icon size={size} />;
}
function State({ node }: { node: Pick<SystemNode, "status"> }) {
  return (
    <span className={`system-state ${node.status}`}>
      <i />
      {stateLabel[node.status]}
    </span>
  );
}
export default function SystemStatus({ api, user }: { api: Api; user: User }) {
  return user.role === "SRE_ADMIN" ? (
    <Topology api={api} />
  ) : (
    <BasicStatus api={api} />
  );
}
function Topology({ api }: { api: Api }) {
  const [snapshot, setSnapshot] = useState<SystemSnapshot | null>(null),
    [selectedId, setSelectedId] = useState(""),
    [busy, setBusy] = useState(false),
    [error, setError] = useState(""),
    [paused, setPaused] = useState(false),
    [query, setQuery] = useState(""),
    [onlyIssues, setOnlyIssues] = useState(false),
    [mobileDetail, setMobileDetail] = useState(false),
    [edgeMode, setEdgeMode] = useState<"request" | "all">("all");
  const canvasRef = useRef<HTMLDivElement>(null);
  const inspectorRef = useRef<HTMLElement>(null);
  const mounted = useRef(true),
    inFlight = useRef(false),
    dialog = useRef<HTMLDivElement>(null);
  const refresh = useCallback(async () => {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);
    try {
      const data: SystemSnapshot = await api("/v1/console/system");
      if (!mounted.current) return;
      setSnapshot(data);
      setError("");
      setSelectedId(
        (previous) =>
          (data.nodes.some((n) => n.id === previous) ? previous : "") ||
          [...data.nodes].sort((a, b) => {
            const score = (n: SystemNode) =>
              n.status === "unavailable"
                ? 100
                : n.status === "degraded"
                  ? n.critical === false
                    ? 30
                    : 60
                  : 0;
            return score(b) - score(a);
          })[0]?.id ||
          "",
      );
    } catch (e) {
      if (mounted.current) setError((e as Error).message);
    } finally {
      inFlight.current = false;
      if (mounted.current) setBusy(false);
    }
  }, [api]);
  useEffect(() => {
    mounted.current = true;
    void refresh();
    return () => {
      mounted.current = false;
    };
  }, [refresh]);
  useEffect(() => {
    if (paused) return;
    const poll = setInterval(() => void refresh(), 15000);
    return () => clearInterval(poll);
  }, [refresh, paused]);
  useEffect(() => {
    if (!mobileDetail) return;
    const previous = document.activeElement as HTMLElement;
    const before = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    const buttons = () =>
      Array.from(
        dialog.current?.querySelectorAll<HTMLElement>(
          "button:not(:disabled),a[href]",
        ) ?? [],
      );
    buttons()[0]?.focus();
    const key = (event: KeyboardEvent) => {
      if (event.key === "Escape") setMobileDetail(false);
      if (event.key !== "Tab") return;
      const list = buttons(),
        first = list[0],
        last = list[list.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    };
    document.addEventListener("keydown", key);
    return () => {
      document.body.style.overflow = before;
      document.removeEventListener("keydown", key);
      previous?.focus();
    };
  }, [mobileDetail]);
  useEffect(() => {
    inspectorRef.current?.scrollTo({ top: 0 });
    if (mobileDetail) {
      dialog.current?.scrollTo({ top: 0 });
      dialog.current
        ?.querySelector<HTMLButtonElement>(".system-close-detail")
        ?.focus({ preventScroll: true });
    }
  }, [selectedId, mobileDetail]);
  const selected = snapshot?.nodes.find((n) => n.id === selectedId) ?? null;
  const connected = useMemo(
    () =>
      new Set(
        snapshot?.edges
          .filter((e) => e.source === selectedId || e.target === selectedId)
          .flatMap((e) => [e.source, e.target]) ?? [],
      ),
    [snapshot, selectedId],
  );
  const choose = (id: string) => {
    setSelectedId(id);
    if (window.matchMedia("(max-width: 850px)").matches) setMobileDetail(true);
    else {
      const target = canvasRef.current?.querySelector<HTMLElement>(
        `[data-system-node="${id}"]`,
      );
      if (target) {
        const r = target.getBoundingClientRect();
        if (r.bottom > window.innerHeight - 30 || r.top < 85)
          target.scrollIntoView({
            block: "center",
            behavior: window.matchMedia("(prefers-reduced-motion: reduce)")
              .matches
              ? "auto"
              : "smooth",
          });
      }
    }
  };
  const issues =
    snapshot?.nodes.filter(
      (n) => n.status === "degraded" || n.status === "unavailable",
    ) ?? [];
  const rows =
    snapshot?.nodes.filter(
      (n) =>
        (!onlyIssues || ["degraded", "unavailable"].includes(n.status)) &&
        `${n.label} ${n.id} ${planeLabel[n.plane]}`
          .toLowerCase()
          .includes(query.toLowerCase()),
    ) ?? [];
  const inspector =
    selected && snapshot ? (
      <Inspector
        node={selected}
        nodes={snapshot.nodes}
        edges={snapshot.edges}
        choose={choose}
      />
    ) : null;
  return (
    <section className="system-page">
      <div className="system-heading">
        <div>
          <span className="eyebrow">INFRASTRUCTURE / SYSTEM TOPOLOGY</span>
          <h1>
            系统拓扑<span className="system-environment">DEVELOPMENT</span>
          </h1>
          <p>从请求入口到执行现场，沿着真实依赖定位系统状态。</p>
        </div>
        <div className="system-actions">
          <button
            className="secondary"
            aria-pressed={paused}
            onClick={() => setPaused((p) => !p)}
          >
            {paused ? <Play size={14} /> : <Pause size={14} />}
            <span>{paused ? "恢复自动刷新" : "暂停自动刷新"}</span>
          </button>
          <button
            className="primary"
            disabled={busy}
            onClick={() => void refresh()}
          >
            <RefreshCw size={14} className={busy ? "spin" : ""} />
            {busy ? "探测中…" : "刷新状态"}
          </button>
        </div>
      </div>
      {error && (
        <div className="banner error" role="alert">
          {snapshot ? "采集失败，正在显示上次成功快照。" : "无法读取系统拓扑。"}{" "}
          {error}
          <button onClick={() => void refresh()}>重试</button>
        </div>
      )}
      {!snapshot ? (
        <div className="system-loading">
          <Network size={32} />
          <h2>{busy ? "正在读取基础设施状态" : "系统拓扑暂不可用"}</h2>
          <p>分别探测各个服务，单个节点失败不会隐藏其它结果。</p>
        </div>
      ) : (
        <>
          <div className={`system-verdict ${snapshot.status}`} role="status">
            <span className="verdict-icon">
              {issues.length ? (
                <AlertTriangle size={19} />
              ) : (
                <ShieldCheck size={19} />
              )}
            </span>
            <div>
              <strong>
                {snapshot.status === "unavailable"
                  ? "部分关键路径不可用"
                  : issues.length
                    ? `系统需要关注 · ${issues.length} 个组件异常`
                    : "已探测组件运行正常"}
              </strong>
              <p>
                {issues.length
                  ? issues
                      .slice(0, 2)
                      .map((n) => `${n.label}：${n.summary}`)
                      .join("；")
                  : `${snapshot.summary.probed} 个组件已探测，${snapshot.summary.unverified} 项依赖未独立验证`}
              </p>
            </div>
            {issues.length > 0 && (
              <button
                className="verdict-action"
                onClick={() => choose(issues[0].id)}
              >
                查看异常
                <ArrowUpRight size={14} />
              </button>
            )}
            <span className="system-updated">
              <Clock size={13} />
              {time(snapshot.observedAt)}
              <small>{paused ? "自动刷新已暂停" : "每 15 秒更新"}</small>
            </span>
          </div>
          <div className="system-summary" aria-label="系统摘要">
            <div>
              <span>探测覆盖</span>
              <strong>
                {snapshot.summary.probed}
                <small>/ {snapshot.summary.total}</small>
              </strong>
              <p>{snapshot.summary.unverified} 项逻辑 / 外部依赖未探测</p>
            </div>
            <div>
              <span>组件健康</span>
              <strong>
                {snapshot.summary.healthy}
                <small>
                  正常 ·{" "}
                  {snapshot.summary.degraded + snapshot.summary.unavailable}{" "}
                  异常
                </small>
              </strong>
              <p>就绪检查与功能信号分别判断</p>
            </div>
            {snapshot.summary.fleetTotal > 0 && (
              <div>
                <span>Fleet 实例</span>
                <strong>
                  {snapshot.summary.fleetReady}
                  <small>/ {snapshot.summary.fleetTotal} 就绪</small>
                </strong>
                <p>共同承载持久状态对象</p>
              </div>
            )}
            <div>
              <span>可准入执行槽位</span>
              <strong>
                {snapshot.summary.availableSlots ?? "—"}
                <small>/ {snapshot.summary.slots ?? "—"}</small>
              </strong>
              <p>
                {snapshot.summary.capacityReporting}/
                {snapshot.summary.executors} 台执行器已上报容量
              </p>
            </div>
          </div>
          <div className="system-workspace">
            <section className="system-map">
              <header className="system-panel-heading">
                <div>
                  <h2>
                    <Network size={17} />
                    架构与依赖
                  </h2>
                  <p>选择组件查看证据 · 连线代表配置依赖</p>
                </div>
                <div className="system-segment" aria-label="拓扑依赖范围">
                  <button
                    aria-pressed={edgeMode === "request"}
                    onClick={() => {
                      setEdgeMode("request");
                      if (
                        selected &&
                        !(
                          [
                            "ingress",
                            "gateway",
                            "control",
                            "execution",
                            "storage",
                          ].includes(selected.plane) &&
                          selected.id !== "body-proxy"
                        )
                      )
                        setSelectedId(
                          snapshot.nodes.find((n) => n.kind === "gateway")
                            ?.id ?? "",
                        );
                    }}
                  >
                    执行主链
                  </button>
                  <button
                    aria-pressed={edgeMode === "all"}
                    onClick={() => setEdgeMode("all")}
                  >
                    全景拓扑
                  </button>
                </div>
              </header>
              <div className="system-canvas" ref={canvasRef}>
                <DependencyLines
                  canvas={canvasRef}
                  edges={snapshot.edges}
                  selectedId={selectedId}
                  mode={edgeMode}
                />
                <div className="system-flow-caption">
                  <span>请求入口</span>
                  <ArrowRight size={13} />
                  <span>鉴权 / 调度</span>
                  <ArrowRight size={13} />
                  <span>执行与结果回传</span>
                </div>
                <div className="system-main-lane">
                  {["ingress", "gateway", "control", "execution"].map(
                    (plane, index) => (
                      <div key={plane} className={`system-plane ${plane}`}>
                        <div className="plane-heading">
                          <span>0{index + 1}</span>
                          <h3>{planeLabel[plane]}</h3>
                        </div>
                        <div className="plane-nodes">
                          {snapshot.nodes
                            .filter(
                              (n) =>
                                n.plane === plane &&
                                n.kind !== "logical" &&
                                n.id !== "body-proxy",
                            )
                            .map((node) => (
                              <Node
                                key={node.id}
                                node={node}
                                selected={selectedId === node.id}
                                connected={connected.has(node.id)}
                                choose={choose}
                              />
                            ))}
                          {plane === "control" && (
                            <div className="system-cell-tags">
                              <span>Scheduler</span>
                              <span>Gateway</span>
                              <span>Body</span>
                              <span>Harness</span>
                              <span>Tunnel</span>
                              <small>持久状态对象 · 未逐对象探测</small>
                            </div>
                          )}
                          {plane === "execution" &&
                            snapshot.nodes
                              .filter((n) => n.kind === "logical")
                              .map((node) => (
                                <Node
                                  key={node.id}
                                  node={node}
                                  selected={selectedId === node.id}
                                  connected={connected.has(node.id)}
                                  choose={choose}
                                />
                              ))}
                        </div>
                        {index < 3 && (
                          <div className="system-connector" aria-hidden="true">
                            <span />
                            <ArrowRight
                              size={14}
                              style={
                                index === 2
                                  ? { transform: "rotate(180deg)" }
                                  : undefined
                              }
                            />
                          </div>
                        )}
                      </div>
                    ),
                  )}
                </div>
                <div className="system-support">
                  <div className="support-heading">
                    <span>支撑依赖</span>
                    <i />
                    <small>独立于请求主路径</small>
                  </div>
                  <div
                    className={`system-support-grid ${edgeMode === "request" ? "main-only" : ""}`}
                  >
                    {(edgeMode === "all"
                      ? ["storage", "observability"]
                      : ["storage"]
                    ).map((plane) => (
                      <div key={plane}>
                        <h3>{planeLabel[plane]}</h3>
                        {snapshot.nodes
                          .filter((n) => n.plane === plane)
                          .map((node) => (
                            <Node
                              key={node.id}
                              node={node}
                              selected={selectedId === node.id}
                              connected={connected.has(node.id)}
                              choose={choose}
                            />
                          ))}
                      </div>
                    ))}
                    {edgeMode === "all" && (
                      <div>
                        <h3>
                          {edgeMode === "all"
                            ? "外部与兼容依赖"
                            : "配置与验证边界"}
                        </h3>
                        {snapshot.nodes
                          .filter((n) =>
                            edgeMode === "all"
                              ? n.plane === "external" || n.id === "body-proxy"
                              : n.plane === "external",
                          )
                          .map((node) => (
                            <Node
                              key={node.id}
                              node={node}
                              selected={selectedId === node.id}
                              connected={connected.has(node.id)}
                              choose={choose}
                            />
                          ))}
                      </div>
                    )}
                  </div>
                </div>
                <div className="system-map-legend">
                  <span>
                    <i className="healthy" />
                    探测正常
                  </span>
                  <span>
                    <i className="degraded" />
                    需要关注
                  </span>
                  <span>
                    <i className="unverified" />
                    未主动验证
                  </span>
                  <span className="connection-key">
                    <b />
                    配置依赖 · 箭头为调用方向
                  </span>
                </div>
              </div>
            </section>
            <aside
              ref={inspectorRef}
              className="system-inspector"
              aria-label="节点详情"
            >
              {inspector}
            </aside>
          </div>
          <section className="system-issues">
            <div className="system-section-heading">
              <h2>
                <AlertTriangle size={16} />
                需要关注的组件<span>{issues.length}</span>
              </h2>
              <p>来源于当前快照，未对服务执行任何恢复或重启操作</p>
            </div>
            {issues.length ? (
              issues.map((node) => (
                <button
                  key={node.id}
                  onClick={() => choose(node.id)}
                  className="system-issue"
                >
                  <span className={`issue-marker ${node.status}`} />
                  <b>{node.label}</b>
                  <span>{node.summary}</span>
                  <small>
                    {node.targets
                      ?.filter((t) => !t.healthy)
                      .map((t) => t.error)
                      .filter((x, i, a) => a.indexOf(x) === i)
                      .join(" / ") ?? `HTTP ${node.httpStatus ?? "无响应"}`}
                  </small>
                  <ChevronRight size={16} />
                </button>
              ))
            ) : (
              <p className="system-no-issues">
                <Check size={16} />
                当前快照未发现已探测组件异常，未验证项请单独核对。
              </p>
            )}
          </section>
          <section className="system-inventory">
            <div className="system-section-heading">
              <h2>
                <Server size={16} />
                实例与容量清单
              </h2>
              <span>{snapshot.nodes.length} 个组件</span>
            </div>
            <div className="system-inventory-controls">
              <label className="system-search">
                <Search size={15} />
                <input
                  aria-label="搜索系统组件"
                  placeholder="搜索名称、标识或分层…"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                />
              </label>
              <button
                aria-pressed={onlyIssues}
                className={`secondary ${onlyIssues ? "filter-on" : ""}`}
                onClick={() => setOnlyIssues((v) => !v)}
              >
                仅看异常{onlyIssues && <Check size={13} />}
              </button>
            </div>
            <div className="system-inventory-wrap">
              <table>
                <thead>
                  <tr>
                    <th>组件 / 标识</th>
                    <th>架构分层</th>
                    <th>状态</th>
                    <th>关键指标</th>
                    <th>探测耗时</th>
                    <th>采集时间</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((node) => (
                    <tr
                      key={node.id}
                      className={node.id === selectedId ? "selected" : ""}
                    >
                      <td>
                        <button
                          onClick={() => choose(node.id)}
                          className="inventory-node"
                        >
                          <NodeIcon kind={node.kind} />
                          <span>
                            <b>{node.label}</b>
                            <small>{node.id}</small>
                          </span>
                        </button>
                      </td>
                      <td>{planeLabel[node.plane]}</td>
                      <td>
                        <State node={node} />
                      </td>
                      <td>{compactMetric(node)}</td>
                      <td className="mono">
                        {node.latencyMs === null ? "—" : `${node.latencyMs} ms`}
                      </td>
                      <td className="mono">{time(node.checkedAt)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {!rows.length && (
                <div className="system-empty">
                  <Search size={24} />
                  <p>没有匹配的组件</p>
                  <button
                    className="secondary"
                    onClick={() => {
                      setQuery("");
                      setOnlyIssues(false);
                    }}
                  >
                    清除筛选
                  </button>
                </div>
              )}
            </div>
          </section>
          <div className="system-explanations">
            <section>
              <h2>
                <Layers size={16} />
                持久状态对象
              </h2>
              <p>以下是逻辑对象类型，并非额外的独立服务器。</p>
              {snapshot.logicalServices.map((service) => (
                <div className="system-logical-service" key={service.name}>
                  <b>{service.name}</b>
                  <span>{service.responsibility}</span>
                  <code>{service.authority}</code>
                </div>
              ))}
            </section>
            <section>
              <h2>
                <LockKeyhole size={16} />
                采集与验证边界
              </h2>
              {snapshot.boundaries.map((item) => (
                <p className="system-boundary" key={item}>
                  <span /> {item}
                </p>
              ))}
              <p className="system-scope">
                全局基础设施 · 仅 SRE_ADMIN 可见 · 不展示跨租户任务或密钥
              </p>
            </section>
          </div>
          {mobileDetail && selected && (
            <div
              className="system-node-backdrop"
              onClick={() => setMobileDetail(false)}
            >
              <div
                ref={dialog}
                className="system-node-dialog"
                role="dialog"
                aria-modal="true"
                aria-label="节点详情"
                onClick={(e) => e.stopPropagation()}
              >
                <div className="system-dialog-toolbar">
                  <span>{selected.label}</span>
                  <button
                    className="icon system-close-detail"
                    aria-label="关闭节点详情"
                    onClick={() => setMobileDetail(false)}
                  >
                    <X size={21} />
                  </button>
                </div>
                {inspector}
              </div>
            </div>
          )}
        </>
      )}
    </section>
  );
}
function DependencyLines({
  canvas,
  edges,
  selectedId,
  mode,
}: {
  canvas: RefObject<HTMLDivElement | null>;
  edges: SystemEdge[];
  selectedId: string;
  mode: string;
}) {
  const [paths, setPaths] = useState<
    { id: string; path: string; label: string }[]
  >([]);
  useEffect(() => {
    const element = canvas.current;
    if (!element) return;
    const draw = () => {
      const rect = element.getBoundingClientRect();
      const next = edges
        .filter((e) => e.source === selectedId || e.target === selectedId)
        .flatMap((edge, index) => {
          const source = element.querySelector<HTMLElement>(
              `[data-system-node="${edge.source}"]`,
            ),
            target = element.querySelector<HTMLElement>(
              `[data-system-node="${edge.target}"]`,
            );
          if (!source || !target) return [];
          const a = source.getBoundingClientRect(),
            b = target.getBoundingClientRect();
          let sx = a.x + a.width / 2 - rect.x,
            sy = a.y + a.height / 2 - rect.y,
            tx = b.x + b.width / 2 - rect.x,
            ty = b.y + b.height / 2 - rect.y;
          let path: string;
          if (Math.abs(sy - ty) > Math.abs(sx - tx) * 0.6) {
            const siblings = edges.filter(
              (e) =>
                e.source === edge.source &&
                (e.source === selectedId || e.target === selectedId),
            );
            sx =
              a.x +
              (a.width * (siblings.indexOf(edge) + 1)) / (siblings.length + 1) -
              rect.x;
            sy += ((ty > sy ? 1 : -1) * a.height) / 2;
            ty += ((ty > sy ? -1 : 1) * b.height) / 2;
            const mid = (sy + ty) / 2 + (index - 2) * 5;
            path = `M ${sx} ${sy} C ${sx} ${mid}, ${tx} ${mid}, ${tx} ${ty}`;
          } else {
            sx += ((tx > sx ? 1 : -1) * a.width) / 2;
            tx += ((tx > sx ? -1 : 1) * b.width) / 2;
            const mid = (sx + tx) / 2;
            path = `M ${sx} ${sy} C ${mid} ${sy}, ${mid} ${ty}, ${tx} ${ty}`;
          }
          return [
            {
              id: `${edge.source}-${edge.target}-${index}`,
              path,
              label: edge.label,
            },
          ];
        });
      setPaths(next);
    };
    draw();
    const observer = new ResizeObserver(draw);
    observer.observe(element);
    window.addEventListener("resize", draw);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", draw);
    };
  }, [canvas, edges, selectedId, mode]);
  return (
    <svg className="system-dependency-lines" aria-hidden="true">
      <defs>
        <marker
          id="system-dependency-arrow"
          markerWidth="6"
          markerHeight="6"
          refX="5"
          refY="3"
          orient="auto"
        >
          <path d="M 0 0 L 6 3 L 0 6" />
        </marker>
      </defs>
      {paths.map((p) => (
        <path key={p.id} d={p.path} markerEnd="url(#system-dependency-arrow)">
          <title>{p.label}</title>
        </path>
      ))}
    </svg>
  );
}
function compactMetric(node: SystemNode) {
  if (node.kind === "daemon")
    return `${numeric(node, "availableSlots") ?? "—"}/${numeric(node, "slots") ?? "—"} 可用槽`;
  if (node.kind === "fleet")
    return `${numeric(node, "ownedCells") ?? "—"} 拥有对象`;
  if (node.kind === "prometheus")
    return `${numeric(node, "targetsUp") ?? "—"}/${numeric(node, "targetsTotal") ?? "—"} 采集正常`;
  return node.summary;
}
function Node({
  node,
  selected,
  connected,
  choose,
}: {
  node: SystemNode;
  selected: boolean;
  connected: boolean;
  choose: (id: string) => void;
}) {
  return (
    <button
      className={`system-node ${selected ? "is-selected" : connected ? "is-connected" : ""} ${node.status}`}
      aria-pressed={selected}
      data-system-node={node.id}
      aria-label={`查看 ${node.label}`}
      onClick={() => choose(node.id)}
    >
      <span className="node-title">
        <NodeIcon kind={node.kind} />
        <b>{node.label}</b>
        <i className={`node-dot ${node.status}`} />
      </span>
      <span className="node-summary">
        {node.kind === "fleet" || node.kind === "daemon"
          ? compactMetric(node)
          : node.summary}
      </span>
      {(node.kind === "fleet" || node.kind === "daemon") && (
        <small>{node.summary}</small>
      )}
    </button>
  );
}
function Inspector({
  node,
  nodes,
  edges,
  choose,
}: {
  node: SystemNode;
  nodes: SystemNode[];
  edges: SystemEdge[];
  choose: (id: string) => void;
}) {
  const relations = edges.filter(
    (e) => e.source === node.id || e.target === node.id,
  );
  return (
    <div className="node-inspector-content" key={node.id}>
      <span className="eyebrow">COMPONENT INSPECTOR</span>
      <div className="inspector-title">
        <span>
          <NodeIcon kind={node.kind} size={21} />
        </span>
        <div>
          <h2>{node.label}</h2>
          <p>
            {kindName[node.kind]} · {node.id}
          </p>
        </div>
      </div>
      <State node={node} />
      <h3 className="inspector-summary">{node.summary}</h3>
      <p className="inspector-description">{node.description}</p>
      {node.endpoint && (
        <div className="inspector-endpoint">
          <span>探测地址</span>
          <code>{node.endpoint}</code>
          {node.telemetryUrl && (
            <>
              <span>内部遥测</span>
              <code>
                {node.telemetryUrl}
                {node.telemetryPath}
              </code>
            </>
          )}
        </div>
      )}
      <div className="inspector-timestamps">
        <span>
          采集于 <b>{time(node.checkedAt)}</b>
        </span>
        <span>
          耗时 <b>{node.latencyMs === null ? "—" : `${node.latencyMs} ms`}</b>
        </span>
      </div>
      {node.targets && (
        <section className="inspector-targets">
          <h3>
            指标抓取目标{" "}
            <small>
              {node.targets.filter((t) => t.healthy).length}/
              {node.targets.length} 正常
            </small>
          </h3>
          {node.targets.map((target) => (
            <div key={target.endpoint}>
              <span
                className={`node-dot ${target.healthy ? "healthy" : "degraded"}`}
              />
              <div>
                <b>{target.endpoint}</b>
                <small>
                  {target.job} ·{" "}
                  {target.lastScrape
                    ? new Date(target.lastScrape).toLocaleTimeString("zh-CN")
                    : "未采集"}
                </small>
              </div>
              <strong>{target.error ?? "正常"}</strong>
            </div>
          ))}
        </section>
      )}
      {node.metrics.length > 0 && (
        <section className="inspector-metrics">
          <h3>采集指标</h3>
          <dl>
            {node.metrics.map((m) => (
              <div
                key={m.key}
                className={
                  typeof m.value === "string" && m.value.length > 35
                    ? "wide"
                    : ""
                }
              >
                <dt>{m.label}</dt>
                <dd>{metricValue(m)}</dd>
              </div>
            ))}
          </dl>
        </section>
      )}
      {node.checks.length > 0 && (
        <section className="inspector-checks">
          <h3>探测证据</h3>
          {node.checks.map((check) => (
            <div key={check.label}>
              <span className={`node-dot ${check.status}`} />
              <span>{check.label}</span>
              <b>
                {check.httpStatus ? `HTTP ${check.httpStatus}` : "无有效响应"}
              </b>
              <small>{check.latencyMs} ms</small>
            </div>
          ))}
        </section>
      )}
      <section className="inspector-relations">
        <h3>
          直接依赖 <small>{relations.length}</small>
        </h3>
        {relations.map((edge, i) => {
          const outgoing = edge.source === node.id;
          const other = nodes.find(
            (n) => n.id === (outgoing ? edge.target : edge.source),
          );
          return (
            other && (
              <button key={i} onClick={() => choose(other.id)}>
                <span className={`node-dot ${other.status}`} />
                <div>
                  <b>{other.label}</b>
                  <small>
                    {outgoing ? "依赖于" : "被依赖于"} · {edge.label}
                  </small>
                </div>
                <ChevronRight size={14} />
              </button>
            )
          );
        })}
        {!relations.length && <p>配置中没有直接依赖</p>}
      </section>
    </div>
  );
}
function BasicStatus({ api }: { api: Api }) {
  const [health, setHealth] = useState<any>(null),
    [error, setError] = useState("");
  const load = useCallback(async () => {
    try {
      setHealth(await api("/healthz"));
      setError("");
    } catch (e) {
      setError((e as Error).message);
    }
  }, [api]);
  useEffect(() => {
    void load();
    const t = setInterval(() => void load(), 10000);
    return () => clearInterval(t);
  }, [load]);
  return (
    <div className="system-page">
      <div className="system-heading">
        <div>
          <span className="eyebrow">SYSTEM / CONNECTIVITY</span>
          <h1>系统状态</h1>
          <p>当前身份可查看基础服务连通性。</p>
        </div>
        <button className="secondary" onClick={() => void load()}>
          <RefreshCw size={14} />
          刷新状态
        </button>
      </div>
      <section className="health-overview">
        <h2>控制平面连接</h2>
        {error && (
          <p className="error" role="alert">
            {error}
          </p>
        )}
        {["gateway", "scheduler"].map((k) => (
          <div className="health-row" key={k}>
            <Activity size={20} />
            <div>
              <b>{k === "gateway" ? "Gateway 网关" : "Scheduler 调度器"}</b>
              <p>{k === "gateway" ? "用户 API 入口" : "远程调度服务"}</p>
            </div>
            <span
              className={`badge ${health && (k === "gateway" ? health.gateway : health.scheduler?.reachable) ? "completed" : "queued"}`}
            >
              {health
                ? (
                    k === "gateway"
                      ? health.gateway
                      : health.scheduler?.reachable
                  )
                  ? "已连接"
                  : "不可用"
                : "检查中"}
            </span>
          </div>
        ))}
      </section>
      <div className="system-access-note">
        <LockKeyhole size={19} />
        <div>
          <h2>全局拓扑需要 SRE 管理权限</h2>
          <p>
            内部地址、节点容量与全局基础设施状态仅对 SRE_ADMIN
            开放，当前工作区的任务访问不受影响。
          </p>
        </div>
      </div>
    </div>
  );
}
