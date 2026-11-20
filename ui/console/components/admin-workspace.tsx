"use client";
import { useCallback, useEffect, useRef, useState } from "react";
import {
  AlertTriangle,
  ArrowUpRight,
  Check,
  ChevronRight,
  ClipboardList,
  Clock,
  FileCheck,
  Layers,
  LockKeyhole,
  RefreshCw,
  Search,
  ShieldCheck,
  Terminal,
  X,
} from "lucide-react";
import { ADMIN_MODULES } from "../lib/admin";
import { labels, date, type Task, type User } from "../lib/console";
import ManagementAuditPanel from "./management-audit";
import TenantDirectory from "./tenant-directory";
import CapacityAllocation from "./capacity-allocation";
import ConfigReleases from "./config-releases";
import RegionGovernance from "./region-governance";
import SystemStatus from "./system-status";
import { roleName } from "./profile";
type Api = (path: string, body?: unknown) => Promise<any>;
export default function AdminWorkspace(props: {
  module: string;
  controlFetch?: typeof fetch;
  user: User;
  api: Api;
  select: (task: Task) => void;
  navigate: (view: string) => void;
}) {
  if (props.module === "capacity") return <CapacityAllocation user={props.user} fetcher={props.controlFetch} />;
  if (props.module === "federation") return <RegionGovernance fetcher={props.controlFetch} />;
  if (props.module === "configs") return <ConfigReleases user={props.user} fetcher={props.controlFetch} />;
  return props.module === "nodes" ? (
    <SystemStatus api={props.api} user={props.user} />
  ) : (
    <Workspace {...props} />
  );
}
function Workspace({
  controlFetch,
  module,
  user,
  api,
  select,
  navigate,
}: {
  module: string;
  controlFetch?: typeof fetch;
  user: User;
  api: Api;
  select: (task: Task) => void;
  navigate: (view: string) => void;
}) {
  const [message, setMessage] = useState("");
  const [data, setData] = useState<any>(null),
    [system, setSystem] = useState<any>(null),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [query, setQuery] = useState(""),
    [filter, setFilter] = useState("all"),
    [review, setReview] = useState<Task | null>(null);
  const active = useRef(true),
    loading = useRef(false);
  const info = ADMIN_MODULES.find((m) => m.id === module)!;
  const load = useCallback(async () => {
    if (loading.current) return;
    loading.current = true;
    setBusy(true);
    try {
      const result = await api("/v1/console/management");
      if (active.current) {
        setData(result);
        setError("");
      }
      if (
        user.role === "SRE_ADMIN" &&
        ["observability", "federation"].includes(module)
      ) {
        const s = await api("/v1/console/system");
        if (active.current) setSystem(s);
      }
    } catch (e) {
      if (active.current) setError((e as Error).message);
    } finally {
      loading.current = false;
      if (active.current) setBusy(false);
    }
  }, [api, module, user.role]);
  useEffect(() => {
    active.current = true;
    setQuery("");
    setFilter("all");
    setSystem(null);
    void load();
    const poll = setInterval(() => void load(), 15000);
    return () => {
      active.current = false;
      clearInterval(poll);
    };
  }, [load]);
  const tasks: any[] = data?.tasks ?? [];
  const target = tasks.filter(
    (t) =>
      (module !== "approvals" || t.category === "uncertain") &&
      (module !== "runtime" || t.execution_kind !== "trace") &&
      (module !== "swarm" || t.execution_kind === "agent") &&
      (filter === "all" || t.category === filter) &&
      `${t.task_id} ${t.tenant_id} ${t.assigned_node_id ?? ""}`
        .toLowerCase()
        .includes(query.toLowerCase()),
  );
  const isTasks = ["queues", "approvals", "runtime", "swarm"].includes(module);
  const unsupported: Record<string, string[]> = {
    tenants: ["普通租户强制准入", "历史月份关账"],
    configs: ["配置发布与版本回滚", "配置 Dry Run"],
    runtime: ["LayerFS 分支写入/合并", "在线调试注入"],
    observability: ["SLO 历史评估", "自动自愈与故障注入"],
    swarm: ["算力市场与竞价", "A2A / MCP 协作编排"],
    federation: ["跨地域迁移", "灾备恢复与纠删码验证"],
    security: ["编辑账号/租户授权", "TEE / SPIFFE / eBPF 与生产发布门控"],
  };
  return (
    <section className="admin-workspace" key={module}>
      <header className="admin-heading">
        <div>
          <span className="eyebrow">
            ADMINISTRATION / {module.toUpperCase()}
          </span>
          <h1>{info.title}</h1>
          <p>{info.description}</p>
        </div>
        <button
          className="secondary"
          disabled={busy}
          onClick={() => void load()}
        >
          <RefreshCw size={14} className={busy ? "spin" : ""} />
          {busy ? "更新中" : "刷新数据"}
        </button>
      </header>
      <div className="admin-context">
        <span>
          <ShieldCheck size={13} />
          {user.scopeTenant === "all"
            ? "全部授权租户"
            : `租户 ${user.scopeTenant}`}
        </span>
        <span>
          <Clock size={13} />
          {data
            ? `最近成功更新 ${new Date(data.refreshedAt).toLocaleTimeString("zh-CN")}`
            : "等待数据"}{" "}
          · 每15秒更新
        </span>
      </div>
      {message && (
        <div className="banner" role="status">
          {message}
          <button onClick={() => setMessage("")} aria-label="关闭管理提示">
            <X size={14} />
          </button>
        </div>
      )}
      {error && (
        <div className="banner error" role="alert">
          读取失败，保留上次成功数据。{error}
          <button onClick={() => void load()}>重试</button>
        </div>
      )}
      {!data ? (
        <div className="admin-empty">
          <Layers size={28} />
          <h2>{error ? "数据暂不可用" : "正在连接管理数据"}</h2>
        </div>
      ) : (
        <>
          <div
            className={`admin-summary ${["configs", "tenants", "security", "federation", "observability"].includes(module) ? "domain-secondary" : ""} ${module === "queues" ? "queue-secondary" : ""}`}
          >
            {[
              ["可访问任务", data.counts.total],
              ["执行中", data.counts.active],
              ["排队中", data.counts.queued],
              ["待核对", data.counts.uncertain],
            ].map(([label, value]) => (
              <div key={label}>
                <span>{label}</span>
                <strong>{value}</strong>
              </div>
            ))}
          </div>
          {module === "queues" && (
            <div className="admin-state-strip">
              {Object.entries(labels)
                .filter(([k]) => k !== "all")
                .map(([key, label]) => (
                  <button
                    key={key}
                    aria-pressed={filter === key}
                    onClick={() => setFilter(filter === key ? "all" : key)}
                  >
                    <span>{label}</span>
                    <strong>{data.counts[key]}</strong>
                  </button>
                ))}
            </div>
          )}
          {module === "approvals" && <ManagementAuditPanel user={user} fetcher={controlFetch} />}
          {module === "approvals" && (
            <div className="admin-notice">
              <FileCheck size={18} />
              <div>
                <b>执行核对，不是通用双人审批流</b>
                <p>
                  只处理状态不确定的真实任务。决定使用服务端版本与具名身份记录，证据与原因必填；不自动重跑未确认的执行。
                </p>
              </div>
            </div>
          )}
          {module === "tenants" && <TenantDirectory user={user} fetcher={controlFetch} />}
          {module === "tenants" && (
            <section className="admin-panel">
              <div className="admin-panel-heading">
                <h2>租户运行分布</h2>
                <span>{data.tenants.length} 个可见租户</span>
              </div>
              <div className="admin-table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>租户</th>
                      <th>任务总数</th>
                      <th>执行中</th>
                      <th>失败</th>
                      <th>待核对</th>
                      <th>配额 / 费用</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.tenants.map((t: any) => (
                      <tr key={t.tenant}>
                        <td className="mono">{t.tenant}</td>
                        <td>{t.total}</td>
                        <td>{t.active}</td>
                        <td>{t.failed}</td>
                        <td>{t.uncertain}</td>
                        <td>未接入</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {!data.tenants.length && (
                  <div className="admin-empty">
                    当前授权范围暂无任务租户；此列表不代表完整租户注册表。
                  </div>
                )}
              </div>
            </section>
          )}
          {isTasks && (
            <section className="admin-panel">
              <div className="admin-panel-heading">
                <h2>
                  {module === "approvals"
                    ? "待核对对象"
                    : module === "runtime"
                      ? "执行现场"
                      : module === "swarm"
                        ? "Agent 执行"
                        : "队列任务"}
                </h2>
                <span>
                  {target.length} 条{data.truncated ? " · 最近1000条样本" : ""}
                </span>
              </div>
              <div className="admin-filters">
                <label>
                  <Search size={15} />
                  <input
                    aria-label="搜索管理任务"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="搜索任务、租户、节点…"
                  />
                </label>
                {module !== "approvals" && (
                  <select
                    aria-label="筛选管理任务状态"
                    value={filter}
                    onChange={(e) => setFilter(e.target.value)}
                  >
                    {Object.entries(labels).map(([k, v]) => (
                      <option key={k} value={k}>
                        {v}
                      </option>
                    ))}
                  </select>
                )}
              </div>
              <div className="admin-table-scroll">
                <table>
                  <thead>
                    <tr>
                      <th>任务 / 说明</th>
                      <th>租户</th>
                      <th>状态</th>
                      <th>节点 / 执行方式</th>
                      <th>创建时间</th>
                      <th>操作</th>
                    </tr>
                  </thead>
                  <tbody>
                    {target.map((t) => (
                      <tr key={t.tenant_id + "/" + t.task_id}>
                        <td>
                          <button
                            className="admin-task"
                            onClick={() => select(t)}
                          >
                            <b>{t.task_id}</b>
                            <small>{t.prompt_preview || "未提供说明"}</small>
                          </button>
                        </td>
                        <td>{t.tenant_id}</td>
                        <td>
                          <span className={`badge ${t.category}`}>
                            {labels[t.category]}
                          </span>
                        </td>
                        <td>
                          <b>{t.assigned_node_id ?? "尚未分配"}</b>
                          <small>
                            {t.execution_kind}
                            {module === "swarm"
                              ? ` · ${t.parent_task_id ? "父任务 " + t.parent_task_id : "未记录可见父任务"} · 依赖 ${t.dependency_count ?? "未提供"}`
                              : ""}
                          </small>
                        </td>
                        <td className="mono">{date(t.created_at)}</td>
                        <td>
                          {module === "approvals" &&
                          user.role === "SRE_ADMIN" &&
                          user.canWrite ? (
                            <button
                              className="secondary"
                              onClick={() => setReview(t)}
                            >
                              证据核对
                            </button>
                          ) : (
                            <button
                              className="admin-link"
                              onClick={() => select(t)}
                            >
                              查看详情
                              <ChevronRight size={14} />
                            </button>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {!target.length && (
                  <div className="admin-empty">
                    <ClipboardList size={26} />
                    <h3>
                      {query || filter !== "all"
                        ? "没有匹配的任务"
                        : "当前没有相关任务"}
                    </h3>
                    <p>这里仅显示当前身份可访问的真实记录。</p>
                    {(query || filter !== "all") && (
                      <button
                        className="secondary"
                        onClick={() => {
                          setQuery("");
                          setFilter("all");
                        }}
                      >
                        清除筛选
                      </button>
                    )}
                  </div>
                )}
              </div>
              {module === "queues" && (
                <p className="admin-footnote">
                  排队原因与精确队列位置尚未由 Scheduler
                  输出，不将任务创建顺序当作调度顺序。
                </p>
              )}
              {module === "swarm" && (
                <p className="admin-footnote">
                  当前为独立 Agent 任务视图；未获得
                  DAG、父子关系或协作事件时，不推断协同链路。打开任务可查看实际执行证据。
                </p>
              )}
            </section>
          )}
          {module === "configs" && (
            <section className="admin-panel">
              <div className="admin-panel-heading">
                <h2>当前有效配置</h2>
                <span>Gateway 运行时 · 只读</span>
              </div>
              <dl className="admin-settings">
                {Object.entries(data.configuration).map(([k, v]) => (
                  <div key={k}>
                    <dt>
                      {{
                        sandboxImage: "允许的沙箱镜像",
                        agentEnabled: "Agent 执行",
                        githubEnabled: "GitHub 登录",
                        identityProvider: "身份服务",
                        gatewayState: "Gateway 状态模式",
                        runtimeProfile: "运行环境",
                      }[k] ?? k}
                    </dt>
                    <dd>
                      {typeof v === "boolean"
                        ? v
                          ? "已启用"
                          : "未启用"
                        : String(v ?? "未配置")}
                    </dd>
                  </div>
                ))}
              </dl>
              <p className="admin-footnote">
                来源为当前 Gateway 配置快照，未提供版本号或配置发布
                API，不显示虚构 Revision 与 CAS 同步成功。
              </p>
            </section>
          )}
          {["observability", "federation"].includes(module) && (
            <section className="admin-panel">
              <div className="admin-panel-heading">
                <h2>
                  {module === "observability"
                    ? "服务与采集证据"
                    : "已部署的 Fleet"}
                </h2>
                <button
                  className="admin-link"
                  onClick={() => navigate("health")}
                >
                  打开完整拓扑
                  <ArrowUpRight size={15} />
                </button>
              </div>
              {user.role !== "SRE_ADMIN" ? (
                <div className="admin-empty">
                  <LockKeyhole size={25} />
                  全局基础设施仅 SRE 管理员可查看。
                </div>
              ) : !system ? (
                <div className="admin-empty">
                  {error ? "探测数据暂不可用，可重试" : "正在读取探测…"}
                </div>
              ) : (
                <>
                  <div className="admin-service-list">
                    {system.nodes
                      .filter((n: any) =>
                        module === "observability"
                          ? ["prometheus", "grafana"].includes(n.kind)
                          : n.kind === "fleet",
                      )
                      .map((n: any) => (
                        <div key={n.id}>
                          <header>
                            <b>{n.label}</b>
                            <span
                              className={`badge ${n.status === "healthy" ? "completed" : "uncertain"}`}
                            >
                              {n.summary}
                            </span>
                          </header>
                          <p>{n.endpoint}</p>
                          {n.targets?.map((t: any) => (
                            <div className="admin-target" key={t.endpoint}>
                              <span>{t.endpoint}</span>
                              <strong>{t.error ?? "正常"}</strong>
                            </div>
                          ))}
                        </div>
                      ))}
                  </div>
                  <p className="admin-footnote">
                    探测时间{" "}
                    {new Date(system.observedAt).toLocaleString("zh-CN")}。
                    {module === "federation"
                      ? "这是同一开发 Fleet 的多实例，不是多地域部署或纠删码副本证明。"
                      : "健康端点与采集目标分别判断；不从瞬时探测推断长期 SLO。"}
                  </p>
                </>
              )}
            </section>
          )}
          {module === "security" && (
            <section className="admin-panel">
              <div className="admin-panel-heading">
                <h2>当前身份与访问策略</h2>
                <span>已验证会话</span>
              </div>
              <dl className="admin-settings">
                {[
                  ["身份", user.sub],
                  ["角色", roleName(user.role)],
                  ["租户范围", user.scopeTenant],
                  ["任务写入", user.canWrite ? "允许，受租户授权约束" : "拒绝"],
                  [
                    "基础设施",
                    user.role === "SRE_ADMIN"
                      ? "允许读取全局拓扑"
                      : "仅基础连通性",
                  ],
                  [
                    "核对决定",
                    user.role === "SRE_ADMIN" && user.canWrite
                      ? "允许，要求版本、原因和证据"
                      : "拒绝",
                  ],
                ].map(([k, v]) => (
                  <div key={k}>
                    <dt>{k}</dt>
                    <dd>{v}</dd>
                  </div>
                ))}
              </dl>
              <p className="admin-footnote">
                GitHub
                资料仅用于展示，角色与租户来自服务端授权。已有任务核对审计可通过任务证据查看，不宣称已接入全平台合规审计链。
              </p>
            </section>
          )}
          {unsupported[module] && (
            <section className="admin-capabilities">
              <h2>
                <LockKeyhole size={15} />
                尚未接入运行后端的能力
              </h2>
              <p>参考设计中的这些能力保留边界说明，当前不会执行模拟操作。</p>
              {unsupported[module].map((label) => (
                <div key={label}>
                  <span>{label}</span>
                  <small>当前 Gateway 未提供对应的持久化管理接口</small>
                </div>
              ))}
            </section>
          )}
        </>
      )}
      {review && (
        <Reconcile
          task={review}
          api={api}
          close={() => setReview(null)}
          done={() => {
            setReview(null);
            setMessage(
              "核对决定已记录，正在读取最新状态；保留阻塞不会释放资源。",
            );
            void load();
          }}
        />
      )}
    </section>
  );
}
function Reconcile({
  task,
  api,
  close,
  done,
}: {
  task: Task;
  api: Api;
  close: () => void;
  done: () => void;
}) {
  const [base, setBase] = useState<any>(null),
    [reason, setReason] = useState(""),
    [evidence, setEvidence] = useState(""),
    [action, setAction] = useState("retain_block"),
    [result, setResult] = useState(""),
    [confirmed, setConfirmed] = useState(false),
    [busy, setBusy] = useState(false),
    [error, setError] = useState("");
  const panel = useRef<HTMLDivElement>(null);
  const load = async () => {
    try {
      setBase(
        await api(
          `/v1/tasks/${encodeURIComponent(task.task_id)}?tenantId=${encodeURIComponent(task.tenant_id)}&includeResult=1`,
        ),
      );
      setError("");
      setConfirmed(false);
    } catch (e) {
      setError((e as Error).message);
    }
  };
  useEffect(() => {
    let live = true;
    api(
      `/v1/tasks/${encodeURIComponent(task.task_id)}?tenantId=${encodeURIComponent(task.tenant_id)}&includeResult=1`,
    )
      .then((r) => {
        if (live) setBase(r);
      })
      .catch((e) => {
        if (live) setError(e.message);
      });
    const prev = document.activeElement as HTMLElement;
    const old = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    panel.current?.querySelector<HTMLButtonElement>("button")?.focus();
    const key = (e: KeyboardEvent) => {
      if (
        e.key === "Escape" &&
        !panel.current?.querySelector<HTMLButtonElement>(
          '[aria-label="关闭核对"]',
        )?.disabled
      )
        close();
      if (e.key === "Tab") {
        const items = Array.from(
            panel.current?.querySelectorAll<HTMLElement>(
              "button:not(:disabled),textarea,select,input",
            ) ?? [],
          ),
          first = items[0],
          last = items[items.length - 1];
        if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last?.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first?.focus();
        }
      }
    };
    document.addEventListener("keydown", key);
    return () => {
      live = false;
      document.body.style.overflow = old;
      document.removeEventListener("keydown", key);
      prev?.focus();
    };
  }, [api, task]);
  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      const body: any = {
        tenant_id: task.tenant_id,
        expected_version: base.version,
        node_id: base.nodeId,
        fencing_token: base.fencingToken,
        action,
        reason,
        evidence,
      };
      if (action === "confirm_completed") {
        body.result = JSON.parse(result);
        if (
          !body.result ||
          Array.isArray(body.result) ||
          !["SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED"].includes(
            body.result.executionOutcome,
          )
        )
          throw new Error("结果必须是包含有效 executionOutcome 的 JSON 对象");
      }
      await api(
        `/v1/tasks/${encodeURIComponent(task.task_id)}/reconcile?tenantId=${encodeURIComponent(task.tenant_id)}`,
        body,
      );
      done();
    } catch (e) {
      setError((e as Error).message + "。输入已保留，请核对最新版本后再提交。");
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="drawer-backdrop">
      <div
        className="drawer admin-review"
        ref={panel}
        role="dialog"
        aria-modal="true"
        aria-label="执行证据核对"
      >
        <header>
          <div>
            <span className="eyebrow">VERSIONED RECONCILIATION</span>
            <h2>执行证据核对</h2>
          </div>
          <button
            className="icon"
            aria-label="关闭核对"
            disabled={busy}
            onClick={close}
          >
            <X size={19} />
          </button>
        </header>
        <div className="drawer-body">
          <h3>{task.task_id}</h3>
          <p className="admin-footnote">
            租户 {task.tenant_id} · 使用当前登录身份提交审计
          </p>
          {error && (
            <div className="banner error" role="alert">
              {error}
            </div>
          )}
          {!base ? (
            <button className="secondary" onClick={() => void load()}>
              重新读取证据
            </button>
          ) : (
            <>
              <dl className="detail-grid">
                <div>
                  <dt>执行阶段</dt>
                  <dd>{base.phase}</dd>
                </div>
                <div>
                  <dt>基线版本</dt>
                  <dd>{base.version}</dd>
                </div>
                <div>
                  <dt>执行节点</dt>
                  <dd>{base.nodeId ?? "—"}</dd>
                </div>
                <div>
                  <dt>Fence</dt>
                  <dd>{base.fencingToken ?? "—"}</dd>
                </div>
              </dl>
              <details className="raw">
                <summary>
                  执行与审计证据（{base.audit?.length ?? 0} 条）
                </summary>
                <pre>{JSON.stringify(base, null, 2)}</pre>
              </details>
              <form className="create-form" onSubmit={submit}>
                <label>
                  核对决定
                  <select
                    value={action}
                    onChange={(e) => setAction(e.target.value)}
                  >
                    <option value="retain_block">保留阻塞，继续收集证据</option>
                    <option value="confirm_not_started">
                      确认未启动，允许重新调度
                    </option>
                    <option value="confirm_completed">
                      确认已完成，提交执行结果
                    </option>
                  </select>
                </label>
                <label>
                  判断原因
                  <textarea
                    required
                    maxLength={512}
                    value={reason}
                    onChange={(e) => setReason(e.target.value)}
                    rows={3}
                  />
                </label>
                <label>
                  证据与依据
                  <textarea
                    required
                    maxLength={4096}
                    value={evidence}
                    onChange={(e) => setEvidence(e.target.value)}
                    rows={5}
                  />
                </label>
                {action === "confirm_completed" && (
                  <label>
                    已确认的执行结果 JSON
                    <textarea
                      required
                      value={result}
                      onChange={(e) => setResult(e.target.value)}
                      rows={5}
                    />
                  </label>
                )}
                <label className="admin-confirm">
                  <input
                    type="checkbox"
                    checked={confirmed}
                    onChange={(e) => setConfirmed(e.target.checked)}
                  />
                  已核对版本与证据，确认按上述决定提交
                </label>
                <div className="admin-review-actions">
                  <button
                    className="secondary"
                    type="button"
                    disabled={busy}
                    onClick={() => void load()}
                  >
                    刷新基线
                  </button>
                  <button
                    className="primary"
                    disabled={busy || !confirmed || base.phase !== "UNCERTAIN"}
                  >
                    {busy ? "提交中…" : "提交核对决定"}
                  </button>
                </div>
                {base.phase !== "UNCERTAIN" && (
                  <p className="error">
                    当前执行不处于 UNCERTAIN，禁止提交核对决定。
                  </p>
                )}
              </form>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

