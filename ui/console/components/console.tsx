"use client";
import SystemStatus from "./system-status";
import { Avatar, Profile, roleName, type GithubProfile } from "./profile";
import { useCallback, useEffect, useRef, useState, FormEvent } from "react";
import {
  Activity,
  ArrowUpRight,
  Box,
  Check,
  ChevronLeft,
  ChevronRight,
  CircleHelp,
  ClipboardList,
  Download,
  Github,
  LayoutDashboard,
  LogOut,
  Menu,
  Plus,
  RefreshCw,
  Search,
  ShieldCheck,
  Terminal,
  X,
  AlertTriangle,
} from "lucide-react";
import {
  labels,
  date,
  type User,
  type Task,
  type Listing,
} from "../lib/console";
export default function Console({
  initialView = "overview",
}: {
  initialView?: "overview" | "health";
}) {
  const [user, setUser] = useState<User | null>(null),
    [boot, setBoot] = useState(true),
    [credential, setCredential] = useState(""),
    [loginError, setLoginError] = useState("");
  const [profileOpen, setProfileOpen] = useState(false),
    [githubProfile, setGithubProfile] = useState<GithubProfile | null>(null),
    [profileBusy, setProfileBusy] = useState(false),
    [profileError, setProfileError] = useState(""),
    [expiresAt, setExpiresAt] = useState(0);
  const [executionKind, setExecutionKind] = useState("trace");
  const listSequence = useRef(0);
  const listFlight = useRef<string | null>(null);
  const token = useRef(""),
    epoch = useRef(0),
    controller = useRef(new AbortController());
  const [data, setData] = useState<Listing | null>(null),
    [error, setError] = useState(""),
    [busy, setBusy] = useState(false),
    [status, setStatus] = useState("all"),
    [query, setQuery] = useState(""),
    [page, setPage] = useState(1),
    [nav, setNav] = useState<string>(initialView),
    [mobile, setMobile] = useState(false);
  const [selected, setSelected] = useState<Task | null>(null),
    [detail, setDetail] = useState<any>(null),
    [detailError, setDetailError] = useState(""),
    [creating, setCreating] = useState(false),
    [mutation, setMutation] = useState(false),
    [notice, setNotice] = useState("");
  const clear = useCallback(() => {
    epoch.current++;
    controller.current.abort();
    controller.current = new AbortController();
    token.current = "";
    setUser(null);
    setProfileOpen(false);
    setGithubProfile(null);
    setProfileBusy(false);
    setProfileError("");
    setExpiresAt(0);
    setData(null);
    setSelected(null);
    setDetail(null);
    setCreating(false);
    setError("");
    setNotice("");
    listSequence.current++;
    listFlight.current = null;
    setBusy(false);
    setQuery("");
    setPage(1);
    setStatus("all");
    setNav(
      typeof window !== "undefined" && window.location.pathname === "/system"
        ? "health"
        : initialView,
    );
    setMobile(false);
  }, [initialView]);
  const api = useCallback(
    async (path: string, body?: unknown) => {
      const version = epoch.current;
      const r = await fetch(path, {
        method: body === undefined ? "GET" : "POST",
        credentials: "same-origin",
        cache: "no-store",
        redirect: "error",
        signal: AbortSignal.any([
          controller.current.signal,
          AbortSignal.timeout(15000),
        ]),
        headers: {
          "content-type": "application/json",
          ...(token.current
            ? {
                authorization: token.current.startsWith("Bearer ")
                  ? token.current
                  : `Bearer ${token.current}`,
              }
            : {}),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      const value = await r
        .json()
        .catch(() => ({ error: "服务暂时不可用，请稍后重试。" }));
      if (version !== epoch.current) throw new Error("会话已改变");
      if (r.status === 401) {
        clear();
        throw new Error("登录已过期，请重新登录。");
      }
      if (!r.ok)
        throw new Error(
          r.status === 403
            ? "当前身份没有操作权限。"
            : String(value.error ?? `请求失败 (${r.status})`),
        );
      return value;
    },
    [clear],
  );
  const login = useCallback(
    async (value = "") => {
      token.current = value.trim();
      try {
        const r = await api("/v1/auth/me");
        setUser(r.user);
        setExpiresAt(r.expiresAt);
        setCredential("");
        setLoginError("");
        const delay = r.expiresAt - Date.now();
        if (delay > 0) {
          const version = epoch.current;
          setTimeout(
            () => {
              if (version === epoch.current) clear();
            },
            Math.min(delay, 2147483647),
          );
        }
      } catch (e) {
        token.current = "";
        if (value) setLoginError((e as Error).message);
      } finally {
        setBoot(false);
      }
    },
    [api, clear],
  );
  useEffect(() => {
    if (controller.current.signal.aborted)
      controller.current = new AbortController();
    void login();
    return () => {
      controller.current.abort();
    };
  }, [login]);
  const loadProfile = useCallback(async () => {
    if (!user?.sub.startsWith("github:")) return;
    const version = epoch.current;
    setProfileBusy(true);
    try {
      const result = await api("/v1/auth/profile");
      if (version === epoch.current) {
        setGithubProfile(result.profile);
        setProfileError("");
      }
    } catch (e) {
      if (version === epoch.current) setProfileError((e as Error).message);
    } finally {
      if (version === epoch.current) setProfileBusy(false);
    }
  }, [api, user?.sub]);
  useEffect(() => {
    setGithubProfile(null);
    setProfileError("");
    void loadProfile();
  }, [loadProfile]);
  const load = useCallback(async () => {
    if (!user) return;
    const key = JSON.stringify([user.sub, page, status, query]);
    if (listFlight.current === key) return;
    listFlight.current = key;
    const sequence = ++listSequence.current;
    setBusy(true);
    try {
      const r = await api(
        `/v1/console/tasks?limit=8&page=${page}&status=${status}&q=${encodeURIComponent(query)}`,
      );
      if (sequence !== listSequence.current) return;
      setData(r);
      setError("");
    } catch (e) {
      if (
        sequence === listSequence.current &&
        (e as Error).name !== "AbortError"
      )
        setError((e as Error).message);
    } finally {
      if (sequence === listSequence.current) {
        listFlight.current = null;
        setBusy(false);
      }
    }
  }, [api, user, page, status, query]);
  useEffect(() => {
    const timer = setTimeout(() => void load(), 180);
    const poll = setInterval(() => void load(), 5000);
    return () => {
      clearTimeout(timer);
      clearInterval(poll);
    };
  }, [load]);
  useEffect(() => {
    if (!selected) return;
    let active = true;
    setDetail(null);
    setDetailError("");
    const run = async () => {
      try {
        const base = `/v1/tasks/${encodeURIComponent(selected.task_id)}?tenantId=${encodeURIComponent(selected.tenant_id)}`;
        const r = await api(base);
        let execution = null;
        try {
          execution = await api(base + "&includeResult=1");
        } catch {}
        if (active) {
          setDetail({ ...r, executionResult: execution });
          setDetailError("");
        }
      } catch (e) {
        if (active) setDetailError((e as Error).message);
      }
    };
    void run();
    const poll = setInterval(() => void run(), 3000);
    return () => {
      active = false;
      clearInterval(poll);
    };
  }, [selected, api]);
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape" && !mutation) {
        setSelected(null);
        setCreating(false);
        setProfileOpen(false);
        setMobile(false);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [mutation]);
  useEffect(() => {
    if (!creating && !selected && !profileOpen) return;
    const previous = document.activeElement as HTMLElement;
    const panel = document.querySelector<HTMLElement>(".drawer");
    const focusable = () =>
      Array.from(
        panel?.querySelectorAll<HTMLElement>(
          "button:not(:disabled),input,textarea,select,summary,a[href]",
        ) ?? [],
      );
    focusable()[0]?.focus();
    const trap = (event: KeyboardEvent) => {
      if (event.key !== "Tab") return;
      const items = focusable();
      if (!items.length) return;
      const first = items[0],
        last = items[items.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", trap);
    const overflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", trap);
      document.body.style.overflow = overflow;
      previous?.focus();
    };
  }, [creating, selected, profileOpen]);
  useEffect(() => {
    const onPop = () =>
      setNav(window.location.pathname === "/system" ? "health" : "overview");
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);
  const filter = (value: string, nextNav = "tasks") => {
    setStatus(value);
    setPage(1);
    setNav(nextNav);
    if (window.location.pathname === "/system")
      window.history.pushState(window.history.state, "", "/");
    setMobile(false);
  };
  async function logout() {
    try {
      await api("/v1/auth/logout", {});
    } catch {}
    clear();
  }
  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const fields = new FormData(event.currentTarget);
    setMutation(true);
    setNotice("");
    try {
      const kind = String(fields.get("kind"));
      const body: any = {
        tenant_id: fields.get("tenant"),
        task_id: fields.get("id"),
        prompt: fields.get("prompt"),
      };
      if (kind !== "trace") {
        body.execution = {
          kind,
          image: data?.capabilities.sandboxImage,
          timeoutMs: 60000,
          cpu: 1,
          memoryMb: 512,
          ...(kind === "container"
            ? { argv: JSON.parse(String(fields.get("argv"))) }
            : { maxTurns: 5, maxTokens: 2048, files: {} }),
        };
        if (
          kind === "container" &&
          (!Array.isArray(body.execution.argv) ||
            !body.execution.argv.every((x: unknown) => typeof x === "string"))
        )
          throw new Error("命令必须是字符串组成的 JSON 数组。");
        body.resource_profile = {
          think_cpu_cores: 1,
          act_memory_mb: 512,
          disk_mb: 1024,
        };
      }
      await api("/v1/tasks", body);
      setCreating(false);
      setNotice(`任务 ${body.task_id} 已提交。`);
      setPage(1);
      setStatus("all");
      await load();
    } catch (e) {
      setNotice((e as Error).message);
    } finally {
      setMutation(false);
    }
  }
  async function cancel() {
    if (!selected) return;
    const reason = window.prompt(
      "请输入取消原因。任务资源会在执行器确认后释放。",
    );
    if (!reason?.trim()) return;
    setMutation(true);
    try {
      await api(
        `/v1/tasks/${encodeURIComponent(selected.task_id)}/cancel?tenantId=${encodeURIComponent(selected.tenant_id)}`,
        { tenant_id: selected.tenant_id, reason },
      );
      setNotice("取消请求已提交，等待执行器确认。");
      await load();
    } catch (e) {
      setDetailError((e as Error).message);
    } finally {
      setMutation(false);
    }
  }
  if (boot)
    return (
      <main className="boot">
        <Box size={28} />
        <p>正在连接工作区…</p>
      </main>
    );
  if (!user)
    return (
      <main className="login">
        <div className="login-brand">
          <Box size={24} />
          <b>ANSTS</b>
          <span>CONSOLE</span>
        </div>
        <section className="login-card">
          <span className="eyebrow">AGENT-NATIVE INFRASTRUCTURE</span>
          <h1>
            每一次执行，
            <br />
            都在掌握之中。
          </h1>
          <p>查看任务、跟踪执行结果，管理你的沙箱工作区。</p>
          <a className="primary github" href="/v1/auth/github">
            <Github size={18} />
            使用 GitHub 登录
            <ArrowUpRight size={17} />
          </a>
          <div className="secure">
            <ShieldCheck size={14} />
            身份验证后，仅显示授权租户的数据
          </div>
          <details>
            <summary>使用短期开发凭据</summary>
            <form
              onSubmit={(e) => {
                e.preventDefault();
                void login(credential);
              }}
            >
              <label>
                访问凭据
                <input
                  type="password"
                  autoComplete="off"
                  value={credential}
                  onChange={(e) => setCredential(e.target.value)}
                  required
                />
              </label>
              <button className="secondary" type="submit">
                连接工作区
              </button>
            </form>
          </details>
          {loginError && (
            <p role="alert" className="error">
              {loginError}
            </p>
          )}
        </section>
        <footer>ANSTS / Agent-Native Sandbox Task Scheduler</footer>
      </main>
    );
  const counts = data?.counts ?? {},
    total = counts.total ?? 0;
  const result = detail?.result?.result ?? detail?.result ?? {};
  const runtime =
    detail?.runtimeResult ?? detail?.executionResult ?? detail?.bodyResult;
  const actualResult = runtime?.result ?? result;
  const title =
    nav === "overview"
      ? "工作区总览"
      : nav === "health"
        ? "系统状态"
        : status === "uncertain"
          ? "待核对任务"
          : "任务管理";
  return (
    <div className="shell">
      <aside className={mobile ? "sidebar open" : "sidebar"}>
        <a className="brand" href="/">
          <span className="brand-icon">
            <Box size={21} />
          </span>
          ANSTS<span className="version">v2.5</span>
        </a>
        <div className="workspace">
          <span className="avatar">
            {user.scopeTenant.slice(0, 1).toUpperCase()}
          </span>
          <div>
            <b>{user.scopeTenant === "all" ? "全部租户" : user.scopeTenant}</b>
            <small>开发工作区</small>
          </div>
          <ShieldCheck size={15} />
        </div>
        <div className="nav-label">工作空间</div>
        <nav>
          <button
            className={nav === "overview" ? "active" : ""}
            onClick={() => filter("all", "overview")}
          >
            <LayoutDashboard size={17} />
            总览
          </button>
          <button
            className={
              nav === "tasks" && status !== "uncertain" ? "active" : ""
            }
            onClick={() => filter("all")}
          >
            <ClipboardList size={17} />
            任务管理<span>{total}</span>
          </button>
          <button
            className={status === "uncertain" ? "active" : ""}
            onClick={() => filter("uncertain")}
          >
            <AlertTriangle size={17} />
            待核对
            {counts.uncertain > 0 && (
              <span className="nav-count">{counts.uncertain}</span>
            )}
          </button>
          <button
            className={nav === "health" ? "active" : ""}
            onClick={() => {
              setNav("health");
              if (window.location.pathname !== "/system")
                window.history.pushState(window.history.state, "", "/system");
              setMobile(false);
            }}
          >
            <Activity size={17} />
            系统状态
          </button>
        </nav>
        <div className="sidebar-bottom">
          <div className="environment">
            <span className="dot" />
            DEVELOPMENT
          </div>
          <div className="profile">
            <button
              className="profile-trigger"
              aria-label="打开个人资料"
              title="个人资料"
              onClick={() => {
                setMobile(false);
                setProfileOpen(true);
              }}
            >
              <Avatar
                profile={githubProfile}
                name={
                  githubProfile?.name ||
                  githubProfile?.login ||
                  (user.sub.startsWith("github:") ? "G" : user.sub)
                }
              />
              <span>
                <b>
                  {githubProfile?.name ||
                    githubProfile?.login ||
                    (user.sub.startsWith("github:") ? "GitHub 用户" : user.sub)}
                </b>
                <small>
                  {githubProfile
                    ? `@${githubProfile.login}`
                    : user.sub.startsWith("github:")
                      ? profileBusy
                        ? "正在获取资料…"
                        : "查看个人资料"
                      : roleName(user.role)}
                </small>
              </span>
            </button>
            <button
              aria-label="退出登录"
              title="退出登录"
              onClick={() => void logout()}
            >
              <LogOut size={16} />
            </button>
          </div>
        </div>
      </aside>
      {mobile && (
        <div className="mobile-scrim" onClick={() => setMobile(false)} />
      )}
      <div className="main">
        <header className="topbar">
          <button
            className="mobile-toggle icon"
            aria-label="打开导航"
            onClick={() => setMobile(!mobile)}
          >
            <Menu size={20} />
          </button>
          <div className="breadcrumb">
            工作空间 <span>/</span> <b>{title}</b>
          </div>
          <div className="topbar-right">
            <span className="scope">
              <ShieldCheck size={13} />
              {user.scopeTenant === "all"
                ? "跨租户管理"
                : `租户 · ${user.scopeTenant}`}
            </span>
            <a
              href="https://github.com"
              target="_blank"
              rel="noreferrer"
              aria-label="GitHub"
            >
              <Github size={17} />
            </a>
          </div>
        </header>
        <main className="content">
          {nav !== "health" && (
            <div className="page-heading">
              <div>
                <div className="eyebrow">
                  WORKSPACE / {nav === "health" ? "HEALTH" : "OVERVIEW"}
                </div>
                <h1>
                  {title}
                  <span className="live-label">
                    <span className="dot" />
                    实时
                  </span>
                </h1>
                <p>从提交到结果，追踪每一个任务的执行状态。</p>
              </div>
              <div className="heading-actions">
                <button
                  className="secondary"
                  disabled={busy}
                  onClick={() => void load()}
                >
                  <RefreshCw size={15} className={busy ? "spin" : ""} />
                  刷新
                </button>
                {user.canWrite && (
                  <button
                    className="primary"
                    onClick={() => {
                      setNotice("");
                      setExecutionKind("trace");
                      setCreating(true);
                    }}
                  >
                    <Plus size={16} />
                    创建任务
                  </button>
                )}
              </div>
            </div>
          )}
          {error && nav !== "health" && (
            <div className="banner error" role="alert">
              {error} {data ? "保留上次成功读取的数据。" : ""}
              <button onClick={() => void load()}>重试</button>
            </div>
          )}
          {notice && !creating && nav !== "health" && (
            <div className="banner" role="status">
              <Check size={16} />
              {notice}
              <button aria-label="关闭提示" onClick={() => setNotice("")}>
                <X size={14} />
              </button>
            </div>
          )}
          {nav === "health" ? (
            <SystemStatus api={api} user={user} />
          ) : (
            <>
              <section className="metrics" aria-label="任务统计">
                {["total", "active", "completed", "uncertain"].map((key, i) => (
                  <button
                    key={key}
                    className={`metric ${key === "uncertain" && counts[key] > 0 ? "attention" : ""}`}
                    onClick={() => filter(key === "total" ? "all" : key)}
                  >
                    <span className="metric-label">
                      {key === "total" ? "可访问任务" : labels[key]}
                      {
                        [
                          <ClipboardList size={16} key="i" />,
                          <Activity size={16} key="i" />,
                          <Check size={16} key="i" />,
                          <AlertTriangle size={16} key="i" />,
                        ][i]
                      }
                    </span>
                    <strong>{data ? (counts[key] ?? 0) : "—"}</strong>
                    <span className="metric-foot">
                      {
                        [
                          "当前授权范围内全部任务",
                          "已分配或正在执行",
                          "已确认执行完成",
                          "需要人工确认执行状态",
                        ][i]
                      }
                      <ArrowUpRight size={13} />
                    </span>
                  </button>
                ))}
              </section>
              <div className="dashboard-grid">
                <section className="task-panel">
                  <div className="panel-title">
                    <h2>{nav === "overview" ? "最近任务" : labels[status]}</h2>
                    <span className="subtle">每 5 秒自动更新</span>
                  </div>
                  <div className="table-controls">
                    <div className="tabs">
                      {["all", "active", "failed", "uncertain"].map((key) => (
                        <button
                          key={key}
                          className={status === key ? "chosen" : ""}
                          onClick={() => {
                            setStatus(key);
                            setPage(1);
                          }}
                        >
                          {key === "all" ? "全部" : labels[key]}
                          {key === "uncertain" && counts.uncertain > 0 && (
                            <span>{counts.uncertain}</span>
                          )}
                        </button>
                      ))}
                    </div>
                    <label className="search">
                      <Search size={15} />
                      <input
                        aria-label="搜索任务或租户"
                        placeholder="搜索任务或租户…"
                        value={query}
                        onChange={(e) => {
                          setQuery(e.target.value);
                          setPage(1);
                        }}
                      />
                    </label>
                  </div>
                  <div className="table-wrap">
                    <table>
                      <thead>
                        <tr>
                          <th>任务 / 租户</th>
                          <th>状态</th>
                          <th>执行方式</th>
                          <th>创建时间</th>
                          <th />
                        </tr>
                      </thead>
                      <tbody>
                        {data?.tasks.map((task) => (
                          <tr key={`${task.tenant_id}/${task.task_id}`}>
                            <td>
                              <button
                                className="task-link"
                                onClick={() => setSelected(task)}
                              >
                                <span className="task-icon">
                                  <Terminal size={16} />
                                </span>
                                <span>
                                  <b>{task.task_id}</b>
                                  <small>{task.tenant_id}</small>
                                </span>
                              </button>
                            </td>
                            <td>
                              <span className={`badge ${task.category}`}>
                                <span />
                                {labels[task.category] ?? task.state}
                              </span>
                            </td>
                            <td>
                              <span className="kind">
                                {task.execution_kind === "agent"
                                  ? "Agent"
                                  : task.execution_kind === "container"
                                    ? "Container"
                                    : "Trace"}
                              </span>
                            </td>
                            <td className="mono time">
                              {date(task.created_at)}
                            </td>
                            <td>
                              <button
                                className="icon"
                                aria-label={`查看 ${task.task_id}`}
                                onClick={() => setSelected(task)}
                              >
                                <ChevronRight size={16} />
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {!data && !error && (
                      <div className="empty">正在读取任务…</div>
                    )}
                    {data && !data.tasks.length && (
                      <div className="empty">
                        <ClipboardList size={30} />
                        <h3>
                          {query || status !== "all"
                            ? "没有匹配的任务"
                            : "工作区准备就绪"}
                        </h3>
                        <p>
                          {query || status !== "all"
                            ? "调整搜索条件，或查看全部任务。"
                            : "创建第一个任务，开始追踪执行。"}
                        </p>
                        <button
                          className="secondary"
                          disabled={
                            !user.canWrite && !query && status === "all"
                          }
                          onClick={() =>
                            query || status !== "all"
                              ? (setQuery(""), filter("all"))
                              : setCreating(true)
                          }
                        >
                          {query || status !== "all"
                            ? "清除筛选"
                            : user.canWrite
                              ? "创建任务"
                              : "等待任务"}
                        </button>
                      </div>
                    )}
                  </div>
                  <div className="table-footer">
                    <span>
                      {data
                        ? `共 ${data.total} 条 · 第 ${page} 页`
                        : "等待连接"}
                      <span className="updated">
                        {data
                          ? ` / ${new Date(data.refreshedAt).toLocaleTimeString("zh-CN")} 更新`
                          : ""}
                      </span>
                    </span>
                    <div>
                      <button
                        className="icon"
                        aria-label="上一页"
                        disabled={page === 1}
                        onClick={() => setPage((p) => p - 1)}
                      >
                        <ChevronLeft size={16} />
                      </button>
                      <button
                        className="icon"
                        aria-label="下一页"
                        disabled={!data || page * 8 >= data.total}
                        onClick={() => setPage((p) => p + 1)}
                      >
                        <ChevronRight size={16} />
                      </button>
                    </div>
                  </div>
                </section>
                <aside className="right-rail">
                  {(data?.attention?.length ?? 0) > 0 && (
                    <section className="attention-card">
                      <div className="panel-title">
                        <h2>
                          <AlertTriangle size={14} />
                          需要核对
                        </h2>
                        <button onClick={() => filter("uncertain")}>
                          查看全部
                          <ArrowUpRight size={13} />
                        </button>
                      </div>
                      <p>执行状态尚未确认，资源保留中</p>
                      {data?.attention.map((task) => (
                        <button
                          key={`${task.tenant_id}/${task.task_id}`}
                          className="attention-task"
                          onClick={() => setSelected(task)}
                        >
                          <span>
                            <b>{task.task_id}</b>
                            <small>{task.tenant_id}</small>
                          </span>
                          <ChevronRight size={14} />
                        </button>
                      ))}
                    </section>
                  )}
                  <section className="distribution">
                    <div className="panel-title">
                      <h2>执行分布</h2>
                      <span className="subtle">{total} 个任务</span>
                    </div>
                    <div className="stack-bar">
                      {[
                        "completed",
                        "active",
                        "queued",
                        "failed",
                        "cancelled",
                        "uncertain",
                      ].map((k) => (
                        <span
                          key={k}
                          className={k}
                          style={{
                            width: `${total ? ((counts[k] ?? 0) / total) * 100 : 0}%`,
                          }}
                        />
                      ))}
                    </div>
                    <div className="distribution-list">
                      {[
                        "completed",
                        "active",
                        "queued",
                        "failed",
                        "cancelled",
                        "uncertain",
                      ].map((k) => (
                        <button key={k} onClick={() => filter(k)}>
                          <span className={`legend-dot ${k}`} />
                          <span>{labels[k]}</span>
                          <b>{data ? (counts[k] ?? 0) : "—"}</b>
                        </button>
                      ))}
                    </div>
                  </section>
                  <div className="rail-note">
                    <CircleHelp size={15} />
                    <p>
                      统计仅包含当前身份可访问的任务。执行中包括已分配与正在运行的任务。
                    </p>
                  </div>
                </aside>
              </div>
            </>
          )}
          <footer className="content-footer">
            <span>
              <span className="dot" />
              {error ? "连接异常" : "ANSTS 控制平面"}
            </span>
            <span>Agent-Native Sandbox Task Scheduler</span>
          </footer>
        </main>
      </div>
      {(creating || selected || profileOpen) && (
        <div
          className="drawer-backdrop"
          onClick={() => {
            if (!mutation) {
              setCreating(false);
              setSelected(null);
              setProfileOpen(false);
            }
          }}
        >
          <section
            role="dialog"
            aria-modal="true"
            aria-label={
              profileOpen ? "个人资料" : creating ? "创建任务" : "任务详情"
            }
            className="drawer"
            onClick={(e) => e.stopPropagation()}
          >
            <header>
              <div>
                <span className="eyebrow">
                  {profileOpen
                    ? "ACCOUNT / PROFILE"
                    : creating
                      ? "NEW EXECUTION"
                      : "TASK INSPECTOR"}
                </span>
                <h2>
                  {profileOpen
                    ? "个人资料"
                    : creating
                      ? "创建任务"
                      : "任务详情"}
                </h2>
              </div>
              <button
                className="icon"
                disabled={mutation}
                aria-label="关闭面板"
                onClick={() => {
                  setCreating(false);
                  setSelected(null);
                  setProfileOpen(false);
                }}
              >
                <X size={20} />
              </button>
            </header>
            <div className="drawer-body">
              {profileOpen ? (
                <Profile
                  user={user}
                  profile={githubProfile}
                  loading={profileBusy}
                  error={profileError}
                  expiresAt={expiresAt}
                  retry={() => void loadProfile()}
                  logout={() => void logout()}
                />
              ) : creating ? (
                <form onSubmit={submit} className="create-form">
                  <p className="form-intro">
                    提交后，调度器会为任务分配执行节点。
                  </p>
                  <label>
                    租户
                    <input
                      name="tenant"
                      defaultValue={
                        user.scopeTenant === "all" ? "" : user.scopeTenant
                      }
                      readOnly={user.scopeTenant !== "all"}
                      required
                      maxLength={128}
                      placeholder="租户标识"
                    />
                  </label>
                  <label>
                    任务 ID
                    <input
                      autoFocus
                      name="id"
                      required
                      pattern="[A-Za-z0-9_.-]+"
                      maxLength={128}
                      placeholder="例如 research-001"
                    />
                    <small>使用英文字母、数字、连字符或下划线</small>
                  </label>
                  <label>
                    任务说明
                    <textarea
                      name="prompt"
                      rows={5}
                      required
                      maxLength={10000}
                      placeholder="描述需要执行的任务…"
                    />
                  </label>
                  <label>
                    执行方式
                    <select
                      name="kind"
                      value={executionKind}
                      onChange={(event) => setExecutionKind(event.target.value)}
                    >
                      <option value="trace">Trace · 调度链路</option>
                      {data?.capabilities.sandboxImage && (
                        <option value="container">Container · 容器命令</option>
                      )}
                      {data?.capabilities.sandboxImage &&
                        data?.capabilities.agentEnabled && (
                          <option value="agent">Agent · 模型执行</option>
                        )}
                    </select>
                  </label>
                  {executionKind === "trace" && (
                    <div className="form-hint">
                      Trace
                      验证任务的调度链路，不会启动容器或调用模型，也不生成运行日志与文件产物。
                    </div>
                  )}
                  {data?.capabilities.sandboxImage &&
                    executionKind !== "trace" && (
                      <>
                        {executionKind === "container" && (
                          <label>
                            容器命令（仅 Container 使用）
                            <textarea
                              name="argv"
                              rows={2}
                              defaultValue={'["sh", "-lc", "echo hello"]'}
                            />
                          </label>
                        )}
                        <div className="form-hint">
                          使用已允许的镜像 {data.capabilities.sandboxImage}
                          。容器与 Agent 默认 1 CPU / 512 MB，执行上限 60 秒。
                        </div>
                      </>
                    )}
                  {notice && (
                    <div role="alert" className="error">
                      {notice}
                    </div>
                  )}
                  <button className="primary" type="submit" disabled={mutation}>
                    <Plus size={16} />
                    {mutation ? "正在提交…" : "提交任务"}
                  </button>
                </form>
              ) : (
                <>
                  <div className="detail-name">
                    <Terminal size={22} />
                    <h3>{selected?.task_id}</h3>
                  </div>
                  <dl className="detail-grid">
                    <div>
                      <dt>租户</dt>
                      <dd>{selected?.tenant_id}</dd>
                    </div>
                    <div>
                      <dt>状态</dt>
                      <dd>
                        {labels[selected?.category ?? ""] ??
                          detail?.state ??
                          selected?.state}
                      </dd>
                    </div>
                    <div>
                      <dt>执行节点</dt>
                      <dd>
                        {detail?.assigned_node_id ??
                          selected?.assigned_node_id ??
                          "尚未分配"}
                      </dd>
                    </div>
                    <div>
                      <dt>创建时间</dt>
                      <dd>{date(selected?.created_at ?? null)}</dd>
                    </div>
                  </dl>
                  {detailError && (
                    <p className="error" role="alert">
                      {detailError}
                    </p>
                  )}
                  <h3 className="detail-heading">执行结果</h3>
                  {!detail && !detailError ? (
                    <p className="subtle">正在读取结果…</p>
                  ) : (
                    <>
                      <pre className="logs">
                        {actualResult.logs ||
                          detail?.executionResult?.runtimeLogs?.logs ||
                          (selected?.execution_kind === "trace"
                            ? detail?.state === "COMPLETED"
                              ? "Trace 调度任务已完成，不生成运行日志或文件产物。"
                              : "Trace 调度任务不生成运行日志或文件产物。"
                            : ["COMPLETED", "FAILED", "DEAD_LETTER"].includes(
                                  detail?.state,
                                )
                              ? "执行已结束，没有记录标准输出。"
                              : "执行尚未产生运行日志。")}
                      </pre>
                      {actualResult.executionOutcome && (
                        <p>
                          执行结果：
                          {(
                            {
                              SUCCEEDED: "执行成功",
                              FAILED: "执行失败",
                              TIMED_OUT: "执行超时",
                              CANCELLED: "已取消",
                            } as Record<string, string>
                          )[actualResult.executionOutcome] ??
                            actualResult.executionOutcome}{" "}
                          · 退出码 {actualResult.exitCode ?? "—"}
                        </p>
                      )}
                      {actualResult.artifacts?.map((a: any) => (
                        <button
                          key={a.path}
                          className="artifact"
                          onClick={() => {
                            const bytes = Uint8Array.from(
                              atob(a.contentBase64 ?? ""),
                              (c) => c.charCodeAt(0),
                            );
                            const url = URL.createObjectURL(new Blob([bytes]));
                            const link = document.createElement("a");
                            link.href = url;
                            link.download =
                              String(a.path).split("/").pop() || "artifact";
                            link.click();
                            setTimeout(() => URL.revokeObjectURL(url), 1000);
                          }}
                        >
                          <Download size={16} />
                          {a.path}
                          <small>{a.size} B</small>
                        </button>
                      ))}
                      <details className="raw">
                        <summary>原始任务数据</summary>
                        <pre>{JSON.stringify(detail, null, 2)}</pre>
                      </details>
                    </>
                  )}
                  {user.canWrite &&
                    !["completed", "failed", "cancelled"].includes(
                      selected?.category ?? "",
                    ) && (
                      <button
                        disabled={mutation}
                        className="danger"
                        onClick={() => void cancel()}
                      >
                        请求取消任务
                      </button>
                    )}
                </>
              )}
            </div>
          </section>
        </div>
      )}
    </div>
  );
}
