export const ADMIN_MODULES = [
  { id: "capacity", title: "容量分配", description: "容量池与租户申请", group: "运行管理" },
  {
    id: "overview",
    title: "平台总览",
    description: "任务态势、异常与容量恢复",
    group: "运行管理",
  },
  {
    id: "queues",
    title: "队列与健康",
    description: "按状态与租户定位等待、失败和执行阻塞",
    group: "运行管理",
  },
  {
    id: "nodes",
    title: "节点与 Cell",
    description: "Gateway、Fleet、执行器与持久对象",
    group: "运行管理",
  },
  {
    id: "tenants",
    title: "租户与 FinOps",
    description: "持久目录、配额策略与授权租户运行分布",
    group: "运行管理",
  },
  {
    id: "tasks",
    title: "任务与诊断",
    description: "执行状态、日志与产物",
    group: "运行管理",
  },
  {
    id: "approvals",
    title: "核对与审计",
    description: "针对不确定执行收集证据并提交版本化决定",
    group: "平台治理",
  },
  {
    id: "configs",
    title: "配置中心",
    description: "当前运行配置与变更接入状态",
    group: "平台治理",
  },
  {
    id: "runtime",
    title: "分布式运行面",
    description: "任务、执行节点、沙箱与运行证据",
    group: "平台治理",
  },
  {
    id: "observability",
    title: "观测与运维",
    description: "真实探测、指标抓取异常与验证边界",
    group: "平台治理",
  },
  {
    id: "swarm",
    title: "Agent 协同",
    description: "Agent 执行、依赖与协作能力",
    group: "平台治理",
  },
  {
    id: "federation",
    title: "联邦与灾备",
    description: "已部署 Fleet 与跨地域能力边界",
    group: "平台治理",
  },
  {
    id: "security",
    title: "安全与 RBAC",
    description: "当前身份、作用域与访问控制",
    group: "平台治理",
  },
];
export const ADMIN_IDS = ADMIN_MODULES.map((m) => m.id);

