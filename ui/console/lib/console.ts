export type User = {
  sub: string;
  role: string;
  scopeTenant: string;
  canWrite: boolean;
};
export type Task = {
  task_id: string;
  tenant_id: string;
  state: string;
  category: string;
  execution_kind: string;
  assigned_node_id: string | null;
  created_at: number | null;
};
export type Listing = {
  tasks: Task[];
  attention: Task[];
  total: number;
  counts: Record<string, number>;
  refreshedAt: number;
  capabilities: { sandboxImage: string | null; agentEnabled: boolean };
};
export const labels: Record<string, string> = {
  all: "全部任务",
  active: "执行中",
  queued: "排队中",
  completed: "已完成",
  failed: "失败",
  cancelled: "已取消",
  uncertain: "待核对",
};
export const date = (v: number | null) =>
  v
    ? new Date(v < 1e12 ? v * 1000 : v).toLocaleString("zh-CN", {
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      })
    : "—";
