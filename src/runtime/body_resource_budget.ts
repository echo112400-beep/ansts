/** Deployment budgets bounded by the detected runtime limits. */
export function bodyResourceBudget(cpuLimit: number, memoryLimitMb: number, cpu?: string, memory?: string): { cpu: number; memoryMb: number } {
  const bounded = (limit: number, configured: string | undefined): number => {
    const value = configured === undefined ? limit : Number(configured);
    if (!Number.isFinite(limit) || limit <= 0 || !Number.isFinite(value) || value <= 0) throw new Error("INVALID_BODY_RESOURCE_BUDGET");
    return Math.min(limit, value);
  };
  return { cpu: bounded(cpuLimit, cpu), memoryMb: Math.floor(bounded(memoryLimitMb, memory)) };
}
/** Missing or unlimited controllers do not override the OS limit. */
export function cgroupResourceLimits(read: (file: string) => string | undefined): { cpu?: number; memoryBytes?: number } {
  const positive = (raw: string | undefined): number | undefined => {
    if (!raw || raw.trim() === "max") return undefined;
    const value = Number(raw.trim());
    return Number.isFinite(value) && value > 0 ? value : undefined;
  };
  const [quota, period] = (read("/sys/fs/cgroup/cpu.max") ?? "").trim().split(/\s+/);
  const v2Quota = positive(quota), v2Period = positive(period);
  const v1Quota = positive(read("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"));
  const v1Period = positive(read("/sys/fs/cgroup/cpu/cpu.cfs_period_us"));
  const cpuValues = [v2Quota && v2Period ? v2Quota / v2Period : undefined, v1Quota && v1Period ? v1Quota / v1Period : undefined].filter((value): value is number => value !== undefined);
  const memoryValues = [positive(read("/sys/fs/cgroup/memory.max")), positive(read("/sys/fs/cgroup/memory/memory.limit_in_bytes"))].filter((value): value is number => value !== undefined);
  return { cpu: cpuValues.length ? Math.min(...cpuValues) : undefined, memoryBytes: memoryValues.length ? Math.min(...memoryValues) : undefined };
}