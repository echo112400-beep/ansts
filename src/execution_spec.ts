/** Portable execution contract shared by control Workers and native runtimes. */
export interface ContainerExecutionSpec { kind: "container"; image: string; argv: string[]; timeoutMs: number; cpu: number; memoryMb: number; }
export interface AgentExecutionSpec { kind: "agent"; image: string; timeoutMs: number; cpu: number; memoryMb: number; maxTurns: number; maxTokens: number; files: Record<string, string>; }
export type ExecutionSpec = ContainerExecutionSpec | AgentExecutionSpec;
export function workspacePath(value: string): boolean { return !!value && value.length <= 256 && !value.includes("\\") && !value.startsWith("/") && !value.split("/").some(part => !part || part === "." || part === ".."); }
export function normalizeExecutionSpec(value: unknown): ExecutionSpec | undefined {
  if (value === undefined) return undefined;
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("INVALID_EXECUTION_SPEC");
  const spec = value as Record<string, unknown>;
  if (spec.kind === "agent") {
    const common = normalizeExecutionSpec({ kind: "container", image: spec.image, argv: ["true"], cpu: spec.cpu, memoryMb: spec.memoryMb, timeoutMs: spec.timeoutMs })!;
    const files = spec.files ?? {};
    if (Object.keys(spec).some(key => !["kind", "image", "timeoutMs", "cpu", "memoryMb", "maxTurns", "maxTokens", "files"].includes(key)) || !Number.isSafeInteger(spec.maxTurns) || Number(spec.maxTurns) < 1 || Number(spec.maxTurns) > 16 || !Number.isSafeInteger(spec.maxTokens) || Number(spec.maxTokens) < 1024 || Number(spec.maxTokens) > 65536 || !files || typeof files !== "object" || Array.isArray(files) || Object.keys(files).length > 16 || Object.entries(files).some(([name, content]) => !workspacePath(name) || typeof content !== "string") || JSON.stringify(spec).length > 8192) throw new Error("INVALID_AGENT_SPEC");
    if (new TextEncoder().encode(JSON.stringify(files)).length > 8192) throw new Error("AGENT_INITIAL_WORKSPACE_LIMIT");
    return { kind: "agent", image: common.image, cpu: common.cpu, memoryMb: common.memoryMb, timeoutMs: common.timeoutMs, maxTurns: Number(spec.maxTurns), maxTokens: Number(spec.maxTokens), files: { ...files } as Record<string, string> };
  }
  if (JSON.stringify(spec).length > 8192) throw new Error("EXECUTION_SPEC_TOO_LARGE");
  if (Object.keys(spec).some(key => !["kind", "image", "argv", "timeoutMs", "cpu", "memoryMb"].includes(key)) || spec.kind !== "container" || typeof spec.image !== "string" || !/^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$/.test(spec.image) || !Array.isArray(spec.argv) || !spec.argv.length || spec.argv.length > 64 || spec.argv.some(value => typeof value !== "string" || value.includes("\0") || value.length > 8192) || !Number.isSafeInteger(spec.timeoutMs) || Number(spec.timeoutMs) < 1 || Number(spec.timeoutMs) > 3600000 || typeof spec.cpu !== "number" || !Number.isFinite(spec.cpu) || spec.cpu <= 0 || spec.cpu > 32 || !Number.isSafeInteger(spec.memoryMb) || Number(spec.memoryMb) < 16 || Number(spec.memoryMb) > 65536) throw new Error("INVALID_EXECUTION_SPEC");
  return { kind: "container", image: spec.image, argv: [...spec.argv] as string[], timeoutMs: Number(spec.timeoutMs), cpu: spec.cpu, memoryMb: Number(spec.memoryMb) };
}
