/** Portable execution contract shared by control Workers and native runtimes. */
export interface ContainerExecutionSpec { kind: "container"; image: string; argv: string[]; timeoutMs: number; cpu: number; memoryMb: number; }
export function normalizeExecutionSpec(value: unknown): ContainerExecutionSpec | undefined {
  if (value === undefined) return undefined;
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("INVALID_EXECUTION_SPEC");
  const spec = value as Record<string, unknown>;
  if (JSON.stringify(spec).length > 8192) throw new Error("EXECUTION_SPEC_TOO_LARGE");
  if (Object.keys(spec).some(key => !["kind", "image", "argv", "timeoutMs", "cpu", "memoryMb"].includes(key)) || spec.kind !== "container" || typeof spec.image !== "string" || !/^[a-zA-Z0-9./:_-]+@sha256:[a-f0-9]{64}$/.test(spec.image) || !Array.isArray(spec.argv) || !spec.argv.length || spec.argv.length > 64 || spec.argv.some(value => typeof value !== "string" || value.includes("\0") || value.length > 8192) || !Number.isSafeInteger(spec.timeoutMs) || Number(spec.timeoutMs) < 1 || Number(spec.timeoutMs) > 3600000 || typeof spec.cpu !== "number" || !Number.isFinite(spec.cpu) || spec.cpu <= 0 || spec.cpu > 32 || !Number.isSafeInteger(spec.memoryMb) || Number(spec.memoryMb) < 16 || Number(spec.memoryMb) > 65536) throw new Error("INVALID_EXECUTION_SPEC");
  return { kind: "container", image: spec.image, argv: [...spec.argv] as string[], timeoutMs: Number(spec.timeoutMs), cpu: spec.cpu, memoryMb: Number(spec.memoryMb) };
}