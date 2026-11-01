import { TaskOperationProvider } from "../persistence/task-operations";
import { TaskOperationExecutorBinding } from "./env";

/** HTTP adapter to the configured internal MCP/tool executor service. */
export class CloudflareTaskOperationProvider implements TaskOperationProvider<unknown> {
  /** Retries require authoritative lookup; this adapter never assumes the remote tool is idempotent. */
  public readonly supportsIdempotency = false;

  constructor(
    private readonly binding: TaskOperationExecutorBinding,
    private readonly tenantId: string,
    private readonly taskId: string,
  ) {}

  public async execute(input: {
    operationId: string;
    idempotencyKey: string;
    attemptId: string;
    toolName: string;
    args: unknown;
  }): Promise<unknown> {
    const body = await this.call("/v1/operations/execute", "POST", {
      tenant_id: this.tenantId,
      task_id: this.taskId,
      operation_id: input.operationId,
      idempotency_key: input.idempotencyKey,
      attempt_id: input.attemptId,
      tool_name: input.toolName,
      args: input.args,
    });
    if (body.status === "FAILED" || body.error) {
      throw new Error(typeof body.message === "string" ? body.message : "Tool executor did not confirm a successful outcome.");
    }
    if (!("result" in body)) throw new Error("Tool executor response did not contain a result.");
    return body.result;
  }

  public async lookup(input: { operationId: string; idempotencyKey: string }): Promise<
    { status: "FOUND"; result: unknown } | { status: "NOT_FOUND" } | { status: "UNKNOWN" }
  > {
    let response: Response;
    try {
      response = await this.binding.fetch(new Request(
        `https://tool-executor.internal/v1/operations/${encodeURIComponent(input.operationId)}?tenant_id=${encodeURIComponent(this.tenantId)}&task_id=${encodeURIComponent(this.taskId)}&idempotency_key=${encodeURIComponent(input.idempotencyKey)}`,
      ));
    } catch {
      return { status: "UNKNOWN" };
    }
    // Error responses cannot establish either completion or authoritative absence.
    // A proxy/service failure may carry a body that resembles a normal lookup result.
    if (!response.ok) return { status: "UNKNOWN" };
    let body: Record<string, unknown>;
    try {
      body = await response.json() as Record<string, unknown>;
    } catch {
      return { status: "UNKNOWN" };
    }
    if (!body || typeof body !== "object" || Array.isArray(body)) return { status: "UNKNOWN" };
    if (body.status === "FOUND" && "result" in body) return { status: "FOUND", result: body.result };
    if (body.status === "NOT_FOUND" && body.authoritative === true) return { status: "NOT_FOUND" };
    return { status: "UNKNOWN" };
  }

  public async executeCompensation(input: {
    operationId: string;
    attemptId: string;
    idempotencyKey: string;
    toolName: string;
    args: unknown;
  }): Promise<boolean> {
    const body = await this.call("/v1/operations/compensate", "POST", {
      tenant_id: this.tenantId,
      task_id: this.taskId,
      operation_id: input.operationId,
      idempotency_key: input.idempotencyKey,
      attempt_id: input.attemptId,
      tool_name: input.toolName,
      args: input.args,
    });
    if (body.status === "COMPENSATED") return true;
    if (body.status === "FAILED") return false;
    throw new Error("Tool executor returned an uncertain compensation outcome.");
  }

  private async call(path: string, method: string, value: unknown): Promise<Record<string, any>> {
    const response = await this.binding.fetch(new Request(`https://tool-executor.internal${path}`, {
      method,
      headers: { "content-type": "application/json" },
      body: JSON.stringify(value),
    }));
    let body: Record<string, any>;
    try {
      body = await response.json() as Record<string, any>;
    } catch {
      throw new Error("Tool executor returned invalid JSON.");
    }
    if (!response.ok) {
      throw new Error(typeof body.message === "string" ? body.message : `Tool executor returned HTTP ${response.status}.`);
    }
    return body;
  }
}
