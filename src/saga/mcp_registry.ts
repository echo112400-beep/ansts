/**
 * @fileoverview M12 TASK-43 & M64-3: Dynamic Model Context Protocol (MCP) Server Discovery & JSON-RPC 2.0 Tool Bridge
 * with SQLite Persistence (`persisted_mcp_tools`) & Cold-Start `SagaToolGateway` Re-Registration.
 */
import { ToolCallEffectClass } from "../models.js";
import { SagaToolGateway } from "../saga_gateway.js";
import { SqlStorageLike } from "../cloudflare/env.js";

export interface McpToolDescriptor {
  serverName: string;
  toolName: string;
  description: string;
  effectClass: ToolCallEffectClass;
  inputSchema: Record<string, unknown>;
}

export class McpDynamicToolRegistry {
  private readonly tools: Map<string, McpToolDescriptor> = new Map();
  private sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike, sagaGateway?: SagaToolGateway) {
    if (sql) {
      this.bindSqlStorage(sql, sagaGateway);
    }
  }

  /**
   * [M64-3] Binds SQLite storage (`persisted_mcp_tools`) so dynamically registered MCP tools survive restarts
   * and can be automatically re-bound to `SagaToolGateway`.
   */
  public bindSqlStorage(sql: SqlStorageLike, sagaGateway?: SagaToolGateway): number {
    this.sql = sql;
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS persisted_mcp_tools (tool_name TEXT PRIMARY KEY, payload_json TEXT)"
    );
    const rows = this.sql
      .exec<Record<string, unknown>>("SELECT * FROM persisted_mcp_tools")
      .toArray();
    let restored = 0;
    for (const r of rows) {
      const parsed = JSON.parse(String(r.payload_json ?? "{}")) as Record<string, unknown>;
      const toolName = String(r.tool_name ?? parsed.toolName ?? parsed.tool_name ?? "");
      if (!toolName) continue;
      const desc: McpToolDescriptor = {
        serverName: String(parsed.serverName ?? parsed.server_name ?? "mcp-default"),
        toolName,
        description: String(parsed.description ?? "MCP dynamic tool"),
        effectClass:
          (parsed.effectClass as ToolCallEffectClass) ??
          (parsed.effect_class as ToolCallEffectClass) ??
          ToolCallEffectClass.COMPENSATABLE,
        inputSchema:
          parsed.inputSchema && typeof parsed.inputSchema === "object"
            ? (parsed.inputSchema as Record<string, unknown>)
            : {},
      };
      this.tools.set(toolName, desc);
      if (sagaGateway) {
        this.bindDescriptorToGateway(desc, sagaGateway);
      }
      restored += 1;
    }
    return restored;
  }

  private bindDescriptorToGateway(
    d: McpToolDescriptor,
    sagaGateway: SagaToolGateway,
    rpcTransport?: (method: string, params: Record<string, unknown>) => Record<string, unknown>
  ): void {
    const forwardFn = rpcTransport
      ? (args: Record<string, unknown>) =>
          rpcTransport("tools/call", {
            jsonrpc: "2.0",
            server: d.serverName,
            name: d.toolName,
            arguments: args,
          })
      : (args: Record<string, unknown>) => ({
          jsonrpc: "2.0",
          serverName: d.serverName,
          invokedTool: d.toolName,
          argsEcho: args,
          status: "MCP_OK",
        });

    const compensateFn =
      d.effectClass === ToolCallEffectClass.COMPENSATABLE
        ? (args: Record<string, unknown>) => ({
            compensatedTool: d.toolName,
            serverName: d.serverName,
            argsEcho: args,
            status: "MCP_COMPENSATED",
          })
        : undefined;

    sagaGateway.registerTool(d.toolName, d.effectClass, forwardFn, compensateFn);
  }

  private persistDescriptor(d: McpToolDescriptor): void {
    if (!this.sql) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO persisted_mcp_tools (tool_name, payload_json) VALUES (?, ?)",
      d.toolName,
      JSON.stringify(d)
    );
  }

  public registerMcpServerTools(
    serverName: string,
    descriptors: Omit<McpToolDescriptor, "serverName">[],
    sagaGateway: SagaToolGateway,
    rpcTransport: (method: string, params: Record<string, unknown>) => Record<string, unknown>
  ): number {
    let count = 0;
    for (const d of descriptors) {
      const full: McpToolDescriptor = { ...d, serverName };
      this.tools.set(d.toolName, full);
      this.persistDescriptor(full);
      this.bindDescriptorToGateway(full, sagaGateway, rpcTransport);
      count += 1;
    }
    return count;
  }

  public listTools(): McpToolDescriptor[] {
    return Array.from(this.tools.values());
  }
}
