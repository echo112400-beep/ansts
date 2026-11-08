/**
 * @fileoverview M12 TASK-43: Dynamic Model Context Protocol (MCP) Server Discovery & JSON-RPC 2.0 Tool Bridge.
 */
import { ToolCallEffectClass } from "../models.js";
import { SagaToolGateway } from "../saga_gateway.js";

export interface McpToolDescriptor {
  serverName: string;
  toolName: string;
  description: string;
  effectClass: ToolCallEffectClass;
  inputSchema: Record<string, unknown>;
}

export class McpDynamicToolRegistry {
  private readonly tools: Map<string, McpToolDescriptor> = new Map();

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
      sagaGateway.registerTool(d.toolName, d.effectClass, (args) => {
        return rpcTransport("tools/call", {
          jsonrpc: "2.0",
          server: serverName,
          name: d.toolName,
          arguments: args,
        });
      });
      count += 1;
    }
    return count;
  }

  public listTools(): McpToolDescriptor[] {
    return Array.from(this.tools.values());
  }
}
