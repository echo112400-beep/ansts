"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.McpDynamicToolRegistry = void 0; /**
 * @fileoverview M12 TASK-43: Dynamic Model Context Protocol (MCP) Server Discovery & JSON-RPC 2.0 Tool Bridge.
 */











class McpDynamicToolRegistry {
  tools = new Map();

  registerMcpServerTools(
  serverName,
  descriptors,
  sagaGateway,
  rpcTransport)
  {
    let count = 0;
    for (const d of descriptors) {
      const full = { ...d, serverName };
      this.tools.set(d.toolName, full);
      sagaGateway.registerTool(d.toolName, d.effectClass, (args) => {
        return rpcTransport("tools/call", {
          jsonrpc: "2.0",
          server: serverName,
          name: d.toolName,
          arguments: args
        });
      });
      count += 1;
    }
    return count;
  }

  listTools() {
    return Array.from(this.tools.values());
  }
}exports.McpDynamicToolRegistry = McpDynamicToolRegistry;