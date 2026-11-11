"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.McpDynamicToolRegistry = void 0;
/**
 * @fileoverview M12 TASK-43 & M64-3: Dynamic Model Context Protocol (MCP) Server Discovery & JSON-RPC 2.0 Tool Bridge
 * with SQLite Persistence (`persisted_mcp_tools`) & Cold-Start `SagaToolGateway` Re-Registration.
 */
const models_js_1 = require("../models.js");
class McpDynamicToolRegistry {
    tools = new Map();
    sql;
    constructor(sql, sagaGateway) {
        if (sql) {
            this.bindSqlStorage(sql, sagaGateway);
        }
    }
    /**
     * [M64-3] Binds SQLite storage (`persisted_mcp_tools`) so dynamically registered MCP tools survive restarts
     * and can be automatically re-bound to `SagaToolGateway`.
     */
    bindSqlStorage(sql, sagaGateway) {
        this.sql = sql;
        this.sql.exec("CREATE TABLE IF NOT EXISTS persisted_mcp_tools (tool_name TEXT PRIMARY KEY, payload_json TEXT)");
        const rows = this.sql
            .exec("SELECT * FROM persisted_mcp_tools")
            .toArray();
        let restored = 0;
        for (const r of rows) {
            const parsed = JSON.parse(String(r.payload_json ?? "{}"));
            const toolName = String(r.tool_name ?? parsed.toolName ?? parsed.tool_name ?? "");
            if (!toolName)
                continue;
            const desc = {
                serverName: String(parsed.serverName ?? parsed.server_name ?? "mcp-default"),
                toolName,
                description: String(parsed.description ?? "MCP dynamic tool"),
                effectClass: parsed.effectClass ??
                    parsed.effect_class ??
                    models_js_1.ToolCallEffectClass.COMPENSATABLE,
                inputSchema: parsed.inputSchema && typeof parsed.inputSchema === "object"
                    ? parsed.inputSchema
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
    bindDescriptorToGateway(d, sagaGateway, rpcTransport) {
        const forwardFn = rpcTransport
            ? (args) => rpcTransport("tools/call", {
                jsonrpc: "2.0",
                server: d.serverName,
                name: d.toolName,
                arguments: args,
            })
            : (args) => ({
                jsonrpc: "2.0",
                serverName: d.serverName,
                invokedTool: d.toolName,
                argsEcho: args,
                status: "MCP_OK",
            });
        const compensateFn = d.effectClass === models_js_1.ToolCallEffectClass.COMPENSATABLE
            ? (args) => ({
                compensatedTool: d.toolName,
                serverName: d.serverName,
                argsEcho: args,
                status: "MCP_COMPENSATED",
            })
            : undefined;
        sagaGateway.registerTool(d.toolName, d.effectClass, forwardFn, compensateFn);
    }
    persistDescriptor(d) {
        if (!this.sql)
            return;
        this.sql.exec("INSERT OR REPLACE INTO persisted_mcp_tools (tool_name, payload_json) VALUES (?, ?)", d.toolName, JSON.stringify(d));
    }
    registerMcpServerTools(serverName, descriptors, sagaGateway, rpcTransport) {
        let count = 0;
        for (const d of descriptors) {
            const full = { ...d, serverName };
            this.tools.set(d.toolName, full);
            this.persistDescriptor(full);
            this.bindDescriptorToGateway(full, sagaGateway, rpcTransport);
            count += 1;
        }
        return count;
    }
    listTools() {
        return Array.from(this.tools.values());
    }
}
exports.McpDynamicToolRegistry = McpDynamicToolRegistry;
