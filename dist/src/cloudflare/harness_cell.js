"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AgentTaskHarnessCell = void 0;
const saga_gateway_js_1 = require("../saga_gateway.js");
class AgentTaskHarnessCell {
    ctx;
    r2Bucket;
    taskId;
    highestFencingToken = 0;
    highestTurnIndex = 0;
    constructor(ctx, r2Bucket, taskId = "task-default") {
        this.ctx = ctx;
        this.r2Bucket = r2Bucket;
        this.taskId = taskId;
        this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS harness_turns (turn_index INTEGER PRIMARY KEY, fencing_token INTEGER, tool_name TEXT, output_json TEXT, r2_key TEXT)");
        this.rehydrateWatermarksFromSql();
    }
    /**
     * [M54-1] Rehydrate `highestFencingToken` and `highestTurnIndex` from persisted `harness_turns` rows
     * on Cell cold-start or live migration so zombie workers holding stale fencing tokens cannot overwrite turns.
     */
    rehydrateWatermarksFromSql() {
        this.highestTurnIndex = 0;
        const rows = this.ctx.storage.sql
            .exec("SELECT * FROM harness_turns")
            .toArray();
        for (const r of rows) {
            const fToken = Number(r.fencing_token ?? 0);
            const tIdx = Number(r.turn_index ?? 0);
            if (fToken > this.highestFencingToken) {
                this.highestFencingToken = fToken;
            }
            if (tIdx > this.highestTurnIndex) {
                this.highestTurnIndex = tIdx;
            }
        }
        return {
            highestFencingToken: this.highestFencingToken,
            highestTurnIndex: this.highestTurnIndex,
        };
    }
    /**
     * [M61-4 & M69-5] Reconciles any committed R2 LTX turn snapshots (`cells/${taskId}/turn-*.ltx`) that were written
     * before an abrupt crash interrupted the subsequent SQLite `INSERT INTO harness_turns`, while rejecting any
     * foreign `taskId` payloads or non-positive turn/fencing metadata.
     */
    async reconcileBreakpointFromR2AndSql() {
        const prefix = `cells/${this.taskId}/`;
        const listed = await this.r2Bucket.list({ prefix });
        let reconciledTurnsCount = 0;
        for (const objInfo of listed.objects) {
            const body = await this.r2Bucket.get(objInfo.key);
            if (!body)
                continue;
            try {
                const parsed = JSON.parse(await body.text());
                if (parsed.taskId && parsed.taskId !== this.taskId) {
                    continue;
                }
                if (body.customMetadata?.taskId && body.customMetadata.taskId !== this.taskId) {
                    continue;
                }
                const tIdx = Number(parsed.turnIndex ?? body.customMetadata?.turnIndex ?? 0);
                const fTok = Number(parsed.fencingToken ?? body.customMetadata?.fencingToken ?? 0);
                if (!Number.isFinite(tIdx) || tIdx <= 0 || !Number.isFinite(fTok) || fTok <= 0)
                    continue;
                const existingSql = this.ctx.storage.sql
                    .exec("SELECT * FROM harness_turns WHERE turn_index = ?", tIdx)
                    .one();
                if (!existingSql || Number(existingSql.fencing_token ?? 0) < fTok) {
                    this.ctx.storage.sql.exec("INSERT OR REPLACE INTO harness_turns (turn_index, fencing_token, tool_name, output_json, r2_key) VALUES (?, ?, ?, ?, ?)", Math.floor(tIdx), Math.floor(fTok), String(parsed.toolName ?? "reconciled_r2_turn"), JSON.stringify(parsed.toolOutput ?? {}), objInfo.key);
                    reconciledTurnsCount += 1;
                }
            }
            catch {
                // ignore malformed object
            }
        }
        const watermarks = this.rehydrateWatermarksFromSql();
        return {
            reconciledTurnsCount,
            highestTurnIndex: watermarks.highestTurnIndex,
            highestFencingToken: watermarks.highestFencingToken,
        };
    }
    /**
     * [M69-5] Truncates and rolls back persisted turns at or after `fromTurnInclusive` (under a valid `fencingToken`)
     * so breakpoint rollbacks reset `highestTurnIndex` cleanly while advancing `highestFencingToken`.
     */
    truncateTurnsFrom(fromTurnInclusive, fencingToken) {
        if (!Number.isFinite(fencingToken) || fencingToken < this.highestFencingToken) {
            throw new Error(`Stale fencing token ${fencingToken} < highest ${this.highestFencingToken} in AgentTaskHarnessCell.truncateTurnsFrom`);
        }
        this.highestFencingToken = Math.floor(fencingToken);
        const rows = this.ctx.storage.sql
            .exec("SELECT * FROM harness_turns")
            .toArray();
        let deleted = 0;
        for (const r of rows) {
            const tIdx = Number(r.turn_index ?? 0);
            if (tIdx >= fromTurnInclusive) {
                this.ctx.storage.sql.exec("DELETE FROM harness_turns WHERE turn_index = ?", tIdx);
                deleted += 1;
            }
        }
        this.rehydrateWatermarksFromSql();
        return deleted;
    }
    getHighestFencingToken() {
        return this.highestFencingToken;
    }
    async executeAndCheckpointTurn(params) {
        if (!Number.isFinite(params.turnIndex) || params.turnIndex <= 0) {
            throw new Error(`Invalid turnIndex '${params.turnIndex}' in AgentTaskHarnessCell`);
        }
        if (!Number.isFinite(params.fencingToken) || params.fencingToken <= 0) {
            throw new Error(`Invalid fencingToken '${params.fencingToken}' in AgentTaskHarnessCell`);
        }
        if (params.fencingToken < this.highestFencingToken) {
            throw new Error(`Stale fencing token ${params.fencingToken} < highest ${this.highestFencingToken} in AgentTaskHarnessCell`);
        }
        // [M61-4] Check if this exact (turnIndex, fencingToken, toolName, canonical toolOutput) was already committed
        const existingRow = this.ctx.storage.sql
            .exec("SELECT * FROM harness_turns WHERE turn_index = ?", params.turnIndex)
            .one();
        if (existingRow &&
            Number(existingRow.fencing_token) === params.fencingToken &&
            String(existingRow.tool_name) === params.toolName) {
            const existingOut = JSON.parse(String(existingRow.output_json ?? "{}"));
            if (JSON.stringify(saga_gateway_js_1.SagaToolGateway.canonicalizeValue(existingOut)) ===
                JSON.stringify(saga_gateway_js_1.SagaToolGateway.canonicalizeValue(params.toolOutput))) {
                return {
                    turnIndex: params.turnIndex,
                    fencingToken: params.fencingToken,
                    toolName: params.toolName,
                    toolOutput: existingOut,
                    r2SnapshotKey: String(existingRow.r2_key),
                    deduplicated: true,
                };
            }
        }
        if ((params.enforceTurnMonotonicity ?? false) &&
            params.fencingToken === this.highestFencingToken &&
            params.turnIndex <= this.highestTurnIndex) {
            throw new Error(`Stale or duplicate turnIndex ${params.turnIndex} <= highestTurnIndex ${this.highestTurnIndex} under fencing token ${params.fencingToken}`);
        }
        this.highestFencingToken = params.fencingToken;
        if (params.turnIndex > this.highestTurnIndex) {
            this.highestTurnIndex = params.turnIndex;
        }
        const r2Key = `cells/${this.taskId}/turn-${params.turnIndex}-f${params.fencingToken}.ltx`;
        const payloadStr = JSON.stringify({
            taskId: this.taskId,
            turnIndex: params.turnIndex,
            fencingToken: params.fencingToken,
            toolName: params.toolName,
            toolOutput: params.toolOutput,
        });
        await this.r2Bucket.put(r2Key, payloadStr, {
            customMetadata: {
                taskId: this.taskId,
                turnIndex: String(params.turnIndex),
                fencingToken: String(params.fencingToken),
            },
        });
        this.ctx.storage.sql.exec("INSERT OR REPLACE INTO harness_turns (turn_index, fencing_token, tool_name, output_json, r2_key) VALUES (?, ?, ?, ?, ?)", params.turnIndex, params.fencingToken, params.toolName, JSON.stringify(params.toolOutput), r2Key);
        return {
            turnIndex: params.turnIndex,
            fencingToken: params.fencingToken,
            toolName: params.toolName,
            toolOutput: params.toolOutput,
            r2SnapshotKey: r2Key,
            deduplicated: false,
        };
    }
    /**
     * M16 TASK-81: Auto-Healing Container Execution across Cell Migration (`OperationInterruptedError` recovery).
     */
    async executeInCompanionContainerWithAutoHeal(action, onContainerRestartAndRehydrate) {
        try {
            const output = await action();
            return { output, recoveredFromMigrationInterrupt: false };
        }
        catch (err) {
            if (err.name === "OperationInterruptedError" || err.message.includes("OperationInterruptedError")) {
                const history = this.getTurnHistory();
                const latestTurn = history.length > 0 ? history[history.length - 1] : null;
                await onContainerRestartAndRehydrate(latestTurn);
                const retryOutput = await action();
                return { output: retryOutput, recoveredFromMigrationInterrupt: true };
            }
            throw err;
        }
    }
    getTurnHistory() {
        const rows = this.ctx.storage.sql
            .exec("SELECT * FROM harness_turns")
            .toArray();
        return rows
            .map((r) => ({
            turnIndex: Number(r.turn_index),
            fencingToken: Number(r.fencing_token),
            toolName: String(r.tool_name),
            toolOutput: JSON.parse(String(r.output_json ?? "{}")),
            r2SnapshotKey: String(r.r2_key),
        }))
            .sort((a, b) => a.turnIndex - b.turnIndex);
    }
    /**
     * [M64-1] Durable Object HTTP `fetch` entrypoint supporting `/execute-turn`, `/reconcile`, and `/history`
     * (with or without `/v1/harness` prefix).
     */
    async fetch(request) {
        const url = new URL(request.url);
        const normalizedPath = url.pathname.replace(/^\/v1\/harness(?=\/|$)/, "") || "/";
        if (request.method === "POST" && normalizedPath === "/execute-turn") {
            try {
                const body = (await request.json());
                const record = await this.executeAndCheckpointTurn({
                    turnIndex: Number(body.turnIndex ?? 1),
                    fencingToken: Number(body.fencingToken ?? 1),
                    toolName: String(body.toolName ?? "shell_exec"),
                    toolOutput: body.toolOutput ?? {},
                    enforceTurnMonotonicity: Boolean(body.enforceTurnMonotonicity ?? false),
                });
                return new Response(JSON.stringify(record), {
                    status: 200,
                    headers: { "content-type": "application/json" },
                });
            }
            catch (err) {
                return new Response(JSON.stringify({ error: err.message }), {
                    status: 409,
                    headers: { "content-type": "application/json" },
                });
            }
        }
        if (request.method === "POST" && normalizedPath === "/reconcile") {
            const reconciled = await this.reconcileBreakpointFromR2AndSql();
            return new Response(JSON.stringify(reconciled), {
                status: 200,
                headers: { "content-type": "application/json" },
            });
        }
        if (request.method === "GET" && (normalizedPath === "/history" || normalizedPath === "/")) {
            return new Response(JSON.stringify({
                taskId: this.taskId,
                highestTurnIndex: this.highestTurnIndex,
                highestFencingToken: this.highestFencingToken,
                turns: this.getTurnHistory(),
            }), {
                status: 200,
                headers: { "content-type": "application/json" },
            });
        }
        return new Response(JSON.stringify({ error: "NOT_FOUND" }), { status: 404 });
    }
}
exports.AgentTaskHarnessCell = AgentTaskHarnessCell;
