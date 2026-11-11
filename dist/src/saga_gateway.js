"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.SagaToolGateway = void 0;
/**
 * @fileoverview M3 TASK-11, M53-1, M60-1/M60-2, M64-3 & M68-5: Unified Idempotent Tool Proxy & Saga Transaction Gateway
 * with Recursive Canonical Nested-JSON Hashing, SQLite-Backed Durable Execution Journal, Automatic Sub-Component
 * SQLite Binding (`compensator` & `stagingGate`), Dynamic Post-Restart Compensation Resolution,
 * Unconditional `COMPENSATABLE` Step Recording & Post-Rollback Invalidation.
 */
const crypto = __importStar(require("node:crypto"));
const models_js_1 = require("./models.js");
const compensator_js_1 = require("./saga/compensator.js");
const staging_js_1 = require("./saga/staging.js");
class SagaToolGateway {
    executionJournal = new Map();
    toolRegistry = new Map();
    compensator;
    stagingGate;
    sql;
    constructor(compensator = new compensator_js_1.SagaCompensatorEngine(), stagingGate = new staging_js_1.StagingCommitAndApprovalGate(), sql) {
        this.compensator = compensator;
        this.stagingGate = stagingGate;
        this.compensator.setToolCompensatorResolver((toolName) => {
            const entry = this.toolRegistry.get(toolName);
            if (!entry || !entry[2])
                return undefined;
            const compFn = entry[2];
            return (forwardPayload) => {
                const args = forwardPayload.args && typeof forwardPayload.args === "object"
                    ? forwardPayload.args
                    : forwardPayload;
                return compFn(args);
            };
        });
        if (sql) {
            this.bindSqlStorage(sql);
        }
        this.compensator.setOnStepCompensated((step) => {
            if (step.idempotencyKey) {
                this.executionJournal.delete(step.idempotencyKey);
                if (this.sql) {
                    this.sql.exec("DELETE FROM saga_execution_journal WHERE idempotency_key = ?", step.idempotencyKey);
                }
            }
            this.invalidateTaskTurnsFrom(step.taskId, step.turnIndex);
        });
    }
    /**
     * [M60-1 & M64-3] Attaches a SQLite storage backend to persist `executionJournal`, `compensator` (`persisted_saga_steps`),
     * and `stagingGate` (`persisted_approval_tickets`, `persisted_staged_effects`) across process restarts.
     */
    bindSqlStorage(sql) {
        this.sql = sql;
        this.compensator.bindSqlStorage(sql);
        this.stagingGate.bindSqlStorage(sql);
        this.sql.exec("CREATE TABLE IF NOT EXISTS saga_execution_journal (idempotency_key TEXT PRIMARY KEY, task_id TEXT, turn_index INTEGER, tool_name TEXT, effect_class TEXT, status TEXT, result_json TEXT, compensation_tool TEXT, executed_at REAL)");
        const rows = this.sql
            .exec("SELECT * FROM saga_execution_journal")
            .toArray();
        let restored = 0;
        for (const r of rows) {
            const entry = {
                idempotency_key: String(r.idempotency_key),
                task_id: String(r.task_id),
                turn_index: Number(r.turn_index ?? 0),
                tool_name: String(r.tool_name),
                effect_class: r.effect_class ?? models_js_1.ToolCallEffectClass.IDEMPOTENT_WRITE,
                status: r.status ?? "SUCCESS",
                result_payload: JSON.parse(String(r.result_json ?? "{}")),
                compensation_tool: r.compensation_tool ? String(r.compensation_tool) : undefined,
                executed_at: Number(r.executed_at ?? Date.now() / 1000),
            };
            this.executionJournal.set(entry.idempotency_key, entry);
            restored += 1;
        }
        return restored;
    }
    exportJournalRecords() {
        return Array.from(this.executionJournal.values());
    }
    importJournalRecords(records) {
        let imported = 0;
        for (const entry of records) {
            this.executionJournal.set(entry.idempotency_key, entry);
            if (this.sql) {
                this.sql.exec("INSERT OR REPLACE INTO saga_execution_journal (idempotency_key, task_id, turn_index, tool_name, effect_class, status, result_json, compensation_tool, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", entry.idempotency_key, entry.task_id, entry.turn_index, entry.tool_name, entry.effect_class, entry.status, JSON.stringify(entry.result_payload), entry.compensation_tool ?? "", entry.executed_at);
            }
            imported += 1;
        }
        return imported;
    }
    registerTool(toolName, effectClass, handler, compensateHandler) {
        this.toolRegistry.set(toolName, [effectClass, handler, compensateHandler]);
    }
    /**
     * [M60-2] Recursively canonicalizes arbitrary JSON values (sorting object keys at all nesting depths
     * while preserving array ordering) so semantically identical nested tool arguments always yield the same hash.
     */
    static canonicalizeValue(val) {
        if (Array.isArray(val)) {
            return val.map((item) => SagaToolGateway.canonicalizeValue(item));
        }
        if (val !== null && typeof val === "object") {
            const record = val;
            const sortedKeys = Object.keys(record).sort();
            const sortedObj = {};
            for (const k of sortedKeys) {
                if (record[k] !== undefined) {
                    sortedObj[k] = SagaToolGateway.canonicalizeValue(record[k]);
                }
            }
            return sortedObj;
        }
        return val;
    }
    static generateIdempotencyKey(taskId, turnIndex, toolName, canonicalArgs) {
        const normalized = SagaToolGateway.canonicalizeValue(canonicalArgs);
        const payload = `${taskId}:${turnIndex}:${toolName}:${JSON.stringify(normalized)}`;
        return crypto.createHash("sha256").update(payload, "utf-8").digest("hex");
    }
    /**
     * [M53-1 & M60-1] Purges stale idempotency journal records (in memory and SQLite) for `taskId`
     * at or after `fromTurnInclusive` when a task is rolled back, breakpoint-recovered, or time-travel forked.
     */
    invalidateTaskTurnsFrom(taskId, fromTurnInclusive = 0) {
        let invalidated = 0;
        for (const [k, log] of this.executionJournal.entries()) {
            if (log.task_id === taskId && log.turn_index >= fromTurnInclusive) {
                this.executionJournal.delete(k);
                if (this.sql) {
                    this.sql.exec("DELETE FROM saga_execution_journal WHERE idempotency_key = ?", k);
                }
                invalidated += 1;
            }
        }
        return invalidated;
    }
    /**
     * [M53-1] Inline tool execution helper that registers or invokes a tool with idempotency caching and Saga registration.
     */
    executeTool(params) {
        this.registerTool(params.toolName, params.effectClass, () => params.actionFn(), params.compensationFn
            ? () => {
                params.compensationFn();
                return { compensated: true };
            }
            : undefined);
        const [result] = this.invokeTool(params.taskId, params.turnIndex, params.toolName, params.args);
        return result;
    }
    /**
     * [M53-1] Rolls back all Saga steps for `taskId` at or after `upToTurnInclusive` in LIFO order,
     * isolates per-step compensation errors, and invalidates corresponding `executionJournal` idempotency keys.
     */
    rollbackAndInvalidateTask(taskId, upToTurnInclusive = 0) {
        const invalidatedIdemKeys = [];
        for (const [k, log] of this.executionJournal.entries()) {
            if (log.task_id === taskId && log.turn_index >= upToTurnInclusive) {
                invalidatedIdemKeys.push(k);
            }
        }
        const rolledBackSteps = this.compensator.rollbackTask(taskId, upToTurnInclusive);
        this.invalidateTaskTurnsFrom(taskId, upToTurnInclusive);
        const compensationErrors = rolledBackSteps
            .filter((s) => Boolean(s.compensationError))
            .map((s) => ({
            stepId: s.stepId,
            turnIndex: s.turnIndex,
            error: s.compensationError,
        }));
        return {
            rolledBackSteps,
            invalidatedIdemKeys,
            compensationErrors,
        };
    }
    invokeTool(taskId, turnIndex, toolName, args) {
        const entry = this.toolRegistry.get(toolName);
        if (!entry) {
            throw new Error(`Tool '${toolName}' is not registered in SagaToolGateway`);
        }
        const [effectClass, handler, compensateHandler] = entry;
        const idemKey = SagaToolGateway.generateIdempotencyKey(taskId, turnIndex, toolName, args);
        if (effectClass !== models_js_1.ToolCallEffectClass.READ_ONLY) {
            const existing = this.executionJournal.get(idemKey);
            if (existing && existing.status === "SUCCESS") {
                return [existing.result_payload, true];
            }
        }
        const result = handler(args);
        const logEntry = {
            idempotency_key: idemKey,
            task_id: taskId,
            turn_index: turnIndex,
            tool_name: toolName,
            effect_class: effectClass,
            status: "SUCCESS",
            result_payload: result,
            compensation_tool: compensateHandler ? `${toolName}_compensate` : undefined,
            executed_at: Date.now() / 1000,
        };
        this.executionJournal.set(idemKey, logEntry);
        if (this.sql) {
            this.sql.exec("INSERT OR REPLACE INTO saga_execution_journal (idempotency_key, task_id, turn_index, tool_name, effect_class, status, result_json, compensation_tool, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", logEntry.idempotency_key, logEntry.task_id, logEntry.turn_index, logEntry.tool_name, logEntry.effect_class, logEntry.status, JSON.stringify(logEntry.result_payload), logEntry.compensation_tool ?? "", logEntry.executed_at);
        }
        if (compensateHandler ||
            effectClass === models_js_1.ToolCallEffectClass.COMPENSATABLE ||
            effectClass === models_js_1.ToolCallEffectClass.COMPENSATABLE_WRITE) {
            this.compensator.recordForwardStep({
                stepId: idemKey.slice(0, 16),
                taskId,
                turnIndex,
                toolName,
                idempotencyKey: idemKey,
                forwardPayload: { args, result },
                compensateHandler,
            });
        }
        return [result, false];
    }
}
exports.SagaToolGateway = SagaToolGateway;
