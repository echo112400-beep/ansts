"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.SagaToolGateway = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));
var _models = require("./models.js");
var _compensator = require("./saga/compensator.js");
var _staging = require("./saga/staging.js");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M3 TASK-11, M53-1 & M60-1/M60-2: Unified Idempotent Tool Proxy & Saga Transaction Gateway
 * with Recursive Canonical Nested-JSON Hashing, SQLite-Backed Durable Execution Journal & Post-Rollback Invalidation.
 */

class SagaToolGateway {
  executionJournal = new Map();
  toolRegistry = new Map();
  compensator;
  stagingGate;
  sql;

  constructor(
  compensator = new _compensator.SagaCompensatorEngine(),
  stagingGate = new _staging.StagingCommitAndApprovalGate(),
  sql)
  {
    this.compensator = compensator;
    this.stagingGate = stagingGate;
    if (sql) {
      this.bindSqlStorage(sql);
    }
    this.compensator.setOnStepCompensated((step) => {
      if (step.idempotencyKey) {
        this.executionJournal.delete(step.idempotencyKey);
        if (this.sql) {
          this.sql.exec(
          "DELETE FROM saga_execution_journal WHERE idempotency_key = ?",
          step.idempotencyKey);

        }
      }
      this.invalidateTaskTurnsFrom(step.taskId, step.turnIndex);
    });
  }

  /**
   * [M60-1] Attaches a SQLite storage backend to persist `executionJournal` across process restarts
   * and immediately rehydrates any previously committed tool execution logs.
   */
  bindSqlStorage(sql) {
    this.sql = sql;
    this.sql.exec(
    "CREATE TABLE IF NOT EXISTS saga_execution_journal (idempotency_key TEXT PRIMARY KEY, task_id TEXT, turn_index INTEGER, tool_name TEXT, effect_class TEXT, status TEXT, result_json TEXT, compensation_tool TEXT, executed_at REAL)");

    const rows = this.sql.
    exec("SELECT * FROM saga_execution_journal").
    toArray();
    let restored = 0;
    for (const r of rows) {
      const entry = {
        idempotency_key: String(r.idempotency_key),
        task_id: String(r.task_id),
        turn_index: Number(r.turn_index ?? 0),
        tool_name: String(r.tool_name),
        effect_class: r.effect_class ?? _models.ToolCallEffectClass.IDEMPOTENT_WRITE,
        status: r.status ?? "SUCCESS",
        result_payload: JSON.parse(String(r.result_json ?? "{}")),
        compensation_tool: r.compensation_tool ? String(r.compensation_tool) : undefined,
        executed_at: Number(r.executed_at ?? Date.now() / 1000)
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
        this.sql.exec(
        "INSERT OR REPLACE INTO saga_execution_journal (idempotency_key, task_id, turn_index, tool_name, effect_class, status, result_json, compensation_tool, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        entry.idempotency_key,
        entry.task_id,
        entry.turn_index,
        entry.tool_name,
        entry.effect_class,
        entry.status,
        JSON.stringify(entry.result_payload),
        entry.compensation_tool ?? "",
        entry.executed_at);

      }
      imported += 1;
    }
    return imported;
  }

  registerTool(
  toolName,
  effectClass,
  handler,
  compensateHandler)
  {
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

  static generateIdempotencyKey(
  taskId,
  turnIndex,
  toolName,
  canonicalArgs)
  {
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
  executeTool(params)







  {
    this.registerTool(
    params.toolName,
    params.effectClass,
    () => params.actionFn(),
    params.compensationFn ?
    () => {
      params.compensationFn();
      return { compensated: true };
    } :
    undefined);

    const [result] = this.invokeTool(params.taskId, params.turnIndex, params.toolName, params.args);
    return result;
  }

  /**
   * [M53-1] Rolls back all Saga steps for `taskId` at or after `upToTurnInclusive` in LIFO order,
   * isolates per-step compensation errors, and invalidates corresponding `executionJournal` idempotency keys.
   */
  rollbackAndInvalidateTask(
  taskId,
  upToTurnInclusive = 0)




  {
    const invalidatedIdemKeys = [];
    for (const [k, log] of this.executionJournal.entries()) {
      if (log.task_id === taskId && log.turn_index >= upToTurnInclusive) {
        invalidatedIdemKeys.push(k);
      }
    }
    const rolledBackSteps = this.compensator.rollbackTask(taskId, upToTurnInclusive);
    this.invalidateTaskTurnsFrom(taskId, upToTurnInclusive);
    const compensationErrors = rolledBackSteps.
    filter((s) => Boolean(s.compensationError)).
    map((s) => ({
      stepId: s.stepId,
      turnIndex: s.turnIndex,
      error: s.compensationError
    }));
    return {
      rolledBackSteps,
      invalidatedIdemKeys,
      compensationErrors
    };
  }

  invokeTool(
  taskId,
  turnIndex,
  toolName,
  args)
  {
    const entry = this.toolRegistry.get(toolName);
    if (!entry) {
      throw new Error(`Tool '${toolName}' is not registered in SagaToolGateway`);
    }
    const [effectClass, handler, compensateHandler] = entry;
    const idemKey = SagaToolGateway.generateIdempotencyKey(taskId, turnIndex, toolName, args);

    if (effectClass !== _models.ToolCallEffectClass.READ_ONLY) {
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
      executed_at: Date.now() / 1000
    };
    this.executionJournal.set(idemKey, logEntry);
    if (this.sql) {
      this.sql.exec(
      "INSERT OR REPLACE INTO saga_execution_journal (idempotency_key, task_id, turn_index, tool_name, effect_class, status, result_json, compensation_tool, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      logEntry.idempotency_key,
      logEntry.task_id,
      logEntry.turn_index,
      logEntry.tool_name,
      logEntry.effect_class,
      logEntry.status,
      JSON.stringify(logEntry.result_payload),
      logEntry.compensation_tool ?? "",
      logEntry.executed_at);

    }

    if (compensateHandler) {
      this.compensator.recordForwardStep({
        stepId: idemKey.slice(0, 16),
        taskId,
        turnIndex,
        toolName,
        idempotencyKey: idemKey,
        forwardPayload: { args, result },
        compensateHandler
      });
    }

    return [result, false];
  }
}exports.SagaToolGateway = SagaToolGateway;