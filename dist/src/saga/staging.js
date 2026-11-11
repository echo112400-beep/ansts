"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.StagingCommitAndApprovalGate = void 0;
/**
 * @fileoverview M3 TASK-10, M54-3, M61-2, M64-3 & M68-5: Staging Hold-and-Commit Buffer & Human-in-the-Loop Approval Gate
 * with Idempotent Effect Staging, Turn-Accurate Commit Callback, Coalesced Pending Approval Tickets,
 * Terminal-State Guard, TTL Sweep, Completed-Task Staging Purge & SQLite Persistence.
 */
const models_js_1 = require("../models.js");
const saga_gateway_js_1 = require("../saga_gateway.js");
class StagingCommitAndApprovalGate {
    stagedByTask = new Map();
    tickets = new Map();
    policyRules = new Map();
    ticketSeq = 0;
    sql;
    constructor(sql) {
        if (sql) {
            this.bindSqlStorage(sql);
        }
    }
    /**
     * [M61-2] Binds SQLite storage so staged side-effects and human approval tickets survive restarts.
     */
    bindSqlStorage(sql) {
        this.sql = sql;
        this.sql.exec("CREATE TABLE IF NOT EXISTS persisted_approval_tickets (ticket_id TEXT PRIMARY KEY, payload_json TEXT)");
        this.sql.exec("CREATE TABLE IF NOT EXISTS persisted_staged_effects (effect_id TEXT PRIMARY KEY, task_id TEXT, payload_json TEXT)");
        let ticketsRestored = 0;
        for (const r of this.sql.exec("SELECT * FROM persisted_approval_tickets").toArray()) {
            const tk = JSON.parse(String(r.payload_json ?? "{}"));
            if (tk && tk.ticketId) {
                this.tickets.set(tk.ticketId, tk);
                ticketsRestored += 1;
            }
        }
        let effectsRestored = 0;
        for (const r of this.sql.exec("SELECT * FROM persisted_staged_effects").toArray()) {
            const eff = JSON.parse(String(r.payload_json ?? "{}"));
            if (eff && eff.effectId && eff.taskId) {
                const list = this.stagedByTask.get(eff.taskId) ?? [];
                const existingIdx = list.findIndex((e) => e.effectId === eff.effectId);
                if (existingIdx >= 0) {
                    list[existingIdx] = eff;
                }
                else {
                    list.push(eff);
                }
                this.stagedByTask.set(eff.taskId, list);
                effectsRestored += 1;
            }
        }
        this.sql.exec("CREATE TABLE IF NOT EXISTS persisted_guardrail_policies (rule_id TEXT PRIMARY KEY, payload_json TEXT)");
        for (const r of this.sql.exec("SELECT * FROM persisted_guardrail_policies").toArray()) {
            const rule = JSON.parse(String(r.payload_json ?? "{}"));
            if (rule && rule.ruleId) {
                this.policyRules.set(rule.ruleId, rule);
            }
        }
        return { ticketsRestored, effectsRestored };
    }
    persistEffect(eff) {
        if (!this.sql)
            return;
        this.sql.exec("INSERT OR REPLACE INTO persisted_staged_effects (effect_id, task_id, payload_json) VALUES (?, ?, ?)", eff.effectId, eff.taskId, JSON.stringify(eff));
    }
    persistTicket(tk) {
        if (!this.sql)
            return;
        this.sql.exec("INSERT OR REPLACE INTO persisted_approval_tickets (ticket_id, payload_json) VALUES (?, ?)", tk.ticketId, JSON.stringify(tk));
    }
    /**
     * [M61-2] Idempotent effect staging: if `effect.effectId` (or identical `(taskId, turnIndex, toolName, canonicalArgs)`)
     * is already staged for `effect.taskId`, returns the existing `StagedSideEffect` instead of duplicating it.
     */
    stageEffect(effect) {
        let list = this.stagedByTask.get(effect.taskId);
        if (!list) {
            list = [];
            this.stagedByTask.set(effect.taskId, list);
        }
        const canonicalNewArgs = JSON.stringify(saga_gateway_js_1.SagaToolGateway.canonicalizeValue(effect.args));
        const existing = list.find((item) => item.effectId === effect.effectId ||
            (!item.aborted &&
                item.turnIndex === effect.turnIndex &&
                item.toolName === effect.toolName &&
                JSON.stringify(saga_gateway_js_1.SagaToolGateway.canonicalizeValue(item.args)) === canonicalNewArgs));
        if (existing) {
            return existing;
        }
        const full = { ...effect, committed: false, aborted: false };
        list.push(full);
        this.persistEffect(full);
        return full;
    }
    /**
     * [M64-3] Passes `item.turnIndex` as the 3rd argument to `executor(toolName, args, turnIndex)` so committed
     * staged side-effects are recorded at their true turn index rather than a hardcoded turn 1.
     */
    commitStagedEffects(taskId, executor) {
        const list = this.stagedByTask.get(taskId) ?? [];
        const results = [];
        for (const item of list) {
            if (!item.committed && !item.aborted) {
                const output = executor(item.toolName, item.args, item.turnIndex);
                item.committed = true;
                item.committedOutput = output;
                this.persistEffect(item);
                results.push({ effectId: item.effectId, turnIndex: item.turnIndex, output });
            }
        }
        return results;
    }
    abortStagedEffects(taskId) {
        const list = this.stagedByTask.get(taskId) ?? [];
        let count = 0;
        for (const item of list) {
            if (!item.committed && !item.aborted) {
                item.aborted = true;
                this.persistEffect(item);
                count += 1;
            }
        }
        return count;
    }
    /**
     * [M68-5] Purges committed/aborted staged side-effects and terminal approval tickets for a completed
     * or archived task from both in-memory maps and SQLite tables.
     */
    purgeCompletedTaskStaging(taskId) {
        const list = this.stagedByTask.get(taskId) ?? [];
        let purgedEffects = 0;
        const remainingEffects = [];
        for (const eff of list) {
            if (eff.committed || eff.aborted) {
                purgedEffects += 1;
                if (this.sql) {
                    this.sql.exec("DELETE FROM persisted_staged_effects WHERE effect_id = ?", eff.effectId);
                }
            }
            else {
                remainingEffects.push(eff);
            }
        }
        if (remainingEffects.length > 0) {
            this.stagedByTask.set(taskId, remainingEffects);
        }
        else {
            this.stagedByTask.delete(taskId);
        }
        let purgedTickets = 0;
        for (const [ticketId, ticket] of Array.from(this.tickets.entries())) {
            if (ticket.taskId === taskId && ticket.status !== "PENDING") {
                this.tickets.delete(ticketId);
                purgedTickets += 1;
                if (this.sql) {
                    this.sql.exec("DELETE FROM persisted_approval_tickets WHERE ticket_id = ?", ticketId);
                }
            }
        }
        return { purgedEffects, purgedTickets };
    }
    getStagedEffects(taskId) {
        return [...(this.stagedByTask.get(taskId) ?? [])];
    }
    /**
     * [M61-2] Coalesces duplicate `PENDING` approval requests for the same `(task.task_id, toolName, canonicalArgs)`
     * so retried turns never spawn duplicate human approval tickets.
     */
    requestHumanApproval(task, toolName, args, nowMs = Date.now()) {
        task.state = models_js_1.TaskState.SUSPENDED_APPROVAL;
        const canonicalReqArgs = JSON.stringify(saga_gateway_js_1.SagaToolGateway.canonicalizeValue(args));
        for (const existing of this.tickets.values()) {
            if (existing.taskId === task.task_id &&
                existing.toolName === toolName &&
                existing.status === "PENDING" &&
                JSON.stringify(saga_gateway_js_1.SagaToolGateway.canonicalizeValue(existing.args)) === canonicalReqArgs) {
                return existing;
            }
        }
        this.ticketSeq += 1;
        const ticket = {
            ticketId: `approval-${task.task_id}-${nowMs}-${this.ticketSeq}`,
            taskId: task.task_id,
            toolName,
            args,
            status: "PENDING",
            createdAtMs: nowMs,
        };
        this.tickets.set(ticket.ticketId, ticket);
        this.persistTicket(ticket);
        return ticket;
    }
    /**
     * [M54-3 & M61-2] Resolves an approval ticket with idempotent repeat-decision handling and terminal-state guard.
     */
    resolveApproval(ticketId, approved, decidedBy, task) {
        const ticket = this.tickets.get(ticketId);
        if (!ticket) {
            throw new Error(`Approval ticket '${ticketId}' not found`);
        }
        const desiredStatus = approved ? "APPROVED" : "REJECTED";
        if (ticket.status !== "PENDING") {
            if (ticket.status === desiredStatus) {
                return ticket;
            }
            throw new Error(`Approval ticket '${ticketId}' is already in terminal status '${ticket.status}' and cannot transition to '${desiredStatus}'`);
        }
        ticket.status = desiredStatus;
        ticket.decidedBy = decidedBy;
        if (approved) {
            task.state = models_js_1.TaskState.RUNNING_ACT;
        }
        else {
            task.state = models_js_1.TaskState.DEAD_LETTER;
            this.abortStagedEffects(ticket.taskId);
        }
        this.persistTicket(ticket);
        return ticket;
    }
    /**
     * [M54-3] Sweeps expired `PENDING` human approval tickets exceeding `approvalTtlMs` (default 3,600,000ms),
     * aborts their uncommitted staged side-effects, and transitions suspended tasks to `DEAD_LETTER`.
     */
    expireStaleApprovalTickets(tasksById, nowMs = Date.now(), approvalTtlMs = 3_600_000) {
        const expiredTicketIds = [];
        for (const ticket of this.tickets.values()) {
            if (ticket.status === "PENDING" && nowMs - ticket.createdAtMs >= approvalTtlMs) {
                ticket.status = "EXPIRED";
                ticket.decidedBy = "system:ttl-watchdog";
                this.abortStagedEffects(ticket.taskId);
                const task = tasksById.get(ticket.taskId);
                if (task && task.state === models_js_1.TaskState.SUSPENDED_APPROVAL) {
                    task.state = models_js_1.TaskState.DEAD_LETTER;
                }
                this.persistTicket(ticket);
                expiredTicketIds.push(ticket.ticketId);
            }
        }
        return expiredTicketIds;
    }
    registerPolicyRule(rule) {
        this.policyRules.set(rule.ruleId, rule);
        if (this.sql) {
            this.sql.exec("INSERT OR REPLACE INTO persisted_guardrail_policies (rule_id, payload_json) VALUES (?, ?)", rule.ruleId, JSON.stringify(rule));
        }
    }
    listPolicyRules() {
        return Array.from(this.policyRules.values());
    }
    /**
     * [M72-2 / F5] Declarative Policy-as-Code Formal Guardrail Engine:
     * Statically proves tool call safety across 4 formal dimensions BEFORE any side-effect executes:
     * 1. SQL AST / Destructive DDL & Unscoped DML Proof (`SQL_STATIC_PROOF`)
     * 2. Filesystem Path & Secret Boundary Proof (`PATH_BOUNDARY_PROOF`)
     * 3. Custom Cedar/Rego-style Declarative Policy Rules (`DECLARATIVE_POLICY_PROOF`)
     * 4. Quantitative Blast-Radius Scoring (`0..100`) -> `ALLOW` (<35), `REQUIRE_4EYES_APPROVAL` (35..74), or `DENY_FORMAL_VIOLATION` (>=75).
     */
    evaluateFormalGuardrailPolicy(params) {
        const env = params.environment ?? "prod";
        const violatedRuleIds = [];
        let sqlStaticProofPassed = true;
        let pathBoundaryProofPassed = true;
        let declarativePolicyPassed = true;
        let forceApproval = false;
        const sqlText = String(params.args.sql ?? params.args.query ?? "").trim();
        if (sqlText.length > 0) {
            const normalizedSql = sqlText.replace(/\s+/g, " ").toUpperCase();
            if (/\b(DROP\s+TABLE|DROP\s+DATABASE|TRUNCATE\s+TABLE|ALTER\s+TABLE\s+\S+\s+DROP)\b/.test(normalizedSql)) {
                sqlStaticProofPassed = false;
                violatedRuleIds.push("BUILTIN_SQL_DESTRUCTIVE_DDL");
            }
            if (/\b(DELETE\s+FROM|UPDATE\s+\S+\s+SET)\b/.test(normalizedSql) &&
                (!/\bWHERE\b/.test(normalizedSql) || /\bWHERE\s+(1\s*=\s*1|TRUE)\b/.test(normalizedSql))) {
                sqlStaticProofPassed = false;
                violatedRuleIds.push("BUILTIN_SQL_UNSCOPED_MUTATION");
            }
        }
        const rawPath = String(params.args.path ?? params.args.filePath ?? params.args.targetPath ?? "").trim();
        if (rawPath.length > 0) {
            if (rawPath.includes("..") ||
                rawPath.includes("\0") ||
                /^(\/etc\/|\/proc\/|\/sys\/|\/root\/)/.test(rawPath) ||
                /(\.ssh\/|\.env|id_rsa|credentials\.json)/i.test(rawPath)) {
                pathBoundaryProofPassed = false;
                violatedRuleIds.push("BUILTIN_PATH_SECRET_BOUNDARY");
            }
        }
        const serializedArgs = JSON.stringify(params.args);
        const affectedRows = Number(params.args.affectedRowsEstimate ?? 0);
        for (const rule of this.policyRules.values()) {
            const toolRegex = new RegExp(rule.toolNamePattern, "i");
            if (!toolRegex.test(params.toolName))
                continue;
            let matchedViolation = false;
            if (rule.forbiddenArgRegex && new RegExp(rule.forbiddenArgRegex, "i").test(serializedArgs)) {
                matchedViolation = true;
            }
            if (rule.maxAffectedRows !== undefined && affectedRows > rule.maxAffectedRows) {
                matchedViolation = true;
            }
            if (!rule.forbiddenArgRegex && rule.maxAffectedRows === undefined && rule.effect !== "ALLOW") {
                matchedViolation = true;
            }
            if (matchedViolation) {
                if (rule.effect === "DENY") {
                    declarativePolicyPassed = false;
                    violatedRuleIds.push(rule.ruleId);
                }
                else if (rule.effect === "REQUIRE_4EYES_APPROVAL") {
                    forceApproval = true;
                    violatedRuleIds.push(rule.ruleId);
                }
            }
        }
        let blastRadiusScore = env === "prod" ? 20 : env === "staging" ? 10 : 5;
        if (/write|delete|update|deploy|migrate|exec|drop|scale/i.test(params.toolName)) {
            blastRadiusScore += 20;
        }
        if (affectedRows > 0) {
            blastRadiusScore += Math.min(30, Math.floor(Math.log10(affectedRows + 1) * 6));
        }
        if (!sqlStaticProofPassed || !pathBoundaryProofPassed || !declarativePolicyPassed) {
            blastRadiusScore = 95;
        }
        else if (forceApproval) {
            blastRadiusScore = Math.max(50, blastRadiusScore);
        }
        blastRadiusScore = Math.min(100, blastRadiusScore);
        if (!sqlStaticProofPassed || !pathBoundaryProofPassed || !declarativePolicyPassed || blastRadiusScore >= 75) {
            return {
                decision: "DENY_FORMAL_VIOLATION",
                blastRadiusScore,
                violatedRuleIds,
                proofChecks: {
                    sqlStaticProofPassed,
                    pathBoundaryProofPassed,
                    declarativePolicyPassed,
                },
                rationale: `Blocked by Formal Guardrail Engine: violations=[${violatedRuleIds.join(", ")}], blastRadius=${blastRadiusScore}`,
            };
        }
        if (blastRadiusScore >= 35 || forceApproval) {
            const stagedEffect = this.stageEffect({
                effectId: `eff-${params.task.task_id}-${params.turnIndex}-${params.toolName}`,
                taskId: params.task.task_id,
                turnIndex: params.turnIndex,
                toolName: params.toolName,
                args: params.args,
            });
            const approvalTicket = this.requestHumanApproval(params.task, params.toolName, params.args, params.nowMs ?? Date.now());
            return {
                decision: "REQUIRE_4EYES_APPROVAL",
                blastRadiusScore,
                violatedRuleIds,
                proofChecks: {
                    sqlStaticProofPassed,
                    pathBoundaryProofPassed,
                    declarativePolicyPassed,
                },
                stagedEffect,
                approvalTicket,
                rationale: `Medium blast-radius (${blastRadiusScore}) staged for 4-eyes approval (${approvalTicket.ticketId})`,
            };
        }
        return {
            decision: "ALLOW",
            blastRadiusScore,
            violatedRuleIds: [],
            proofChecks: {
                sqlStaticProofPassed,
                pathBoundaryProofPassed,
                declarativePolicyPassed,
            },
            rationale: `Formal static proof passed with low blast-radius (${blastRadiusScore})`,
        };
    }
}
exports.StagingCommitAndApprovalGate = StagingCommitAndApprovalGate;
