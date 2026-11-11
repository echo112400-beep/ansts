"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SharedBlackboardAndGitArbiter = void 0;
const saga_gateway_js_1 = require("../saga_gateway.js");
class SharedBlackboardAndGitArbiter {
    board = new Map();
    consecutiveConflictsByAgent = new Map();
    static CONFLICT_AGING_BOOST_PER_ROUND = 150;
    sql;
    constructor(sql) {
        if (sql) {
            this.bindSqlStorage(sql);
        }
    }
    /**
     * [M61-1] Binds SQLite storage so A2A Blackboard entries and conflict-aging counters survive restarts.
     */
    bindSqlStorage(sql) {
        this.sql = sql;
        this.sql.exec("CREATE TABLE IF NOT EXISTS a2a_blackboard_entries (key TEXT PRIMARY KEY, value_json TEXT, version INTEGER, author_agent_id TEXT, updated_at_ms INTEGER)");
        this.sql.exec("CREATE TABLE IF NOT EXISTS a2a_agent_conflicts (agent_id TEXT PRIMARY KEY, conflict_count INTEGER)");
        const rows = this.sql
            .exec("SELECT * FROM a2a_blackboard_entries")
            .toArray();
        let restored = 0;
        for (const r of rows) {
            const entry = {
                key: String(r.key),
                value: JSON.parse(String(r.value_json ?? "null")),
                version: Number(r.version ?? 1),
                authorAgentId: String(r.author_agent_id ?? ""),
                updatedAtMs: Number(r.updated_at_ms ?? Date.now()),
            };
            this.board.set(entry.key, entry);
            restored += 1;
        }
        const cRows = this.sql
            .exec("SELECT * FROM a2a_agent_conflicts")
            .toArray();
        for (const cr of cRows) {
            this.consecutiveConflictsByAgent.set(String(cr.agent_id), Number(cr.conflict_count ?? 0));
        }
        return restored;
    }
    /**
     * [M61-1] Supports idempotent CAS retries: if `currentVersion === expectedVersion + 1`,
     * `existing.authorAgentId === authorAgentId`, and the canonicalized `value` matches `existing.value`,
     * returns the existing committed entry (`deduplicated: true`) instead of throwing a false CAS conflict.
     */
    putCas(key, value, expectedVersion, authorAgentId) {
        const existing = this.board.get(key);
        const currentVersion = existing ? existing.version : 0;
        if (currentVersion !== expectedVersion) {
            if (existing &&
                currentVersion === expectedVersion + 1 &&
                existing.authorAgentId === authorAgentId &&
                JSON.stringify(saga_gateway_js_1.SagaToolGateway.canonicalizeValue(existing.value)) ===
                    JSON.stringify(saga_gateway_js_1.SagaToolGateway.canonicalizeValue(value))) {
                return { committed: true, deduplicated: true, entry: existing };
            }
            throw new Error(`Blackboard CAS conflict on key '${key}': expected v${expectedVersion}, actual v${currentVersion}`);
        }
        const next = {
            key,
            value,
            version: currentVersion + 1,
            authorAgentId,
            updatedAtMs: Date.now(),
        };
        this.board.set(key, next);
        if (this.sql) {
            this.sql.exec("INSERT OR REPLACE INTO a2a_blackboard_entries (key, value_json, version, author_agent_id, updated_at_ms) VALUES (?, ?, ?, ?, ?)", next.key, JSON.stringify(next.value), next.version, next.authorAgentId, next.updatedAtMs);
        }
        return { committed: true, deduplicated: false, entry: next };
    }
    get(key) {
        return this.board.get(key);
    }
    /**
     * [M54-4 & M61-1] Arbitrates overlapping Git file patches across concurrent agents:
     * - Coalesces duplicate patch submissions from the same `agentId` within the same round
     *   (merging unique `touchedFiles` and taking `Math.max` priorityScore) so retried agent submissions
     *   never self-conflict or double-increment conflict counters.
     * - Applies `effectivePriority = priorityScore + consecutiveConflicts * 150` anti-starvation boost.
     */
    arbitrateGitPatchTouches(patches) {
        const coalescedByAgent = new Map();
        const coalescedDuplicateAgents = [];
        for (const p of patches) {
            const existing = coalescedByAgent.get(p.agentId);
            if (existing) {
                if (!coalescedDuplicateAgents.includes(p.agentId)) {
                    coalescedDuplicateAgents.push(p.agentId);
                }
                for (const f of p.touchedFiles) {
                    existing.touchedFilesSet.add(f);
                }
                existing.priorityScore = Math.max(existing.priorityScore, p.priorityScore);
            }
            else {
                coalescedByAgent.set(p.agentId, {
                    agentId: p.agentId,
                    touchedFilesSet: new Set(p.touchedFiles),
                    priorityScore: p.priorityScore,
                });
            }
        }
        const dedupedPatches = Array.from(coalescedByAgent.values()).map((item) => ({
            agentId: item.agentId,
            touchedFiles: Array.from(item.touchedFilesSet),
            priorityScore: item.priorityScore,
        }));
        const effectiveScores = {};
        for (const p of dedupedPatches) {
            const priorConflicts = this.consecutiveConflictsByAgent.get(p.agentId) ?? 0;
            effectiveScores[p.agentId] =
                p.priorityScore + priorConflicts * SharedBlackboardAndGitArbiter.CONFLICT_AGING_BOOST_PER_ROUND;
        }
        const sorted = [...dedupedPatches].sort((a, b) => effectiveScores[b.agentId] - effectiveScores[a.agentId]);
        const claimedFiles = new Set();
        const acceptedAgentIds = [];
        const conflictedAgentIds = [];
        for (const p of sorted) {
            const hasOverlap = p.touchedFiles.some((f) => claimedFiles.has(f));
            if (hasOverlap) {
                conflictedAgentIds.push(p.agentId);
                const nextCount = (this.consecutiveConflictsByAgent.get(p.agentId) ?? 0) + 1;
                this.consecutiveConflictsByAgent.set(p.agentId, nextCount);
                if (this.sql) {
                    this.sql.exec("INSERT OR REPLACE INTO a2a_agent_conflicts (agent_id, conflict_count) VALUES (?, ?)", p.agentId, nextCount);
                }
            }
            else {
                acceptedAgentIds.push(p.agentId);
                this.consecutiveConflictsByAgent.set(p.agentId, 0);
                if (this.sql) {
                    this.sql.exec("INSERT OR REPLACE INTO a2a_agent_conflicts (agent_id, conflict_count) VALUES (?, ?)", p.agentId, 0);
                }
                for (const f of p.touchedFiles)
                    claimedFiles.add(f);
            }
        }
        return {
            acceptedAgentIds,
            conflictedAgentIds,
            effectiveScores,
            coalescedDuplicateAgents,
        };
    }
    getAgentConflictCount(agentId) {
        return this.consecutiveConflictsByAgent.get(agentId) ?? 0;
    }
}
exports.SharedBlackboardAndGitArbiter = SharedBlackboardAndGitArbiter;
