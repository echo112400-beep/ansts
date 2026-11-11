"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.SchedulerV25SingleWritePathStore = void 0;
/**
 * @fileoverview v2.5 Single-Write-Path Scheduler Persistence & Queue Deduplication Store (`b566a53`), M55-1, M60-5 & M64-2.
 * Hardened with tenant-scoped cross-task idempotency lookup, content-fingerprint mergeable duplicate coalescing,
 * priority promotion on merge, 100% `TaskSpec` field serialization/rehydration fidelity, and anti-regressive RustFS S3 LTX restoration.
 */
const env_js_1 = require("./env.js");
const models_js_1 = require("../models.js");
const scheduler_js_1 = require("../scheduler.js");
class SchedulerV25SingleWritePathStore {
    sql;
    seenIdempotencyKeys = new Map();
    seenFingerprints = new Map();
    maxFencingToken = 0;
    constructor(sql = new env_js_1.InMemoryCellSqlStorage()) {
        this.sql = sql;
        this.sql.exec("CREATE TABLE IF NOT EXISTS v25_scheduler_tasks (task_id TEXT PRIMARY KEY, tenant_id TEXT, priority INTEGER, state TEXT, fencing_token INTEGER, assigned_node_id TEXT, idem_key TEXT, spec_json TEXT)");
        const existingRows = this.sql.exec("SELECT * FROM v25_scheduler_tasks").toArray();
        for (const r of existingRows) {
            const taskId = String(r.task_id);
            if (r.idem_key) {
                const scopedKey = `${String(r.tenant_id ?? "default")}::${String(r.idem_key)}`;
                this.seenIdempotencyKeys.set(scopedKey, taskId);
            }
            const restored = this.rowToTaskSpec(r);
            for (const mKey of restored.merged_idempotency_keys) {
                this.seenIdempotencyKeys.set(`${restored.tenant_id}::${mKey}`, taskId);
            }
            if (restored.content_fingerprint) {
                this.seenFingerprints.set(restored.content_fingerprint, taskId);
            }
            const ft = Number(r.fencing_token ?? 0);
            if (ft > this.maxFencingToken) {
                this.maxFencingToken = ft;
            }
        }
    }
    commitTaskSingleWritePath(task, assignedNodeId, options) {
        if (!task.content_fingerprint) {
            task.content_fingerprint = scheduler_js_1.CoreScheduler5D.computeTaskContentFingerprint(task);
        }
        const idemKey = task.idempotency_key ?? `idem:${task.tenant_id}:${task.task_id}`;
        const scopedKey = `${task.tenant_id}::${idemKey}`;
        let mappedTaskId = this.seenIdempotencyKeys.get(scopedKey);
        let mergedBy = "IDEMPOTENCY_KEY";
        if (!mappedTaskId && options?.mergeDuplicateInFlight) {
            const fpTaskId = this.seenFingerprints.get(task.content_fingerprint);
            if (fpTaskId) {
                mappedTaskId = fpTaskId;
                mergedBy = "CONTENT_FINGERPRINT";
            }
        }
        if (mappedTaskId) {
            const existing = this.sql
                .exec("SELECT * FROM v25_scheduler_tasks WHERE task_id = ?", mappedTaskId)
                .one() ??
                this.sql
                    .exec("SELECT * FROM v25_scheduler_tasks WHERE task_id = ?", task.task_id)
                    .one();
            if (existing) {
                const restored = this.rowToTaskSpec(existing);
                const isTerminal = restored.state === models_js_1.TaskState.COMPLETED ||
                    restored.state === models_js_1.TaskState.DEAD_LETTER ||
                    restored.state === models_js_1.TaskState.FAILED;
                if (mergedBy === "IDEMPOTENCY_KEY" || !isTerminal) {
                    restored.merged_duplicate_count += 1;
                    if (task.task_id !== restored.task_id && !restored.merged_request_ids.includes(task.task_id)) {
                        restored.merged_request_ids.push(task.task_id);
                    }
                    if (task.idempotency_key && !restored.merged_idempotency_keys.includes(task.idempotency_key)) {
                        restored.merged_idempotency_keys.push(task.idempotency_key);
                    }
                    this.seenIdempotencyKeys.set(scopedKey, restored.task_id);
                    let priorityPromoted = false;
                    if (task.priority < restored.priority && !isTerminal) {
                        restored.priority = task.priority;
                        priorityPromoted = true;
                    }
                    this.sql.exec("INSERT OR REPLACE INTO v25_scheduler_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, idem_key, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", restored.task_id, restored.tenant_id, restored.priority, restored.state, restored.lease_fencing_token, restored.assigned_node_id ?? "", restored.idempotency_key ?? idemKey, JSON.stringify(restored));
                    return {
                        deduplicated: true,
                        mergedBy,
                        priorityPromoted,
                        fencingToken: restored.lease_fencing_token,
                        task: restored,
                    };
                }
            }
        }
        this.seenIdempotencyKeys.set(scopedKey, task.task_id);
        this.seenFingerprints.set(task.content_fingerprint, task.task_id);
        if (task.lease_fencing_token > this.maxFencingToken) {
            this.maxFencingToken = task.lease_fencing_token;
        }
        else {
            this.maxFencingToken += 1;
            task.lease_fencing_token = this.maxFencingToken;
        }
        if (assignedNodeId !== undefined) {
            task.assigned_node_id = assignedNodeId ?? undefined;
        }
        this.sql.exec("INSERT OR REPLACE INTO v25_scheduler_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, idem_key, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", task.task_id, task.tenant_id, task.priority, task.state, task.lease_fencing_token, task.assigned_node_id ?? "", idemKey, JSON.stringify(task));
        return { deduplicated: false, fencingToken: task.lease_fencing_token, task };
    }
    getPersistedTask(taskId) {
        const r = this.sql
            .exec("SELECT * FROM v25_scheduler_tasks WHERE task_id = ?", taskId)
            .one();
        if (!r)
            return null;
        return this.rowToTaskSpec(r);
    }
    listPersistedTasks() {
        const rows = this.sql.exec("SELECT * FROM v25_scheduler_tasks").toArray();
        return rows.map((r) => this.rowToTaskSpec(r));
    }
    /**
     * [M64-2] Restores 100% of `TaskSpec` fields (including `resource_profile`, `prefix_Cache_hash`,
     * `retry_count`, `preempted_count`, `last_preempted_at`, `dead_letter_reason`, and `created_at`).
     */
    rowToTaskSpec(r) {
        const parsed = JSON.parse(String(r.spec_json ?? "{}"));
        const rawProfile = parsed.resource_profile && typeof parsed.resource_profile === "object"
            ? parsed.resource_profile
            : undefined;
        const t = new models_js_1.TaskSpec({
            task_id: String(r.task_id),
            tenant_id: String(r.tenant_id),
            priority: Number(r.priority ?? models_js_1.TaskPriority.P2_STANDARD),
            prompt: String(parsed.prompt ?? ""),
            idempotency_key: parsed.idempotency_key
                ? String(parsed.idempotency_key)
                : r.idem_key
                    ? String(r.idem_key)
                    : undefined,
            max_turns: parsed.max_turns !== undefined ? Number(parsed.max_turns) : undefined,
            timeout_seconds: parsed.timeout_seconds !== undefined ? Number(parsed.timeout_seconds) : undefined,
            resource_profile: rawProfile,
            dependencies: Array.isArray(parsed.dependencies) ? parsed.dependencies.map(String) : undefined,
            parent_task_id: parsed.parent_task_id ? String(parsed.parent_task_id) : undefined,
            expected_SubAgent_count: parsed.expected_SubAgent_count !== undefined ? Number(parsed.expected_SubAgent_count) : undefined,
            prefix_Cache_hash: parsed.prefix_Cache_hash ? String(parsed.prefix_Cache_hash) : "",
            content_fingerprint: parsed.content_fingerprint ? String(parsed.content_fingerprint) : undefined,
            merged_duplicate_count: parsed.merged_duplicate_count !== undefined ? Number(parsed.merged_duplicate_count) : 0,
            merged_request_ids: Array.isArray(parsed.merged_request_ids)
                ? parsed.merged_request_ids.map(String)
                : [],
            merged_idempotency_keys: Array.isArray(parsed.merged_idempotency_keys)
                ? parsed.merged_idempotency_keys.map(String)
                : undefined,
        });
        if (parsed.current_turn !== undefined) {
            t.current_turn = Number(parsed.current_turn);
        }
        if (parsed.retry_count !== undefined) {
            t.retry_count = Number(parsed.retry_count);
        }
        if (parsed.preempted_count !== undefined) {
            t.preempted_count = Number(parsed.preempted_count);
        }
        if (parsed.last_preempted_at !== undefined) {
            t.last_preempted_at = Number(parsed.last_preempted_at);
        }
        if (parsed.dead_letter_reason) {
            t.dead_letter_reason = String(parsed.dead_letter_reason);
        }
        if (parsed.created_at !== undefined) {
            t.created_at = Number(parsed.created_at);
        }
        if (!t.content_fingerprint) {
            t.content_fingerprint = scheduler_js_1.CoreScheduler5D.computeTaskContentFingerprint(t);
        }
        t.state = r.state ?? models_js_1.TaskState.QUEUED;
        t.lease_fencing_token = Number(r.fencing_token ?? 0);
        if (r.assigned_node_id && String(r.assigned_node_id).length > 0) {
            t.assigned_node_id = String(r.assigned_node_id);
        }
        else if (parsed.assigned_node_id && String(parsed.assigned_node_id).length > 0) {
            t.assigned_node_id = String(parsed.assigned_node_id);
        }
        return t;
    }
    async flushLtxSnapshotToS3(s3Endpoint, nodeId) {
        try {
            const rows = this.sql.exec("SELECT * FROM v25_scheduler_tasks").toArray();
            const resp = await fetch(`${s3Endpoint.replace(/\/$/, "")}/ansts-fleet/ltx/${encodeURIComponent(nodeId)}.json`, {
                method: "PUT",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ nodeId, maxFencingToken: this.maxFencingToken, rows }),
            });
            return resp.ok;
        }
        catch {
            return false;
        }
    }
    /**
     * [M55-1] Anti-regressive S3 LTX snapshot restoration:
     * Only overwrites a local task row if the incoming snapshot row has `fencing_token >= local.fencing_token`,
     * preventing lagging S3 snapshots from regressing a task's fencing epoch or resurrecting completed tasks.
     */
    async restoreLtxSnapshotFromS3(s3Endpoint, nodeId) {
        try {
            const resp = await fetch(`${s3Endpoint.replace(/\/$/, "")}/ansts-fleet/ltx/${encodeURIComponent(nodeId)}.json`);
            if (!resp.ok)
                return 0;
            const data = (await resp.json());
            if (data.maxFencingToken !== undefined && Number(data.maxFencingToken) > this.maxFencingToken) {
                this.maxFencingToken = Number(data.maxFencingToken);
            }
            let restored = 0;
            for (const r of data.rows ?? []) {
                const taskId = String(r.task_id);
                const incomingToken = Number(r.fencing_token ?? 0);
                const existingLocal = this.sql
                    .exec("SELECT * FROM v25_scheduler_tasks WHERE task_id = ?", taskId)
                    .one();
                if (existingLocal && Number(existingLocal.fencing_token ?? 0) > incomingToken) {
                    continue;
                }
                this.sql.exec("INSERT OR REPLACE INTO v25_scheduler_tasks (task_id, tenant_id, priority, state, fencing_token, assigned_node_id, idem_key, spec_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", r.task_id, r.tenant_id, r.priority, r.state, r.fencing_token, r.assigned_node_id ?? "", r.idem_key, r.spec_json);
                if (r.idem_key) {
                    const scopedKey = `${String(r.tenant_id ?? "default")}::${String(r.idem_key)}`;
                    this.seenIdempotencyKeys.set(scopedKey, taskId);
                }
                const restoredSpec = this.rowToTaskSpec(r);
                if (restoredSpec.content_fingerprint) {
                    this.seenFingerprints.set(restoredSpec.content_fingerprint, taskId);
                }
                if (incomingToken > this.maxFencingToken) {
                    this.maxFencingToken = incomingToken;
                }
                restored += 1;
            }
            return restored;
        }
        catch {
            return 0;
        }
    }
}
exports.SchedulerV25SingleWritePathStore = SchedulerV25SingleWritePathStore;
