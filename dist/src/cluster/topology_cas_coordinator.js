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
exports.TopologyCasCoordinator = void 0;
/**
 * @fileoverview M25, M55-2 & M61-1: Cluster Topology Compare-And-Swap (CAS) Membership Coordinator
 * with Member Deduplication, Idempotent CAS Retry, SHA-256 Ring Digest & SQLite Persistence.
 */
const crypto = __importStar(require("node:crypto"));
class TopologyCasCoordinator {
    generation = 0;
    members = [];
    sql;
    constructor(sql) {
        this.sql = sql;
        if (this.sql) {
            this.sql.exec("CREATE TABLE IF NOT EXISTS v25_topology_cas_state (id TEXT PRIMARY KEY, generation INTEGER, members_json TEXT, ring_digest TEXT)");
            const rows = this.sql
                .exec("SELECT * FROM v25_topology_cas_state WHERE id = ?", "global")
                .toArray();
            if (rows.length > 0) {
                this.generation = Number(rows[0].generation ?? 0);
                this.members = Array.from(new Set(JSON.parse(String(rows[0].members_json ?? "[]"))));
            }
        }
    }
    getGeneration() {
        return this.generation;
    }
    getMembers() {
        return [...this.members];
    }
    computeRingDigest() {
        return crypto
            .createHash("sha256")
            .update(`gen:${this.generation}:members:${this.members.slice().sort().join(",")}`, "utf-8")
            .digest("hex");
    }
    persist() {
        const ringDigest = this.computeRingDigest();
        if (this.sql) {
            this.sql.exec("INSERT OR REPLACE INTO v25_topology_cas_state (id, generation, members_json, ring_digest) VALUES (?, ?, ?, ?)", "global", this.generation, JSON.stringify(this.members), ringDigest);
        }
        return ringDigest;
    }
    /**
     * [M61-1] Supports idempotent CAS retry when `expectedGeneration === this.generation - 1`
     * and the normalized sorted member set matches `this.members`.
     */
    updateTopologyCas(expectedGeneration, nextMembers) {
        const dedupedNext = Array.from(new Set(nextMembers));
        if (expectedGeneration !== this.generation) {
            const currentSorted = this.members.slice().sort().join(",");
            const nextSorted = dedupedNext.slice().sort().join(",");
            if (expectedGeneration === this.generation - 1 && currentSorted === nextSorted) {
                return {
                    generation: this.generation,
                    members: [...this.members],
                    ringDigest: this.computeRingDigest(),
                    deduplicated: true,
                };
            }
            throw new Error(`Topology CAS conflict: expected gen=${expectedGeneration}, actual=${this.generation}`);
        }
        this.generation += 1;
        this.members = dedupedNext;
        const ringDigest = this.persist();
        return { generation: this.generation, members: [...this.members], ringDigest, deduplicated: false };
    }
    forceSyncMembers(nextMembers) {
        const dedupedNext = Array.from(new Set(nextMembers));
        const currentSorted = this.members.slice().sort().join(",");
        const nextSorted = dedupedNext.slice().sort().join(",");
        if (this.generation > 0 && currentSorted === nextSorted) {
            return {
                generation: this.generation,
                members: [...this.members],
                ringDigest: this.computeRingDigest(),
            };
        }
        this.generation += 1;
        this.members = dedupedNext;
        const ringDigest = this.persist();
        return { generation: this.generation, members: [...this.members], ringDigest };
    }
}
exports.TopologyCasCoordinator = TopologyCasCoordinator;
