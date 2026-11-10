"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.TopologyCasCoordinator = void 0;



var crypto = _interopRequireWildcard(require("node:crypto"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M25, M55-2 & M61-1: Cluster Topology Compare-And-Swap (CAS) Membership Coordinator
 * with Member Deduplication, Idempotent CAS Retry, SHA-256 Ring Digest & SQLite Persistence.
 */class TopologyCasCoordinator {
  generation = 0;
  members = [];
  sql;

  constructor(sql) {
    this.sql = sql;
    if (this.sql) {
      this.sql.exec(
      "CREATE TABLE IF NOT EXISTS v25_topology_cas_state (id TEXT PRIMARY KEY, generation INTEGER, members_json TEXT, ring_digest TEXT)");

      const rows = this.sql.
      exec("SELECT * FROM v25_topology_cas_state WHERE id = ?", "global").
      toArray();
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
    return crypto.
    createHash("sha256").
    update(`gen:${this.generation}:members:${this.members.slice().sort().join(",")}`, "utf-8").
    digest("hex");
  }

  persist() {
    const ringDigest = this.computeRingDigest();
    if (this.sql) {
      this.sql.exec(
      "INSERT OR REPLACE INTO v25_topology_cas_state (id, generation, members_json, ring_digest) VALUES (?, ?, ?, ?)",
      "global",
      this.generation,
      JSON.stringify(this.members),
      ringDigest);

    }
    return ringDigest;
  }

  /**
   * [M61-1] Supports idempotent CAS retry when `expectedGeneration === this.generation - 1`
   * and the normalized sorted member set matches `this.members`.
   */
  updateTopologyCas(expectedGeneration, nextMembers)




  {
    const dedupedNext = Array.from(new Set(nextMembers));
    if (expectedGeneration !== this.generation) {
      const currentSorted = this.members.slice().sort().join(",");
      const nextSorted = dedupedNext.slice().sort().join(",");
      if (expectedGeneration === this.generation - 1 && currentSorted === nextSorted) {
        return {
          generation: this.generation,
          members: [...this.members],
          ringDigest: this.computeRingDigest(),
          deduplicated: true
        };
      }
      throw new Error(`Topology CAS conflict: expected gen=${expectedGeneration}, actual=${this.generation}`);
    }
    this.generation += 1;
    this.members = dedupedNext;
    const ringDigest = this.persist();
    return { generation: this.generation, members: [...this.members], ringDigest, deduplicated: false };
  }

  forceSyncMembers(nextMembers)



  {
    const dedupedNext = Array.from(new Set(nextMembers));
    const currentSorted = this.members.slice().sort().join(",");
    const nextSorted = dedupedNext.slice().sort().join(",");
    if (this.generation > 0 && currentSorted === nextSorted) {
      return {
        generation: this.generation,
        members: [...this.members],
        ringDigest: this.computeRingDigest()
      };
    }
    this.generation += 1;
    this.members = dedupedNext;
    const ringDigest = this.persist();
    return { generation: this.generation, members: [...this.members], ringDigest };
  }
}exports.TopologyCasCoordinator = TopologyCasCoordinator;