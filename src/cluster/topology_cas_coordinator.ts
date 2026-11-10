/**
 * @fileoverview M25 & M55-2: Cluster Topology Compare-And-Swap (CAS) Membership Coordinator
 * with Member Deduplication, SHA-256 Ring Digest & SQLite Persistence.
 */
import * as crypto from "node:crypto";
import { SqlStorageLike } from "../cloudflare/env.js";

export class TopologyCasCoordinator {
  private generation = 0;
  private members: string[] = [];
  private readonly sql?: SqlStorageLike;

  constructor(sql?: SqlStorageLike) {
    this.sql = sql;
    if (this.sql) {
      this.sql.exec(
        "CREATE TABLE IF NOT EXISTS v25_topology_cas_state (id TEXT PRIMARY KEY, generation INTEGER, members_json TEXT, ring_digest TEXT)"
      );
      const rows = this.sql
        .exec<Record<string, unknown>>("SELECT * FROM v25_topology_cas_state WHERE id = ?", "global")
        .toArray();
      if (rows.length > 0) {
        this.generation = Number(rows[0].generation ?? 0);
        this.members = Array.from(new Set(JSON.parse(String(rows[0].members_json ?? "[]")) as string[]));
      }
    }
  }

  public getGeneration(): number {
    return this.generation;
  }

  public getMembers(): string[] {
    return [...this.members];
  }

  public computeRingDigest(): string {
    return crypto
      .createHash("sha256")
      .update(`gen:${this.generation}:members:${this.members.slice().sort().join(",")}`, "utf-8")
      .digest("hex");
  }

  private persist(): string {
    const ringDigest = this.computeRingDigest();
    if (this.sql) {
      this.sql.exec(
        "INSERT OR REPLACE INTO v25_topology_cas_state (id, generation, members_json, ring_digest) VALUES (?, ?, ?, ?)",
        "global",
        this.generation,
        JSON.stringify(this.members),
        ringDigest
      );
    }
    return ringDigest;
  }

  public updateTopologyCas(expectedGeneration: number, nextMembers: string[]): {
    generation: number;
    members: string[];
    ringDigest: string;
  } {
    if (expectedGeneration !== this.generation) {
      throw new Error(`Topology CAS conflict: expected gen=${expectedGeneration}, actual=${this.generation}`);
    }
    this.generation += 1;
    this.members = Array.from(new Set(nextMembers));
    const ringDigest = this.persist();
    return { generation: this.generation, members: [...this.members], ringDigest };
  }

  public forceSyncMembers(nextMembers: string[]): {
    generation: number;
    members: string[];
    ringDigest: string;
  } {
    this.generation += 1;
    this.members = Array.from(new Set(nextMembers));
    const ringDigest = this.persist();
    return { generation: this.generation, members: [...this.members], ringDigest };
  }
}
