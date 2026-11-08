"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.MockDurableObjectState = exports.InMemoryR2Bucket = exports.InMemoryQueueProducer = exports.InMemoryKvNamespace = exports.InMemoryCellSqlStorage = void 0; /**
 * @fileoverview M7 TASK-25: Cloudflare Workers & denoland/celld Unified Runtime Bindings & SQL/KV/R2 Mock Layer.
 */































































































































class InMemoryCellSqlStorage {
  tables = new Map();
  txSnapshot = null;

  exec(query, ...bindings) {
    const q = query.trim().replace(/\s+/g, " ");
    const upper = q.toUpperCase();

    if (upper === "BEGIN" || upper === "BEGIN TRANSACTION") {
      this.txSnapshot = new Map();
      for (const [tName, rows] of this.tables.entries()) {
        const clonedRows = new Map();
        for (const [pk, r] of rows.entries()) {
          clonedRows.set(pk, { ...r });
        }
        this.txSnapshot.set(tName, clonedRows);
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper === "COMMIT" || upper === "COMMIT TRANSACTION") {
      this.txSnapshot = null;
      return { toArray: () => [], one: () => null };
    }

    if (upper === "ROLLBACK" || upper === "ROLLBACK TRANSACTION") {
      if (this.txSnapshot) {
        this.tables = this.txSnapshot;
        this.txSnapshot = null;
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("CREATE TABLE") || upper.startsWith("CREATE INDEX") || upper.startsWith("PRAGMA")) {
      const m = q.match(/CREATE TABLE(?: IF NOT EXISTS)?\s+([a-zA-Z0-9_]+)/i);
      if (m) {
        const tbl = m[1];
        if (!this.tables.has(tbl)) {
          this.tables.set(tbl, new Map());
        }
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("INSERT") || upper.startsWith("REPLACE")) {
      const m = q.match(/(?:INSERT(?: OR REPLACE)?|REPLACE)\s+INTO\s+([a-zA-Z0-9_]+)\s*\(([^)]+)\)/i);
      if (m) {
        const tblName = m[1];
        const cols = m[2].split(",").map((c) => c.trim());
        let tbl = this.tables.get(tblName);
        if (!tbl) {
          tbl = new Map();
          this.tables.set(tblName, tbl);
        }
        const row = {};
        cols.forEach((col, i) => {
          row[col] = bindings[i];
        });
        const pk = String(bindings[0] ?? `${tbl.size + 1}`);
        tbl.set(pk, row);
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("UPDATE ")) {
      const m = q.match(/UPDATE\s+([a-zA-Z0-9_]+)\s+SET\s+(.+?)\s+WHERE\s+([a-zA-Z0-9_]+)\s*=\s*\?/i);
      if (m) {
        const tbl = this.tables.get(m[1]);
        const setClause = m[2];
        const pkValue = String(bindings[bindings.length - 1]);
        if (tbl && tbl.has(pkValue)) {
          const existing = tbl.get(pkValue);
          const setCols = setClause.split(",").map((part) => part.split("=")[0].trim());
          setCols.forEach((c, idx) => {
            if (idx < bindings.length - 1) {
              existing[c] = bindings[idx];
            }
          });
        }
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("DELETE FROM")) {
      const m = q.match(/DELETE FROM\s+([a-zA-Z0-9_]+)(?:\s+WHERE\s+(.+))?/i);
      if (m) {
        const tbl = this.tables.get(m[1]);
        if (tbl) {
          if (m[2] && bindings.length > 0) {
            const condCols = m[2].split(/\s+AND\s+/i).map((c) => c.split("=")[0].trim());
            for (const [pk, row] of tbl.entries()) {
              const matchAll = condCols.every((col, idx) => String(row[col]) === String(bindings[idx]));
              if (matchAll) tbl.delete(pk);
            }
          } else {
            tbl.clear();
          }
        }
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("SELECT ")) {
      const mTable = q.match(/FROM\s+([a-zA-Z0-9_]+)/i);
      if (mTable) {
        const tbl = this.tables.get(mTable[1]);
        if (!tbl) return { toArray: () => [], one: () => null };
        let rows = Array.from(tbl.values());

        const mWhere = q.match(/WHERE\s+(.+?)(?:\s+ORDER BY|\s+LIMIT|$)/i);
        if (mWhere && bindings.length > 0) {
          const condCols = mWhere[1].split(/\s+AND\s+/i).map((c) => c.split("=")[0].trim());
          rows = rows.filter((r) =>
          condCols.every((col, idx) => String(r[col]) === String(bindings[idx])));

        }

        const mOrder = q.match(/ORDER BY\s+([a-zA-Z0-9_]+)(?:\s+(ASC|DESC))?/i);
        if (mOrder) {
          const orderCol = mOrder[1];
          const desc = (mOrder[2] ?? "ASC").toUpperCase() === "DESC";
          rows.sort((a, b) => {
            const va = a[orderCol];
            const vb = b[orderCol];
            if (va === vb) return 0;
            const cmp = va > vb ? 1 : -1;
            return desc ? -cmp : cmp;
          });
        }

        if (upper.includes("COUNT(*)")) {
          const cntRow = { cnt: rows.length, "COUNT(*)": rows.length };
          return { toArray: () => [cntRow], one: () => cntRow };
        }
        return {
          toArray: () => rows,
          one: () => rows[0] ?? null
        };
      }
    }

    return { toArray: () => [], one: () => null };
  }
}exports.InMemoryCellSqlStorage = InMemoryCellSqlStorage;

class MockDurableObjectState {
  id;
  storage;
  kvMap = new Map();
  alarmTime = null;

  constructor(idStr = "cell-default-01") {
    this.id = { toString: () => idStr, name: idStr };
    const sql = new InMemoryCellSqlStorage();
    this.storage = {
      sql,
      get: async (key) => this.kvMap.get(key),
      put: async (key, value) => {
        this.kvMap.set(key, value);
      },
      delete: async (key) => this.kvMap.delete(key),
      setAlarm: async (scheduledTime) => {
        this.alarmTime = typeof scheduledTime === "number" ? scheduledTime : scheduledTime.getTime();
      },
      getAlarm: async () => this.alarmTime
    };
  }

  async blockConcurrencyWhile(callback) {
    return callback();
  }
}exports.MockDurableObjectState = MockDurableObjectState;

class InMemoryKvNamespace {
  store = new Map();
  async get(key) {
    return this.store.get(key) ?? null;
  }
  async put(key, value) {
    this.store.set(key, value);
  }
  async delete(key) {
    this.store.delete(key);
  }
}exports.InMemoryKvNamespace = InMemoryKvNamespace;

class InMemoryR2Bucket {
  objects =
  new Map();
  seq = 0;

  async put(
  key,
  value,
  options)
  {
    const existing = this.objects.get(key);
    if (options?.onlyIf?.etagDoesNotMatch === "*" && existing) {
      return null;
    }
    if (options?.onlyIf?.etagMatches && (!existing || existing.etag !== options.onlyIf.etagMatches)) {
      return null;
    }
    this.seq += 1;
    const bytes =
    typeof value === "string" ?
    new TextEncoder().encode(value) :
    value instanceof Uint8Array ?
    value :
    new Uint8Array(value);
    const etag = `"etag-${this.seq}-${bytes.byteLength}"`;
    this.objects.set(key, {
      bytes,
      etag,
      customMetadata: options?.customMetadata ?? {}
    });
    return { key, etag, size: bytes.byteLength };
  }

  async get(key) {
    const obj = this.objects.get(key);
    if (!obj) return null;
    return {
      key,
      etag: obj.etag,
      size: obj.bytes.byteLength,
      customMetadata: obj.customMetadata,
      arrayBuffer: async () => obj.bytes.slice().buffer,
      text: async () => new TextDecoder().decode(obj.bytes)
    };
  }

  async delete(key) {
    this.objects.delete(key);
  }

  async list(options) {
    const prefix = options?.prefix ?? "";
    const out = [];
    for (const [k, v] of this.objects.entries()) {
      if (k.startsWith(prefix)) {
        out.push({ key: k, etag: v.etag, size: v.bytes.byteLength });
      }
    }
    return { objects: out };
  }
}exports.InMemoryR2Bucket = InMemoryR2Bucket;

class InMemoryQueueProducer {
  sentMessages = [];
  async send(body, options) {
    this.sentMessages.push({ body, delaySeconds: options?.delaySeconds });
  }
}exports.InMemoryQueueProducer = InMemoryQueueProducer;