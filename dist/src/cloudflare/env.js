"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.MockDurableObjectState = exports.InMemoryR2Bucket = exports.InMemoryQueueProducer = exports.InMemoryKvNamespace = exports.InMemoryCellSqlStorage = exports.DurableDiskCellSqlStorage = void 0;var fs = _interopRequireWildcard(require("node:fs"));
var path = _interopRequireWildcard(require("node:path"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;}



































































































































class InMemoryCellSqlStorage {
  tables = new Map();
  primaryKeysByTable = new Map();
  txSnapshot = null;
  storageFilePath;

  constructor(storageFilePath) {
    this.storageFilePath = storageFilePath ?? null;
    if (this.storageFilePath) {
      this.rehydrateFromDisk();
    }
  }

  rehydrateFromDisk() {
    if (!this.storageFilePath || !fs.existsSync(this.storageFilePath)) return;
    try {
      const raw = JSON.parse(fs.readFileSync(this.storageFilePath, "utf-8"));



      if (raw.primaryKeysByTable) {
        for (const [tblName, pkCols] of Object.entries(raw.primaryKeysByTable)) {
          if (Array.isArray(pkCols)) this.primaryKeysByTable.set(tblName, pkCols);
        }
      }
      if (raw.tables) {
        for (const [tblName, rowsObj] of Object.entries(raw.tables)) {
          const rowMap = new Map();
          for (const [pk, r] of Object.entries(rowsObj)) {
            rowMap.set(pk, r);
          }
          this.tables.set(tblName, rowMap);
        }
      }
    } catch {

      // ignore corrupted or partial file
    }}

  flushToDisk() {
    if (!this.storageFilePath || this.txSnapshot) return;
    try {
      fs.mkdirSync(path.dirname(this.storageFilePath), { recursive: true });
      const serialized = {};
      for (const [tblName, rowMap] of this.tables.entries()) {
        const rowsObj = {};
        for (const [pk, r] of rowMap.entries()) {
          rowsObj[pk] = r;
        }
        serialized[tblName] = rowsObj;
      }
      const pkSerialized = {};
      for (const [tblName, pkCols] of this.primaryKeysByTable.entries()) {
        pkSerialized[tblName] = pkCols;
      }
      const tmpPath = this.storageFilePath + ".tmp";
      fs.writeFileSync(
      tmpPath,
      JSON.stringify({ updatedAtMs: Date.now(), primaryKeysByTable: pkSerialized, tables: serialized }));

      fs.renameSync(tmpPath, this.storageFilePath);
    } catch {

      // ignore on read-only fs
    }}

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
      this.flushToDisk();
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
      const m = q.match(/CREATE TABLE(?: IF NOT EXISTS)?\s+([a-zA-Z0-9_]+)\s*\((.+)\)/i);
      if (m) {
        const tbl = m[1];
        const bodyDef = m[2];
        if (!this.tables.has(tbl)) {
          this.tables.set(tbl, new Map());
        }
        // [M59-1] Parse composite PRIMARY KEY (col1, col2, ...) if present so multi-column PK rows never overwrite one another
        const compositePkMatch = bodyDef.match(/PRIMARY\s+KEY\s*\(([^)]+)\)/i);
        if (compositePkMatch) {
          const pkCols = compositePkMatch[1].split(",").map((c) => c.trim());
          this.primaryKeysByTable.set(tbl, pkCols);
        } else {
          const inlinePkMatch = bodyDef.match(/([a-zA-Z0-9_]+)\s+[a-zA-Z0-9_]+\s+PRIMARY\s+KEY/i);
          if (inlinePkMatch) {
            this.primaryKeysByTable.set(tbl, [inlinePkMatch[1].trim()]);
          }
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
        const pkCols = this.primaryKeysByTable.get(tblName);
        const pk =
        pkCols && pkCols.length > 0 && pkCols.every((c) => c in row) ?
        pkCols.map((c) => String(row[c])).join("::") :
        String(bindings[0] ?? `${tbl.size + 1}`);
        tbl.set(pk, row);
        this.flushToDisk();
      }
      return { toArray: () => [], one: () => null };
    }

    if (upper.startsWith("UPDATE ")) {
      const m = q.match(/UPDATE\s+([a-zA-Z0-9_]+)\s+SET\s+(.+?)\s+WHERE\s+(.+)$/i);
      if (m) {
        const tblName = m[1];
        const tbl = this.tables.get(tblName);
        const setClause = m[2];
        const whereClause = m[3];
        if (tbl) {
          const setCols = setClause.split(",").map((part) => part.split("=")[0].trim());
          const whereCols = whereClause.split(/\s+AND\s+/i).map((c) => c.split("=")[0].trim());
          const whereBindings = bindings.slice(setCols.length);
          for (const existing of tbl.values()) {
            const matched = whereCols.every((col, idx) => String(existing[col]) === String(whereBindings[idx]));
            if (matched) {
              setCols.forEach((c, idx) => {
                existing[c] = bindings[idx];
              });
            }
          }
        }
      }
      this.flushToDisk();
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
          // [M59-1] Flush DELETE mutations to disk so deleted rows never resurrect on restart
          this.flushToDisk();
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
        let whereBindingsConsumed = 0;
        if (mWhere && bindings.length > 0) {
          const condCols = mWhere[1].split(/\s+AND\s+/i).map((c) => c.split("=")[0].trim());
          whereBindingsConsumed = condCols.length;
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

        // [M59-1] Enforce LIMIT N (literal or bound parameter `?`)
        const mLimit = q.match(/LIMIT\s+(\?|\d+)/i);
        if (mLimit) {
          const rawLim =
          mLimit[1] === "?" ? Number(bindings[whereBindingsConsumed] ?? rows.length) : Number(mLimit[1]);
          if (Number.isFinite(rawLim) && rawLim >= 0) {
            rows = rows.slice(0, Math.floor(rawLim));
          }
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

  async get(key, nowMs = Date.now()) {
    const entry = this.store.get(key);
    if (!entry) return null;
    if (entry.expiresAtMs !== null && nowMs >= entry.expiresAtMs) {
      this.store.delete(key);
      return null;
    }
    return entry.value;
  }

  async put(
  key,
  value,
  options)
  {
    const nowMs = options?.nowMs ?? Date.now();
    const ttlSec = options?.expirationTtl;
    const expiresAtMs =
    ttlSec !== undefined && Number.isFinite(ttlSec) && ttlSec > 0 ? nowMs + ttlSec * 1000 : null;
    this.store.set(key, { value, expiresAtMs });
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

class DurableDiskCellSqlStorage extends InMemoryCellSqlStorage {
  constructor(storagePathOrDir, cellName) {
    const resolvedPath = cellName ?
    path.join(storagePathOrDir, `${cellName}.sqlite.json`) :
    fs.existsSync(storagePathOrDir) && fs.statSync(storagePathOrDir).isDirectory() ?
    path.join(storagePathOrDir, "cell-default.sqlite.json") :
    storagePathOrDir;
    super(resolvedPath);
  }
}exports.DurableDiskCellSqlStorage = DurableDiskCellSqlStorage;