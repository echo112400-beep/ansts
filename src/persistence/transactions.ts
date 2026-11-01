import { PersistenceObjectStorage, PersistenceSqlStorage } from "./contracts";

export class AsyncCellTransactionError extends Error {
  public readonly code = "ASYNC_CELL_TRANSACTION";

  constructor() {
    super("Cell SQL transactions must be synchronous; perform external I/O after commit.");
    this.name = "AsyncCellTransactionError";
  }
}

export class CellDurabilityUnavailableError extends Error {
  public readonly code = "CELL_DURABILITY_UNAVAILABLE";

  constructor() {
    super("The active runtime does not expose Celld storage.sync; durable confirmation is unavailable.");
    this.name = "CellDurabilityUnavailableError";
  }
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return Boolean(value && (typeof value === "object" || typeof value === "function") && "then" in value);
}

/** Run only local SQL mutations atomically. The callback cannot await another Cell or service. */
export function withCellTransaction<T>(storage: PersistenceObjectStorage | PersistenceSqlStorage, callback: () => T): T {
  // Celld/Cloudflare transactions belong to storage, not storage.sql. Accept the
  // old SQL-only adapter for existing isolated tests, but prefer the runtime owner.
  const owner = typeof storage.transactionSync === "function" ? storage : ("sql" in storage ? storage.sql : storage);
  if (typeof owner.transactionSync !== "function") throw new TypeError("Cell storage.transactionSync is unavailable.");
  return owner.transactionSync(() => {
    const result = callback();
    if (isThenable(result)) {
      throw new AsyncCellTransactionError();
    }
    return result;
  });
}

/** Wait for Celld's configured proof after the local transaction has closed. */
export async function confirmCellWrite(storage: PersistenceObjectStorage): Promise<void> {
  if (typeof storage.sync !== "function") {
    throw new CellDurabilityUnavailableError();
  }
  await storage.sync();
}
