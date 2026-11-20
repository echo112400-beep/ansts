import { readBoundedJson } from "./migration_request.js";
import type { DurableObjectNamespaceLike, DurableObjectStateLike } from "./env.js";
import { equalBodyToken } from "./body_auth.js";
import { ControlError } from "../control/contracts.js";
import {
  restoreIntoEmptyOfflineDatabase, restoreIntoFreshCelldObject, type AuthorityArchive,
} from "../finops/authority_archive.js";

interface RestoreEnvironment {
  ANSTS_AUTHORITY_IMPORT_TOKEN?: string;
  ANSTS_AUTHORITY_ARCHIVE_KEY?: string;
  ANSTS_AUTHORITY_IMPORT_RUNTIME?: string;
  ANSTS_TENANT_DIRECTORY_CELL?: DurableObjectNamespaceLike;
  ANSTS_TENANT_CONTROL_CELL?: DurableObjectNamespaceLike;
}

type ImportState = DurableObjectStateLike & {
  storage: { list?(options: { limit: number }): Promise<ReadonlyMap<string, unknown>> };
};

const maxBytes = 32 * 1024 * 1024 + 65536;

function scope(request: Request) {
  const path = new URL(request.url).pathname;
  if (path === "/restore/directory") return { kind: "DIRECTORY", tenantId: null } as const;
  const match = /^\/restore\/tenant\/([a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127})$/.exec(path);
  if (match) return { kind: "TENANT", tenantId: match[1] } as const;
}

function authorize(request: Request, env: RestoreEnvironment) {
  const token = env.ANSTS_AUTHORITY_IMPORT_TOKEN;
  const key = env.ANSTS_AUTHORITY_ARCHIVE_KEY;
  const runtime = env.ANSTS_AUTHORITY_IMPORT_RUNTIME;
  if (!token || token.length < 32 || !key || key.length < 32 || equalBodyToken(token, key) ||
      (runtime !== "celld" && runtime !== "open-orange")) {
    throw new ControlError("AUTHORITY_IMPORT_DISABLED", 503);
  }
  if (!equalBodyToken(request.headers.get("authorization") ?? "", "Bearer " + token)) {
    throw new ControlError("SERVICE_AUTH_REQUIRED", 401);
  }
  return { key, runtime };
}

function reject(error: unknown) {
  return error instanceof ControlError
    ? Response.json({ code: error.code }, { status: error.status })
    : Response.json({ code: "AUTHORITY_IMPORT_FAILED" }, { status: 500 });
}

class AuthorityImportCell {
  constructor(private readonly ctx: ImportState, private readonly env: RestoreEnvironment) {}

  async fetch(request: Request): Promise<Response> {
    try {
      const { key, runtime } = authorize(request, this.env);
      const expected = scope(request);
      if (!expected || request.method !== "POST") return new Response(null, { status: 404 });
      const archive = await readBoundedJson<AuthorityArchive>(request, maxBytes, "FINOPS_ARCHIVE_INVALID");
      const storage = this.ctx.storage;
      if (typeof storage.transactionSync !== "function") {
        throw new ControlError("DURABLE_TRANSACTION_REQUIRED", 503);
      }
      const sql = { sql: storage.sql, transactionSync: storage.transactionSync.bind(storage) };
      if (runtime === "celld") {
        if (typeof storage.list !== "function") throw new ControlError("AUTHORITY_IMPORT_DISABLED", 503);
        return Response.json(await restoreIntoFreshCelldObject({
          ...sql, getAlarm: storage.getAlarm.bind(storage), list: storage.list.bind(storage),
        }, archive, key, expected));
      }
      return Response.json(restoreIntoEmptyOfflineDatabase(sql, archive, key, expected));
    } catch (error) {
      return reject(error);
    }
  }
}

// Preserve the authority class names and object names in a dedicated import deployment.
export class TenantDirectoryCell extends AuthorityImportCell {}
export class TenantControlCell extends AuthorityImportCell {}

export default {
  async fetch(request: Request, env: RestoreEnvironment): Promise<Response> {
    try {
      authorize(request, env);
      const expected = scope(request);
      if (!expected || request.method !== "POST") return new Response(null, { status: 404 });
      const directory = expected.kind === "DIRECTORY";
      const namespace = directory ? env.ANSTS_TENANT_DIRECTORY_CELL : env.ANSTS_TENANT_CONTROL_CELL;
      if (!namespace) throw new ControlError("AUTHORITY_IMPORT_DISABLED", 503);
      const name = directory ? "tenant-directory-v1" : "tenant-control-v1:" + expected.tenantId;
      return await namespace.get(namespace.idFromName(name)).fetch(request);
    } catch (error) {
      return reject(error);
    }
  },
};
