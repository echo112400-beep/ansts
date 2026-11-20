import type { DurableObjectNamespaceLike, DurableObjectStateLike } from "./env.js";
import { equalBodyToken } from "./body_auth.js";
import { AnstsSchedulerCell as SchedulerRuntime } from "./scheduler_cell.js";
import { CapacityInventoryCell as CapacityRuntime } from "./capacity_inventory_cell.js";
import { CapacityRequestStore } from "../cluster/capacity_request_store.js";
import { ControlError } from "../control/contracts.js";
import { maxControlCheckpointBytes, restoreControlCheckpoint, signControlCheckpoint, type SignedControlCheckpoint } from "../control/control_checkpoint.js";
import { readBoundedJson } from "./migration_request.js";

interface CheckpointEnv {
  ANSTS_CONTROL_CHECKPOINT_MODE?: string;
  ANSTS_CONTROL_CHECKPOINT_TOKEN?: string;
  ANSTS_CONTROL_CHECKPOINT_KEY?: string;
  ANSTS_SCHEDULER_CELL?: DurableObjectNamespaceLike;
  ANSTS_CAPACITY_INVENTORY_CELL?: DurableObjectNamespaceLike;
}

function route(request: Request) {
  const url = new URL(request.url);
  const scheduler = /^\/checkpoint\/scheduler\/([a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127})$/.exec(url.pathname);
  if (scheduler) return { plane: "scheduler", name: scheduler[1] } as const;
  if (url.pathname === "/checkpoint/capacity") return { plane: "capacity", name: "capacity-inventory-v1" } as const;
}

function authorize(request: Request, env: CheckpointEnv) {
  const { ANSTS_CONTROL_CHECKPOINT_MODE: mode, ANSTS_CONTROL_CHECKPOINT_TOKEN: token, ANSTS_CONTROL_CHECKPOINT_KEY: key } = env;
  if ((mode !== "export" && mode !== "import") || !token || token.length < 32 ||
      !key || key.length < 32 || equalBodyToken(token, key)) {
    throw new ControlError("CHECKPOINT_DISABLED", 503);
  }
  if (!equalBodyToken(request.headers.get("authorization") ?? "", "Bearer " + token)) {
    throw new ControlError("SERVICE_AUTH_REQUIRED", 401);
  }
  if (request.method !== (mode === "export" ? "GET" : "POST")) {
    throw new ControlError("METHOD_NOT_ALLOWED", 405);
  }
  return { mode, key };
}

function failure(error: unknown) {
  return error instanceof ControlError
    ? Response.json({ code: error.code }, { status: error.status })
    : Response.json({ code: "CHECKPOINT_FAILED" }, { status: 500 });
}

class CheckpointCell {
  constructor(private readonly ctx: DurableObjectStateLike, private readonly env: CheckpointEnv,
     private readonly plane: "scheduler" | "capacity") {}

  async fetch(request: Request): Promise<Response> {
    try {
      const scope = route(request);
      if (!scope || scope.plane !== this.plane) return new Response(null, { status: 404 });
      const { mode, key } = authorize(request, this.env);
      const storage = this.ctx.storage;
      if (typeof storage.transactionSync !== "function") throw new ControlError("DURABLE_TRANSACTION_REQUIRED", 503);
      const db = { sql: storage.sql, transactionSync: storage.transactionSync.bind(storage) };
      const id = this.ctx.id.toString();
      if (mode === "export") {
        this.initialize(db);
        return Response.json(signControlCheckpoint(db, scope, id, await storage.getAlarm(), key), {
          headers: { "cache-control": "no-store" },
        });
      }
      const sourceId = new URL(request.url).searchParams.get("sourceId");
      if (!sourceId || sourceId.length > 256) throw new ControlError("CHECKPOINT_SOURCE_REQUIRED", 422);
      const archive = await readBoundedJson<SignedControlCheckpoint>(request, maxControlCheckpointBytes + 1024, "CHECKPOINT_INVALID");
      if (await storage.getAlarm() !== null) throw new ControlError("CHECKPOINT_DESTINATION_INITIALIZED", 409);
      this.initialize(db);
      return Response.json(restoreControlCheckpoint(db, id, archive, { ...scope, sourceId }, key));
    } catch (error) {
      return failure(error);
    }
  }

  private initialize(db: { sql: DurableObjectStateLike["storage"]["sql"]; transactionSync: NonNullable<DurableObjectStateLike["storage"]["transactionSync"]> }) {
    if (this.plane === "scheduler") {
      new SchedulerRuntime(this.ctx);
    } else {
      new CapacityRuntime(this.ctx);
      new CapacityRequestStore(db, "checkpoint");
    }
  }
}

export class AnstsSchedulerCell extends CheckpointCell {
  constructor(ctx: DurableObjectStateLike, env: CheckpointEnv) { super(ctx, env, "scheduler"); }
}
export class CapacityInventoryCell extends CheckpointCell {
  constructor(ctx: DurableObjectStateLike, env: CheckpointEnv) { super(ctx, env, "capacity"); }
}

export default {
  async fetch(request: Request, env: CheckpointEnv): Promise<Response> {
    try {
      const scope = route(request);
      if (!scope) return new Response(null, { status: 404 });
      authorize(request, env);
      const namespace = scope.plane === "scheduler" ? env.ANSTS_SCHEDULER_CELL : env.ANSTS_CAPACITY_INVENTORY_CELL;
      if (!namespace) throw new ControlError("CHECKPOINT_DISABLED", 503);
      return await namespace.get(namespace.idFromName(scope.name)).fetch(request);
    } catch (error) {
      return failure(error);
    }
  },
};
