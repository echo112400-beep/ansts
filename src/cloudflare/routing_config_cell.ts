import { CloudflareCelldEnv, DurableObjectState } from "./env";
import { RoutingConfigCell as RoutingConfigStorageCell } from "../persistence/routing-config";

/** Cloudflare/Celld Durable Object wrapper; the persistence owner stores immutable config history. */
export class RoutingConfigCell {
  public readonly owner: RoutingConfigStorageCell;

  constructor(public readonly ctx: DurableObjectState, public readonly env: CloudflareCelldEnv) {
    this.owner = new RoutingConfigStorageCell(ctx, env);
  }

  public fetch(request: Request): Promise<Response> {
    return this.owner.fetch(request);
  }
}
