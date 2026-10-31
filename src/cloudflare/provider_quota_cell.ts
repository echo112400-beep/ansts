import { CloudflareCelldEnv, DurableObjectState } from "./env";
import { ProviderQuotaCell as ProviderQuotaStorageCell } from "../persistence/provider-quota";

/** Cloudflare/Celld Durable Object wrapper; the persistence owner contains all quota SQL and rules. */
export class ProviderQuotaCell {
  public readonly owner: ProviderQuotaStorageCell;

  constructor(public readonly ctx: DurableObjectState, public readonly env: CloudflareCelldEnv) {
    this.owner = new ProviderQuotaStorageCell(ctx, env);
  }

  public fetch(request: Request): Promise<Response> {
    return this.owner.fetch(request);
  }
}
