import { SnapshotBlockStore } from "../checkpoint/snapshot_block_store";
import { CloudflareCelldEnv } from "./env";

/** R2 adapter for application-managed immutable snapshot/model-response blocks. */
export class R2SnapshotBlockStore implements SnapshotBlockStore {
  constructor(private readonly bucket: CloudflareCelldEnv["ANSTS_CHECKPOINTS_R2"]) {
    if (!bucket) throw new TypeError("ANSTS_CHECKPOINTS_R2 is required for durable Turn snapshots.");
  }

  public async putIfAbsent(key: string, bytes: Uint8Array): Promise<"CREATED" | "EXISTS"> {
    const result = await this.bucket.put(key, bytes, {
      // R2 evaluates this condition atomically with the write; a losing writer must read/verify.
      onlyIf: { etagDoesNotMatch: "*" },
    });
    return result ? "CREATED" : "EXISTS";
  }

  public async get(key: string): Promise<Uint8Array | null> {
    const object = await this.bucket.get(key);
    if (!object) return null;
    return new Uint8Array(await object.arrayBuffer());
  }

  public async delete(key: string): Promise<void> {
    await this.bucket.delete(key);
  }
}
