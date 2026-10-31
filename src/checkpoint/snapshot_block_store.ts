/**
 * Application-managed immutable block storage used by TaskCell Turn snapshots.
 *
 * Implementations must make `putIfAbsent` conditional at the object-store boundary.
 * This interface alone, and in-memory test doubles, do not establish production
 * durability or replication guarantees.
 */
export interface SnapshotBlockStore {
  putIfAbsent(key: string, bytes: Uint8Array): Promise<"CREATED" | "EXISTS">;
  get(key: string): Promise<Uint8Array | null>;
  delete(key: string): Promise<void>;
}

export async function sha256Hex(bytes: Uint8Array): Promise<string> {
  const crypto = (globalThis as unknown as {
    crypto: { subtle: { digest(algorithm: string, data: Uint8Array): Promise<ArrayBuffer> } };
  }).crypto;
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

export async function putAndVerifyBlock(
  store: SnapshotBlockStore,
  key: string,
  bytes: Uint8Array,
  expectedSha256: string,
): Promise<void> {
  const expected = new Uint8Array(bytes);
  await store.putIfAbsent(key, expected);
  const readBack = await store.get(key);
  if (readBack === null) throw new Error(`Snapshot block ${key} was not readable after upload.`);
  if (readBack.byteLength !== expected.byteLength || await sha256Hex(readBack) !== expectedSha256) {
    throw new Error(`Snapshot block ${key} failed read-back verification.`);
  }
}
