export interface DigestedJson {
  canonicalJson: string;
  sha256: string;
}

/** Canonical JSON makes payload identity independent of object property insertion order. */
export async function digestJson(value: unknown): Promise<DigestedJson> {
  const canonicalJson = canonicalize(value, new Set<object>());
  const bytes = new TextEncoder().encode(canonicalJson);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  const sha256 = Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
  return { canonicalJson, sha256 };
}

function canonicalize(value: unknown, ancestors: Set<object>): string {
  if (value === null || typeof value === "boolean" || typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new TypeError("Durable payload numbers must be finite.");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    if (ancestors.has(value)) throw new TypeError("Durable payload cannot contain a cycle.");
    ancestors.add(value);
    const result = `[${value.map((entry) => canonicalize(entry, ancestors)).join(",")}]`;
    ancestors.delete(value);
    return result;
  }
  if (typeof value === "object") {
    const object = value as Record<string, unknown>;
    const prototype = Object.getPrototypeOf(object);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new TypeError("Durable payloads must contain only JSON-compatible objects.");
    }
    if (ancestors.has(object)) throw new TypeError("Durable payload cannot contain a cycle.");
    ancestors.add(object);
    const fields = Object.keys(object)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalize(object[key], ancestors)}`);
    ancestors.delete(object);
    return `{${fields.join(",")}}`;
  }
  throw new TypeError("Durable payloads must be JSON-compatible.");
}
