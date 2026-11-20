import { ControlError } from "../control/contracts.js";

export async function readBoundedJson<T>(request: Request, maxBytes: number, invalidCode: string): Promise<T> {
  const reader = request.body?.getReader();
  if (!reader) throw new ControlError(invalidCode, 422);
  const chunks: Uint8Array[] = [];
  let bytes = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > maxBytes) {
        await reader.cancel();
        throw new ControlError("PAYLOAD_TOO_LARGE", 413);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8")) as T;
  } catch {
    throw new ControlError(invalidCode, 422);
  }
}
