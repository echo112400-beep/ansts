/** @fileoverview Unambiguous tenant/task storage identities with legacy read compatibility. */
const PREFIX = "ansts:v1:";
export function taskStorageId(tenantId: string, taskId: string): string {
  return PREFIX + Buffer.from(JSON.stringify([tenantId, taskId])).toString("base64url");
}
export function publicTaskId(storageId: string): { tenantId: string; taskId: string } | null {
  if (!storageId.startsWith(PREFIX)) return null;
  try {
    const value = JSON.parse(Buffer.from(storageId.slice(PREFIX.length), "base64url").toString("utf8"));
    return Array.isArray(value) && value.length === 2 && value.every(part => typeof part === "string") ? { tenantId: value[0], taskId: value[1] } : null;
  } catch { return null; }
}
export function normalizeTaskStorageId(tenantId: string, taskId: string): string {
  const identity = publicTaskId(taskId);
  if (identity?.tenantId === tenantId) return taskId;
  return taskStorageId(tenantId, taskId);
}
