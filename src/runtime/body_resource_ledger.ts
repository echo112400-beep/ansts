/** Persistent container reservations. Release only after Engine exit/removal is confirmed. */
import * as fs from "node:fs";
import * as path from "node:path";

export interface BodyResources { cpu: number; memoryMb: number; }
export class BodyResourceLedger {
  private readonly entries = new Map<string, BodyResources>();
  constructor(private readonly directory: string, readonly total: BodyResources) {
    if (!Number.isFinite(total.cpu) || total.cpu <= 0 || !Number.isSafeInteger(total.memoryMb) || total.memoryMb < 1) throw new Error("INVALID_RESOURCE_BUDGET");
    fs.mkdirSync(directory, { recursive: true });
    for (const name of fs.readdirSync(directory).filter(name => name.endsWith(".json"))) {
      const entry = JSON.parse(fs.readFileSync(path.join(directory, name), "utf8"));
      if (!/^[a-f0-9]{64}\.json$/.test(name) || entry.schema_version !== 1 || !this.valid(entry.resources)) throw new Error("INVALID_RESOURCE_RESERVATION");
      this.entries.set(name.slice(0, -5), entry.resources);
    }
  }
  private valid(resources: BodyResources): boolean { return !!resources && Number.isFinite(resources.cpu) && resources.cpu > 0 && Number.isSafeInteger(resources.memoryMb) && resources.memoryMb > 0; }
  private sync(): void { const fd = fs.openSync(this.directory, "r"); try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); } }
  reserve(key: string, resources: BodyResources): boolean {
    if (!/^[a-f0-9]{64}$/.test(key) || !this.valid(resources)) throw new Error("INVALID_RESOURCE_RESERVATION");
    const { available } = this.snapshot();
    if (this.entries.has(key) || resources.cpu > available.cpu + 1e-9 || resources.memoryMb > available.memoryMb) return false;
    const file = path.join(this.directory, key + ".json");
    const fd = fs.openSync(file, "wx", 0o600);
    try { fs.writeFileSync(fd, JSON.stringify({ schema_version: 1, resources })); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    this.sync(); this.entries.set(key, { ...resources }); return true;
  }
  release(key: string): void {
    if (!this.entries.has(key)) return;
    fs.unlinkSync(path.join(this.directory, key + ".json")); this.sync(); this.entries.delete(key);
  }
  keys(): string[] { return [...this.entries.keys()]; }
  snapshot(): { total: BodyResources; reserved: BodyResources; available: BodyResources; reservations: number } {
    const reserved = [...this.entries.values()].reduce((sum, value) => ({ cpu: sum.cpu + value.cpu, memoryMb: sum.memoryMb + value.memoryMb }), { cpu: 0, memoryMb: 0 });
    return { total: { ...this.total }, reserved, available: { cpu: Math.max(0, this.total.cpu - reserved.cpu), memoryMb: Math.max(0, this.total.memoryMb - reserved.memoryMb) }, reservations: this.entries.size };
  }
}
