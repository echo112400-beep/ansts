/** Durable evidence that Engine cleanup still needs confirmation. */
import * as fs from "node:fs";
import * as path from "node:path";
export class BodyCleanupLedger {
  constructor(private readonly directory: string) { fs.mkdirSync(directory, { recursive: true }); }
  pending(): string[] { return fs.readdirSync(this.directory).filter(name => /^[a-f0-9]{64}\.pending$/.test(name)).map(name => name.slice(0, -8)); }
  private syncDirectory(): void { const fd = fs.openSync(this.directory, "r"); try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); } }
  reserve(key: string): void {
    if (!/^[a-f0-9]{64}$/.test(key)) throw new Error("INVALID_CLEANUP_IDENTITY");
    const target = path.join(this.directory, key + ".pending");
    const fd = fs.openSync(target, "wx", 0o600);
    try { fs.writeFileSync(fd, JSON.stringify({ schema_version: 1, reserved_at: Date.now() })); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    this.syncDirectory();
  }
  confirmRemoved(key: string): void {
    if (!/^[a-f0-9]{64}$/.test(key)) throw new Error("INVALID_CLEANUP_IDENTITY");
    fs.unlinkSync(path.join(this.directory, key + ".pending")); this.syncDirectory();
  }
  confirmAllRemoved(): void { for (const key of this.pending()) this.confirmRemoved(key); }
}