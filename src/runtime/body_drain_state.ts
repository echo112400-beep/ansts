/** Drain survives process restarts until explicitly resumed. */
import * as fs from "node:fs";
import * as path from "node:path";
export class BodyDrainState {
  private value = false;
  constructor(private readonly file: string) {
    if (fs.existsSync(file)) {
      const saved = JSON.parse(fs.readFileSync(file, "utf8"));
      if (saved.schema_version !== 1 || typeof saved.draining !== "boolean") throw new Error("INVALID_DRAIN_STATE");
      this.value = saved.draining;
    }
  }
  get draining(): boolean { return this.value; }
  set(draining: boolean): void {
    const fd = fs.openSync(this.file + ".tmp", "w", 0o600);
    try { fs.writeFileSync(fd, JSON.stringify({ schema_version: 1, draining })); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    fs.renameSync(this.file + ".tmp", this.file);
    this.value = draining;
    const directory = fs.openSync(path.dirname(this.file), "r");
    try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  }
}