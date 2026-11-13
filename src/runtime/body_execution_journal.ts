/** @fileoverview Durable execution admission: uncertain attempts require reconciliation, never automatic replay. */
import * as fs from "node:fs";
import * as path from "node:path";
import { createHash } from "node:crypto";

export class BodyExecutionJournal {
  constructor(private readonly directory: string) { fs.mkdirSync(directory, { recursive: true }); }
  private file(taskId: string): string {
    return path.join(this.directory, createHash("sha256").update(taskId).digest("hex") + ".json");
  }
  admit(taskId: string): { admitted: boolean; result?: Record<string, unknown> } {
    const file = this.file(taskId);
    try {
      const fd = fs.openSync(file, "wx");
      try { fs.writeFileSync(fd, JSON.stringify({ state: "RUNNING" })); fs.fsyncSync(fd); }
      finally { fs.closeSync(fd); }
      return { admitted: true };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      const saved = JSON.parse(fs.readFileSync(file, "utf8"));
      return { admitted: false, result: saved.result };
    }
  }
  complete(taskId: string, result: Record<string, unknown>): void {
    const file = this.file(taskId);
    const temporary = file + ".tmp";
    const fd = fs.openSync(temporary, "w");
    try { fs.writeFileSync(fd, JSON.stringify({ state: "COMPLETED", result })); fs.fsyncSync(fd); }
    finally { fs.closeSync(fd); }
    fs.renameSync(temporary, file);
  }
}
