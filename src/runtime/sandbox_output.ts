/** Bounded decoders; archives are inspected as data and never extracted onto the host. */
export class SandboxLogs {
  private pending = Buffer.alloc(0);
  private retained = Buffer.alloc(0);
  private total = 0;
  push(chunk: Buffer): void {
    this.pending = Buffer.concat([this.pending, chunk]);
    while (this.pending.length >= 8) {
      const size = this.pending.readUInt32BE(4);
      if (size > 1048576 || ![1, 2].includes(this.pending[0])) throw new Error("INVALID_SANDBOX_LOG_FRAME");
      if (this.pending.length < size + 8) break;
      const data = this.pending.subarray(8, size + 8);
      this.total += size;
      this.retained = Buffer.concat([this.retained, data.subarray(0, Math.max(0, 8192 - this.retained.length))]);
      this.pending = this.pending.subarray(size + 8);
    }
    if (this.pending.length > 1048584) throw new Error("INVALID_SANDBOX_LOG_FRAME");
  }
  snapshot(): { logs: string; logsTruncated: boolean; logsTailLimited: boolean } { return { logs: this.retained.toString("utf8"), logsTruncated: this.total > 8192, logsTailLimited: false }; }
  finish(): void { if (this.pending.length) throw new Error("INVALID_SANDBOX_LOG_FRAME"); }
}
export interface SandboxArtifact { path: string; size: number; contentBase64: string; }
/** Docker archive does not include tmpfs mounts. Run this fixed collector inside the sandbox. */
export const SANDBOX_ARTIFACT_COLLECTOR = `
const fs = require('node:fs');
const artifacts = []; let bytes = 0, visits = 0, artifactsTruncated = false;
// Freeze remaining task descendants before copying the final workspace snapshot.
const stopped = new Set();
for (let round = 0; round < 64; round++) {
  let found = false;
  for (const name of fs.readdirSync('/proc')) {
    const pid = Number(name);
    if (!/^[0-9]+$/.test(name) || pid <= 1 || pid === process.pid || stopped.has(pid)) continue;
    try { process.kill(pid, 'SIGSTOP'); stopped.add(pid); found = true; } catch (error) { if (error.code !== 'ESRCH') artifactsTruncated = true; }
  }
  if (!found) break;
}
function walk(directory, depth) {
  if (depth > 8 || visits >= 256) { artifactsTruncated = true; return; }
  if (!fs.realpathSync(directory).startsWith('/workspace')) { artifactsTruncated = true; return; }
  const dir = fs.opendirSync(directory);
  try {
    let entry;
    while ((entry = dir.readSync())) {
      if (++visits > 256) { artifactsTruncated = true; break; }
      const file = directory + '/' + entry.name;
      if (entry.isDirectory()) { walk(file, depth + 1); continue; }
      if (!entry.isFile()) { artifactsTruncated = true; continue; }
      const relative = file.slice('/workspace/'.length);
      if (Buffer.byteLength(relative) > 256 || relative.includes('\\\\')) { artifactsTruncated = true; continue; }
      let fd;
      try {
        fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
        const info = fs.fstatSync(fd), actual = fs.realpathSync('/proc/self/fd/' + fd);
        if (!actual.startsWith('/workspace/') || !info.isFile() || info.size > 8192 || artifacts.length >= 16 || bytes + info.size > 12288) { artifactsTruncated = true; continue; }
        const content = Buffer.alloc(info.size); const read = fs.readSync(fd, content, 0, content.length, 0);
        artifacts.push({ path: relative, size: read, contentBase64: content.subarray(0, read).toString('base64') }); bytes += read;
      } catch { artifactsTruncated = true; } finally { if (fd !== undefined) fs.closeSync(fd); }
    }
  } finally { dir.closeSync(); }
}
try { walk('/workspace', 0); } catch { artifactsTruncated = true; }
console.log(JSON.stringify({ artifacts, artifactsTruncated }));
`;
