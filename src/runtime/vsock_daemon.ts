/**
 * @fileoverview M11 TASK-40: Length-Prefixed Binary Framing Unix Domain / virtio-vsock Host Daemon.
 */
import * as net from "node:net";
import * as fs from "node:fs";

export class ZeroTrustVsockHostDaemon {
  private server: net.Server | null = null;
  private readonly allowedHosts: Set<string>;

  constructor(allowedHosts: string[] = ["api.openai.com", "api.anthropic.com", "r2.ansts.internal"]) {
    this.allowedHosts = new Set(allowedHosts);
  }

  public static encodeFrame(payload: Record<string, unknown>): Buffer {
    const body = Buffer.from(JSON.stringify(payload), "utf-8");
    const header = Buffer.alloc(4);
    header.writeUInt32BE(body.length, 0);
    return Buffer.concat([header, body]);
  }

  public static decodeFrame(frame: Buffer): Record<string, unknown> {
    if (frame.length < 4) {
      throw new Error("Vsock frame shorter than 4-byte length header");
    }
    const len = frame.readUInt32BE(0);
    const jsonStr = frame.subarray(4, 4 + len).toString("utf-8");
    return JSON.parse(jsonStr) as Record<string, unknown>;
  }

  public async startUnixSocketServer(socketPath: string): Promise<void> {
    try {
      if (fs.existsSync(socketPath)) {
        fs.unlinkSync(socketPath);
      }
    } catch {
      // ignore cleanup error
    }
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        socket.on("data", (chunk: Buffer) => {
          const req = ZeroTrustVsockHostDaemon.decodeFrame(chunk);
          if (req.type === "EGRESS_CHECK") {
            const host = String(req.hostname ?? "");
            const allowed = this.allowedHosts.has(host);
            socket.write(
              ZeroTrustVsockHostDaemon.encodeFrame({
                requestId: req.requestId,
                hostname: host,
                allowed,
                status: allowed ? 200 : 403,
              })
            );
          } else {
            socket.write(
              ZeroTrustVsockHostDaemon.encodeFrame({
                requestId: req.requestId,
                status: 200,
                echo: req,
              })
            );
          }
        });
      });
      this.server.on("error", reject);
      this.server.listen(socketPath, () => resolve());
    });
  }

  public async sendClientFrameOverUnixSocket(
    socketPath: string,
    payload: Record<string, unknown>
  ): Promise<Record<string, unknown>> {
    return new Promise((resolve, reject) => {
      const client = net.createConnection(socketPath, () => {
        client.write(ZeroTrustVsockHostDaemon.encodeFrame(payload));
      });
      client.on("data", (chunk: Buffer) => {
        try {
          const decoded = ZeroTrustVsockHostDaemon.decodeFrame(chunk);
          client.end();
          resolve(decoded);
        } catch (err) {
          client.destroy();
          reject(err);
        }
      });
      client.on("error", reject);
    });
  }

  public async stop(): Promise<void> {
    if (!this.server) return;
    return new Promise((resolve) => {
      this.server!.close(() => {
        this.server = null;
        resolve();
      });
    });
  }
}
