/**
 * @fileoverview M11 TASK-40 & M57-6: Length-Prefixed Binary Framing Unix Domain / virtio-vsock Host Daemon
 * with Stream Reassembly Buffer, Multi-Frame Pipelining Support & Frame Length Validation.
 */
import * as net from "node:net";
import * as fs from "node:fs";

export class ZeroTrustVsockHostDaemon {
  private server: net.Server | null = null;
  private readonly allowedHosts: Set<string>;
  public readonly maxFrameBytes: number;

  constructor(
    allowedHosts: string[] = ["api.openai.com", "api.anthropic.com", "r2.ansts.internal"],
    maxFrameBytes = 4 * 1024 * 1024
  ) {
    this.allowedHosts = new Set(allowedHosts);
    this.maxFrameBytes = maxFrameBytes;
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
    if (frame.length < 4 + len) {
      throw new Error(`Incomplete Vsock frame: expected ${4 + len} bytes, received ${frame.length} bytes`);
    }
    const jsonStr = frame.subarray(4, 4 + len).toString("utf-8");
    return JSON.parse(jsonStr) as Record<string, unknown>;
  }

  private handleDecodedRequest(req: Record<string, unknown>): Record<string, unknown> {
    if (req.type === "EGRESS_CHECK") {
      const host = String(req.hostname ?? "");
      const allowed = this.allowedHosts.has(host);
      return {
        requestId: req.requestId,
        hostname: host,
        allowed,
        status: allowed ? 200 : 403,
      };
    }
    return {
      requestId: req.requestId,
      status: 200,
      echo: req,
    };
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
        let rxBuf = Buffer.alloc(0);
        socket.on("data", (chunk: Buffer) => {
          rxBuf = Buffer.concat([rxBuf, chunk]);
          while (rxBuf.length >= 4) {
            const bodyLen = rxBuf.readUInt32BE(0);
            if (bodyLen > this.maxFrameBytes) {
              socket.destroy(new Error(`Vsock frame exceeds maxFrameBytes (${bodyLen} > ${this.maxFrameBytes})`));
              return;
            }
            if (rxBuf.length < 4 + bodyLen) {
              // Wait for remaining fragmented bytes
              break;
            }
            const fullFrame = rxBuf.subarray(0, 4 + bodyLen);
            rxBuf = rxBuf.subarray(4 + bodyLen);
            try {
              const req = ZeroTrustVsockHostDaemon.decodeFrame(fullFrame);
              const resp = this.handleDecodedRequest(req);
              socket.write(ZeroTrustVsockHostDaemon.encodeFrame(resp));
            } catch (err) {
              socket.write(
                ZeroTrustVsockHostDaemon.encodeFrame({
                  status: 400,
                  error: (err as Error).message,
                })
              );
            }
          }
        });
        socket.on("error", () => {
          // Prevent unhandled socket error crashes
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
      let rxBuf = Buffer.alloc(0);
      const client = net.createConnection(socketPath, () => {
        client.write(ZeroTrustVsockHostDaemon.encodeFrame(payload));
      });
      client.on("data", (chunk: Buffer) => {
        rxBuf = Buffer.concat([rxBuf, chunk]);
        if (rxBuf.length < 4) return;
        const bodyLen = rxBuf.readUInt32BE(0);
        if (rxBuf.length < 4 + bodyLen) return;
        try {
          const decoded = ZeroTrustVsockHostDaemon.decodeFrame(rxBuf.subarray(0, 4 + bodyLen));
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
