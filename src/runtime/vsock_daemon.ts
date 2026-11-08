/**
 * @fileoverview M11 TASK-40: Length-Prefixed Binary Framing Unix Domain / virtio-vsock Host Daemon.
 */
import * as net from "node:net";

/** Bytes per frame header: 4-byte big-endian body length + 4-byte big-endian CRC32. */
export const LENGTH_PREFIX_CRC_HEADER_BYTES = 8;

const CRC32_TABLE: Uint32Array = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let bit = 0; bit < 8; bit++) {
      crc = crc & 1 ? 0xedb88320 ^ (crc >>> 1) : crc >>> 1;
    }
    table[i] = crc >>> 0;
  }
  return table;
})();

/** Standard CRC-32 (IEEE 802.3, reflected, poly 0xEDB88320) over a byte buffer. */
export function computeCrc32(buffer: Buffer): number {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc = CRC32_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

export interface LengthPrefixedCrcFrame {
  length: number;
  crc32: number;
  payload: Record<string, unknown>;
}

/**
 * Encodes one control/vsock command into a `[length][crc32][json body]` frame so the receiving tunnel
 * relay can detect truncation or bit rot before parsing the payload. Any JSON-serializable command
 * object (e.g. a `DisaggregatedControlCommand`) may be framed.
 */
export function encodeLengthPrefixedCrcFrame(payload: object): Buffer {
  const body = Buffer.from(JSON.stringify(payload), "utf-8");
  const header = Buffer.alloc(LENGTH_PREFIX_CRC_HEADER_BYTES);
  header.writeUInt32BE(body.length, 0);
  header.writeUInt32BE(computeCrc32(body), 4);
  return Buffer.concat([header, body]);
}

/**
 * Decodes one or more concatenated length-prefixed CRC frames. Throws on a truncated frame or a CRC mismatch.
 */
export function decodeLengthPrefixedCrcFrames(buffer: Buffer): {
  frames: LengthPrefixedCrcFrame[];
  nextOffset: number;
} {
  const frames: LengthPrefixedCrcFrame[] = [];
  let offset = 0;
  while (offset < buffer.length) {
    if (buffer.length - offset < LENGTH_PREFIX_CRC_HEADER_BYTES) {
      throw new Error("Truncated length-prefixed CRC frame header");
    }
    const length = buffer.readUInt32BE(offset);
    const expectedCrc = buffer.readUInt32BE(offset + 4);
    const bodyStart = offset + LENGTH_PREFIX_CRC_HEADER_BYTES;
    const bodyEnd = bodyStart + length;
    if (bodyEnd > buffer.length) {
      throw new Error(
        `Truncated length-prefixed CRC frame body: declared ${length} bytes, available ${buffer.length - bodyStart}`,
      );
    }
    const body = buffer.subarray(bodyStart, bodyEnd);
    const actualCrc = computeCrc32(body);
    if (actualCrc !== expectedCrc) {
      throw new Error(
        `CRC32 mismatch on length-prefixed frame: expected ${expectedCrc.toString(16)}, got ${actualCrc.toString(16)}`,
      );
    }
    frames.push({
      length,
      crc32: actualCrc,
      payload: JSON.parse(body.toString("utf-8")) as Record<string, unknown>,
    });
    offset = bodyEnd;
  }
  return { frames, nextOffset: offset };
}

export class ZeroTrustVsockHostDaemon {
  private server: net.Server | null = null;
  private readonly allowedHosts: Set<string>;

  constructor(allowedHosts: string[] = ["api.openai.com", "api.anthropic.com"]) {
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
