"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ZeroTrustVsockHostDaemon = exports.LENGTH_PREFIX_CRC_HEADER_BYTES = void 0;exports.computeCrc32 = computeCrc32;exports.decodeLengthPrefixedCrcFrames = decodeLengthPrefixedCrcFrames;exports.encodeLengthPrefixedCrcFrame = encodeLengthPrefixedCrcFrame;


var net = _interopRequireWildcard(require("node:net"));var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M11 TASK-40: Length-Prefixed Binary Framing Unix Domain / virtio-vsock Host Daemon.
 */ /** Bytes per frame header: 4-byte big-endian body length + 4-byte big-endian CRC32. */
const LENGTH_PREFIX_CRC_HEADER_BYTES = exports.LENGTH_PREFIX_CRC_HEADER_BYTES = 8;

const CRC32_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let bit = 0; bit < 8; bit++) {
      crc = crc & 1 ? 0xedb88320 ^ crc >>> 1 : crc >>> 1;
    }
    table[i] = crc >>> 0;
  }
  return table;
})();

/** Standard CRC-32 (IEEE 802.3, reflected, poly 0xEDB88320) over a byte buffer. */
function computeCrc32(buffer) {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc = CRC32_TABLE[(crc ^ byte) & 0xff] ^ crc >>> 8;
  }
  return (crc ^ 0xffffffff) >>> 0;
}







/**
 * Encodes one control/vsock command into a `[length][crc32][json body]` frame so the receiving tunnel
 * relay can detect truncation or bit rot before parsing the payload. Any JSON-serializable command
 * object (e.g. a `DisaggregatedControlCommand`) may be framed.
 */
function encodeLengthPrefixedCrcFrame(payload) {
  const body = Buffer.from(JSON.stringify(payload), "utf-8");
  const header = Buffer.alloc(LENGTH_PREFIX_CRC_HEADER_BYTES);
  header.writeUInt32BE(body.length, 0);
  header.writeUInt32BE(computeCrc32(body), 4);
  return Buffer.concat([header, body]);
}

/**
 * Decodes one or more concatenated length-prefixed CRC frames. Throws on a truncated frame or a CRC mismatch.
 */
function decodeLengthPrefixedCrcFrames(buffer)


{
  const frames = [];
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
        `Truncated length-prefixed CRC frame body: declared ${length} bytes, available ${buffer.length - bodyStart}`
      );
    }
    const body = buffer.subarray(bodyStart, bodyEnd);
    const actualCrc = computeCrc32(body);
    if (actualCrc !== expectedCrc) {
      throw new Error(
        `CRC32 mismatch on length-prefixed frame: expected ${expectedCrc.toString(16)}, got ${actualCrc.toString(16)}`
      );
    }
    frames.push({
      length,
      crc32: actualCrc,
      payload: JSON.parse(body.toString("utf-8"))
    });
    offset = bodyEnd;
  }
  return { frames, nextOffset: offset };
}

class ZeroTrustVsockHostDaemon {
  server = null;
  allowedHosts;

  constructor(allowedHosts = ["api.openai.com", "api.anthropic.com"]) {
    this.allowedHosts = new Set(allowedHosts);
  }

  static encodeFrame(payload) {
    const body = Buffer.from(JSON.stringify(payload), "utf-8");
    const header = Buffer.alloc(4);
    header.writeUInt32BE(body.length, 0);
    return Buffer.concat([header, body]);
  }

  static decodeFrame(frame) {
    if (frame.length < 4) {
      throw new Error("Vsock frame shorter than 4-byte length header");
    }
    const len = frame.readUInt32BE(0);
    const jsonStr = frame.subarray(4, 4 + len).toString("utf-8");
    return JSON.parse(jsonStr);
  }

  async startUnixSocketServer(socketPath) {
    return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        socket.on("data", (chunk) => {
          const req = ZeroTrustVsockHostDaemon.decodeFrame(chunk);
          if (req.type === "EGRESS_CHECK") {
            const host = String(req.hostname ?? "");
            const allowed = this.allowedHosts.has(host);
            socket.write(
              ZeroTrustVsockHostDaemon.encodeFrame({
                requestId: req.requestId,
                allowed,
                status: allowed ? 200 : 403
              })
            );
          } else {
            socket.write(
              ZeroTrustVsockHostDaemon.encodeFrame({
                requestId: req.requestId,
                status: 200,
                echo: req
              })
            );
          }
        });
      });
      this.server.on("error", reject);
      this.server.listen(socketPath, () => resolve());
    });
  }

  async stop() {
    if (!this.server) return;
    return new Promise((resolve) => {
      this.server.close(() => {
        this.server = null;
        resolve();
      });
    });
  }
}exports.ZeroTrustVsockHostDaemon = ZeroTrustVsockHostDaemon;