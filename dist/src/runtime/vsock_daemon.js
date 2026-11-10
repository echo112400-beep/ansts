"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.ZeroTrustVsockHostDaemon = void 0;


var net = _interopRequireWildcard(require("node:net"));
var fs = _interopRequireWildcard(require("node:fs"));function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M11 TASK-40: Length-Prefixed Binary Framing Unix Domain / virtio-vsock Host Daemon.
 */class ZeroTrustVsockHostDaemon {
  server = null;
  allowedHosts;

  constructor(allowedHosts = ["api.openai.com", "api.anthropic.com", "r2.ansts.internal"]) {
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
    try {
      if (fs.existsSync(socketPath)) {
        fs.unlinkSync(socketPath);
      }
    } catch {

      // ignore cleanup error
    }return new Promise((resolve, reject) => {
      this.server = net.createServer((socket) => {
        socket.on("data", (chunk) => {
          const req = ZeroTrustVsockHostDaemon.decodeFrame(chunk);
          if (req.type === "EGRESS_CHECK") {
            const host = String(req.hostname ?? "");
            const allowed = this.allowedHosts.has(host);
            socket.write(
            ZeroTrustVsockHostDaemon.encodeFrame({
              requestId: req.requestId,
              hostname: host,
              allowed,
              status: allowed ? 200 : 403
            }));

          } else {
            socket.write(
            ZeroTrustVsockHostDaemon.encodeFrame({
              requestId: req.requestId,
              status: 200,
              echo: req
            }));

          }
        });
      });
      this.server.on("error", reject);
      this.server.listen(socketPath, () => resolve());
    });
  }

  async sendClientFrameOverUnixSocket(
  socketPath,
  payload)
  {
    return new Promise((resolve, reject) => {
      const client = net.createConnection(socketPath, () => {
        client.write(ZeroTrustVsockHostDaemon.encodeFrame(payload));
      });
      client.on("data", (chunk) => {
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