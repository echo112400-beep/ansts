"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.ZeroTrustVsockHostDaemon = void 0;
/**
 * @fileoverview M11 TASK-40, M57-6 & M66-4: Length-Prefixed Binary Framing Unix Domain / virtio-vsock Host Daemon
 * with Stream Reassembly Buffer, Multi-Frame Pipelining Support, Frame Length Validation & Client Timeout Guard.
 */
const net = __importStar(require("node:net"));
const fs = __importStar(require("node:fs"));
class ZeroTrustVsockHostDaemon {
    server = null;
    allowedHosts;
    maxFrameBytes;
    constructor(allowedHosts = ["api.openai.com", "api.anthropic.com", "r2.ansts.internal"], maxFrameBytes = 4 * 1024 * 1024) {
        this.allowedHosts = new Set(allowedHosts);
        const rawMax = Number(maxFrameBytes);
        this.maxFrameBytes =
            Number.isFinite(rawMax) && rawMax > 0 ? Math.floor(rawMax) : 4 * 1024 * 1024;
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
        if (frame.length < 4 + len) {
            throw new Error(`Incomplete Vsock frame: expected ${4 + len} bytes, received ${frame.length} bytes`);
        }
        const jsonStr = frame.subarray(4, 4 + len).toString("utf-8");
        return JSON.parse(jsonStr);
    }
    handleDecodedRequest(req) {
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
    async startUnixSocketServer(socketPath) {
        try {
            if (fs.existsSync(socketPath)) {
                fs.unlinkSync(socketPath);
            }
        }
        catch {
            // ignore cleanup error
        }
        return new Promise((resolve, reject) => {
            this.server = net.createServer((socket) => {
                let rxBuf = Buffer.alloc(0);
                socket.on("data", (chunk) => {
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
                        }
                        catch (err) {
                            socket.write(ZeroTrustVsockHostDaemon.encodeFrame({
                                status: 400,
                                error: err.message,
                            }));
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
    async sendClientFrameOverUnixSocket(socketPath, payload, timeoutMs = 5000) {
        return new Promise((resolve, reject) => {
            let rxBuf = Buffer.alloc(0);
            const client = net.createConnection(socketPath, () => {
                client.write(ZeroTrustVsockHostDaemon.encodeFrame(payload));
            });
            if (Number.isFinite(timeoutMs) && timeoutMs > 0) {
                client.setTimeout(timeoutMs, () => {
                    client.destroy(new Error(`Vsock client socket timed out after ${timeoutMs}ms`));
                });
            }
            client.on("data", (chunk) => {
                rxBuf = Buffer.concat([rxBuf, chunk]);
                if (rxBuf.length < 4)
                    return;
                const bodyLen = rxBuf.readUInt32BE(0);
                if (bodyLen > this.maxFrameBytes) {
                    client.destroy();
                    reject(new Error(`Vsock response frame exceeds maxFrameBytes (${bodyLen} > ${this.maxFrameBytes})`));
                    return;
                }
                if (rxBuf.length < 4 + bodyLen)
                    return;
                try {
                    const decoded = ZeroTrustVsockHostDaemon.decodeFrame(rxBuf.subarray(0, 4 + bodyLen));
                    client.end();
                    resolve(decoded);
                }
                catch (err) {
                    client.destroy();
                    reject(err);
                }
            });
            client.on("error", reject);
        });
    }
    async stop() {
        if (!this.server)
            return;
        return new Promise((resolve) => {
            this.server.close(() => {
                this.server = null;
                resolve();
            });
        });
    }
}
exports.ZeroTrustVsockHostDaemon = ZeroTrustVsockHostDaemon;
