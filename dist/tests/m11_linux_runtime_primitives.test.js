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
/**
 * @fileoverview M11 Real Linux Runtime Primitives Suite (4 tests).
 */
const node_test_1 = require("node:test");
const assert = __importStar(require("node:assert/strict"));
const fs = __importStar(require("node:fs"));
const os = __importStar(require("node:os"));
const path = __importStar(require("node:path"));
const net = __importStar(require("node:net"));
const index_js_1 = require("../src/index.js");
(0, node_test_1.describe)("M11 Linux Runtime Primitives Suite", () => {
    (0, node_test_1.it)("1. LinuxCgroupV2AndPsiDriver writes cpu.max, memory.high, cgroup.freeze and parses /proc/pressure/memory", () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-cgroup-"));
        const driver = new index_js_1.LinuxCgroupV2AndPsiDriver(tmpDir);
        const limits = driver.writePhaseLimits("task-cg-1", "THINK", 0.25, 256, 512);
        assert.equal(limits.cpuMaxContent, "25000 100000");
        assert.equal(driver.setFreezeState("task-cg-1", true), "1");
        const psi = index_js_1.LinuxCgroupV2AndPsiDriver.parseProcPressureMemory("some avg10=12.45 avg60=5.00 avg300=1.00 total=100\nfull avg10=3.20 avg60=1.00 avg300=0.00 total=20\n");
        assert.equal(psi.someAvg10, 12.45);
        assert.equal(psi.fullAvg10, 3.2);
        fs.rmSync(tmpDir, { recursive: true, force: true });
    });
    (0, node_test_1.it)("2. CompanionContainerAndOverlayDriver creates OverlayFS mount directories and captures upperdir diff", () => {
        const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-ovl-"));
        const driver = new index_js_1.CompanionContainerAndOverlayDriver(tmpDir);
        const layout = driver.createOverlayMountLayout("task-ovl-1");
        fs.writeFileSync(path.join(layout.upperDir, "result.txt"), "hello overlay", "utf-8");
        const snap = driver.captureUpperdirDeltaSnapshot("task-ovl-1", 1);
        assert.deepEqual(snap.changedFiles, ["result.txt"]);
        fs.rmSync(tmpDir, { recursive: true, force: true });
    });
    (0, node_test_1.it)("3. ZeroTrustVsockHostDaemon frames binary RPC over Unix socket and enforces egress allowlist", async () => {
        const sockPath = path.join(os.tmpdir(), `ansts-vsock-${Date.now()}.sock`);
        const daemon = new index_js_1.ZeroTrustVsockHostDaemon(["api.openai.com"]);
        await daemon.startUnixSocketServer(sockPath);
        const response = await new Promise((resolve, reject) => {
            const client = net.createConnection(sockPath, () => {
                client.write(index_js_1.ZeroTrustVsockHostDaemon.encodeFrame({
                    requestId: "req-1",
                    type: "EGRESS_CHECK",
                    hostname: "untrusted.evil.org",
                }));
            });
            client.on("data", (chunk) => {
                resolve(index_js_1.ZeroTrustVsockHostDaemon.decodeFrame(chunk));
                client.end();
            });
            client.on("error", reject);
        });
        assert.equal(response.allowed, false);
        assert.equal(response.status, 403);
        await daemon.stop();
    });
    (0, node_test_1.it)("4. EphemeralHeavyExecutorMigrator migrates upperdir snapshot across nodes via RustFS/R2", async () => {
        const bucket = new index_js_1.InMemoryR2Bucket();
        const migrator = new index_js_1.EphemeralHeavyExecutorMigrator(bucket);
        const res = await migrator.migrateHeavyTaskAcrossNodes({
            taskId: "t-mig-1",
            turnIndex: 3,
            sourceNodeId: "node-1",
            targetNodeId: "node-2",
            newFencingToken: 4,
            upperdirSnapshotBytes: new Uint8Array([1, 2, 3, 4, 5]),
        });
        assert.equal(res.targetRestored, true);
    });
});
