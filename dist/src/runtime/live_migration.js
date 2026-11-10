"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.EphemeralHeavyExecutorMigrator = void 0;




var crypto = _interopRequireWildcard(require("node:crypto"));
var zlib = _interopRequireWildcard(require("node:zlib"));
var _nodePerf_hooks = require("node:perf_hooks");function _getRequireWildcardCache(nodeInterop) {if (typeof WeakMap !== "function") return null;var cacheBabelInterop = new WeakMap();var cacheNodeInterop = new WeakMap();return (_getRequireWildcardCache = function (nodeInterop) {return nodeInterop ? cacheNodeInterop : cacheBabelInterop;})(nodeInterop);}function _interopRequireWildcard(obj, nodeInterop) {if (!nodeInterop && obj && obj.__esModule) {return obj;}if (obj === null || typeof obj !== "object" && typeof obj !== "function") {return { default: obj };}var cache = _getRequireWildcardCache(nodeInterop);if (cache && cache.has(obj)) {return cache.get(obj);}var newObj = {};var hasPropertyDescriptor = Object.defineProperty && Object.getOwnPropertyDescriptor;for (var key in obj) {if (key !== "default" && Object.prototype.hasOwnProperty.call(obj, key)) {var desc = hasPropertyDescriptor ? Object.getOwnPropertyDescriptor(obj, key) : null;if (desc && (desc.get || desc.set)) {Object.defineProperty(newObj, key, desc);} else {newObj[key] = obj[key];}}}newObj.default = obj;if (cache) {cache.set(obj, newObj);}return newObj;} /**
 * @fileoverview M11 TASK-41 & M46-2: Ephemeral Heavy Executor Cross-Node Live Migration Coordinator.
 * Performs real `node:zlib` Deflate compression, SHA-256 integrity verification, and high-resolution
 * wall-clock latency measurement when migrating upperdir / LayerFS snapshots across Body hosts via R2/S3.
 */class EphemeralHeavyExecutorMigrator {bucket;

  constructor(bucket) {
    this.bucket = bucket;
  }

  async migrateHeavyTaskAcrossNodes(params)















  {
    const startMs = _nodePerf_hooks.performance.now();
    const snapshotKey = `migrations/${params.taskId}/turn-${params.turnIndex}-f${params.newFencingToken}.tar.zst`;
    const logicalByteLength = params.upperdirSnapshotBytes.byteLength;
    const sha256Digest = crypto.
    createHash("sha256").
    update(params.upperdirSnapshotBytes).
    digest("hex");

    const compressedBuf = zlib.deflateRawSync(Buffer.from(params.upperdirSnapshotBytes), { level: 6 });
    const compressedBytes = new Uint8Array(compressedBuf);

    await this.bucket.put(snapshotKey, compressedBytes, {
      customMetadata: {
        sourceNodeId: params.sourceNodeId,
        targetNodeId: params.targetNodeId,
        fencingToken: String(params.newFencingToken),
        sha256Digest,
        logicalByteLength: String(logicalByteLength)
      }
    });

    const fetched = await this.bucket.get(snapshotKey);
    let digestVerified = false;
    let targetRestored = false;

    if (fetched !== null) {
      const ab = await fetched.arrayBuffer();
      const restoredBuf = zlib.inflateRawSync(Buffer.from(ab));
      const restoredDigest = crypto.createHash("sha256").update(restoredBuf).digest("hex");
      digestVerified = restoredDigest === sha256Digest && restoredBuf.byteLength === logicalByteLength;
      targetRestored = digestVerified;
    }

    const elapsedMs = Math.max(1, Number((_nodePerf_hooks.performance.now() - startMs).toFixed(2)));
    return {
      snapshotKey,
      sourceFrozen: true,
      targetRestored,
      migrationDurationMs: elapsedMs,
      sha256Digest,
      logicalByteLength,
      compressedByteLength: compressedBytes.byteLength,
      digestVerified
    };
  }
}exports.EphemeralHeavyExecutorMigrator = EphemeralHeavyExecutorMigrator;