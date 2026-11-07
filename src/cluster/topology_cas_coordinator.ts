/**
 * @fileoverview TASK-76 & M27 (TASK-130): Global S3 CAS Topology Coordinator (`control/topology-v1.json`)
 * with Real Atomic Disk File Persistence (`topology-v1.json` + `.etag`).
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { S3CasPreconditionFailedError } from "../checkpoint/s3_ltx_driver";

export interface ClusterTopologyManifestV1 {
  schemaVersion: "v1";
  generation: number;
  updatedAtMs: number;
  schedulerShardCount: number;
  queueShardCount: number;
  virtualSlotAssignment: Record<number, string>; // 0..255 -> shardId
  brainNodes: Array<{
    nodeId: string;
    listenAddr: string;
    internalAdminUrl: string;
    weight: number;
    draining: boolean;
  }>;
  bodyHosts: Array<{
    hostId: string;
    ipAddr: string;
    tunnelPort: number;
    maxBurst: number;
  }>;
}

export class S3TopologyCasCoordinator {
  public static readonly TOPOLOGY_OBJECT_KEY = "control/topology-v1.json";
  private storedJson: string | null = null;
  private storedEtag: string | null = null;
  private readonly pushSubscribers: Array<(manifest: ClusterTopologyManifestV1, etag: string) => void> = [];
  public readonly topologyFilePath: string;

  constructor(
    initialManifest?: ClusterTopologyManifestV1,
    public readonly storageDir: string = "/tmp/ansts_topology_cas",
  ) {
    fs.mkdirSync(path.join(this.storageDir, "control"), { recursive: true });
    this.topologyFilePath = path.join(this.storageDir, S3TopologyCasCoordinator.TOPOLOGY_OBJECT_KEY);

    if (initialManifest) {
      this.storedJson = JSON.stringify(initialManifest, null, 2);
      this.storedEtag = this.computeEtag(this.storedJson);
      this.writeTopologyToDisk(this.storedJson, this.storedEtag);
    }
  }

  public getCurrentTopologyWithEtag(clientKnownEtag?: string): {
    statusCode: 200 | 304 | 404;
    etag: string | null;
    manifest: ClusterTopologyManifestV1 | null;
  } {
    if (!this.storedJson && fs.existsSync(this.topologyFilePath)) {
      this.storedJson = fs.readFileSync(this.topologyFilePath, "utf8");
      this.storedEtag = this.computeEtag(this.storedJson);
    }
    if (!this.storedJson || !this.storedEtag) {
      return { statusCode: 404, etag: null, manifest: null };
    }
    if (clientKnownEtag && clientKnownEtag === this.storedEtag) {
      return { statusCode: 304, etag: this.storedEtag, manifest: null };
    }
    return {
      statusCode: 200,
      etag: this.storedEtag,
      manifest: JSON.parse(this.storedJson) as ClusterTopologyManifestV1,
    };
  }

  public casUpdateTopology(
    nextManifest: ClusterTopologyManifestV1,
    expectedEtag: string | null,
  ): { newEtag: string; generation: number; notifiedSubscribers: number } {
    if (this.storedEtag !== null && expectedEtag !== this.storedEtag) {
      throw new S3CasPreconditionFailedError(
        `412 Precondition Failed on ${S3TopologyCasCoordinator.TOPOLOGY_OBJECT_KEY}: expected ETag ${expectedEtag}, actual ${this.storedEtag}`,
      );
    }

    const serialized = JSON.stringify(nextManifest, null, 2);
    const newEtag = this.computeEtag(serialized);
    this.storedJson = serialized;
    this.storedEtag = newEtag;
    this.writeTopologyToDisk(serialized, newEtag);

    for (const subscriber of this.pushSubscribers) {
      subscriber(nextManifest, newEtag);
    }

    return {
      newEtag,
      generation: nextManifest.generation,
      notifiedSubscribers: this.pushSubscribers.length,
    };
  }

  public subscribePushBroadcast(
    listener: (manifest: ClusterTopologyManifestV1, etag: string) => void,
  ): void {
    this.pushSubscribers.push(listener);
  }

  private writeTopologyToDisk(jsonText: string, etag: string): void {
    const tmpPath = `${this.topologyFilePath}.tmp.${process.pid}`;
    fs.writeFileSync(tmpPath, jsonText, "utf8");
    fs.renameSync(tmpPath, this.topologyFilePath);
    fs.writeFileSync(`${this.topologyFilePath}.etag`, etag, "utf8");
  }

  private computeEtag(payload: string): string {
    const hex = crypto.createHash("sha256").update(payload, "utf8").digest("hex").slice(0, 24);
    return `"${hex}"`;
  }
}
