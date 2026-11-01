/**
 * @fileoverview TASK-76 (Q2): Global S3 CAS Topology Coordinator (`s3://ansts-fleet/control/topology-v1.json`).
 *
 * Provides a Single Source of Truth for Non-K8s manual scale-out fleets:
 * - Conditional ETag CAS updates (`If-Match` / `If-None-Match`) on `control/topology-v1.json`
 * - Short-interval ETag polling (`304 Not Modified` fast-path) + active push broadcast to Edge Workers & `celld` nodes
 */

import * as crypto from "node:crypto";
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

  constructor(initialManifest?: ClusterTopologyManifestV1) {
    if (initialManifest) {
      this.storedJson = JSON.stringify(initialManifest);
      this.storedEtag = this.computeEtag(this.storedJson);
    }
  }

  public getCurrentTopologyWithEtag(clientKnownEtag?: string): {
    statusCode: 200 | 304 | 404;
    etag: string | null;
    manifest: ClusterTopologyManifestV1 | null;
  } {
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

    const serialized = JSON.stringify(nextManifest);
    const newEtag = this.computeEtag(serialized);
    this.storedJson = serialized;
    this.storedEtag = newEtag;

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

  private computeEtag(payload: string): string {
    const hex = crypto.createHash("sha256").update(payload, "utf8").digest("hex").slice(0, 24);
    return `"${hex}"`;
  }
}
