/**
 * @fileoverview M10 TASK-35: RustFS / AWS S3 SigV4 CAS (`If-None-Match: *`) LTX Snapshot Driver.
 */
import * as crypto from "node:crypto";
import { R2BucketLike } from "../cloudflare/env.js";

export class S3CasPreconditionFailedError extends Error {
  public readonly objectKey: string;
  constructor(objectKey: string) {
    super(`HTTP 412 Precondition Failed: S3 CAS object '${objectKey}' already exists (Split-Brain prevented)`);
    this.name = "S3CasPreconditionFailedError";
    this.objectKey = objectKey;
  }
}

export class RustFsS3LtxDriver {
  private readonly bucket: R2BucketLike;
  private readonly accessKeyId: string;
  private readonly secretAccessKey: string;
  private readonly region: string;

  constructor(
    bucket: R2BucketLike,
    accessKeyId = "ansts_rustfs_admin",
    secretAccessKey = "ansts_rustfs_secret_key_2026",
    region = "us-east-1"
  ) {
    this.bucket = bucket;
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.region = region;
  }

  public buildSigV4AuthorizationHeader(params: {
    method: string;
    canonicalUri: string;
    payloadSha256: string;
    isoDate: string;
  }): string {
    const dateStamp = params.isoDate.slice(0, 8);
    const scope = `${dateStamp}/${this.region}/s3/aws4_request`;
    const stringToSign = `AWS4-HMAC-SHA256\n${params.isoDate}\n${scope}\n${params.payloadSha256}`;
    const signature = crypto
      .createHmac("sha256", `${this.secretAccessKey}:${dateStamp}`)
      .update(stringToSign, "utf-8")
      .digest("hex");
    return `AWS4-HMAC-SHA256 Credential=${this.accessKeyId}/${scope}, SignedHeaders=host;if-none-match;x-amz-content-sha256, Signature=${signature}`;
  }

  public async putImmutableLtxSegment(params: {
    cellId: string;
    txId: number;
    fencingToken: number;
    ltxBytes: Uint8Array;
  }): Promise<{ key: string; etag: string; sha256: string; authHeader: string }> {
    const key = `cells/${params.cellId}/ltx/${String(params.txId).padStart(10, "0")}-f${params.fencingToken}.ltx`;
    const sha256 = crypto.createHash("sha256").update(params.ltxBytes).digest("hex");
    const authHeader = this.buildSigV4AuthorizationHeader({
      method: "PUT",
      canonicalUri: `/${key}`,
      payloadSha256: sha256,
      isoDate: "20260402T000000Z",
    });

    const res = await this.bucket.put(key, params.ltxBytes, {
      onlyIf: { etagDoesNotMatch: "*" },
      customMetadata: {
        cellId: params.cellId,
        txId: String(params.txId),
        fencingToken: String(params.fencingToken),
        sha256,
      },
    });
    if (!res) {
      throw new S3CasPreconditionFailedError(key);
    }
    return { key, etag: res.etag, sha256, authHeader };
  }
}
