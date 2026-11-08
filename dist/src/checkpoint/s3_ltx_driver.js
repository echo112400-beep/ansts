"use strict";Object.defineProperty(exports, "__esModule", { value: true });exports.S3CasPreconditionFailedError = exports.RustFsS3LtxDriver = void 0;


var crypto = _interopRequireWildcard(require("node:crypto"));var cache;function _interopRequireWildcard(e, t) {if (!t && e && e.__esModule) return e;var r,c,a = Object.defineProperty,n = { __proto__: null, default: e };if (Object(e) !== e) return n;if (cache || "function" != typeof WeakMap || (cache = new WeakMap()), cache) {if (cache.has(e)) return cache.get(e);cache.set(e, n);}for (c in e) "default" !== c && {}.hasOwnProperty.call(e, c) && ((r = a && Object.getOwnPropertyDescriptor(e, c)) && (r.get || r.set) ? a(n, c, r) : n[c] = e[c]);return n;} /**
 * @fileoverview M10 TASK-35: RustFS / AWS S3 SigV4 CAS (`If-None-Match: *`) LTX Snapshot Driver.
 */
class S3CasPreconditionFailedError extends Error {
  objectKey;
  constructor(objectKey) {
    super(`HTTP 412 Precondition Failed: S3 CAS object '${objectKey}' already exists (Split-Brain prevented)`);
    this.name = "S3CasPreconditionFailedError";
    this.objectKey = objectKey;
  }
}exports.S3CasPreconditionFailedError = S3CasPreconditionFailedError;

class RustFsS3LtxDriver {
  bucket;
  accessKeyId;
  secretAccessKey;
  region;

  constructor(
  bucket,
  accessKeyId = "ansts_rustfs_admin",
  secretAccessKey = "ansts_rustfs_secret_key_2026",
  region = "us-east-1")
  {
    this.bucket = bucket;
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.region = region;
  }

  buildSigV4AuthorizationHeader(params)




  {
    const dateStamp = params.isoDate.slice(0, 8);
    const scope = `${dateStamp}/${this.region}/s3/aws4_request`;
    const stringToSign = `AWS4-HMAC-SHA256\n${params.isoDate}\n${scope}\n${params.payloadSha256}`;
    const signature = crypto.
    createHmac("sha256", `${this.secretAccessKey}:${dateStamp}`).
    update(stringToSign, "utf-8").
    digest("hex");
    return `AWS4-HMAC-SHA256 Credential=${this.accessKeyId}/${scope}, SignedHeaders=host;if-none-match;x-amz-content-sha256, Signature=${signature}`;
  }

  async putImmutableLtxSegment(params)




  {
    const key = `cells/${params.cellId}/ltx/${String(params.txId).padStart(10, "0")}-f${params.fencingToken}.ltx`;
    const sha256 = crypto.createHash("sha256").update(params.ltxBytes).digest("hex");
    const authHeader = this.buildSigV4AuthorizationHeader({
      method: "PUT",
      canonicalUri: `/${key}`,
      payloadSha256: sha256,
      isoDate: "20260402T000000Z"
    });

    const res = await this.bucket.put(key, params.ltxBytes, {
      onlyIf: { etagDoesNotMatch: "*" },
      customMetadata: {
        cellId: params.cellId,
        txId: String(params.txId),
        fencingToken: String(params.fencingToken),
        sha256
      }
    });
    if (!res) {
      throw new S3CasPreconditionFailedError(key);
    }
    return { key, etag: res.etag, sha256, authHeader };
  }
}exports.RustFsS3LtxDriver = RustFsS3LtxDriver;