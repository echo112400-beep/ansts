/**
 * @fileoverview Minimal Path-Style S3 Client with Real AWS SigV4 Request Signing
 * (`AWS4-HMAC-SHA256`) for RustFS / S3 cross-node state synchronization.
 *
 * The control plane previously issued plain unsigned `fetch` calls against RustFS,
 * which the server rejects with HTTP 403 `AccessDenied`; every cross-gateway sync
 * silently degraded to `false` and each node kept divergent in-memory state.
 * Requests are signed when credentials are configured and sent anonymously otherwise
 * (anonymous-access S3 endpoints and in-test S3 doubles).
 */
import * as crypto from "node:crypto";

export interface S3SigV4Credentials {
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
}

export interface S3Response {
  ok: boolean;
  status: number;
  body: string | null;
}

const EMPTY_SHA256 = crypto.createHash("sha256").update("").digest("hex");
const REQUEST_TIMEOUT_MS = 5000;

/** RFC 3986 percent-encoding for a single path segment (`S3` canonical rules). */
function encodeSegment(segment: string): string {
  return encodeURIComponent(segment).replace(
    /[!'()*]/g,
    (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`
  );
}

function canonicalUriOf(path: string): string {
  return path
    .split("/")
    .map((segment) => encodeSegment(segment))
    .join("/");
}

export class S3SigV4Client {
  private readonly endpoint: string;
  private readonly credentials: S3SigV4Credentials | null;
  private readonly ensuredBuckets = new Set<string>();

  constructor(endpoint: string, credentials: S3SigV4Credentials | null = null) {
    this.endpoint = endpoint.trim().replace(/\/+$/, "");
    this.credentials = credentials;
  }

  public async ensureBucket(bucket: string): Promise<boolean> {
    if (this.ensuredBuckets.has(bucket)) return true;
    const head = await this.request("HEAD", `/${bucket}`, Buffer.alloc(0), {});
    if (head.ok) {
      this.ensuredBuckets.add(bucket);
      return true;
    }
    if (head.status === 403) return false;
    const create = await this.request("PUT", `/${bucket}`, Buffer.alloc(0), {});
    if (create.ok || create.status === 409) {
      this.ensuredBuckets.add(bucket);
      return true;
    }
    return false;
  }

  public async getObject(bucket: string, key: string): Promise<S3Response> {
    if (!(await this.ensureBucket(bucket))) {
      return { ok: false, status: 404, body: null };
    }
    return this.request("GET", `/${bucket}/${canonicalUriOf(key).replace(/^\//, "")}`, Buffer.alloc(0), {});
  }

  public async putObject(
    bucket: string,
    key: string,
    body: string,
    contentType = "application/json"
  ): Promise<S3Response> {
    if (!(await this.ensureBucket(bucket))) {
      return { ok: false, status: 404, body: null };
    }
    return this.request(
      "PUT",
      `/${bucket}/${canonicalUriOf(key).replace(/^\//, "")}`,
      Buffer.from(body, "utf-8"),
      { "content-type": contentType }
    );
  }

  private async request(
    method: string,
    canonicalPath: string,
    payload: Buffer,
    extraHeaders: Record<string, string>
  ): Promise<S3Response> {
    const headers: Record<string, string> = { ...extraHeaders };
    if (this.credentials) {
      const payloadHash =
        payload.length === 0 ? EMPTY_SHA256 : crypto.createHash("sha256").update(payload).digest("hex");
      const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, "");
      const dateStamp = amzDate.slice(0, 8);
      const host = new URL(this.endpoint).host;

      const signableHeaders: Record<string, string> = {
        host,
        "x-amz-content-sha256": payloadHash,
        "x-amz-date": amzDate,
      };
      const signedHeaderNames = Object.keys(signableHeaders).sort();
      const canonicalHeaders = signedHeaderNames.map((h) => `${h}:${signableHeaders[h]}\n`).join("");
      const signedHeaders = signedHeaderNames.join(";");

      const canonicalRequest = [
        method,
        canonicalPath,
        "",
        canonicalHeaders,
        signedHeaders,
        payloadHash,
      ].join("\n");
      const scope = `${dateStamp}/${this.credentials.region}/s3/aws4_request`;
      const stringToSign = [
        "AWS4-HMAC-SHA256",
        amzDate,
        scope,
        crypto.createHash("sha256").update(canonicalRequest, "utf-8").digest("hex"),
      ].join("\n");
      const signingKey = this.deriveSigningKey(dateStamp);
      const signature = crypto.createHmac("sha256", signingKey).update(stringToSign, "utf-8").digest("hex");

      headers["x-amz-content-sha256"] = payloadHash;
      headers["x-amz-date"] = amzDate;
      headers.authorization =
        `AWS4-HMAC-SHA256 Credential=${this.credentials.accessKeyId}/${scope}, ` +
        `SignedHeaders=${signedHeaders}, Signature=${signature}`;
    }

    try {
      const resp = await fetch(`${this.endpoint}${canonicalPath}`, {
        method,
        headers,
        body: method === "GET" || method === "HEAD" ? undefined : payload,
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      const body = method === "HEAD" ? null : await resp.text();
      return { ok: resp.ok, status: resp.status, body };
    } catch {
      return { ok: false, status: 0, body: null };
    }
  }

  private deriveSigningKey(dateStamp: string): Buffer {
    const secretAccessKey = this.credentials?.secretAccessKey ?? "";
    const region = this.credentials?.region ?? "us-east-1";
    const kDate = crypto
      .createHmac("sha256", `AWS4${secretAccessKey}`)
      .update(dateStamp, "utf-8")
      .digest();
    const kRegion = crypto.createHmac("sha256", kDate).update(region, "utf-8").digest();
    const kService = crypto.createHmac("sha256", kRegion).update("s3", "utf-8").digest();
    return crypto.createHmac("sha256", kService).update("aws4_request", "utf-8").digest();
  }
}
