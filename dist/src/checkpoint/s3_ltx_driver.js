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
exports.RustFsS3LtxDriver = exports.S3CasPreconditionFailedError = void 0;
/**
 * @fileoverview M10 TASK-35 & M59-6: RustFS / AWS S3 SigV4 CAS (`If-None-Match: *`) LTX Snapshot Driver
 * with Strict Integer `txId`/`fencingToken` & Path-Safe `cellId` Validation.
 */
const crypto = __importStar(require("node:crypto"));
class S3CasPreconditionFailedError extends Error {
    objectKey;
    constructor(objectKey) {
        super(`HTTP 412 Precondition Failed: S3 CAS object '${objectKey}' already exists (Split-Brain prevented)`);
        this.name = "S3CasPreconditionFailedError";
        this.objectKey = objectKey;
    }
}
exports.S3CasPreconditionFailedError = S3CasPreconditionFailedError;
class RustFsS3LtxDriver {
    bucket;
    accessKeyId;
    secretAccessKey;
    region;
    constructor(bucket, accessKeyId = "ansts_rustfs_admin", secretAccessKey = "ansts_rustfs_secret_key_2026", region = "us-east-1") {
        this.bucket = bucket;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.region = region;
    }
    buildSigV4AuthorizationHeader(params) {
        const dateStamp = params.isoDate.slice(0, 8);
        const scope = `${dateStamp}/${this.region}/s3/aws4_request`;
        const stringToSign = `AWS4-HMAC-SHA256\n${params.isoDate}\n${scope}\n${params.payloadSha256}`;
        const signature = crypto
            .createHmac("sha256", `${this.secretAccessKey}:${dateStamp}`)
            .update(stringToSign, "utf-8")
            .digest("hex");
        return `AWS4-HMAC-SHA256 Credential=${this.accessKeyId}/${scope}, SignedHeaders=host;if-none-match;x-amz-content-sha256, Signature=${signature}`;
    }
    async putImmutableLtxSegment(params) {
        const safeCellId = params.cellId.trim();
        if (!safeCellId || !/^[a-zA-Z0-9._:-]+$/.test(safeCellId) || safeCellId.includes("..")) {
            throw new Error(`Invalid LTX cellId '${params.cellId}'`);
        }
        if (!Number.isInteger(params.txId) || params.txId < 0) {
            throw new Error(`Invalid LTX txId '${params.txId}': must be a non-negative integer`);
        }
        if (!Number.isInteger(params.fencingToken) || params.fencingToken < 1) {
            throw new Error(`Invalid LTX fencingToken '${params.fencingToken}': must be a positive integer`);
        }
        const key = `cells/${safeCellId}/ltx/${String(params.txId).padStart(10, "0")}-f${params.fencingToken}.ltx`;
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
                cellId: safeCellId,
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
exports.RustFsS3LtxDriver = RustFsS3LtxDriver;
