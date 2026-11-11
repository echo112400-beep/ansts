"use strict";
/**
 * @fileoverview M36 P0 (TASK-151): Control-Plane OIDC / HMAC-SHA256 JWT Role-Based Access Control
 * (`ConsoleRbacAuthGuard`) & Hash-Chained Tamper-Evident Audit Log.
 *
 * Roles & Permissions Matrix:
 * - `VIEWER`: read-only (`console:read`)
 * - `OPERATOR`: `console:read`, `tasks:dispatch`, `hitl:approve`, `tunnels:drain`
 * - `SRE_ADMIN`: all permissions (`*`, including `slots:migrate`, `canary:rollback`, `spot:evacuate`, `chaos:run`, `gc:sweep`)
 */
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
exports.ConsoleRbacAuthGuard = exports.ConsoleRbacForbiddenError = void 0;
const crypto = __importStar(require("node:crypto"));
const fs = __importStar(require("node:fs"));
const path = __importStar(require("node:path"));
const ROLE_PERMISSION_MAP = {
    VIEWER: new Set(["console:read"]),
    OPERATOR: new Set(["console:read", "tasks:dispatch", "hitl:approve", "tunnels:drain"]),
    SRE_ADMIN: new Set([
        "console:read",
        "tasks:dispatch",
        "hitl:approve",
        "tunnels:drain",
        "slots:migrate",
        "canary:rollback",
        "spot:evacuate",
        "chaos:run",
        "gc:sweep",
    ]),
};
class ConsoleRbacForbiddenError extends Error {
    constructor(message) {
        super(message);
        this.name = "ConsoleRbacForbiddenError";
    }
}
exports.ConsoleRbacForbiddenError = ConsoleRbacForbiddenError;
class ConsoleRbacAuthGuard {
    auditFilePath;
    hmacSecret;
    auditLog = [];
    lastChainHash = "0000000000000000000000000000000000000000000000000000000000000000";
    constructor(secret = "ansts_v27_console_oidc_rbac_hmac_secret", auditFilePath = "/tmp/ansts_rbac_audit/console_audit.jsonl") {
        this.auditFilePath = auditFilePath;
        this.hmacSecret = Buffer.from(secret, "utf8");
        fs.mkdirSync(path.dirname(this.auditFilePath), { recursive: true });
    }
    issueOperatorJwt(params) {
        const headerB64 = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
        const nowSec = Math.floor(Date.now() / 1000);
        const claims = {
            sub: params.operatorId,
            role: params.role,
            tenantScope: params.tenantScope ?? "*",
            iat: nowSec,
            exp: nowSec + (params.ttlSeconds ?? 3600),
        };
        const payloadB64 = Buffer.from(JSON.stringify(claims)).toString("base64url");
        const sig = crypto
            .createHmac("sha256", this.hmacSecret)
            .update(`${headerB64}.${payloadB64}`, "utf8")
            .digest("base64url");
        return `${headerB64}.${payloadB64}.${sig}`;
    }
    verifyAndAuthorizeAction(jwtToken, requiredPermission) {
        const parts = jwtToken.split(".");
        if (parts.length !== 3) {
            throw new ConsoleRbacForbiddenError("401 MalformedOperatorJwt");
        }
        const [headerB64, payloadB64, signature] = parts;
        const expectedSig = crypto
            .createHmac("sha256", this.hmacSecret)
            .update(`${headerB64}.${payloadB64}`, "utf8")
            .digest("base64url");
        if (signature !== expectedSig) {
            throw new ConsoleRbacForbiddenError("401 InvalidJwtHmacSignature");
        }
        const claims = JSON.parse(Buffer.from(payloadB64, "base64url").toString("utf8"));
        const nowSec = Math.floor(Date.now() / 1000);
        if (nowSec > claims.exp) {
            throw new ConsoleRbacForbiddenError("401 ExpiredOperatorJwt");
        }
        const allowedSet = ROLE_PERMISSION_MAP[claims.role];
        const allowed = Boolean(allowedSet && allowedSet.has(requiredPermission));
        const seq = this.auditLog.length + 1;
        const prevChainHash = this.lastChainHash;
        const timestampMs = Date.now();
        const entryChainHash = crypto
            .createHash("sha256")
            .update(`${seq}|${claims.sub}|${claims.role}|${requiredPermission}|${allowed}|${prevChainHash}|${timestampMs}`)
            .digest("hex");
        const auditEntry = {
            seq,
            operatorId: claims.sub,
            role: claims.role,
            permission: requiredPermission,
            allowed,
            prevChainHash,
            entryChainHash,
            timestampMs,
        };
        this.lastChainHash = entryChainHash;
        this.auditLog.push(auditEntry);
        const fd = fs.openSync(this.auditFilePath, "a", 0o644);
        try {
            fs.writeFileSync(fd, `${JSON.stringify(auditEntry)}\n`, "utf8");
            fs.fsyncSync(fd);
        }
        finally {
            fs.closeSync(fd);
        }
        if (!allowed) {
            throw new ConsoleRbacForbiddenError(`403 Forbidden: Role '${claims.role}' (sub: ${claims.sub}) lacks permission '${requiredPermission}'`);
        }
        return { claims, auditEntry };
    }
}
exports.ConsoleRbacAuthGuard = ConsoleRbacAuthGuard;
