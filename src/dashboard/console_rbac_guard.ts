/**
 * @fileoverview M36 P0 (TASK-151): Control-Plane OIDC / HMAC-SHA256 JWT Role-Based Access Control
 * (`ConsoleRbacAuthGuard`) & Hash-Chained Tamper-Evident Audit Log.
 *
 * Roles & Permissions Matrix:
 * - `VIEWER`: read-only (`console:read`)
 * - `OPERATOR`: `console:read`, `tasks:dispatch`, `hitl:approve`, `tunnels:drain`
 * - `SRE_ADMIN`: all permissions (`*`, including `slots:migrate`, `canary:rollback`, `spot:evacuate`, `chaos:run`, `gc:sweep`)
 */

import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";

export type ConsoleRole = "VIEWER" | "OPERATOR" | "SRE_ADMIN";

export type ConsolePermission =
  | "console:read"
  | "tasks:dispatch"
  | "hitl:approve"
  | "tunnels:drain"
  | "slots:migrate"
  | "canary:rollback"
  | "spot:evacuate"
  | "chaos:run"
  | "gc:sweep";

export interface OperatorJwtClaims {
  sub: string;
  role: ConsoleRole;
  tenantScope: string;
  iat: number;
  exp: number;
}

export interface RbacAuditLogEntry {
  seq: number;
  operatorId: string;
  role: ConsoleRole;
  permission: ConsolePermission;
  allowed: boolean;
  prevChainHash: string;
  entryChainHash: string;
  timestampMs: number;
}

const ROLE_PERMISSION_MAP: Record<ConsoleRole, Set<ConsolePermission>> = {
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

export function permissionsForConsoleRole(role: ConsoleRole): ReadonlySet<ConsolePermission> {
  return ROLE_PERMISSION_MAP[role] ?? new Set();
}

export class ConsoleRbacForbiddenError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ConsoleRbacForbiddenError";
  }
}

export class ConsoleRbacAuthGuard {
  private readonly hmacSecret: Buffer;
  public readonly auditLog: RbacAuditLogEntry[] = [];
  public lastChainHash = "0000000000000000000000000000000000000000000000000000000000000000";

  constructor(
    secret: string = "ansts_v27_console_oidc_rbac_hmac_secret",
    public readonly auditFilePath: string = "/tmp/ansts_rbac_audit/console_audit.jsonl",
  ) {
    this.hmacSecret = Buffer.from(secret, "utf8");
    fs.mkdirSync(path.dirname(this.auditFilePath), { recursive: true });
  }

  public issueOperatorJwt(params: {
    operatorId: string;
    role: ConsoleRole;
    tenantScope?: string;
    ttlSeconds?: number;
  }): string {
    const headerB64 = Buffer.from(JSON.stringify({ alg: "HS256", typ: "JWT" })).toString("base64url");
    const nowSec = Math.floor(Date.now() / 1000);
    const claims: OperatorJwtClaims = {
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

  public verifyAndAuthorizeAction(
    jwtToken: string,
    requiredPermission: ConsolePermission,
  ): { claims: OperatorJwtClaims; auditEntry: RbacAuditLogEntry } {
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

    const claims = JSON.parse(Buffer.from(payloadB64, "base64url").toString("utf8")) as OperatorJwtClaims;
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

    const auditEntry: RbacAuditLogEntry = {
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
    } finally {
      fs.closeSync(fd);
    }

    if (!allowed) {
      throw new ConsoleRbacForbiddenError(
        `403 Forbidden: Role '${claims.role}' (sub: ${claims.sub}) lacks permission '${requiredPermission}'`,
      );
    }

    return { claims, auditEntry };
  }
}
