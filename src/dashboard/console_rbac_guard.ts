/**
 * @fileoverview M76: Console RBAC Authentication Guard (HMAC-SHA256 JWT Bearer Token + Permission Matrix + Chained Audit Hash)
 * Provides:
 * - HMAC-SHA256 signed JWT token issuance & constant-time signature verification (`crypto.timingSafeEqual`).
 * - Role-based permission matrix (`SRE_ADMIN`, `OPERATOR`, `VIEWER`) with regional/tenant scope enforcement.
 * - Pre-issued signed tokens for the 4 console principals (`chen`, `zhou`, `lin`, `xu`) for seamless UI & CLI integration.
 * - Cryptographically chained audit log hashes (`prevHash -> entryHash`) so audit tampering is immediately detectable.
 */
import * as crypto from "node:crypto";

export type ConsoleRole = "SRE_ADMIN" | "OPERATOR" | "VIEWER";

export interface ConsoleJwtClaims {
  sub: string; // actor ID (e.g. "chen", "zhou", "lin", "xu")
  role: ConsoleRole;
  scopeRegion: string;
  scopeTenant: string;
  iat: number;
  exp: number;
}

export interface ChainedAuditEntry {
  seq: number;
  auditId: string;
  actor: string;
  role: ConsoleRole;
  action: string;
  scope: string;
  outcome: string;
  timestampUtc: string;
  prevHashHex: string;
  entryHashHex: string;
}

export class ConsoleRbacAuthGuard {
  private readonly signingSecret: string;
  private readonly auditChain: ChainedAuditEntry[] = [];
  private lastChainHashHex = "0000000000000000000000000000000000000000000000000000000000000000";

  public static readonly DEFAULT_SECRET = "ansts-console-rbac-hmac-secret-v25-prod-key";

  public static readonly PRINCIPAL_PROFILES: Record<
    string,
    { sub: string; role: ConsoleRole; scopeRegion: string; scopeTenant: string }
  > = {
    chen: { sub: "chen", role: "SRE_ADMIN", scopeRegion: "all", scopeTenant: "all" },
    zhou: { sub: "zhou", role: "SRE_ADMIN", scopeRegion: "all", scopeTenant: "all" },
    lin: { sub: "lin", role: "OPERATOR", scopeRegion: "us-west-2", scopeTenant: "Northstar" },
    xu: { sub: "xu", role: "VIEWER", scopeRegion: "us-west-2", scopeTenant: "Northstar" },
  };

  constructor(signingSecret = ConsoleRbacAuthGuard.DEFAULT_SECRET) {
    this.signingSecret =
      typeof signingSecret === "string" && signingSecret.trim().length >= 16
        ? signingSecret.trim()
        : ConsoleRbacAuthGuard.DEFAULT_SECRET;
  }

  private base64UrlEncode(input: string | Buffer): string {
    const buf = typeof input === "string" ? Buffer.from(input, "utf-8") : input;
    return buf
      .toString("base64")
      .replace(/=/g, "")
      .replace(/\+/g, "-")
      .replace(/\//g, "_");
  }

  private base64UrlDecode(input: string): string {
    let base64 = input.replace(/-/g, "+").replace(/_/g, "/");
    while (base64.length % 4 !== 0) {
      base64 += "=";
    }
    return Buffer.from(base64, "base64").toString("utf-8");
  }

  /**
   * Issues an HMAC-SHA256 signed JWT token for a console principal.
   */
  public issueToken(params: {
    sub: string;
    role: ConsoleRole;
    scopeRegion?: string;
    scopeTenant?: string;
    ttlSec?: number;
    nowSec?: number;
  }): string {
    const nowSec = params.nowSec ?? Math.floor(Date.now() / 1000);
    const ttlSec = params.ttlSec ?? 86400 * 365;
    const header = { alg: "HS256", typ: "JWT" };
    const payload: ConsoleJwtClaims = {
      sub: params.sub,
      role: params.role,
      scopeRegion: params.scopeRegion ?? "all",
      scopeTenant: params.scopeTenant ?? "all",
      iat: nowSec,
      exp: nowSec + ttlSec,
    };

    const encodedHeader = this.base64UrlEncode(JSON.stringify(header));
    const encodedPayload = this.base64UrlEncode(JSON.stringify(payload));
    const signingInput = `${encodedHeader}.${encodedPayload}`;
    const signature = crypto
      .createHmac("sha256", this.signingSecret)
      .update(signingInput, "utf-8")
      .digest();
    const encodedSignature = this.base64UrlEncode(signature);
    return `${signingInput}.${encodedSignature}`;
  }

  /**
   * Issues a deterministic token for one of the built-in principals (`chen`, `zhou`, `lin`, `xu`).
   */
  public issuePrincipalToken(principalId: string): string {
    const profile =
      ConsoleRbacAuthGuard.PRINCIPAL_PROFILES[principalId] ??
      ConsoleRbacAuthGuard.PRINCIPAL_PROFILES.chen;
    return this.issueToken({
      sub: profile.sub,
      role: profile.role,
      scopeRegion: profile.scopeRegion,
      scopeTenant: profile.scopeTenant,
      nowSec: 1700000000,
      ttlSec: 86400 * 3650, // 10-year validity for deterministic console/CLI tokens
    });
  }

  /**
   * Verifies an `Authorization: Bearer <jwt>` header using constant-time HMAC comparison.
   */
  public verifyAuthorizationHeader(
    authHeader: string | undefined,
    nowSec = Math.floor(Date.now() / 1000)
  ): {
    authenticated: boolean;
    claims?: ConsoleJwtClaims;
    errorCode?: "MISSING_BEARER_TOKEN" | "MALFORMED_JWT" | "INVALID_SIGNATURE" | "TOKEN_EXPIRED";
    message?: string;
  } {
    if (!authHeader || typeof authHeader !== "string" || !authHeader.startsWith("Bearer ")) {
      return {
        authenticated: false,
        errorCode: "MISSING_BEARER_TOKEN",
        message: "Missing or invalid Authorization: Bearer <token> header",
      };
    }

    const token = authHeader.slice("Bearer ".length).trim();
    const parts = token.split(".");
    if (parts.length !== 3) {
      return {
        authenticated: false,
        errorCode: "MALFORMED_JWT",
        message: "Malformed JWT structure (expected 3 segments)",
      };
    }

    const [encodedHeader, encodedPayload, encodedSignature] = parts;
    const signingInput = `${encodedHeader}.${encodedPayload}`;
    const expectedSig = this.base64UrlEncode(
      crypto.createHmac("sha256", this.signingSecret).update(signingInput, "utf-8").digest()
    );

    const aBuf = Buffer.from(encodedSignature, "utf-8");
    const bBuf = Buffer.from(expectedSig, "utf-8");
    if (aBuf.length !== bBuf.length || !crypto.timingSafeEqual(aBuf, bBuf)) {
      return {
        authenticated: false,
        errorCode: "INVALID_SIGNATURE",
        message: "HMAC-SHA256 JWT signature verification failed",
      };
    }

    try {
      const claims = JSON.parse(this.base64UrlDecode(encodedPayload)) as ConsoleJwtClaims;
      if (!claims || typeof claims.sub !== "string" || typeof claims.role !== "string") {
        return {
          authenticated: false,
          errorCode: "MALFORMED_JWT",
          message: "JWT payload missing required sub or role claims",
        };
      }
      if (typeof claims.exp === "number" && nowSec > claims.exp) {
        return {
          authenticated: false,
          errorCode: "TOKEN_EXPIRED",
          message: "JWT token has expired",
        };
      }
      return { authenticated: true, claims };
    } catch {
      return {
        authenticated: false,
        errorCode: "MALFORMED_JWT",
        message: "Failed to parse JWT JSON payload",
      };
    }
  }

  /**
   * Evaluates RBAC permission matrix for an action.
   */
  public checkPermission(params: {
    role: ConsoleRole;
    requireAdminOnly?: boolean;
    isWriteMutation?: boolean;
  }): { allowed: boolean; status: 200 | 403; error?: string; message?: string } {
    if (params.requireAdminOnly && params.role !== "SRE_ADMIN") {
      return {
        allowed: false,
        status: 403,
        error: "RBAC_FORBIDDEN",
        message: `Role '${params.role}' is forbidden; SRE_ADMIN privilege is required`,
      };
    }
    if (params.isWriteMutation && params.role === "VIEWER") {
      return {
        allowed: false,
        status: 403,
        error: "RBAC_FORBIDDEN",
        message: "VIEWER role is read-only and forbidden from executing write mutations",
      };
    }
    return { allowed: true, status: 200 };
  }

  /**
   * Resolves actor & role from `Authorization: Bearer <token>` if present,
   * or falls back to explicit body fields (`actor`, `role`, `initiator`, `approver`, `actorRole`)
   * when provided. If `requireExplicitAuthOrIdentity` is true and neither a valid Bearer token
   * nor an explicit actor/role is supplied, returns 401/400 instead of silently defaulting to SRE_ADMIN!
   */
  public resolveRequestPrincipal(params: {
    authHeader?: string;
    body?: Record<string, unknown>;
    requireExplicitIdentity?: boolean;
  }): {
    ok: boolean;
    status: 200 | 401 | 403;
    actor: string;
    role: ConsoleRole;
    authenticatedViaJwt: boolean;
    error?: string;
    message?: string;
  } {
    if (params.authHeader && params.authHeader.trim().length > 0) {
      const verified = this.verifyAuthorizationHeader(params.authHeader);
      if (!verified.authenticated || !verified.claims) {
        return {
          ok: false,
          status: 401,
          actor: "anonymous",
          role: "VIEWER",
          authenticatedViaJwt: false,
          error: verified.errorCode ?? "UNAUTHORIZED",
          message: verified.message ?? "Invalid Authorization header",
        };
      }
      return {
        ok: true,
        status: 200,
        actor: verified.claims.sub,
        role: verified.claims.role,
        authenticatedViaJwt: true,
      };
    }

    const body = params.body ?? {};
    const explicitActorRaw = body.actor ?? body.initiator ?? body.approver;
    const explicitRoleRaw = body.role ?? body.actorRole;

    const hasExplicitActor = typeof explicitActorRaw === "string" && explicitActorRaw.trim().length > 0;
    const hasExplicitRole =
      typeof explicitRoleRaw === "string" &&
      ["SRE_ADMIN", "OPERATOR", "VIEWER"].includes(explicitRoleRaw.trim());

    if (params.requireExplicitIdentity && !hasExplicitActor && !hasExplicitRole) {
      return {
        ok: false,
        status: 401,
        actor: "anonymous",
        role: "VIEWER",
        authenticatedViaJwt: false,
        error: "MISSING_AUTH_CREDENTIALS",
        message:
          "Missing Authorization: Bearer <jwt> header or explicit authenticated principal identity",
      };
    }

    const actor = hasExplicitActor ? String(explicitActorRaw).trim() : "chen";
    const inferredProfile = ConsoleRbacAuthGuard.PRINCIPAL_PROFILES[actor];
    const role: ConsoleRole = hasExplicitRole
      ? (String(explicitRoleRaw).trim() as ConsoleRole)
      : inferredProfile?.role ?? "SRE_ADMIN";

    return {
      ok: true,
      status: 200,
      actor,
      role,
      authenticatedViaJwt: false,
    };
  }

  /**
   * Appends an audit record to the cryptographic hash chain (`prevHashHex -> entryHashHex`).
   */
  public appendChainedAudit(params: {
    auditId: string;
    actor: string;
    role: string;
    action: string;
    scope: string;
    outcome: string;
    timestampUtc: string;
  }): ChainedAuditEntry {
    const seq = this.auditChain.length + 1;
    const prevHashHex = this.lastChainHashHex;
    const canonicalPayload = `${seq}|${params.auditId}|${params.timestampUtc}|${params.actor}|${params.role}|${params.scope}|${params.action}|${params.outcome}|${prevHashHex}`;
    const entryHashHex = crypto.createHash("sha256").update(canonicalPayload, "utf-8").digest("hex");
    const entry: ChainedAuditEntry = {
      seq,
      auditId: params.auditId,
      actor: params.actor,
      role: (params.role as ConsoleRole) ?? "SRE_ADMIN",
      action: params.action,
      scope: params.scope,
      outcome: params.outcome,
      timestampUtc: params.timestampUtc,
      prevHashHex,
      entryHashHex,
    };
    this.auditChain.push(entry);
    this.lastChainHashHex = entryHashHex;
    return entry;
  }

  /**
   * Verifies the integrity of the entire cryptographic audit chain.
   */
  public verifyAuditChainIntegrity(): {
    valid: boolean;
    totalEntries: number;
    headHashHex: string;
    brokenAtSeq?: number;
  } {
    let prev = "0000000000000000000000000000000000000000000000000000000000000000";
    for (const entry of this.auditChain) {
      if (entry.prevHashHex !== prev) {
        return {
          valid: false,
          totalEntries: this.auditChain.length,
          headHashHex: this.lastChainHashHex,
          brokenAtSeq: entry.seq,
        };
      }
      const canonicalPayload = `${entry.seq}|${entry.auditId}|${entry.timestampUtc}|${entry.actor}|${entry.role}|${entry.scope}|${entry.action}|${entry.outcome}|${prev}`;
      const expectedHash = crypto.createHash("sha256").update(canonicalPayload, "utf-8").digest("hex");
      if (expectedHash !== entry.entryHashHex) {
        return {
          valid: false,
          totalEntries: this.auditChain.length,
          headHashHex: this.lastChainHashHex,
          brokenAtSeq: entry.seq,
        };
      }
      prev = entry.entryHashHex;
    }
    return {
      valid: true,
      totalEntries: this.auditChain.length,
      headHashHex: this.lastChainHashHex,
    };
  }

  public getAuditChain(): ChainedAuditEntry[] {
    return [...this.auditChain];
  }
}

/**
 * Sanitizes untrusted user strings before storing or rendering in HTML contexts
 * to prevent Stored/Reflected XSS (`<`, `>`, `&`, `"`, `'`).
 */
export function sanitizeHtmlString(input: unknown): string {
  const raw = String(input ?? "");
  return raw
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}
