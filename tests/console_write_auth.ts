/**
 * Test-only bearer tokens for console writes. The secret is a fixture, not a deployment secret.
 */
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";

export const TEST_CONSOLE_RBAC_SECRET = "ci-console-rbac-test-secret";

const guard = new ConsoleRbacAuthGuard(TEST_CONSOLE_RBAC_SECRET, `/tmp/ansts-console-rbac-${process.pid}.jsonl`);

export const consoleTokens = {
  admin: guard.issueOperatorJwt({ operatorId: "chen", role: "SRE_ADMIN" }),
  zhou: guard.issueOperatorJwt({ operatorId: "zhou", role: "SRE_ADMIN" }),
  viewer: guard.issueOperatorJwt({ operatorId: "xu", role: "VIEWER" }),
  operator: guard.issueOperatorJwt({ operatorId: "lin", role: "OPERATOR" }),
};

process.env.ANSTS_CONSOLE_RBAC_SECRET = TEST_CONSOLE_RBAC_SECRET;
process.env.ANSTS_CONSOLE_TOKEN = consoleTokens.admin;

const rawFetch = globalThis.fetch.bind(globalThis);
globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
  const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
  const method = String(init?.method ?? (typeof input !== "string" && !(input instanceof URL) ? input.method : "GET")).toUpperCase();
  const guarded =
    method !== "GET" &&
    method !== "HEAD" &&
    (url.includes("/v1beta1/management/") || url.includes("/v1beta1/console/"));
  if (!guarded) return rawFetch(input, init);
  const headers = new Headers(init?.headers);
  if (!headers.has("authorization") && process.env.ANSTS_CONSOLE_TOKEN) {
    headers.set("authorization", `Bearer ${process.env.ANSTS_CONSOLE_TOKEN}`);
  }
  return rawFetch(input, { ...init, headers });
}) as typeof fetch;
