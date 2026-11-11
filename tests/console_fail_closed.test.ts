/**
 * Console fail-closed checks: missing write fields, missing bearer, escaped markup, no fake success.
 */
import { describe, it, before, after } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as http from "node:http";
import * as vm from "node:vm";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";

const SECRET = "ci-console-rbac-test-secret";

function requestJson(
  port: number,
  method: string,
  path: string,
  body: string | undefined,
  headers: Record<string, string>,
): Promise<{ status: number; json: Record<string, unknown> }> {
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: "127.0.0.1", port, method, path, headers },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () => {
          const raw = Buffer.concat(chunks).toString("utf8");
          resolve({ status: res.statusCode ?? 0, json: raw ? (JSON.parse(raw) as Record<string, unknown>) : {} });
        });
      },
    );
    req.on("error", reject);
    if (body) req.write(body);
    req.end();
  });
}

describe("console writes fail closed", () => {
  let server: AnstsLocalAndComposeServer;
  let port = 0;
  let admin = "";

  before(async () => {
    process.env.ANSTS_CONSOLE_RBAC_SECRET = SECRET;
    const guard = new ConsoleRbacAuthGuard(SECRET, `/tmp/ansts-console-fail-closed-${process.pid}.jsonl`);
    admin = guard.issueOperatorJwt({ operatorId: "chen", role: "SRE_ADMIN" });
    const tmpDir = `/tmp/ansts-console-fail-closed-${process.pid}-${Date.now()}`;
    fs.mkdirSync(tmpDir, { recursive: true });
    server = new AnstsLocalAndComposeServer({ host: "127.0.0.1", nodeId: "celld-gw-01", nodeRole: "gateway", storageDir: tmpDir });
    const started = await server.start(0, 0);
    port = started.apiPort;
  });

  after(async () => {
    await server.stop();
  });

  it("rejects an empty cancel without inventing a task id", async () => {
    const res = await requestJson(port, "POST", "/v1beta1/management/tasks/cancel", "{}", {
      "content-type": "application/json",
      authorization: `Bearer ${admin}`,
    });
    assert.equal(res.status, 400);
    assert.equal(res.json.error, "MISSING_OR_INVALID_FIELD");
  });

  it("rejects a console write that has no bearer token", async () => {
    const res = await requestJson(port, "POST", "/v1beta1/management/tasks/cancel", JSON.stringify({ taskId: "task-x", mode: "graceful" }), {
      "content-type": "application/json",
    });
    assert.equal(res.status, 401);
  });

  it("stores approval text and the console escapes it before innerHTML", async () => {
    const marker = "<b>marker</b>";
    const created = await requestJson(
      port,
      "POST",
      "/v1beta1/management/approvals/create",
      JSON.stringify({ type: "QUOTA_EMERGENCY_BOOST", target: marker, region: "us-west-2", tenant: "Northstar" }),
      { "content-type": "application/json", authorization: `Bearer ${admin}` },
    );
    assert.equal(created.status, 201);
    const state = await requestJson(port, "GET", "/v1beta1/management/state", undefined, {});
    const approvals = state.json.approvals as Array<{ target: string }>;
    assert.ok(approvals.some((row) => row.target === marker));

    const html = fs.readFileSync("docs/index.html", "utf8");
    const source = html.match(/function escapeHtml\(value\) \{[\s\S]*?\n  \}/);
    assert.ok(source);
    const escapeHtml = vm.runInNewContext(`${source[0]}; escapeHtml;`);
    const escaped = escapeHtml(marker);
    assert.equal(escaped.includes("<b>"), false);
    assert.ok(escaped.includes("&lt;b&gt;"));
    assert.ok(html.includes("escapeHtml(a.target)"));
    assert.equal(html.includes("executeLocalEngineMirror"), false);
  });

  it("returns a rejected offline result instead of a fake 200", () => {
    const html = fs.readFileSync("docs/index.html", "utf8");
    const source = html.match(/function offlineApiResult\(method\) \{[\s\S]*?\n  \}/);
    assert.ok(source);
    const offlineApiResult = vm.runInNewContext(`${source[0]}; offlineApiResult;`);
    const write = offlineApiResult("POST");
    assert.equal(write.ok, false);
    assert.equal(write.status, 503);
    assert.equal(write.data.error, "WRITE_REJECTED_OFFLINE");
    const read = offlineApiResult("GET");
    assert.equal(read.ok, false);
    assert.equal(read.status, 503);
  });
});
