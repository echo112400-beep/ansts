/**
 * @fileoverview Console agent tool flow with TanStack AI's fakeText adapter.
 * This is not a real model and does not call a provider.
 */
import { describe, it, before, after } from "node:test";
import * as assert from "node:assert/strict";
import * as fs from "node:fs";
import * as http from "node:http";
import { AnstsLocalAndComposeServer } from "../src/server_main.js";
import { ConsoleRbacAuthGuard } from "../src/dashboard/console_rbac_guard.js";
import { importEsm, installConsoleAgentTestAdapter } from "../src/console/console_agent.js";

const SECRET = "ci-console-rbac-test-secret";

function request(
  port: number,
  headers: Record<string, string>,
  body: unknown,
): Promise<{ status: number; text: string }> {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = http.request(
      {
        host: "127.0.0.1",
        port,
        method: "POST",
        path: "/v1beta1/console/agent/chat",
        headers: { "content-type": "application/json", "content-length": Buffer.byteLength(payload), ...headers },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (chunk) => chunks.push(chunk));
        res.on("end", () => resolve({ status: res.statusCode ?? 0, text: Buffer.concat(chunks).toString("utf8") }));
      },
    );
    req.on("error", reject);
    req.write(payload);
    req.end();
  });
}

function eventsOf(raw: string): Array<Record<string, unknown>> {
  return raw
    .split("\n\n")
    .map((frame) => frame.split("\n").filter((line) => line.startsWith("data:")).map((line) => line.slice(5).trim()).join("\n"))
    .filter(Boolean)
    .map((data) => JSON.parse(data) as Record<string, unknown>);
}

function chatBody(runId: string, messages: unknown[], extra: Record<string, unknown> = {}): Record<string, unknown> {
  return { threadId: "agent-test", runId, messages, tools: [], context: [], ...extra };
}

describe("console agent fake adapter", () => {
  let server: AnstsLocalAndComposeServer;
  let port = 0;
  let admin = "";
  let viewer = "";
  let fake: { setResponses: (responses: unknown[]) => void };

  before(async () => {
    delete process.env.ANSTS_CONSOLE_AGENT_PROVIDER;
    delete process.env.ANSTS_CONSOLE_AGENT_MODEL;
    delete process.env.OPENAI_API_KEY;
    process.env.ANSTS_CONSOLE_RBAC_SECRET = SECRET;
    const guard = new ConsoleRbacAuthGuard(SECRET, `/tmp/ansts-console-agent-${process.pid}.jsonl`);
    admin = guard.issueOperatorJwt({ operatorId: "chen", role: "SRE_ADMIN" });
    viewer = guard.issueOperatorJwt({ operatorId: "xu", role: "VIEWER" });
    const testing = await importEsm<{ fakeText: () => typeof fake }>("@tanstack/ai/testing");
    fake = testing.fakeText();
    installConsoleAgentTestAdapter(fake);
    const tmpDir = `/tmp/ansts-console-agent-${process.pid}-${Date.now()}`;
    fs.mkdirSync(tmpDir, { recursive: true });
    server = new AnstsLocalAndComposeServer({
      host: "127.0.0.1",
      nodeId: "celld-gw-01",
      nodeRole: "gateway",
      storageDir: tmpDir,
    });
    port = (await server.start(0, 0)).apiPort;
  });

  after(async () => {
    installConsoleAgentTestAdapter(null);
    await server.stop();
  });

  it("returns 503 when no model is configured and the request is not the fake test path", async () => {
    const res = await request(port, {}, chatBody("run-503", [{ id: "m1", role: "user", content: [{ type: "text", text: "hello" }] }]));
    assert.equal(res.status, 503);
    assert.equal(JSON.parse(res.text).error, "AGENT_MODEL_UNCONFIGURED");
  });

  it("reads live management nodes, refuses unapproved and unauthorized writes, then audits an approved scale", async () => {
    const state = await server.managementPlane.buildFullManagementState();
    const liveNodeId = String((state.nodes as Array<{ id: string }>)[0].id);
    const user = { id: "m-read", role: "user", content: [{ type: "text", text: "nodes" }] };

    fake.setResponses([{ toolCalls: [{ name: "get_nodes", input: {} }] }, { text: "listed" }]);
    const read = await request(
      port,
      { "x-ansts-agent-test": "fake" },
      chatBody("run-read", [user]),
    );
    assert.equal(read.status, 200);
    const readEvents = eventsOf(read.text);
    const readResult = readEvents.find((event) => event.type === "TOOL_CALL_RESULT");
    assert.ok(readResult);
    assert.match(String(readResult.content), new RegExp(liveNodeId));

    fake.setResponses([{ toolCalls: [{ name: "scale_node", input: { action: "scale_out", nodeId: "body-host-90", region: "us-west-2", role: "body" } }] }]);
    const anonymous = await request(
      port,
      { "x-ansts-agent-test": "fake" },
      chatBody("run-anon", [user]),
    );
    const anonEvents = eventsOf(anonymous.text);
    assert.equal(anonEvents.some((event) => (event.outcome as { type?: string } | undefined)?.type === "interrupt"), false);
    const anonAudit = await server.managementPlane.buildFullManagementState();
    assert.equal(
      (anonAudit.auditLogs as Array<{ action: string }>).some((row) => row.action.includes("body-host-90")),
      false,
    );

    fake.setResponses([{ toolCalls: [{ name: "scale_node", input: { action: "scale_out", region: "us-west-2", role: "body" } }] }]);
    const missing = await request(
      port,
      { "x-ansts-agent-test": "fake", authorization: `Bearer ${admin}` },
      chatBody("run-missing", [user]),
    );
    const missingEvents = eventsOf(missing.text);
    assert.equal(missingEvents.some((event) => (event.outcome as { type?: string } | undefined)?.type === "interrupt"), false);

    fake.setResponses([{ toolCalls: [{ name: "scale_node", input: { action: "scale_out", nodeId: "body-host-91", region: "us-west-2", role: "body" } }] }]);
    const viewerRes = await request(
      port,
      { "x-ansts-agent-test": "fake", authorization: `Bearer ${viewer}` },
      chatBody("run-viewer", [user]),
    );
    assert.equal(eventsOf(viewerRes.text).some((event) => (event.outcome as { type?: string } | undefined)?.type === "interrupt"), false);

    fake.setResponses([
      { toolCalls: [{ name: "scale_node", input: { action: "scale_out", nodeId: "body-host-92", region: "us-west-2", role: "body" } }] },
      { text: "scaled" },
    ]);
    const proposed = await request(
      port,
      { "x-ansts-agent-test": "fake", authorization: `Bearer ${admin}` },
      chatBody("run-propose", [user]),
    );
    const proposedEvents = eventsOf(proposed.text);
    const finished = proposedEvents.find((event) => event.type === "RUN_FINISHED");
    const interrupt = ((finished?.outcome as { interrupts?: Array<{ id: string }> } | undefined)?.interrupts ?? [])[0];
    assert.ok(interrupt);
    const before = await server.managementPlane.buildFullManagementState();
    assert.equal((before.auditLogs as Array<{ action: string }>).some((row) => row.action.includes("body-host-92")), false);
    const snapshot = proposedEvents.find((event) => event.type === "MESSAGES_SNAPSHOT");
    const approved = await request(
      port,
      { "x-ansts-agent-test": "fake", authorization: `Bearer ${admin}` },
      chatBody("run-approve", snapshot?.messages, {
        parentRunId: "run-propose",
        resume: [{ interruptId: interrupt.id, status: "resolved", payload: { approved: true } }],
      }),
    );
    assert.equal(approved.status, 200);
    const after = await server.managementPlane.buildFullManagementState();
    const audit = (after.auditLogs as Array<{ actor: string; action: string }>).find((row) => row.action.includes("agent 发起 scale_node") && row.action.includes("body-host-92"));
    assert.ok(audit);
    assert.equal(audit.actor, "chen");
  });
});
