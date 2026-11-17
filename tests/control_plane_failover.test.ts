import { it } from "node:test";
import * as assert from "node:assert/strict";
import { controlPlaneFetch } from "../src/cloudflare/control_plane_fetch.js";
it("fails over reads, chooses a healthy entry before writes and never replays ambiguous writes", async () => {
  const original = globalThis.fetch, calls: string[] = [];
  globalThis.fetch = async (input, init) => {
    const url = new URL(String(input)); calls.push(`${init?.method ?? "GET"} ${url.host}${url.pathname}`);
    if (url.host === "down") throw new Error("connection refused");
    if (init?.method === "POST") throw new Error("write response lost");
    return Response.json({ healthy: true });
  };
  try {
    assert.equal((await controlPlaneFetch("https://down,https://healthy", "/tasks")).status, 200);
    calls.length = 0;
    await assert.rejects(controlPlaneFetch("https://down,https://healthy,https://third", "/submit", { method: "POST", body: "{}" }), /response lost/);
    assert.equal(calls.filter(call => call.startsWith("POST")).length, 1);
    assert.equal(calls.at(-1), "POST healthy/submit");
  } finally { globalThis.fetch = original; }
});
