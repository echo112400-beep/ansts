/**
 * @fileoverview celld-loaded worker service start: the scheduler comes up through `celld dev`,
 * and task state lives in the scheduler Durable Object rather than the in-process gateway.
 */
import { describe, it } from "node:test";
import * as assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import {
  celldWorkerDevArgs,
  prepareCelldWorkerProject,
  resolveCelldWorkerStart,
} from "../src/cloudflare/celld_worker_service.js";

const ROOT = process.cwd();

function readJson(filePath: string): Record<string, unknown> {
  return JSON.parse(fs.readFileSync(filePath, "utf8")) as Record<string, unknown>;
}

describe("celld-loaded worker service start", () => {
  it("stages the worker project celld loads, including the SQLite Durable Object classes", () => {
    const projectDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-celld-stage-"));
    const markerDir = path.join(projectDir, ".celld", "dev");
    fs.mkdirSync(markerDir, { recursive: true });
    fs.writeFileSync(path.join(markerDir, "keep.txt"), "durable-state");

    const staged = prepareCelldWorkerProject({ rootDir: ROOT, projectDir });
    assert.equal(staged.projectDir, projectDir);
    assert.equal(fs.readFileSync(path.join(markerDir, "keep.txt"), "utf8"), "durable-state");

    const config = readJson(staged.wranglerPath);
    assert.equal(config.main, "src/cloudflare/worker.ts");
    assert.equal(config.name, "ansts-celld-brain");
    const bindings = (config.durable_objects as { bindings: Array<{ name: string; class_name: string }> }).bindings;
    assert.deepEqual(
      bindings.map((binding) => [binding.name, binding.class_name]),
      [
        ["ANSTS_SCHEDULER_CELL", "AnstsSchedulerCell"],
        ["ANSTS_HARNESS_CELL", "AgentTaskHarnessCell"],
        ["ANSTS_TUNNEL_CELL", "TunnelGatewayCell"],
      ]
    );
    const sqliteClasses = (config.migrations as Array<{ new_sqlite_classes?: string[] }>)[0].new_sqlite_classes;
    assert.deepEqual(sqliteClasses, ["AnstsSchedulerCell", "AgentTaskHarnessCell", "TunnelGatewayCell"]);

    const stagedWorker = fs.readFileSync(path.join(projectDir, "src/cloudflare/worker.ts"));
    const sourceWorker = fs.readFileSync(path.join(ROOT, "src/cloudflare/worker.ts"));
    assert.deepEqual(stagedWorker, sourceWorker);
    assert.ok(fs.existsSync(path.join(projectDir, "migrations")));
  });

  it("builds a celld dev command and refuses to point it at the in-process gateway", () => {
    const args = celldWorkerDevArgs({
      projectDir: "/srv/ansts-celld",
      host: "127.0.0.1",
      port: 9876,
      logs: true,
      clean: false,
    });
    assert.deepEqual(args, ["dev", "/srv/ansts-celld", "--host", "127.0.0.1", "--port", "9876", "--no-watch", "--logs"]);
    assert.equal(args.includes("--clean"), false);
    assert.equal(args.some((arg) => arg.includes("server_main")), false);

    const cleaned = celldWorkerDevArgs({ projectDir: "/srv/ansts-celld", port: 18081, clean: true, logs: false });
    assert.deepEqual(cleaned, ["dev", "/srv/ansts-celld", "--host", "127.0.0.1", "--port", "18081", "--no-watch", "--clean"]);

    assert.throws(() => celldWorkerDevArgs({ projectDir: "/srv/ansts-celld", port: 0 }), /CELLD_PORT/);
    assert.throws(() => prepareCelldWorkerProject({ rootDir: ROOT, projectDir: ROOT }), /project directory/);
  });

  it("the start script execs celld dev against the staged worker project", () => {
    const script = fs.readFileSync(path.join(ROOT, "scripts/celld_worker_up.sh"), "utf8");
    assert.match(script, /print-exec/);
    assert.match(script, /exec\s+"\$CELLD_BIN"/);
    assert.equal(script.includes("server_main"), false);
    const resolved = resolveCelldWorkerStart({
      rootDir: ROOT,
      projectDir: path.join(os.tmpdir(), "ansts-celld-resolve-unused"),
      host: "127.0.0.1",
      port: 9876,
      clean: false,
      logs: true,
    });
    assert.equal(resolved.args[0], "dev");
    assert.equal(resolved.args[1], resolved.projectDir);
    assert.equal(fs.existsSync(path.join(resolved.projectDir, "src/cloudflare/worker.ts")), true);
  });

  it("celld loads the worker and the scheduler Durable Object survives a process restart", async (t) => {
    const celldBin = process.env.CELLD_BIN || "";
    const esbuildBin = process.env.CELLD_ESBUILD || "";
    if (!celldBin || !esbuildBin || !fs.existsSync(celldBin) || !fs.existsSync(esbuildBin)) {
      t.skip("CELLD_BIN and CELLD_ESBUILD are required for the live celld process check");
      return;
    }

    const projectDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-celld-live-"));
    const staged = prepareCelldWorkerProject({ rootDir: ROOT, projectDir });
    const port = 18976;
    const taskId = `celld-do-live-${Date.now()}`;
    const childEnv = { ...process.env, CELLD_ESBUILD: esbuildBin };
    let child: ChildProcess | null = null;

    const stop = async (): Promise<void> => {
      if (!child || child.exitCode !== null || child.killed) {
        child = null;
        return;
      }
      const proc = child;
      child = null;
      const exited = new Promise<void>((resolve) => proc.once("exit", () => resolve()));
      proc.kill("SIGTERM");
      const timer = setTimeout(() => proc.kill("SIGKILL"), 3000);
      await exited;
      clearTimeout(timer);
    };

    const start = async (clean: boolean): Promise<string> => {
      const args = celldWorkerDevArgs({
        projectDir: staged.projectDir,
        host: "127.0.0.1",
        port,
        clean,
        logs: true,
      });
      const stderr: string[] = [];
      child = spawn(celldBin, args, { env: childEnv, stdio: ["ignore", "pipe", "pipe"] });
      child.stdout?.on("data", (chunk: Buffer) => stderr.push(chunk.toString("utf8")));
      child.stderr?.on("data", (chunk: Buffer) => stderr.push(chunk.toString("utf8")));
      const deadline = Date.now() + 30_000;
      let lastError = "";
      let workerReady = false;
      let runtimeReady = false;
      while (Date.now() < deadline) {
        if (child.exitCode !== null) {
          throw new Error(`celld exited ${child.exitCode}: ${stderr.join("").slice(-2000)}`);
        }
        try {
          const health = await fetch(`http://127.0.0.1:${port}/healthz`);
          workerReady = health.ok;
          const runtime = await fetch(`http://127.0.0.1:${port}/.well-known/celld/health`);
          runtimeReady = runtime.status === 200;
          if (workerReady && runtimeReady) break;
          lastError = `healthz ${health.status} celld ${runtime.status} ${await runtime.text()}`;
        } catch (err) {
          lastError = (err as Error).message;
        }
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
      if (!workerReady || !runtimeReady) {
        throw new Error(`celld did not become ready: ${lastError}; ${stderr.join("").slice(-2000)}`);
      }
      const internal = stderr.join("").match(/internal listening on (\S+)/);
      return internal ? internal[1] : "";
    };

    try {
      await start(true);
      const workerHealth = (await (await fetch(`http://127.0.0.1:${port}/healthz`)).json()) as {
        status?: string;
        runtime?: string;
      };
      assert.equal(workerHealth.status, "ok");
      assert.equal(workerHealth.runtime, "denoland/celld-cloudflare-workers");
      const celldHealth = await fetch(`http://127.0.0.1:${port}/.well-known/celld/health`);
      assert.equal(celldHealth.status, 200);

      const submit = await fetch(`http://127.0.0.1:${port}/v1/scheduler/submit`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: taskId, tenant_id: "probe", priority: 1, prompt: "durable object" }),
      });
      assert.equal(submit.status, 202);
      const before = (await (await fetch(`http://127.0.0.1:${port}/v1/scheduler/tasks`)).json()) as {
        cellId?: string;
        tasks?: Array<{ task_id?: string; state?: string }>;
      };
      assert.match(String(before.cellId), /^[0-9a-f]{64}$/);
      assert.equal(before.tasks?.some((task) => task.task_id === taskId && task.state === "QUEUED"), true);
      const cellId = String(before.cellId);

      await stop();
      const internal = await start(false);
      const after = (await (await fetch(`http://127.0.0.1:${port}/v1/scheduler/tasks`)).json()) as {
        cellId?: string;
        tasks?: Array<{ task_id?: string; prompt?: string; state?: string }>;
      };
      assert.equal(after.cellId, cellId);
      const restored = after.tasks?.find((task) => task.task_id === taskId);
      assert.equal(restored?.state, "QUEUED");
      assert.equal(restored?.prompt, "durable object");

      assert.match(internal, /^127\.0\.0\.1:\d+$/);
      const stateResp = await fetch(`http://${internal}/state`);
      assert.equal(stateResp.status, 200);
      const state = (await stateResp.json()) as { owned_cells?: number; published?: string[] };
      assert.equal(state.owned_cells, 1);
      assert.equal(state.published?.includes(`AnstsSchedulerCell:${cellId}`), true);
    } finally {
      await stop();
      fs.rmSync(projectDir, { recursive: true, force: true });
    }
  });
});
