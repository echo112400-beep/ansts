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

    const staged = prepareCelldWorkerProject({ rootDir: ROOT, projectDir, role: "scheduler" });
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
    assert.equal(bindings.some((binding) => binding.class_name === "AnstsGatewayCell"), false);

    const gatewayDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-celld-gateway-stage-"));
    const gateway = prepareCelldWorkerProject({
      rootDir: ROOT,
      projectDir: gatewayDir,
      role: "gateway",
      schedulerUrl: "http://127.0.0.1:9876",
    });
    const gatewayConfig = readJson(gateway.wranglerPath);
    assert.equal(gatewayConfig.main, "src/cloudflare/gateway_worker.ts");
    assert.equal(gatewayConfig.name, "ansts-celld-gateway");
    const gatewayBindings = (gatewayConfig.durable_objects as { bindings: Array<{ name: string; class_name: string }> }).bindings;
    assert.deepEqual(
      gatewayBindings.map((binding) => [binding.name, binding.class_name]),
      [["ANSTS_GATEWAY_CELL", "AnstsGatewayCell"]]
    );
    assert.equal(fs.readFileSync(path.join(gatewayDir, ".dev.vars"), "utf8").includes("ANSTS_SCHEDULER_URL=http://127.0.0.1:9876"), true);
    assert.equal(fs.readFileSync(path.join(gatewayDir, ".dev.vars"), "utf8").includes("ANSTS_CONSOLE_RBAC_SECRET"), false);
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
    const schedulerScript = fs.readFileSync(path.join(ROOT, "scripts/celld_scheduler_up.sh"), "utf8");
    const gatewayScript = fs.readFileSync(path.join(ROOT, "scripts/celld_gateway_up.sh"), "utf8");
    assert.match(schedulerScript, /CELLD_ROLE=scheduler/);
    assert.match(gatewayScript, /CELLD_ROLE=gateway/);
    assert.match(gatewayScript, /ANSTS_SCHEDULER_URL/);
    assert.equal(schedulerScript.includes("server_main"), false);
    assert.equal(gatewayScript.includes("server_main"), false);
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

  it("gateway and scheduler are separate celld processes and survive each other's restart", async (t) => {
    const celldBin = process.env.CELLD_BIN || "";
    const esbuildBin = process.env.CELLD_ESBUILD || "";
    if (!celldBin || !esbuildBin || !fs.existsSync(celldBin) || !fs.existsSync(esbuildBin)) {
      t.skip("CELLD_BIN and CELLD_ESBUILD are required for the live celld process check");
      return;
    }

    const schedulerDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-celld-sched-"));
    const gatewayDir = fs.mkdtempSync(path.join(os.tmpdir(), "ansts-celld-gw-"));
    const schedulerPort = 18976;
    const gatewayPort = 18977;
    const schedulerUrl = `http://127.0.0.1:${schedulerPort}`;
    const gatewayUrl = `http://127.0.0.1:${gatewayPort}`;
    prepareCelldWorkerProject({ rootDir: ROOT, projectDir: schedulerDir, role: "scheduler" });
    prepareCelldWorkerProject({
      rootDir: ROOT,
      projectDir: gatewayDir,
      role: "gateway",
      schedulerUrl,
    });
    const taskId = `split-live-${Date.now()}`;
    const childEnv = { ...process.env, CELLD_ESBUILD: esbuildBin };
    const children = new Map<string, ChildProcess>();

    const stopNamed = async (name: string): Promise<void> => {
      const proc = children.get(name);
      if (!proc || proc.exitCode !== null) {
        children.delete(name);
        return;
      }
      const exited = new Promise<void>((resolve) => proc.once("exit", () => resolve()));
      proc.kill("SIGTERM");
      const timer = setTimeout(() => proc.kill("SIGKILL"), 3000);
      await exited;
      clearTimeout(timer);
      children.delete(name);
    };

    const startNamed = async (name: string, projectDir: string, port: number, clean: boolean): Promise<void> => {
      const args = celldWorkerDevArgs({ projectDir, host: "127.0.0.1", port, clean, logs: true });
      const logs: string[] = [];
      const proc = spawn(celldBin, args, { env: childEnv, stdio: ["ignore", "pipe", "pipe"] });
      children.set(name, proc);
      proc.stdout?.on("data", (chunk: Buffer) => logs.push(chunk.toString("utf8")));
      proc.stderr?.on("data", (chunk: Buffer) => logs.push(chunk.toString("utf8")));
      const deadline = Date.now() + 30_000;
      let lastError = "";
      while (Date.now() < deadline) {
        if (proc.exitCode !== null) {
          throw new Error(`${name} exited ${proc.exitCode}: ${logs.join("").slice(-2000)}`);
        }
        try {
          const health = await fetch(`http://127.0.0.1:${port}/healthz`);
          const runtime = await fetch(`http://127.0.0.1:${port}/.well-known/celld/health`);
          if (health.ok && runtime.status === 200) return;
          lastError = `healthz ${health.status} celld ${runtime.status}`;
        } catch (err) {
          lastError = (err as Error).message;
        }
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
      throw new Error(`${name} did not become ready: ${lastError}; ${logs.join("").slice(-2000)}`);
    };

    try {
      await startNamed("scheduler", schedulerDir, schedulerPort, true);
      await startNamed("gateway", gatewayDir, gatewayPort, true);
      const schedulerHealth = (await (await fetch(`${schedulerUrl}/healthz`)).json()) as { controlPlane?: string };
      const gatewayHealth = (await (await fetch(`${gatewayUrl}/health`)).json()) as {
        controlPlane?: string;
        planes?: { scheduler?: string };
        scheduler?: { reachable?: boolean };
      };
      assert.equal(schedulerHealth.controlPlane, "celld-scheduler");
      assert.equal(gatewayHealth.controlPlane, "celld-gateway");
      assert.equal(gatewayHealth.planes?.scheduler, "remote-http");
      assert.equal(gatewayHealth.scheduler?.reachable, true);
      assert.equal((await fetch(`${schedulerUrl}/v1/tasks`)).status, 404);
      assert.equal((await fetch(`${gatewayUrl}/v1/scheduler/tasks`)).status, 404);

      const registered = await fetch(`${schedulerUrl}/v1/scheduler/register-node`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ node_id: "body-host-01", total_cpu_cores: 8, free_cpu_cores: 8 }),
      });
      assert.equal(registered.status, 200);
      const submit = await fetch(`${gatewayUrl}/v1/tasks`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: taskId, tenant_id: "probe", priority: 1, prompt: "cross worker" }),
      });
      assert.equal(submit.status, 202);
      const submitBody = (await submit.json()) as { controlPlane?: string; state?: string; bodyExecution?: unknown };
      assert.equal(submitBody.controlPlane, "celld-gateway");
      assert.equal(submitBody.state, "LEASED");
      assert.equal(submitBody.bodyExecution, null);
      const onScheduler = (await (await fetch(`${schedulerUrl}/v1/scheduler/tasks`)).json()) as {
        cellId?: string;
        tasks?: Array<{ task_id?: string }>;
      };
      assert.equal(onScheduler.tasks?.some((task) => task.task_id === taskId), true);
      const gatewayState = (await (await fetch(`${gatewayUrl}/v1beta1/gateway/state`)).json()) as {
        cellId?: string;
        dispatches?: Array<{ task_id?: string; prompt?: string }>;
      };
      const gatewayCellId = String(gatewayState.cellId);
      const schedulerCellId = String(onScheduler.cellId);

      const schedulerPid = children.get("scheduler")?.pid;
      await stopNamed("gateway");
      assert.equal(children.get("scheduler")?.exitCode ?? null, null);
      assert.equal(children.get("scheduler")?.pid, schedulerPid);
      const stillThere = (await (await fetch(`${schedulerUrl}/v1/scheduler/tasks`)).json()) as {
        tasks?: Array<{ task_id?: string }>;
      };
      assert.equal(stillThere.tasks?.some((task) => task.task_id === taskId), true);

      await startNamed("gateway", gatewayDir, gatewayPort, false);
      const restoredGateway = (await (await fetch(`${gatewayUrl}/v1beta1/gateway/state`)).json()) as {
        cellId?: string;
        dispatches?: Array<{ task_id?: string; prompt?: string }>;
      };
      assert.equal(restoredGateway.cellId, gatewayCellId);
      assert.equal(
        restoredGateway.dispatches?.some((row) => row.task_id === taskId && row.prompt === "cross worker"),
        true
      );
      assert.equal(children.get("scheduler")?.pid, schedulerPid);

      await stopNamed("scheduler");
      const down = await fetch(`${gatewayUrl}/v1/tasks`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ task_id: "split-while-down", tenant_id: "probe", priority: 1, prompt: "no" }),
      });
      assert.equal(down.status, 503);
      const downBody = (await down.json()) as { error?: string };
      assert.equal(downBody.error, "SCHEDULER_UNREACHABLE");
      assert.equal(children.get("gateway")?.exitCode ?? null, null);

      await startNamed("scheduler", schedulerDir, schedulerPort, false);
      const after = (await (await fetch(`${schedulerUrl}/v1/scheduler/tasks`)).json()) as {
        cellId?: string;
        tasks?: Array<{ task_id?: string; prompt?: string }>;
      };
      assert.equal(after.cellId, schedulerCellId);
      assert.equal(after.tasks?.some((task) => task.task_id === taskId && task.prompt === "cross worker"), true);
      assert.notEqual(children.get("scheduler")?.pid, schedulerPid);
    } finally {
      await stopNamed("gateway");
      await stopNamed("scheduler");
      fs.rmSync(schedulerDir, { recursive: true, force: true });
      fs.rmSync(gatewayDir, { recursive: true, force: true });
    }
  });
});
