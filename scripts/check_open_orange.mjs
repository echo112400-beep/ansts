import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const workers = {
  scheduler: { manifest: "deploy/celld/wrangler.celld.json", port: "8788" },
  gateway: { manifest: "deploy/celld/wrangler.gateway.json", port: "8789" },
  body: { manifest: "deploy/celld/wrangler.body.json", port: "8791" },
  checkpoint: { manifest: "deploy/open-orange/wrangler.control-checkpoint.json", base: "deploy/open-orange", port: "8792" },
  authority: { manifest: "deploy/open-orange/wrangler.authority-import.json", base: "deploy/open-orange", port: "8793" },
};
const args = process.argv.slice(2);
const serve = args[0] === "--serve";
if (serve) args.shift();
const [role = "scheduler", inputPort] = args;
const worker = Object.hasOwn(workers, role) ? workers[role] : undefined;
const port = inputPort ?? worker?.port;
if (!worker || args.length > 2 || !/^\d+$/.test(port) || Number(port) < 1 || Number(port) > 65535) {
  throw new Error("Usage: node scripts/check_open_orange.mjs [--serve] [scheduler|gateway|body|checkpoint|authority] [port]");
}

const config = JSON.parse(readFileSync(join(root, worker.manifest), "utf8"));
const manifestBase = resolve(root, worker.base ?? ".");
config.main = resolve(manifestBase, config.main);
for (const database of config.d1_databases ?? []) {
  if (database.migrations_dir) database.migrations_dir = resolve(manifestBase, database.migrations_dir);
}
delete config.$schema;

const directory = mkdtempSync(join(tmpdir(), "ansts-orange-doctor-"));
try {
  const configPath = join(directory, "wrangler.json");
  writeFileSync(configPath, JSON.stringify(config));
  const configArgs = ["--config", configPath, "--port", port];
  const env = { ...process.env };
  if (serve && role === "gateway" && env.ANSTS_SYSTEM_TOPOLOGY_FILE) {
    env.ANSTS_SYSTEM_TOPOLOGY_JSON = readFileSync(env.ANSTS_SYSTEM_TOPOLOGY_FILE, "utf8");
    env.ORANGE_WORKER_VARS = [env.ORANGE_WORKER_VARS, "ANSTS_SYSTEM_TOPOLOGY_JSON"].filter(Boolean).join(",");
  }
  const options = { cwd: root, env, stdio: "inherit" };
  if (serve) {
    const child = spawn("orange", ["serve", ...configArgs, "--ip", process.env.ANSTS_ORANGE_LISTEN_HOST ?? "127.0.0.1"], options);
    const stop = (signal) => child.kill(signal);
    process.on("SIGTERM", stop);
    process.on("SIGINT", stop);
    try {
      process.exitCode = await new Promise((resolve, reject) => {
        child.once("error", reject);
        child.once("close", (status, signal) => resolve(status ?? (signal === "SIGINT" ? 130 : 143)));
      });
    } finally {
      process.off("SIGTERM", stop);
      process.off("SIGINT", stop);
    }
  } else {
    const result = spawnSync("orange", ["doctor", ...configArgs, "--json"], options);
    if (result.error) throw result.error;
    process.exitCode = result.status ?? 1;
  }
} finally {
  rmSync(directory, { recursive: true, force: true });
}
