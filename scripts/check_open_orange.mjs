import { spawn, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const workers = {
  scheduler: "wrangler.celld.json",
  gateway: "wrangler.gateway.json",
  body: "wrangler.body.json",
};
const ports = { scheduler: "8788", gateway: "8789", body: "8791" };
const args = process.argv.slice(2);
const serve = args[0] === "--serve";
if (serve) args.shift();
const [role = "scheduler", port = ports[role]] = args;
if (!Object.hasOwn(workers, role) || args.length > 2 || !/^\d+$/.test(port) || Number(port) < 1 || Number(port) > 65535) {
  throw new Error("Usage: node scripts/check_open_orange.mjs [--serve] [scheduler|gateway|body] [port]");
}

const config = JSON.parse(readFileSync(join(root, "deploy/celld", workers[role]), "utf8"));
config.main = resolve(root, config.main);
for (const database of config.d1_databases ?? []) {
  if (database.migrations_dir) database.migrations_dir = resolve(root, database.migrations_dir);
}
delete config.$schema;

const directory = mkdtempSync(join(tmpdir(), "ansts-orange-doctor-"));
try {
  const configPath = join(directory, "wrangler.json");
  writeFileSync(configPath, JSON.stringify(config));
  const configArgs = ["--config", configPath, "--port", port];
  const options = { cwd: root, stdio: "inherit" };
  if (serve) {
    const child = spawn("orange", ["serve", ...configArgs, "--ip", "127.0.0.1"], options);
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
