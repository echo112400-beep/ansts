/**
 * @fileoverview Start path for the ANSTS scheduler service as a denoland/celld-loaded worker.
 * `celld dev` bundles `src/cloudflare/worker.ts`, injects the SQLite Durable Object bindings, and
 * serves the worker. Cell state stays in that project's `.celld/dev` store. This module does not
 * start the in-process Node gateway.
 */
import * as fs from "node:fs";
import * as path from "node:path";

export interface CelldWorkerProject {
  projectDir: string;
  wranglerPath: string;
}

export interface CelldWorkerDevOptions {
  projectDir: string;
  host?: string;
  port?: number;
  clean?: boolean;
  logs?: boolean;
}

export interface CelldWorkerStartOptions {
  rootDir?: string;
  projectDir?: string;
  host?: string;
  port?: number;
  clean?: boolean;
  logs?: boolean;
}

function resolveRoot(rootDir?: string): string {
  return path.resolve(rootDir ?? process.cwd());
}

/**
 * Copies the celld Wrangler config, worker sources, and migrations into a directory `celld dev` can load.
 * An existing `.celld/` directory inside the project is left in place so a restart keeps Durable Object state.
 */
export function prepareCelldWorkerProject(options: { rootDir?: string; projectDir?: string } = {}): CelldWorkerProject {
  const rootDir = resolveRoot(options.rootDir);
  const projectDir = path.resolve(options.projectDir ?? path.join(rootDir, ".celld", "worker-service"));
  const srcDir = path.join(rootDir, "src");
  if (projectDir === rootDir || projectDir === srcDir || projectDir.startsWith(srcDir + path.sep)) {
    throw new Error(
      `celld project directory must sit outside the repository root and src/ (refusing ${projectDir})`
    );
  }

  const configSource = path.join(rootDir, "deploy", "celld", "wrangler.celld.json");
  const workerSource = path.join(rootDir, "src", "cloudflare", "worker.ts");
  if (!fs.existsSync(configSource)) {
    throw new Error(`celld wrangler config not found: ${configSource}`);
  }
  if (!fs.existsSync(workerSource)) {
    throw new Error(`celld worker entry not found: ${workerSource}`);
  }

  fs.mkdirSync(projectDir, { recursive: true });
  const wranglerPath = path.join(projectDir, "wrangler.json");
  fs.copyFileSync(configSource, wranglerPath);
  fs.rmSync(path.join(projectDir, "src"), { recursive: true, force: true });
  fs.cpSync(srcDir, path.join(projectDir, "src"), { recursive: true });
  const migrations = path.join(rootDir, "migrations");
  if (fs.existsSync(migrations)) {
    fs.rmSync(path.join(projectDir, "migrations"), { recursive: true, force: true });
    fs.cpSync(migrations, path.join(projectDir, "migrations"), { recursive: true });
  }
  return { projectDir, wranglerPath };
}

/** Arguments for the `celld` executable. The first argument is the `dev` subcommand. */
export function celldWorkerDevArgs(options: CelldWorkerDevOptions): string[] {
  const port = options.port ?? 9876;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(`CELLD_PORT must be an integer from 1 to 65535, got ${String(options.port)}`);
  }
  const host = options.host ?? "127.0.0.1";
  if (!host.trim()) {
    throw new Error("CELLD_HOST must be a non-empty address");
  }
  const args = ["dev", options.projectDir, "--host", host, "--port", String(port), "--no-watch"];
  if (options.logs) args.push("--logs");
  if (options.clean) args.push("--clean");
  return args;
}

/** Stages the project and returns the `celld dev` arguments that serve it. */
export function resolveCelldWorkerStart(options: CelldWorkerStartOptions = {}): { projectDir: string; args: string[] } {
  const prepared = prepareCelldWorkerProject({ rootDir: options.rootDir, projectDir: options.projectDir });
  return {
    projectDir: prepared.projectDir,
    args: celldWorkerDevArgs({
      projectDir: prepared.projectDir,
      host: options.host,
      port: options.port,
      clean: options.clean,
      logs: options.logs,
    }),
  };
}

function startFromProcessEnv(): { projectDir: string; args: string[] } {
  const port = process.env.CELLD_PORT === undefined || process.env.CELLD_PORT === "" ? 9876 : Number(process.env.CELLD_PORT);
  return resolveCelldWorkerStart({
    rootDir: process.cwd(),
    projectDir: process.env.CELLD_PROJECT_DIR,
    host: process.env.CELLD_HOST,
    port,
    clean: process.env.CELLD_CLEAN === "1",
    logs: process.env.CELLD_LOGS !== "0",
  });
}

function runCli(argv: string[]): void {
  if (argv[0] !== "print-exec") {
    process.stderr.write("usage: node dist/src/cloudflare/celld_worker_service.js print-exec\n");
    process.exit(2);
  }
  const started = startFromProcessEnv();
  process.stdout.write(`${started.args.join("\n")}\n`);
}

const invokedDirectly =
  typeof require === "function" && typeof module !== "undefined" && require.main === module;

if (invokedDirectly) {
  try {
    runCli(process.argv.slice(2));
  } catch (err) {
    process.stderr.write(`${(err as Error).message}\n`);
    process.exit(1);
  }
}
