/**
 * @fileoverview Start paths for the two celld workers. The scheduler and the gateway each get
 * their own project directory, wrangler config, listen port, and `.celld/dev` store.
 * Body execution remains a separate Node process.
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

export type CelldWorkerRole = "scheduler" | "gateway";

export interface CelldWorkerStartOptions {
  rootDir?: string;
  role?: CelldWorkerRole;
  projectDir?: string;
  host?: string;
  port?: number;
  clean?: boolean;
  logs?: boolean;
  /** Gateway only. Written into the staged project's `.dev.vars` so celld injects it. */
  schedulerUrl?: string;
}

const ROLE_DEFAULTS: Record<CelldWorkerRole, { config: string; entry: string; dirName: string; port: number }> = {
  scheduler: {
    config: "deploy/celld/wrangler.celld.json",
    entry: "src/cloudflare/worker.ts",
    dirName: "scheduler-worker",
    port: 9876,
  },
  gateway: {
    config: "deploy/celld/wrangler.gateway.json",
    entry: "src/cloudflare/gateway_worker.ts",
    dirName: "gateway-worker",
    port: 9877,
  },
};

export function celldWorkerRoleDefaults(role: CelldWorkerRole): { config: string; entry: string; port: number; dirName: string } {
  return ROLE_DEFAULTS[role];
}

function resolveRoot(rootDir?: string): string {
  return path.resolve(rootDir ?? process.cwd());
}

/**
 * Copies the celld Wrangler config, worker sources, and migrations into a directory `celld dev` can load.
 * An existing `.celld/` directory inside the project is left in place so a restart keeps Durable Object state.
 */
export function prepareCelldWorkerProject(
  options: { rootDir?: string; projectDir?: string; role?: CelldWorkerRole; schedulerUrl?: string } = {}
): CelldWorkerProject {
  const role = options.role ?? "scheduler";
  const defaults = ROLE_DEFAULTS[role];
  const rootDir = resolveRoot(options.rootDir);
  const projectDir = path.resolve(options.projectDir ?? path.join(rootDir, ".celld", defaults.dirName));
  const srcDir = path.join(rootDir, "src");
  if (projectDir === rootDir || projectDir === srcDir || projectDir.startsWith(srcDir + path.sep)) {
    throw new Error(
      `celld project directory must sit outside the repository root and src/ (refusing ${projectDir})`
    );
  }

  const configSource = path.join(rootDir, defaults.config);
  const workerSource = path.join(rootDir, defaults.entry);
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
  if (role === "gateway") {
    const lines = [`ANSTS_SCHEDULER_URL=${options.schedulerUrl?.trim() || process.env.ANSTS_SCHEDULER_URL?.trim() || ""}`];
    const secret = process.env.ANSTS_CONSOLE_RBAC_SECRET?.trim() ?? "";
    if (secret) lines.push(`ANSTS_CONSOLE_RBAC_SECRET=${secret}`);
    if (process.env.ANSTS_SCHEDULER_SERVICE_TOKEN) lines.push(`ANSTS_SCHEDULER_SERVICE_TOKEN=${process.env.ANSTS_SCHEDULER_SERVICE_TOKEN}`);
    fs.writeFileSync(path.join(projectDir, ".dev.vars"), `${lines.join("\n")}\n`);
  } else if (process.env.ANSTS_BODY_AUTH_SECRET) {
    fs.writeFileSync(path.join(projectDir, ".dev.vars"), `ANSTS_BODY_AUTH_SECRET=${process.env.ANSTS_BODY_AUTH_SECRET}\n`);
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
export function resolveCelldWorkerStart(options: CelldWorkerStartOptions = {}): { projectDir: string; args: string[]; role: CelldWorkerRole } {
  const role = options.role ?? "scheduler";
  const defaults = ROLE_DEFAULTS[role];
  const prepared = prepareCelldWorkerProject({
    rootDir: options.rootDir,
    projectDir: options.projectDir,
    role,
    schedulerUrl: options.schedulerUrl,
  });
  return {
    role,
    projectDir: prepared.projectDir,
    args: celldWorkerDevArgs({
      projectDir: prepared.projectDir,
      host: options.host,
      port: options.port ?? defaults.port,
      clean: options.clean,
      logs: options.logs,
    }),
  };
}

function roleFromEnv(): CelldWorkerRole {
  const raw = (process.env.CELLD_ROLE ?? "scheduler").trim();
  if (raw !== "scheduler" && raw !== "gateway") {
    throw new Error(`CELLD_ROLE must be scheduler or gateway, got '${raw}'`);
  }
  return raw;
}

function startFromProcessEnv(): { projectDir: string; args: string[] } {
  const role = roleFromEnv();
  const defaults = ROLE_DEFAULTS[role];
  const port = process.env.CELLD_PORT === undefined || process.env.CELLD_PORT === "" ? defaults.port : Number(process.env.CELLD_PORT);
  return resolveCelldWorkerStart({
    rootDir: process.cwd(),
    role,
    projectDir: process.env.CELLD_PROJECT_DIR,
    host: process.env.CELLD_HOST,
    port,
    clean: process.env.CELLD_CLEAN === "1",
    logs: process.env.CELLD_LOGS !== "0",
    schedulerUrl: process.env.ANSTS_SCHEDULER_URL,
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
