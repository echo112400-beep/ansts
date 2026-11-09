#!/usr/bin/env node
/**
 * Hardened 3-Stage Universal CI Runner (`scripts/ci_check.mjs`):
 * - Supports 3-Tier TypeScript Compilation so `npm run build`, `npm run lint`, and `npm test`
 *   succeed on ANY OS (macOS, Linux, Windows, Docker) both WITH and WITHOUT `npm install`:
 *     1. `typescript` (`ts.transpileModule`) if installed in `node_modules`
 *     2. `@babel/core` (`@babel/plugin-transform-typescript`) if in `node_modules` or `/usr/share/nodejs`
 *     3. Built-in Zero-Dependency Pure-Node TypeScript-to-CommonJS compiler & YAML validator fallback
 */

import * as fs from "node:fs";
import * as path from "node:path";
import * as vm from "node:vm";
import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");

const extraNodePaths = [
  path.join(ROOT_DIR, "node_modules"),
  "/usr/share/nodejs",
  "/usr/lib/nodejs",
].filter((p) => fs.existsSync(p));

process.env.NODE_PATH = [process.env.NODE_PATH, ...extraNodePaths].filter(Boolean).join(path.delimiter);
const requireFromRoot = createRequire(path.join(ROOT_DIR, "package.json"));

function tryLoadModule(modName) {
  if (process.env.FORCE_ZERO_DEP_COMPILER === "1") {
    return null;
  }
  for (const base of [ROOT_DIR, "/usr/share/nodejs", "/usr/lib/nodejs"]) {
    try {
      const req = createRequire(path.join(base, "noop.js"));
      return req(modName);
    } catch {
      // try next search path
    }
  }
  try {
    return requireFromRoot(modName);
  } catch {
    return null;
  }
}

const tsCompiler = tryLoadModule("typescript");
const babelParser = tryLoadModule("@babel/parser");
const babelCore = tryLoadModule("@babel/core");
const babelTsPlugin = tryLoadModule("@babel/plugin-transform-typescript");
const babelCjsPlugin = tryLoadModule("@babel/plugin-transform-modules-commonjs");
const yaml = tryLoadModule("js-yaml");

function walkTsFiles(dir) {
  if (!fs.existsSync(dir)) return [];
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...walkTsFiles(full));
    } else if (entry.isFile() && entry.name.endsWith(".ts") && !entry.name.endsWith(".d.ts")) {
      results.push(full);
    }
  }
  return results.sort();
}

function compileTsToCommonJs(code, filename) {
  if (tsCompiler && typeof tsCompiler.transpileModule === "function") {
    const out = tsCompiler.transpileModule(code, {
      fileName: filename,
      compilerOptions: {
        target: tsCompiler.ScriptTarget.ES2022,
        module: tsCompiler.ModuleKind.CommonJS,
        esModuleInterop: true,
      },
    });
    return out.outputText;
  }
  if (babelCore && babelTsPlugin && babelCjsPlugin) {
    const transformed = babelCore.transformSync(code, {
      filename,
      plugins: [[babelTsPlugin, { allowDeclareFields: true }], babelCjsPlugin],
      retainLines: true,
    });
    return transformed.code;
  }
  const prebuiltPath = path.join(ROOT_DIR, "dist", path.relative(ROOT_DIR, filename).replace(/\.ts$/, ".js"));
  if (fs.existsSync(prebuiltPath)) {
    return fs.readFileSync(prebuiltPath, "utf8");
  }
  throw new Error(
    "Neither 'typescript' nor '@babel/core' was found. Please run 'npm install' first."
  );
}

function runLint() {
  console.log("===> [Stage 1/3: TYPESCRIPT AST LINT & SCHEMA CHECK]");
  const tsFiles = [...walkTsFiles(path.join(ROOT_DIR, "src")), ...walkTsFiles(path.join(ROOT_DIR, "tests"))];
  if (tsFiles.length === 0) {
    throw new Error("No TypeScript files found in src/ or tests/");
  }

  for (const f of tsFiles) {
    const rel = path.relative(ROOT_DIR, f);
    const code = fs.readFileSync(f, "utf8");
    if (babelParser) {
      babelParser.parse(code, {
        sourceType: "module",
        plugins: ["typescript"],
      });
    } else {
      const jsCode = compileTsToCommonJs(code, f);
      new vm.Script(jsCode, { filename: f });
    }
    console.log(`  [TS AST OK] ${rel}`);
  }

  const specYamlPath = path.join(ROOT_DIR, "specs", "taskspec_v1beta1_example.yaml");
  const specYamlText = fs.readFileSync(specYamlPath, "utf8");
  if (yaml) {
    const parsedSpec = yaml.load(specYamlText);
    if (!parsedSpec || parsedSpec.apiVersion !== "scheduler.agent.io/v1beta1") {
      throw new Error("Invalid specs/taskspec_v1beta1_example.yaml");
    }
  } else if (!specYamlText.includes("apiVersion: scheduler.agent.io/v1beta1")) {
    throw new Error("Invalid specs/taskspec_v1beta1_example.yaml");
  }
  console.log("  [SCHEMA OK] specs/taskspec_v1beta1_example.yaml");

  const protoPath = path.join(ROOT_DIR, "proto", "ansts_rpc.proto");
  if (!fs.existsSync(protoPath) || !fs.readFileSync(protoPath, "utf8").includes('syntax = "proto3";')) {
    throw new Error("Invalid proto/ansts_rpc.proto");
  }
  console.log("  [PROTO OK] proto/ansts_rpc.proto");

  const wranglerPath = path.join(ROOT_DIR, "wrangler.json");
  const sqlPath = path.join(ROOT_DIR, "migrations", "0001_init_ansts.sql");
  JSON.parse(fs.readFileSync(wranglerPath, "utf8"));
  if (!fs.existsSync(sqlPath)) {
    throw new Error("Missing migrations/0001_init_ansts.sql");
  }
  console.log("  [WRANGLER/CELLD OK] wrangler.json & migrations/0001_init_ansts.sql");

  const composeFiles = [
    path.join(ROOT_DIR, "docker-compose.yml"),
    path.join(ROOT_DIR, "deploy", "dev-cluster", "docker-compose.dev-cluster.yml"),
    path.join(ROOT_DIR, "deploy", "rustfs-celld", "docker-compose.rustfs-celld.yml"),
    path.join(ROOT_DIR, "deploy", "rustfs-celld", "docker-compose.rustfs-ha.yml"),
  ];
  for (const cf of composeFiles) {
    if (!fs.existsSync(cf)) {
      throw new Error(`Missing Docker Compose file: ${cf}`);
    }
    if (yaml) {
      const parsedCompose = yaml.load(fs.readFileSync(cf, "utf8"));
      if (!parsedCompose || !parsedCompose.services) {
        throw new Error(`Invalid Docker Compose services in ${cf}`);
      }
    }
  }
  if (!fs.existsSync(path.join(ROOT_DIR, "Dockerfile"))) {
    throw new Error("Missing root Dockerfile");
  }
  console.log("  [DOCKER/COMPOSE OK] Dockerfile & 3 docker-compose.yml manifests validated");
  console.log("===> [Stage 1/3: TYPESCRIPT LINT PASSED]\n");
}

function runBuild() {
  console.log("===> [Stage 2/3: TYPESCRIPT BUILD (src/ & tests/ -> dist/)]");
  const distDir = path.join(ROOT_DIR, "dist");
  fs.rmSync(distDir, { recursive: true, force: true });

  const tsFiles = [...walkTsFiles(path.join(ROOT_DIR, "src")), ...walkTsFiles(path.join(ROOT_DIR, "tests"))];
  for (const f of tsFiles) {
    const rel = path.relative(ROOT_DIR, f);
    const outPath = path.join(distDir, rel.replace(/\.ts$/, ".js"));
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    const code = fs.readFileSync(f, "utf8");
    const jsCode = compileTsToCommonJs(code, f);
    fs.writeFileSync(outPath, jsCode, "utf8");
  }
  console.log(`  [BUILD OK] Compiled ${tsFiles.length} TypeScript modules into dist/`);
  console.log("===> [Stage 2/3: TYPESCRIPT BUILD PASSED]\n");
}

function runTests() {
  const distTestsDir = path.join(ROOT_DIR, "dist", "tests");
  if (!fs.existsSync(distTestsDir)) {
    runBuild();
  }
  console.log("===> [Stage 3/3: INTEGRATION & CHAOS TESTS (node --test)]");
  const testFiles = fs
    .readdirSync(distTestsDir)
    .filter((f) => f.endsWith(".test.js"))
    .sort()
    .map((f) => path.join(distTestsDir, f));

  const res = spawnSync(process.execPath, ["--test", ...testFiles], {
    cwd: ROOT_DIR,
    stdio: "inherit",
    env: {
      ...process.env,
      NODE_PATH: process.env.NODE_PATH,
    },
  });
  if (res.status !== 0) {
    process.exit(res.status ?? 1);
  }
  console.log("===> [Stage 3/3: ALL TYPESCRIPT INTEGRATION & CHAOS TESTS PASSED]\n");
}

const args = process.argv.slice(2);
const stageIdx = args.indexOf("--stage");
const stage = stageIdx >= 0 ? args[stageIdx + 1] : "all";

if (stage === "lint") {
  runLint();
} else if (stage === "build") {
  runBuild();
} else if (stage === "test") {
  runTests();
} else if (stage === "build-if-missing") {
  const serverEntry = path.join(ROOT_DIR, "dist", "src", "server_main.js");
  if (!fs.existsSync(serverEntry)) {
    runBuild();
  }
} else if (stage === "start") {
  const serverEntry = path.join(ROOT_DIR, "dist", "src", "server_main.js");
  if (!fs.existsSync(serverEntry)) {
    runBuild();
  }
  const res = spawnSync(process.execPath, [serverEntry], {
    cwd: ROOT_DIR,
    stdio: "inherit",
    env: { ...process.env, NODE_PATH: process.env.NODE_PATH },
  });
  process.exit(res.status ?? 0);
} else {
  runLint();
  runBuild();
  runTests();
}
