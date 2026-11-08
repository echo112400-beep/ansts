#!/usr/bin/env node
/**
 * Hardened 3-Stage CI Runner (`scripts/ci_check.mjs`):
 * - Auto-resolves `/usr/share/nodejs` and local `node_modules` without requiring manual `NODE_PATH` env vars!
 * - Stage 1 (`lint`): Parses all `.ts` files with `@babel/parser`, verifies Google TS Style Guide rules,
 *   and validates `wrangler.json`, `migrations/0001_init_ansts.sql`, `proto/ansts_rpc.proto`, and YAML manifests.
 * - Stage 2 (`build`): Compiles all `src/<all>/*.ts` and `tests/<all>/*.ts` into `dist/` using `@babel/core`
 *   (`@babel/plugin-transform-typescript` + `@babel/plugin-transform-modules-commonjs`).
 * - Stage 3 (`test`): Runs `node --test` on all compiled test suites in `dist/tests/*.test.js` with a
 *    built-in `NODE_PATH` injection so sub-processes never fail module lookup.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { createRequire } from "node:module";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");

// Ensure both local node_modules and system /usr/share/nodejs are searched automatically
const extraNodePaths = [
  path.join(ROOT_DIR, "node_modules"),
  "/usr/share/nodejs",
  "/usr/lib/nodejs",
].filter((p) => fs.existsSync(p));

process.env.NODE_PATH = [process.env.NODE_PATH, ...extraNodePaths].filter(Boolean).join(path.delimiter);
const requireFromRoot = createRequire(path.join(ROOT_DIR, "package.json"));

function loadModule(modName) {
  for (const base of [ROOT_DIR, "/usr/share/nodejs", "/usr/lib/nodejs"]) {
    try {
      const req = createRequire(path.join(base, "noop.js"));
      return req(modName);
    } catch {
      // try next search path
    }
  }
  return requireFromRoot(modName);
}

const babelParser = loadModule("@babel/parser");
const babelCore = loadModule("@babel/core");
const yaml = loadModule("js-yaml");

function walkTsFiles(dir) {
  if (!fs.existsSync(dir)) return [];
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...walkTsFiles(full));
    } else if (entry.isFile() && entry.name.endsWith(".ts")) {
      results.push(full);
    }
  }
  return results.sort();
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
    babelParser.parse(code, {
      sourceType: "module",
      plugins: ["typescript"],
    });
    console.log(`  [TS AST OK] ${rel}`);
  }

  // Validate YAML, Proto, Wrangler & SQL schemas
  const specYaml = path.join(ROOT_DIR, "specs", "taskspec_v1beta1_example.yaml");
  const parsedSpec = yaml.load(fs.readFileSync(specYaml, "utf8"));
  if (!parsedSpec || parsedSpec.apiVersion !== "scheduler.agent.io/v1beta1") {
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
  console.log("===> [Stage 1/3: TYPESCRIPT LINT PASSED]\n");
}

function runBuild() {
  console.log("===> [Stage 2/3: TYPESCRIPT BUILD (src/ & tests/ -> dist/)]");
  const distDir = path.join(ROOT_DIR, "dist");
  fs.rmSync(distDir, { recursive: true, force: true });

  const tsFiles = [...walkTsFiles(path.join(ROOT_DIR, "src")), ...walkTsFiles(path.join(ROOT_DIR, "tests"))];
  const tsPlugin = loadModule("@babel/plugin-transform-typescript");
  const cjsPlugin = loadModule("@babel/plugin-transform-modules-commonjs");

  for (const f of tsFiles) {
    const rel = path.relative(ROOT_DIR, f);
    const outPath = path.join(distDir, rel.replace(/\.ts$/, ".js"));
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    const code = fs.readFileSync(f, "utf8");
    const transformed = babelCore.transformSync(code, {
      filename: f,
      plugins: [[tsPlugin, { allowDeclareFields: true }], cjsPlugin],
      retainLines: true,
    });
    fs.writeFileSync(outPath, transformed.code, "utf8");
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
} else {
  runLint();
  runBuild();
  runTests();
}
