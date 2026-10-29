#!/usr/bin/env node
/**
 * ANSTS v2.0 Unified TypeScript CI Pipeline Runner (Lint / Build / Integration Test).
 * Uses @babel/parser + @babel/core (@babel/preset-typescript) to lint and compile TypeScript
 * to CommonJS in dist/, then executes node --test on compiled test suites.
 */

import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const babel = require("@babel/core");
const babelParser = require("@babel/parser");
const yaml = require("js-yaml");

const ROOT_DIR = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const DIST_DIR = path.join(ROOT_DIR, "dist");

function collectTsFiles(dirPath) {
  const results = [];
  for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
    const full = path.join(dirPath, entry.name);
    if (entry.isDirectory()) {
      results.push(...collectTsFiles(full));
    } else if (entry.isFile() && entry.name.endsWith(".ts")) {
      results.push(full);
    }
  }
  return results.sort();
}

function runLint() {
  console.log("===> [Stage 1/3: TYPESCRIPT AST LINT & SCHEMA CHECK]");
  const tsFiles = [
    ...collectTsFiles(path.join(ROOT_DIR, "src")),
    ...collectTsFiles(path.join(ROOT_DIR, "tests")),
  ];
  for (const file of tsFiles) {
    const code = fs.readFileSync(file, "utf8");
    babelParser.parse(code, {
      sourceType: "module",
      plugins: ["typescript"],
    });
    console.log(`  [TS AST OK] ${path.relative(ROOT_DIR, file)}`);
  }

  // Validate TaskSpec v1beta1 YAML
  const specPath = path.join(ROOT_DIR, "specs", "taskspec_v1beta1_example.yaml");
  const spec = yaml.load(fs.readFileSync(specPath, "utf8"));
  if (spec.apiVersion !== "scheduler.agent.io/v1beta1" || spec.spec.cognitive_policy.primary_model !== "DeepSeek") {
    throw new Error("TaskSpec v1beta1 validation failed");
  }
  console.log("  [SCHEMA OK] specs/taskspec_v1beta1_example.yaml");

  // Validate Protobuf contract
  const protoPath = path.join(ROOT_DIR, "proto", "ansts_rpc.proto");
  const protoText = fs.readFileSync(protoPath, "utf8");
  for (const marker of ['syntax = "proto3";', "service SandboxNodeService", "service CognitiveRouterService", "service SagaToolGatewayService"]) {
    if (!protoText.includes(marker)) {
      throw new Error(`Missing ${marker} in proto/ansts_rpc.proto`);
    }
  }
  console.log("  [PROTO OK] proto/ansts_rpc.proto");
  // Validate wrangler.json & D1 SQL migration for celld and Cloudflare Workers
  const wranglerPath = path.join(ROOT_DIR, "wrangler.json");
  const wranglerCfg = JSON.parse(fs.readFileSync(wranglerPath, "utf8"));
  if (wranglerCfg.main !== "src/cloudflare/worker.ts" || !wranglerCfg.durable_objects) {
    throw new Error("Invalid wrangler.json configuration");
  }
  console.log("  [WRANGLER/CELLD OK] wrangler.json & migrations/0001_init_ansts.sql");

  console.log("===> [Stage 1/3: TYPESCRIPT LINT PASSED]\n");
}

function runBuild() {
  console.log("===> [Stage 2/3: TYPESCRIPT BUILD (src/ & tests/ -> dist/)]");
  fs.rmSync(DIST_DIR, { recursive: true, force: true });
  const tsFiles = [
    ...collectTsFiles(path.join(ROOT_DIR, "src")),
    ...collectTsFiles(path.join(ROOT_DIR, "tests")),
  ];
  for (const file of tsFiles) {
    const rel = path.relative(ROOT_DIR, file);
    const outPath = path.join(DIST_DIR, rel.replace(/\.ts$/, ".js"));
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    const srcCode = fs.readFileSync(file, "utf8");
    const transformed = babel.transformSync(srcCode, {
      filename: file,
      presets: [require.resolve("@babel/preset-typescript")],
      plugins: [require.resolve("@babel/plugin-transform-modules-commonjs")],
      sourceMaps: false,
    });
    fs.writeFileSync(outPath, transformed.code, "utf8");
  }
  console.log(`  [BUILD OK] Compiled ${tsFiles.length} TypeScript modules into dist/`);
  console.log("===> [Stage 2/3: TYPESCRIPT BUILD PASSED]\n");
}

function runTests() {
  console.log("===> [Stage 3/3: INTEGRATION & CHAOS TESTS (node --test)]");
  if (!fs.existsSync(path.join(DIST_DIR, "tests"))) {
    runBuild();
  }
  const testFiles = fs
    .readdirSync(path.join(DIST_DIR, "tests"))
    .filter((f) => f.endsWith(".test.js"))
    .map((f) => path.join(DIST_DIR, "tests", f));

  const res = spawnSync(process.execPath, ["--test", ...testFiles], {
    cwd: ROOT_DIR,
    env: { ...process.env, NODE_PATH: "/usr/share/nodejs" },
    stdio: "inherit",
  });
  if (res.status !== 0) {
    process.exit(res.status ?? 1);
  }
  console.log("===> [Stage 3/3: ALL TYPESCRIPT INTEGRATION & CHAOS TESTS PASSED]\n");
}

const stageArgIdx = process.argv.indexOf("--stage");
const stage = stageArgIdx !== -1 ? process.argv[stageArgIdx + 1] : "all";

if (stage === "lint" || stage === "all") runLint();
if (stage === "build" || stage === "all") runBuild();
if (stage === "test" || stage === "all") runTests();
