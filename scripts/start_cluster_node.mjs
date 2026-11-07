#!/usr/bin/env node
/**
 * ANSTS v2.5 Multi-Instance Docker Compose Node Entrypoint.
 * Compiles TypeScript if dist/ is missing, then launches MultiNodeClusterDaemonServer
 * configured via environment variables (ANSTS_NODE_ID, ANSTS_NODE_ROLE, ANSTS_LISTEN_PORT, etc.).
 */

import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";

const ROOT_DIR = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const DIST_INDEX = path.join(ROOT_DIR, "dist/src/index.js");

if (!fs.existsSync(DIST_INDEX)) {
  console.log("[Boot] Compiling TypeScript sources to dist/...");
  const build = spawnSync("node", ["scripts/ci_check.mjs", "--stage", "build"], {
    cwd: ROOT_DIR,
    stdio: "inherit",
  });
  if (build.status !== 0) {
    process.exit(build.status ?? 1);
  }
}

const require = createRequire(import.meta.url);
const { MultiNodeClusterDaemonServer } = require(DIST_INDEX);

const nodeId = process.env.ANSTS_NODE_ID ?? "brain-node-01";
const role = process.env.ANSTS_NODE_ROLE ?? "brain";
const listenPort = parseInt(process.env.ANSTS_LISTEN_PORT ?? "8080", 10);
const storageDir = process.env.ANSTS_STORAGE_DIR ?? `/var/lib/ansts/${nodeId}`;
const peerBrainUrls = process.env.ANSTS_PEER_BRAIN_URLS
  ? JSON.parse(process.env.ANSTS_PEER_BRAIN_URLS)
  : {
      "brain-node-01": "http://celld-brain-01:8081",
      "brain-node-02": "http://celld-brain-02:8082",
      "brain-node-03": "http://celld-brain-03:8083",
    };
const peerBodyUrls = process.env.ANSTS_PEER_BODY_URLS
  ? JSON.parse(process.env.ANSTS_PEER_BODY_URLS)
  : {
      "body-host-01": "http://body-daemon-01:9501",
      "body-host-02": "http://body-daemon-02:9502",
    };

const server = new MultiNodeClusterDaemonServer({
  nodeId,
  role,
  listenPort,
  peerBrainUrls,
  peerBodyUrls,
  storageDir,
});

server.start().then((port) => {
  console.log(`[ANSTS v2.5 Cluster Node Ready] nodeId=${nodeId} role=${role} listening=0.0.0.0:${port}`);
});
