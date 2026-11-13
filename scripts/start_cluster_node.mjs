#!/usr/bin/env node
/**
 * Unified Multi-Role Cluster Node Entrypoint (`scripts/start_cluster_node.mjs`)
 * Compatible with `Dockerfile.dev` and `deploy/dev-cluster/docker-compose.dev-cluster.yml` on `feat/celld-v25`.
 */
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");
const require = createRequire(path.join(ROOT_DIR, "package.json"));

const { MultiNodeDevServer } = require(path.join(ROOT_DIR, "dist", "src", "index.js"));

const listenPort = Number(process.env.ANSTS_LISTEN_PORT ?? process.env.ANSTS_PORT ?? 8080);
const dashPort = Number(process.env.ANSTS_DASHBOARD_PORT ?? listenPort + 100);
const server = new MultiNodeDevServer({
  host: process.env.ANSTS_HOST ?? "0.0.0.0",
  nodeId: process.env.ANSTS_NODE_ID ?? "ansts-dev-node-01",
  nodeRole: process.env.ANSTS_NODE_ROLE ?? "gateway",
});

server.start(listenPort, dashPort).then(({ apiPort, dashboardPort }) => {
  console.log(
    JSON.stringify({
      nodeId: server.nodeId,
      nodeRole: server.nodeRole,
      apiPort,
      dashboardPort,
      status: "READY",
    })
  );
});

const shutdown = () => {
  server.stop().then(() => process.exit(0));
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

if (process.env.ANSTS_BODY_CELL_URL && process.env.ANSTS_NODE_ROLE === "body") {
  await import("./body_cell_agent.mjs");
}
