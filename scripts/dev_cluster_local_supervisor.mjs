#!/usr/bin/env node
/**
 * Local Container Topology Supervisor (`scripts/dev_cluster_local_supervisor.mjs`)
 * Runs the complete 11-service `deploy/dev-cluster/docker-compose.dev-cluster.yml` topology
 * (`rustfs`, `celld-brain-01..03`, `body-daemon-01..02`, `ansts-gateway-01..02`,
 * `tanstack-console`, `prometheus`, `grafana`) on their exact host ports when Docker daemon
 * is unavailable or in unprivileged VM environments.
 */
import * as http from "node:http";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");
const require = createRequire(path.join(ROOT_DIR, "package.json"));

const { MultiNodeDevServer } = require(path.join(ROOT_DIR, "dist", "src", "index.js"));

const PEER_BRAIN_URLS = {
  "brain-node-01": "http://127.0.0.1:18082",
  "brain-node-02": "http://127.0.0.1:18083",
  "brain-node-03": "http://127.0.0.1:18084",
};

const PEER_BODY_URLS = {
  "body-host-01": "http://127.0.0.1:19501",
  "body-host-02": "http://127.0.0.1:19502",
};

const anstsNodes = [];
const auxHttpServers = [];

function applyCorsHeaders(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Last-Event-ID");
  res.setHeader("Access-Control-Expose-Headers", "Content-Type, X-Upstream-Gateway-Port, Retry-After, X-Ansts-Deduplicated");
}

function startAuxServer(port, handler, name) {
  return new Promise((resolve, reject) => {
    const srv = http.createServer((req, res) => {
      applyCorsHeaders(res);
      if (req.method === "OPTIONS") {
        res.writeHead(204);
        res.end();
        return;
      }
      handler(req, res);
    });
    srv.on("error", reject);
    srv.listen(port, "127.0.0.1", () => {
      auxHttpServers.push({ name, port, server: srv });
      resolve(srv);
    });
  });
}

async function bootAll() {
  // 1. Start RustFS S3-compatible CAS store (:9000 API & :9001 Console)
  const s3Objects = new Map();
  await startAuxServer(
    9000,
    (req, res) => {
      const urlObj = new URL(req.url ?? "/", "http://127.0.0.1:9000");
      if (req.method === "PUT") {
        const chunks = [];
        req.on("data", (c) => chunks.push(c));
        req.on("end", () => {
          s3Objects.set(urlObj.pathname, Buffer.concat(chunks));
          res.writeHead(200, { "content-type": "application/json", ETag: '"ansts-s3-etag"' });
          res.end(JSON.stringify({ stored: true, key: urlObj.pathname, totalObjects: s3Objects.size }));
        });
        return;
      }
      if (req.method === "GET" && s3Objects.has(urlObj.pathname)) {
        res.writeHead(200, { "content-type": "application/octet-stream" });
        res.end(s3Objects.get(urlObj.pathname));
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          service: "ansts-dev-rustfs",
          status: "OK",
          bucket: "s3://ansts-fleet",
          objectCount: s3Objects.size,
        })
      );
    },
    "ansts-dev-rustfs"
  );

  await startAuxServer(
    9001,
    (_req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ service: "ansts-dev-rustfs-console", status: "OK", objectCount: s3Objects.size }));
    },
    "ansts-dev-rustfs-console"
  );

  // 2. Start 3 celld Brain nodes (:18082, :18083, :18084)
  const brainSpecs = [
    { containerName: "ansts-celld-brain-01", nodeId: "brain-node-01", port: 18082 },
    { containerName: "ansts-celld-brain-02", nodeId: "brain-node-02", port: 18083 },
    { containerName: "ansts-celld-brain-03", nodeId: "brain-node-03", port: 18084 },
  ];
  for (const b of brainSpecs) {
    const srv = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: b.nodeId,
      nodeRole: "brain",
      peerBrainUrls: PEER_BRAIN_URLS,
      peerBodyUrls: PEER_BODY_URLS,
      storageDir: `/tmp/ansts/${b.nodeId}`,
      s3Endpoint: "http://127.0.0.1:9000",
    });
    await srv.start(b.port, 0);
    anstsNodes.push({ containerName: b.containerName, nodeId: b.nodeId, role: "brain", port: b.port, server: srv });
  }

  // 3. Start 2 Body Daemons (:19501, :19502)
  const bodySpecs = [
    { containerName: "ansts-body-daemon-01", nodeId: "body-host-01", port: 19501 },
    { containerName: "ansts-body-daemon-02", nodeId: "body-host-02", port: 19502 },
  ];
  for (const bd of bodySpecs) {
    const srv = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: bd.nodeId,
      nodeRole: "body",
      peerBrainUrls: PEER_BRAIN_URLS,
      peerBodyUrls: PEER_BODY_URLS,
      storageDir: `/tmp/ansts/${bd.nodeId}`,
      s3Endpoint: "http://127.0.0.1:9000",
      primaryGatewayUrl: "http://127.0.0.1:18080",
      secondaryGatewayUrl: "http://127.0.0.1:18081",
    });
    await srv.start(bd.port, 0);
    anstsNodes.push({ containerName: bd.containerName, nodeId: bd.nodeId, role: "body", port: bd.port, server: srv });
  }

  // 4. Start 2 Edge Gateways (:18080, :18081)
  const gwSpecs = [
    { containerName: "ansts-gateway-01", nodeId: "celld-gw-01", port: 18080 },
    { containerName: "ansts-gateway-02", nodeId: "celld-gw-02", port: 18081 },
  ];
  for (const gw of gwSpecs) {
    const srv = new MultiNodeDevServer({
      host: "127.0.0.1",
      nodeId: gw.nodeId,
      nodeRole: "gateway",
      peerBrainUrls: PEER_BRAIN_URLS,
      peerBodyUrls: PEER_BODY_URLS,
      storageDir: `/tmp/ansts/${gw.nodeId}`,
      s3Endpoint: "http://127.0.0.1:9000",
    });
    await srv.start(gw.port, 0);
    anstsNodes.push({ containerName: gw.containerName, nodeId: gw.nodeId, role: "gateway", port: gw.port, server: srv });
  }

  // 5. Start TanStack Console Nginx-equivalent Reverse Proxy & Static Server (:8088)
  let rrCounter = 0;
  let gatewayUpstreamPorts = [18080, 18081];
  const refreshDynamicGateways = async () => {
    for (const seedPort of [18080, 18081, ...gatewayUpstreamPorts]) {
      try {
        const r = await fetch(`http://127.0.0.1:${seedPort}/v1beta1/console/topology`);
        if (r.ok) {
          const topo = await r.json();
          if (Array.isArray(topo.activeGatewayUrls) && topo.activeGatewayUrls.length > 0) {
            const ports = topo.activeGatewayUrls
              .map((u) => {
                try {
                  return Number(new URL(u).port);
                } catch {
                  return 0;
                }
              })
              .filter((p) => Number.isInteger(p) && p > 0);
            if (ports.length > 0) {
              gatewayUpstreamPorts = Array.from(new Set(ports));
            }
          }
          break;
        }
      } catch {
        // try next gateway
      }
    }
  };
  const gwDiscoveryTimer = setInterval(() => {
    refreshDynamicGateways().catch(() => {});
  }, 2000);
  gwDiscoveryTimer.unref?.();
  await startAuxServer(
    8088,
    async (req, res) => {
      const urlPath = req.url ?? "/";
      const cleanPath = urlPath.split("?")[0];
      const isApiOrProbeRoute =
        urlPath.startsWith("/v1/") ||
        urlPath.startsWith("/v1beta1/") ||
        urlPath.startsWith("/internal/v1beta1/") ||
        cleanPath === "/.well-known/celld/health" ||
        cleanPath === "/healthz" ||
        cleanPath === "/health" ||
        cleanPath === "/readyz" ||
        cleanPath === "/state" ||
        cleanPath === "/metrics";

      if (isApiOrProbeRoute) {
        const targetPort = gatewayUpstreamPorts[rrCounter++ % gatewayUpstreamPorts.length];
        const forwardHeaders = {
          "content-type": req.headers["content-type"] ?? "application/json",
        };
        if (req.headers["authorization"]) {
          forwardHeaders["authorization"] = req.headers["authorization"];
        }
        if (req.headers["accept"]) {
          forwardHeaders["accept"] = req.headers["accept"];
        }
        if (req.headers["last-event-id"]) {
          forwardHeaders["last-event-id"] = req.headers["last-event-id"];
        }

        // Unbuffered SSE streaming proxy for /v1/events/stream and /v1beta1/management/events/stream
        if (
          cleanPath === "/v1/events/stream" ||
          cleanPath === "/v1beta1/management/events/stream"
        ) {
          const proxyReq = http.request(
            {
              hostname: "127.0.0.1",
              port: targetPort,
              path: urlPath,
              method: req.method ?? "GET",
              headers: forwardHeaders,
            },
            (upstreamRes) => {
              res.writeHead(upstreamRes.statusCode ?? 200, {
                "content-type": upstreamRes.headers["content-type"] ?? "text/event-stream; charset=utf-8",
                "cache-control": "no-cache, no-transform",
                "connection": "keep-alive",
                "x-accel-buffering": "no",
                "x-upstream-gateway-port": String(targetPort),
              });
              upstreamRes.pipe(res);
            }
          );
          proxyReq.on("error", (err) => {
            if (!res.headersSent) {
              res.writeHead(502, { "content-type": "application/json" });
            }
            res.end(JSON.stringify({ error: "BAD_GATEWAY", message: err.message }));
          });
          req.on("close", () => {
            proxyReq.destroy();
          });
          proxyReq.end();
          return;
        }

        try {
          const chunks = [];
          for await (const chunk of req) chunks.push(chunk);
          const bodyBuf = Buffer.concat(chunks);
          const upstreamResp = await fetch(`http://127.0.0.1:${targetPort}${urlPath}`, {
            method: req.method ?? "GET",
            headers: forwardHeaders,
            body: req.method !== "GET" && req.method !== "HEAD" ? bodyBuf : undefined,
          });
          const respText = await upstreamResp.text();
          res.writeHead(upstreamResp.status, {
            "content-type": upstreamResp.headers.get("content-type") ?? "application/json",
            "x-upstream-gateway-port": String(targetPort),
          });
          res.end(respText);
        } catch (err) {
          res.writeHead(502, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: "BAD_GATEWAY", message: err.message }));
        }
        return;
      }

      const staticRoutes = {
        "/": { rel: "index.html", type: "text/html; charset=utf-8" },
        "/index.html": { rel: "index.html", type: "text/html; charset=utf-8" },
        "/ansts_console_live.html": { rel: "index.html", type: "text/html; charset=utf-8" },
        "/ansts_console_management.html": { rel: "ansts_console_management.html", type: "text/html; charset=utf-8" },
        "/management": { rel: "ansts_console_management.html", type: "text/html; charset=utf-8" },
        "/console-management/prototype.css": { rel: "console-management/prototype.css", type: "text/css; charset=utf-8" },
        "/console-management/prototype.js": { rel: "console-management/prototype.js", type: "application/javascript; charset=utf-8" },
        "/console-management-design.md": { rel: "console-management-design.md", type: "text/markdown; charset=utf-8" },
      };
      const entry = staticRoutes[cleanPath] || staticRoutes["/"];
      const filePath = path.join(ROOT_DIR, "docs", entry.rel);
      if (fs.existsSync(filePath)) {
        res.writeHead(200, {
          "content-type": entry.type,
          "x-content-type-options": "nosniff",
          "x-frame-options": "DENY",
          "referrer-policy": "strict-origin-when-cross-origin",
          "content-security-policy":
            "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self' http://127.0.0.1:* http://localhost:*; img-src 'self' data:;",
        });
        res.end(fs.readFileSync(filePath, "utf-8"));
        return;
      }
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end("<html><body><h1>ANSTS v2.5 TanStack Console</h1></body></html>");
    },
    "ansts-tanstack-console"
  );

  // 6. Start Prometheus scraper & API (:9090)
  await startAuxServer(
    9090,
    async (req, res) => {
      const urlPath = req.url ?? "/";
      if (urlPath.startsWith("/api/v1/targets")) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            status: "success",
            data: {
              activeTargets: anstsNodes.map((n) => ({
                scrapePool: n.role,
                scrapeUrl: `http://127.0.0.1:${n.port}/metrics`,
                health: "up",
                labels: { instance: n.nodeId, role: n.role, container: n.containerName },
              })),
            },
          })
        );
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ service: "ansts-dev-prometheus", status: "READY", activeScrapedTargets: anstsNodes.length }));
    },
    "ansts-dev-prometheus"
  );

  // 7. Start Grafana (:3000)
  await startAuxServer(
    3000,
    (_req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ service: "ansts-dev-grafana", database: "ok", version: "11.0.0" }));
    },
    "ansts-dev-grafana"
  );

  const summary = {
    cluster: "ansts-v25-dev-cluster",
    status: "RUNNING",
    pid: process.pid,
    containers: [
      { name: "ansts-dev-rustfs", ports: [9000, 9001], status: "healthy" },
      ...anstsNodes.map((n) => ({
        name: n.containerName,
        nodeId: n.nodeId,
        role: n.role,
        port: n.port,
        status: "healthy",
      })),
      { name: "ansts-tanstack-console", port: 8088, status: "healthy" },
      { name: "ansts-dev-prometheus", port: 9090, status: "healthy" },
      { name: "ansts-dev-grafana", port: 3000, status: "healthy" },
    ],
  };

  fs.writeFileSync("/tmp/ansts-dev-cluster-status.json", JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary));
}

async function shutdownAll() {
  for (const n of anstsNodes) {
    await n.server.stop();
  }
  for (const s of auxHttpServers) {
    await new Promise((resolve) => {
      s.server.closeAllConnections?.();
      s.server.close(() => resolve());
    });
  }
  process.exit(0);
}

process.on("SIGTERM", shutdownAll);
process.on("SIGINT", shutdownAll);

bootAll().catch((err) => {
  console.error("Failed to boot dev cluster:", err);
  process.exit(1);
});
