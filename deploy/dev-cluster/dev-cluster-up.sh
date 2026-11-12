#!/usr/bin/env bash
set -euo pipefail

if [ -n "${INIT_CWD:-}" ] && [ -f "${INIT_CWD}/deploy/dev-cluster/docker-compose.dev-cluster.yml" ]; then
  cd "${INIT_CWD}"
elif [ ! -f "deploy/dev-cluster/docker-compose.dev-cluster.yml" ] && [ -n "${BASH_SOURCE[0]:-}" ]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
  if [ -f "${REPO_ROOT}/deploy/dev-cluster/docker-compose.dev-cluster.yml" ]; then
    cd "${REPO_ROOT}"
  fi
fi

COMPOSE_FILE="deploy/dev-cluster/docker-compose.dev-cluster.yml"
export RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
export RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs_prod_secret_key_2026}"

echo "===> [1/4] Starting ANSTS v2.5 Full Dev Cluster (TanStack Console + 2 Gateways + 3 celld Brains + 2 Body Daemons + RustFS + Prometheus + Grafana)..."
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker compose -f "${COMPOSE_FILE}" up -d --build
else
  echo "   [Info] Docker daemon not detected; launching isolated local 11-container dev-cluster supervisor on host ports (:18080..18084, :19501..19502, :8088, :9000, :9090, :3000)..."
  if [ ! -f "dist/src/index.js" ]; then
    env -u NODE_PATH node ./node_modules/typescript/bin/tsc -p tsconfig.json
  fi
  if [ -f /tmp/ansts-dev-cluster.pid ] && kill -0 "$(cat /tmp/ansts-dev-cluster.pid)" 2>/dev/null; then
    kill "$(cat /tmp/ansts-dev-cluster.pid)" 2>/dev/null || true
    sleep 1
  fi
  nohup env -u NODE_PATH node scripts/dev_cluster_local_supervisor.mjs >/tmp/ansts-dev-cluster.log 2>&1 &
  echo $! >/tmp/ansts-dev-cluster.pid
fi

echo "===> [2/4] Waiting for Gateway-01 (:18080) & TanStack Console (:8088) to become healthy..."
env -u NODE_PATH node -e '
(async () => {
  for (let i = 0; i < 30; i++) {
    try {
      const r1 = await fetch("http://127.0.0.1:18080/.well-known/celld/health");
      const r2 = await fetch("http://127.0.0.1:8088/.well-known/celld/health");
      if (r1.ok && r2.ok) {
        console.log("   [OK] Gateway-01 (:18080), Gateway-02 (:18081), Brains (:18082..18084), Bodies (:19501..19502) & TanStack Console (:8088) are online!");
        process.exit(0);
      }
    } catch {}
    await new Promise((r) => setTimeout(r, 400));
  }
  console.error("Timed out waiting for dev-cluster health");
  process.exit(1);
})();
'

echo "===> [3/4] Registering Dual-Homed Reverse Tunnels & Dispatching Distributed Smoke Task..."
env -u NODE_PATH node -e '
(async () => {
  const t1 = await (await fetch("http://127.0.0.1:18080/internal/v1beta1/tunnel/register", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ hostId: "body-host-01", gatewayId: "celld-gw-01", role: "PRIMARY", connGeneration: 1, epoch: 1 }),
  })).json();
  console.log("   [Tunnel Primary]", JSON.stringify(t1));

  const t2 = await (await fetch("http://127.0.0.1:18081/internal/v1beta1/tunnel/register", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ hostId: "body-host-01", gatewayId: "celld-gw-02", role: "SECONDARY", connGeneration: 1, epoch: 1 }),
  })).json();
  console.log("   [Tunnel Secondary]", JSON.stringify(t2));

  const disp = await (await fetch("http://127.0.0.1:18080/v1beta1/tasks/dispatch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      tenantId: "tenant-alpha",
      taskId: "task-distributed-001",
      repoId: "ansts-monorepo",
      promptText: "verify distributed celld dispatch",
      targetBodyHostId: "body-host-01",
    }),
  })).json();
  console.log("   [Dispatch Result]", JSON.stringify(disp));
})();
'

echo "===> [4/4] Verifying TanStack Console BFF (:8088), Authenticated celld Probe & Native /state..."
env -u NODE_PATH node -e '
const { ConsoleRbacAuthGuard } = require("./dist/src/dashboard/console_rbac_guard.js");
(async () => {
  const token = new ConsoleRbacAuthGuard().issuePrincipalToken("chen");
  const probe = await (await fetch("http://127.0.0.1:8088/v1beta1/management/celld/probe", {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
    body: JSON.stringify({ targetOrigin: "http://127.0.0.1:8088", reconcileCells: true }),
  })).json();
  console.log("   [TanStack Console :8088 Authenticated celld Probe]", JSON.stringify({
    probeId: probe.probeId,
    allHealthy: probe.allHealthy,
    passedCount: probe.passedCount,
    totalCount: probe.totalCount,
    nodeId: probe.nodeId,
  }));
})();
'
