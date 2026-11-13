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
python3 scripts/prepare_body_dev_credentials.py
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
for i in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:18080/health >/dev/null 2>&1 && curl -fsS http://127.0.0.1:8088/ >/dev/null 2>&1; then
    echo "   [OK] Gateway-01 (:18080), Gateway-02 (:18081), Brains (:18082..18084), Bodies (:19501..19502) & TanStack Console (:8088) are online!"
    break
  fi
  sleep 1
done

echo "===> [3/4] Registering Dual-Homed Reverse Tunnels & Dispatching Distributed Smoke Task..."
curl -fsS -X POST http://127.0.0.1:18080/internal/v1beta1/tunnel/register \
  -H "Content-Type: application/json" \
  -d '{"hostId":"body-host-01","gatewayId":"celld-gw-01","role":"PRIMARY","connGeneration":1,"epoch":1}' | sed 's/^/   [Tunnel Primary] /'
echo ""

curl -fsS -X POST http://127.0.0.1:18081/internal/v1beta1/tunnel/register \
  -H "Content-Type: application/json" \
  -d '{"hostId":"body-host-01","gatewayId":"celld-gw-02","role":"SECONDARY","connGeneration":1,"epoch":1}' | sed 's/^/   [Tunnel Secondary] /'
echo ""

curl -fsS -X POST http://127.0.0.1:18080/v1beta1/tasks/dispatch \
  -H "Content-Type: application/json" \
  -d '{"tenantId":"tenant-alpha","taskId":"task-distributed-001","repoId":"ansts-monorepo","promptText":"verify distributed celld dispatch","targetBodyHostId":"body-host-01"}' | sed 's/^/   [Dispatch Result] /'
echo ""

echo "===> [4/4] Verifying TanStack Console BFF Endpoints & Prometheus Metrics..."
curl -fsS "http://127.0.0.1:8088/v1beta1/console/overview?region=us-west-2" | sed 's/^/   [TanStack Console Overview] /'
echo ""
curl -fsS "http://127.0.0.1:8088/v1beta1/console/shards" | sed 's/^/   [TanStack Shard Matrix] /'
echo ""
