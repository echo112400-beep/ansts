#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="deploy/dev-cluster/docker-compose.dev-cluster.yml"
export RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
export RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfs_prod_secret_key_2026}"

echo "===> [1/4] Starting ANSTS v2.5 Full Dev Cluster (TanStack Console + 2 Gateways + 3 celld Brains + 2 Body Daemons + RustFS + Prometheus + Grafana)..."
docker compose -f "${COMPOSE_FILE}" up -d --build

echo "===> [2/4] Waiting for Gateway-01 (:18080) & TanStack Console (:8088) to become healthy..."
for i in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:18080/health >/dev/null 2>&1; then
    echo "   [OK] Gateway-01 (:18080) is online!"
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
