#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="deploy/dev-cluster/docker-compose.dev-cluster.yml"

# Bypass any corporate/local HTTP proxy for loopback & RFC1918 health probes.
# NOTE: proxy settings commonly use wildcards like `127.*`, which curl does NOT honour,
# which would make every localhost probe below return `502 Bad Gateway`.
export no_proxy="127.0.0.1,localhost,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
export NO_PROXY="${no_proxy}"

echo "===> [1/4] Starting ANSTS v2.5 Full Dev Cluster (TanStack Console + 2 Gateways + 3 celld Brains + 2 Body Daemons + RustFS + Prometheus + Grafana)..."
docker compose -f "${COMPOSE_FILE}" up -d --build

echo "===> [2/4] Waiting for Gateway-01 (:18080) & TanStack Console (:8088) to become healthy..."
for i in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:18080/health >/dev/null 2>&1; then
    echo "  [OK] Gateway-01 (:18080) is online!"
    break
  fi
  sleep 1
done

echo "===> [3/4] Registering Dual-Homed Reverse Tunnels & Dispatching Distributed Smoke Task..."
curl -fsS -X POST http://127.0.0.1:18080/internal/v1beta1/tunnel/register \
  -H "Content-Type: application/json" \
  -d '{"hostId":"body-host-01","gatewayId":"celld-gw-01","role":"PRIMARY","connGeneration":1,"epoch":1}' | sed 's/^/  [Tunnel Primary] /'
echo ""

curl -fsS -X POST http://127.0.0.1:18081/internal/v1beta1/tunnel/register \
  -H "Content-Type: application/json" \
  -d '{"hostId":"body-host-01","gatewayId":"celld-gw-02","role":"SECONDARY","connGeneration":1,"epoch":1}' | sed 's/^/  [Tunnel Secondary] /'
echo ""

# Smoke task id must be unique per run: WAL writes use `If-None-Match` create-once
# semantics, so a fixed id makes every re-run of this script report
# `412 PreconditionFailed: ...turn_0001.wal already exists on disk`.
SMOKE_TASK_ID="task-distributed-$(date +%Y%m%d%H%M%S)-${RANDOM}"
DISPATCH_RESPONSE=$(curl -fsS -X POST http://127.0.0.1:18080/v1beta1/tasks/dispatch \
  -H "Content-Type: application/json" \
  -d "{\"tenantId\":\"tenant-alpha\",\"taskId\":\"${SMOKE_TASK_ID}\",\"repoId\":\"ansts-monorepo\",\"promptText\":\"verify distributed celld dispatch\",\"targetBodyHostId\":\"body-host-01\"}")
echo "  [Dispatch Result] ${DISPATCH_RESPONSE}"
echo ""
case "${DISPATCH_RESPONSE}" in
  *'"status":"EXECUTED"'*) ;;
  *)
    echo "  [FAIL] Distributed smoke task ${SMOKE_TASK_ID} was not executed." >&2
    exit 1
    ;;
esac

echo "===> [4/4] Verifying TanStack Console BFF Endpoints & Prometheus Metrics..."
curl -fsS "http://127.0.0.1:8088/v1beta1/console/overview?region=us-west-2" | sed 's/^/  [TanStack Console Overview] /'
echo ""
curl -fsS "http://127.0.0.1:8088/v1beta1/console/shards" | sed 's/^/  [TanStack Shard Matrix] /'
echo ""
echo "================================================================================"
echo "  ANSTS v2.5 Dev Cluster Ready!"
echo "  - TanStack Management Console UI : http://localhost:8088"
echo "  - Gateway-01 / BFF API           : http://localhost:18080"
echo "  - Prometheus Metrics UI          : http://localhost:9090"
echo "  - Grafana Dashboard (admin/admin): http://localhost:3000"
echo "  - RustFS S3 Console              : http://localhost:9001"
echo "================================================================================"
