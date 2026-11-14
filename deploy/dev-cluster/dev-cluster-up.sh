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

echo "===> [1/4] Starting ANSTS v2.5 Full Dev Cluster (TanStack Console + 2 Gateways + 3 celld Brains + Body celld + 2 Native Daemons + RustFS + Prometheus + Grafana)..."
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker compose -f "${COMPOSE_FILE}" stop body-daemon-01 body-daemon-02
  docker compose -f "${COMPOSE_FILE}" up -d --build
else
  echo 'Docker is required for the independent Body celld + native Daemon cluster.' >&2
  exit 1
fi

echo "===> [2/4] Waiting for Gateway-01 (:18080) & TanStack Console (:8088) to become healthy..."
for i in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:18080/health >/dev/null 2>&1 && curl -fsS http://127.0.0.1:8088/ >/dev/null 2>&1; then
    echo "   [OK] Gateway-01 (:18080), Gateway-02 (:18081), Brains (:18082..18084), Bodies (:19501..19502) & TanStack Console (:8088) are online!"
    break
  fi
  sleep 1
done

echo "===> [3/4] Verifying independent Body control and native execution..."
docker compose -f "$COMPOSE_FILE" run --rm -T --no-deps \
  -e ANSTS_SMOKE_GATEWAY=http://ansts-gateway-01:8080 \
  -e ANSTS_SMOKE_BRAIN=http://body-celld:8080 \
  -e ANSTS_SMOKE_DAEMON=http://body-daemon-01:9501 \
  body-state-migrate node /app/scripts/verify_body_attempt_protocol.mjs

echo "===> [4/4] Verifying the authenticated console..."
docker compose -f "$COMPOSE_FILE" run --rm -T --no-deps \
  -e ANSTS_SMOKE_CONSOLE=http://tanstack-console \
  body-state-migrate node /app/scripts/verify_console_deployment.mjs
