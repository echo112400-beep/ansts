#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="deploy/dev-cluster/docker-compose.dev-cluster.yml"

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker compose -f "${COMPOSE_FILE}" down -v
fi

if [ -f /tmp/ansts-dev-cluster.pid ]; then
  PID="$(cat /tmp/ansts-dev-cluster.pid)"
  if kill -0 "${PID}" 2>/dev/null; then
    kill "${PID}" 2>/dev/null || true
    echo "Stopped local 11-container dev-cluster supervisor (PID ${PID})."
  fi
  rm -f /tmp/ansts-dev-cluster.pid /tmp/ansts-dev-cluster-status.json
fi
