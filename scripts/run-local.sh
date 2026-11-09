#!/usr/bin/env bash
# Start ANSTS v2.2 Control Plane & LayerFS Service on the Local Host
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

if [ ! -f "dist/src/server_main.js" ]; then
  node scripts/ci_check.mjs --stage build
fi

export ANSTS_HOST="${ANSTS_HOST:-127.0.0.1}"
export ANSTS_PORT="${ANSTS_PORT:-8080}"
export ANSTS_DASHBOARD_PORT="${ANSTS_DASHBOARD_PORT:-8090}"
export ANSTS_WORKER_NODES="${ANSTS_WORKER_NODES:-3}"

exec node dist/src/server_main.js
