#!/usr/bin/env bash
# Start the gateway celld worker. It calls the scheduler at ANSTS_SCHEDULER_URL and does not
# start the scheduler process or the Node host.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CELLD_ROLE=gateway
export CELLD_PORT="${CELLD_PORT:-9877}"
if [[ -z "${ANSTS_SCHEDULER_URL:-}" ]]; then
  export ANSTS_SCHEDULER_URL="http://127.0.0.1:${CELLD_SCHEDULER_PORT:-9876}"
fi
export CELLD_PROJECT_DIR="${CELLD_PROJECT_DIR:-$ROOT/.celld/gateway-worker}"
exec bash "$ROOT/scripts/celld_worker_up.sh"
