#!/usr/bin/env bash
# Start the scheduler celld worker. It does not start the gateway or the Node host.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CELLD_ROLE=scheduler
export CELLD_PORT="${CELLD_PORT:-9876}"
export CELLD_PROJECT_DIR="${CELLD_PROJECT_DIR:-$ROOT/.celld/scheduler-worker}"
exec bash "$ROOT/scripts/celld_worker_up.sh"
