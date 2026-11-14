#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CELLD_ROLE=body
export CELLD_PORT="${CELLD_PORT:-9878}"
export ANSTS_SCHEDULER_URL="${ANSTS_SCHEDULER_URL:-http://127.0.0.1:9876}"
export CELLD_PROJECT_DIR="${CELLD_PROJECT_DIR:-$ROOT/.celld/body-worker}"
exec bash "$ROOT/scripts/celld_worker_up.sh"
