#!/usr/bin/env bash
# Shared launcher for one celld worker. Callers set CELLD_ROLE.
# Prefer scripts/celld_scheduler_up.sh and scripts/celld_gateway_up.sh.
#
# Environment:
#   CELLD_ROLE         scheduler, gateway or body (required)
#   CELLD_BIN          celld executable (default: `celld` on PATH). Verified with 0.6.3.
#   CELLD_ESBUILD      esbuild executable (default: the platform binary in node_modules)
#   CELLD_HOST         Worker listener host (default: 127.0.0.1)
#   CELLD_PORT         Worker listener port (scheduler 9876, gateway 9877, body 9878)
#   CELLD_PROJECT_DIR  Staged project directory
#   ANSTS_SCHEDULER_URL  Gateway/Body. Base URL of the scheduler worker.
#   CELLD_CLEAN=1      Delete that project's .celld/dev before start
#   CELLD_LOGS=0       Hide celld node logs
set -euo pipefail

if [[ "${CELLD_ROLE:-}" != "scheduler" && "${CELLD_ROLE:-}" != "gateway" && "${CELLD_ROLE:-}" != "body" ]]; then
  echo "[celld-worker] ERROR: set CELLD_ROLE=scheduler or CELLD_ROLE=gateway, or run scripts/celld_scheduler_up.sh / scripts/celld_gateway_up.sh" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -z "${CELLD_BIN:-}" ]]; then
  if command -v celld >/dev/null 2>&1; then
    CELLD_BIN="$(command -v celld)"
  else
    echo "[celld-worker] ERROR: celld not found; set CELLD_BIN to the denoland/celld 0.6.3 binary" >&2
    exit 1
  fi
fi
if [[ ! -x "$CELLD_BIN" ]]; then
  echo "[celld-worker] ERROR: CELLD_BIN is not executable: $CELLD_BIN" >&2
  exit 1
fi

if [[ -z "${CELLD_ESBUILD:-}" ]]; then
  for candidate in \
    "$ROOT/node_modules/@esbuild/linux-x64/bin/esbuild" \
    "$ROOT/node_modules/@esbuild/linux-arm64/bin/esbuild" \
    "$ROOT/node_modules/@esbuild/darwin-arm64/bin/esbuild" \
    "$ROOT/node_modules/@esbuild/darwin-x64/bin/esbuild" \
    "$ROOT/node_modules/esbuild/bin/esbuild"; do
    if [[ -x "$candidate" ]]; then
      CELLD_ESBUILD="$candidate"
      break
    fi
  done
fi
if [[ -z "${CELLD_ESBUILD:-}" || ! -x "$CELLD_ESBUILD" ]]; then
  echo "[celld-worker] ERROR: no esbuild binary found; run 'npm install' or set CELLD_ESBUILD" >&2
  exit 1
fi
export CELLD_ESBUILD

ENTRY="$ROOT/dist/src/cloudflare/celld_worker_service.js"
if [[ ! -f "$ENTRY" ]]; then
  node "$ROOT/scripts/ci_check.mjs" --stage build
fi

ARGS_FILE="$(mktemp)"
node "$ENTRY" print-exec > "$ARGS_FILE"
mapfile -t CELLD_ARGS < "$ARGS_FILE"
rm -f "$ARGS_FILE"
if [[ ${#CELLD_ARGS[@]} -eq 0 || "${CELLD_ARGS[0]}" != "dev" ]]; then
  echo "[celld-worker] ERROR: print-exec did not return a celld dev command" >&2
  exit 1
fi

echo "[celld-worker] celld=$CELLD_BIN esbuild=$CELLD_ESBUILD project=${CELLD_ARGS[1]} listen=${CELLD_HOST:-127.0.0.1}:${CELLD_PORT:-9876}"
exec "$CELLD_BIN" "${CELLD_ARGS[@]}"
