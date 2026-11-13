#!/usr/bin/env bash
set -euo pipefail
PROJECT=/tmp/ansts-gateway
mkdir -p "$PROJECT"
cp /app/deploy/celld/wrangler.gateway.json "$PROJECT/wrangler.json"
cp -R /app/src "$PROJECT/"
printf 'ANSTS_SCHEDULER_URL=%s\n' "$ANSTS_SCHEDULER_URL" > "$PROJECT/.dev.vars"
exec celld dev "$PROJECT" --host 0.0.0.0 --port 8080 --no-watch --logs