#!/usr/bin/env bash
set -euo pipefail
PROJECT=/tmp/ansts-gateway
mkdir -p "$PROJECT"
cp /app/deploy/celld/wrangler.gateway.json "$PROJECT/wrangler.json"
cp -R /app/src "$PROJECT/"
printf 'ANSTS_SCHEDULER_URL=%s\n' "$ANSTS_SCHEDULER_URL" > "$PROJECT/.dev.vars"
if [[ -n "${ANSTS_GATEWAY_TOKEN_FILE:-}" ]]; then
  printf 'ANSTS_SCHEDULER_SERVICE_TOKEN=%s\n' "$(cat "$ANSTS_GATEWAY_TOKEN_FILE")" >> "$PROJECT/.dev.vars"
fi
exec celld dev "$PROJECT" --host 0.0.0.0 --port 8080 --no-watch --logs
