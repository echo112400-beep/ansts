#!/usr/bin/env bash
set -euo pipefail
PROJECT=/var/lib/ansts-body/project
mkdir -p "$PROJECT"
cp /app/deploy/celld/wrangler.body.json "$PROJECT/wrangler.json"
cp -R /app/src "$PROJECT/"
BODY_KEY="$(cat "$ANSTS_BODY_AUTH_SECRET_FILE")"
[[ "$BODY_KEY" =~ ^[a-f0-9]{64}$ ]] || exit 1
[[ "$ANSTS_SCHEDULER_URL" =~ ^https?://[a-zA-Z0-9._:-]+$ ]] || exit 1
printf 'ANSTS_BODY_AUTH_SECRET=%s\nANSTS_SCHEDULER_URL=%s\n' "$BODY_KEY" "$ANSTS_SCHEDULER_URL" > "$PROJECT/.dev.vars"
unset BODY_KEY
exec celld dev "$PROJECT" --host 0.0.0.0 --port 8080 --no-watch --logs
