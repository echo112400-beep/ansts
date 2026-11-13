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
if [[ -n "${ANSTS_USER_AUTH_SECRET_FILE:-}" ]]; then
  USER_KEY="$(cat "$ANSTS_USER_AUTH_SECRET_FILE")"
  [[ "$USER_KEY" =~ ^[a-f0-9]{64}$ ]] || exit 1
  printf 'ANSTS_USER_AUTH_SECRET=%s\nANSTS_CONSOLE_RBAC_SECRET=%s\n' "$USER_KEY" "$USER_KEY" >> "$PROJECT/.dev.vars"
  if [[ -f "$ANSTS_USER_AUTH_SECRET_FILE.previous" && -f "$ANSTS_USER_AUTH_SECRET_FILE.previous-until" ]]; then
    PREVIOUS_KEY="$(cat "$ANSTS_USER_AUTH_SECRET_FILE.previous")"
    PREVIOUS_UNTIL="$(cat "$ANSTS_USER_AUTH_SECRET_FILE.previous-until")"
    [[ "$PREVIOUS_KEY" =~ ^[a-f0-9]{64}$ && "$PREVIOUS_UNTIL" =~ ^[0-9]+$ ]] || exit 1
    printf 'ANSTS_USER_AUTH_PREVIOUS_SECRET=%s\nANSTS_USER_AUTH_PREVIOUS_UNTIL=%s\n' "$PREVIOUS_KEY" "$PREVIOUS_UNTIL" >> "$PROJECT/.dev.vars"
    unset PREVIOUS_KEY PREVIOUS_UNTIL
  fi
  unset USER_KEY
fi
exec celld dev "$PROJECT" --host 0.0.0.0 --port 8080 --no-watch --logs
