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
printf 'ANSTS_SCHEDULER_URLS=%s\n' "${ANSTS_SCHEDULER_URLS:-$ANSTS_SCHEDULER_URL}" >> "$PROJECT/.dev.vars"
if [[ -n "${ANSTS_BODY_PROXY_URLS:-}" ]]; then printf 'ANSTS_BODY_PROXY_URLS=%s\n' "$ANSTS_BODY_PROXY_URLS" >> "$PROJECT/.dev.vars"; fi
if [[ -n "${ANSTS_BODY_NAMESPACE_PREFIX:-}" ]]; then printf 'ANSTS_BODY_NAMESPACE_PREFIX=%s\n' "$ANSTS_BODY_NAMESPACE_PREFIX" >> "$PROJECT/.dev.vars"; fi
if [[ -f "$ANSTS_BODY_AUTH_SECRET_FILE.previous" && -f "$ANSTS_BODY_AUTH_SECRET_FILE.previous-until" ]]; then
  PREVIOUS_KEY="$(cat "$ANSTS_BODY_AUTH_SECRET_FILE.previous")"
  PREVIOUS_UNTIL="$(cat "$ANSTS_BODY_AUTH_SECRET_FILE.previous-until")"
  [[ "$PREVIOUS_KEY" =~ ^[a-f0-9]{64}$ && "$PREVIOUS_UNTIL" =~ ^[0-9]+$ ]] || exit 1
  printf 'ANSTS_BODY_AUTH_PREVIOUS_SECRET=%s\nANSTS_BODY_AUTH_PREVIOUS_UNTIL=%s\n' "$PREVIOUS_KEY" "$PREVIOUS_UNTIL" >> "$PROJECT/.dev.vars"
  unset PREVIOUS_KEY PREVIOUS_UNTIL
fi
exec celld dev "$PROJECT" --host 0.0.0.0 --port 8080 --no-watch --logs
