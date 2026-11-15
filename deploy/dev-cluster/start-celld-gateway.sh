#!/usr/bin/env bash
set -euo pipefail
PROJECT=/tmp/ansts-gateway
mkdir -p "$PROJECT"
cp /app/deploy/celld/wrangler.gateway.json "$PROJECT/wrangler.json"
cp -R /app/src "$PROJECT/"
printf 'ANSTS_SCHEDULER_URL=%s\n' "$ANSTS_SCHEDULER_URL" > "$PROJECT/.dev.vars"
printf 'ANSTS_GATEWAY_STATE_MODE=remote\nANSTS_SCHEDULER_URLS=%s\n' "${ANSTS_SCHEDULER_URLS:-$ANSTS_SCHEDULER_URL}" >> "$PROJECT/.dev.vars"
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
if [[ -n "${ANSTS_SANDBOX_IMAGE:-}" ]]; then printf 'ANSTS_SANDBOX_IMAGE=%s\n' "$ANSTS_SANDBOX_IMAGE" >> "$PROJECT/.dev.vars"; fi
for setting in ANSTS_OIDC_ISSUER ANSTS_OIDC_AUDIENCE ANSTS_OIDC_JWKS_URL ANSTS_GITHUB_CLIENT_ID ANSTS_PUBLIC_ORIGIN ANSTS_AGENT_ENABLED ANSTS_RUNTIME_PROFILE; do
  if [[ -n "${!setting:-}" ]]; then
    [[ "${!setting}" != *$'\n'* && "${!setting}" != *$'\r'* ]] || exit 1
    printf '%s=%s\n' "$setting" "${!setting}" >> "$PROJECT/.dev.vars"
  fi
done
if [[ -n "${ANSTS_GITHUB_CLIENT_SECRET_FILE:-}" ]]; then
  GITHUB_SECRET="$(cat "$ANSTS_GITHUB_CLIENT_SECRET_FILE")"
  [[ "$GITHUB_SECRET" != *$'\n'* && "$GITHUB_SECRET" != *$'\r'* ]] || exit 1
  printf 'ANSTS_GITHUB_CLIENT_SECRET=%s\n' "$GITHUB_SECRET" >> "$PROJECT/.dev.vars"
  unset GITHUB_SECRET
fi
if [[ -n "${ANSTS_GITHUB_IDENTITIES_FILE:-}" ]]; then
  GITHUB_IDENTITIES="$(cat "$ANSTS_GITHUB_IDENTITIES_FILE")"
  [[ "$GITHUB_IDENTITIES" != *$'\n'* && "$GITHUB_IDENTITIES" != *$'\r'* ]] || exit 1
  printf 'ANSTS_GITHUB_IDENTITIES_JSON=%s\n' "$GITHUB_IDENTITIES" >> "$PROJECT/.dev.vars"
  unset GITHUB_IDENTITIES
fi
if [[ -n "${ANSTS_SYSTEM_TOPOLOGY_FILE:-}" ]]; then
  [[ -f "$ANSTS_SYSTEM_TOPOLOGY_FILE" && $(wc -c < "$ANSTS_SYSTEM_TOPOLOGY_FILE") -le 32768 ]] || exit 1
  TOPOLOGY_JSON="$(tr -d '\r\n' < "$ANSTS_SYSTEM_TOPOLOGY_FILE")"
  printf 'ANSTS_SYSTEM_TOPOLOGY_JSON=%s\n' "$TOPOLOGY_JSON" >> "$PROJECT/.dev.vars"
  unset TOPOLOGY_JSON
fi
exec celld dev "$PROJECT" --host 0.0.0.0 --port 8080 --no-watch --logs
