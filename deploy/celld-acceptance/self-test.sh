#!/usr/bin/env bash
set -Eeuo pipefail

HARNESS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
TMP_DIR="$(mktemp -d /tmp/celld-acceptance-self-test.XXXXXX)"
trap 'rm -rf -- "$TMP_DIR"' EXIT
STATE_DIR="$TMP_DIR/state"
BIN_DIR="$TMP_DIR/bin"
mkdir -m 700 "$BIN_DIR"

init_output="$TMP_DIR/init-output.txt"
"$HARNESS_DIR/runtime.sh" init "$STATE_DIR" > "$init_output" 2>&1
ACCESS_KEY="$(awk -F= '$1 == "ACCESS_KEY" { print $2; exit }' "$STATE_DIR/control/state.conf")"
SECRET_KEY="$(awk -F= '$1 == "SECRET_KEY" { print $2; exit }' "$STATE_DIR/control/state.conf")"
[[ -n "$ACCESS_KEY" && -n "$SECRET_KEY" ]]
! grep -Fq -- "$ACCESS_KEY" "$init_output"
! grep -Fq -- "$SECRET_KEY" "$init_output"
[[ "$(stat -c '%a' "$STATE_DIR/control/state.conf")" == 600 ]]
[[ "$(stat -c '%a' "$STATE_DIR/control/compose.env")" == 600 ]]

cat > "$BIN_DIR/docker" <<'DOCKER_STUB'
#!/usr/bin/env bash
if [[ "${1:-}" == compose && "${2:-}" == version ]]; then
  printf 'Docker Compose version v2.0.0 (self-test stub)\n'
  exit 0
fi
exit 1
DOCKER_STUB

cat > "$BIN_DIR/celld-mock" <<'CELLD_STUB'
#!/usr/bin/env bash
if [[ "${1:-}" == --version ]]; then
  printf 'celld 0.6.3 (self-test stub)\n'
  exit 0
fi
if [[ "${1:-}" == diagnose ]]; then
  [[ "${AWS_ACCESS_KEY_ID:-}" =~ ^acceptance[0-9a-f]{24}$ ]]
  [[ "${AWS_SECRET_ACCESS_KEY:-}" =~ ^local[0-9a-f]{48}$ ]]
  [[ "${AWS_CONFIG_FILE:-}" == /dev/null ]]
  [[ "${AWS_SHARED_CREDENTIALS_FILE:-}" == /dev/null ]]
  [[ -z "${HTTP_PROXY:-}${HTTPS_PROXY:-}${ALL_PROXY:-}${http_proxy:-}${https_proxy:-}${all_proxy:-}" ]]
  [[ "${NO_PROXY:-}" == *127.0.0.1* ]]
  printf 'mock celld diagnostic received isolated test credentials\n'
  exit 0
fi
exit 2
CELLD_STUB

chmod 700 "$BIN_DIR/docker" "$BIN_DIR/celld-mock"
diagnose_output="$TMP_DIR/diagnose-output.txt"
PATH="$BIN_DIR:$PATH" \
  CELLD_ACCEPTANCE_STATE_DIR="$STATE_DIR" \
  CELLD_BIN="$BIN_DIR/celld-mock" \
  "$HARNESS_DIR/runtime.sh" diagnose > "$diagnose_output" 2>&1 || {
    cat "$diagnose_output" >&2
    exit 1
  }
grep -Fq 'mock celld diagnostic received isolated test credentials' "$diagnose_output"
! grep -Fq -- "$ACCESS_KEY" "$diagnose_output"
! grep -Fq -- "$SECRET_KEY" "$diagnose_output"

printf 'PASS: init and a mocked Celld command keep generated credentials out of terminal output.\n'
