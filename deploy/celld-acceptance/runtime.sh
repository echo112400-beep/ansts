#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
REPO_ROOT="$(realpath "$SCRIPT_DIR/../..")"
COMMAND="${1:-help}"
if (($# > 0)); then shift; fi

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'USAGE'
Usage:
  runtime.sh init ABSOLUTE_STATE_DIR [S3_PORT [CONSOLE_PORT [WORKER_PORT]]]
  CELLD_ACCEPTANCE_STATE_DIR=ABSOLUTE_STATE_DIR runtime.sh COMMAND

Commands:
  help                 Show this help.
  bucket-up            Start local RustFS, create the isolated bucket if needed.
  bucket-down          Stop RustFS without deleting its named bucket volume.
  deploy               Build and upload the isolated probe Worker.
  node-start           Start one Celld node with bucket-confirmed durability.
  node-stop            Gracefully stop the Celld process.
  node-crash            SIGKILL the Celld process to simulate compute loss.
  compute-discard      Delete only the marked state's Celld working directory.
  simulate-compute-loss  Crash, discard compute state, and start a fresh node.
  diagnose             Run Celld's writable bucket diagnostic.
  verify               Run the isolated bucket, restart, and disk-loss checks.

State and credentials are created in the supplied directory outside the repo.
The script never reads .env and never removes the RustFS named volumes.
USAGE
}

canonicalize_state_dir() {
  local requested="$1"
  [[ "$requested" = /* ]] || die "state directory must be an absolute path: $requested"
  [[ ! -L "$requested" ]] || die "state directory may not be a symlink: $requested"
  mkdir -p -- "$(dirname -- "$requested")"
  STATE_DIR="$(realpath -m -- "$requested")"
  [[ "$STATE_DIR" != "/" ]] || die "refusing to use / as the state directory"
  [[ "$STATE_DIR" != "$REPO_ROOT" ]] || die "state directory must be outside the repository"
  case "$REPO_ROOT/" in
    "$STATE_DIR"/*) die "state directory is an ancestor of the repository: $STATE_DIR" ;;
  esac
  case "$STATE_DIR/" in
    "$REPO_ROOT"/*) die "state directory must be outside the repository: $STATE_DIR" ;;
  esac
}

config_value() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; found = 1; exit } END { if (!found) exit 1 }' "$CONFIG_FILE"
}

load_state() {
  [[ -n "${CELLD_ACCEPTANCE_STATE_DIR:-}" ]] || die "set CELLD_ACCEPTANCE_STATE_DIR to the state directory created by 'runtime.sh init'"
  canonicalize_state_dir "$CELLD_ACCEPTANCE_STATE_DIR"
  [[ -f "$STATE_DIR/.celld-acceptance-state" ]] || die "not an initialized harness state directory: $STATE_DIR"
  CONFIG_FILE="$STATE_DIR/control/state.conf"
  [[ -f "$CONFIG_FILE" ]] || die "missing harness state config: $CONFIG_FILE"
  [[ -f "$STATE_DIR/control/compose.env" ]] || die "missing isolated Compose env file"
  STATE_ID="$(config_value STATE_ID)"
  BUCKET="$(config_value BUCKET)"
  ACCESS_KEY="$(config_value ACCESS_KEY)"
  SECRET_KEY="$(config_value SECRET_KEY)"
  S3_PORT="$(config_value S3_PORT)"
  CONSOLE_PORT="$(config_value CONSOLE_PORT)"
  WORKER_PORT="$(config_value WORKER_PORT)"
  PROJECT_NAME="celld-acceptance-$STATE_ID"
  CONTROL_DIR="$STATE_DIR/control"
  COMPUTE_DIR="$STATE_DIR/compute"
  IO_DIR="$STATE_DIR/probe-io"
  PID_FILE="$CONTROL_DIR/celld.pid"
  NODE_LOG="$CONTROL_DIR/celld.log"
  REPORT_FILE="$CONTROL_DIR/acceptance-report.md"
  BASE_URL="http://127.0.0.1:$WORKER_PORT"
  S3_URL="http://127.0.0.1:$S3_PORT"
  CELLD_BIN="${CELLD_BIN:-$(command -v celld || true)}"
  [[ -n "$CELLD_BIN" ]] || die "celld 0.6.3 or later must be on PATH (installed runtime is 0.6.3)"
  command -v docker >/dev/null || die "Docker is required"
  command -v curl >/dev/null || die "curl is required"
  docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is required"
}

compose() {
  docker compose \
    --project-directory "$SCRIPT_DIR" \
    --env-file "$CONTROL_DIR/compose.env" \
    --project-name "$PROJECT_NAME" \
    --file "$SCRIPT_DIR/compose.yaml" \
    "$@"
}

aws_cli() {
  compose run --rm --no-deps aws --endpoint-url http://rustfs:9000 "$@"
}

aws_cli_with_probe_io() {
  compose run --rm --no-deps --volume "$IO_DIR:/acceptance" aws \
    --endpoint-url http://rustfs:9000 "$@"
}

local_curl() {
  env \
    -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    NO_PROXY=127.0.0.1,localhost \
    no_proxy=127.0.0.1,localhost \
    curl "$@"
}

init_state() {
  (($# >= 1 && $# <= 4)) || die "init expects ABSOLUTE_STATE_DIR [S3_PORT [CONSOLE_PORT [WORKER_PORT]]]"
  local requested="$1"
  local s3_port="${2:-19000}"
  local console_port="${3:-19001}"
  local worker_port="${4:-19876}"
  canonicalize_state_dir "$requested"
  for port in "$s3_port" "$console_port" "$worker_port"; do
    [[ "$port" =~ ^[0-9]+$ ]] || die "ports must be integers from 1024 to 65535"
    ((port >= 1024 && port <= 65535)) || die "ports must be integers from 1024 to 65535"
  done
  [[ "$s3_port" != "$console_port" && "$s3_port" != "$worker_port" && "$console_port" != "$worker_port" ]] || die "S3, console, and Worker ports must be distinct"

  if [[ -e "$STATE_DIR/.celld-acceptance-state" ]]; then
    printf 'Harness state already initialized: %s\n' "$STATE_DIR"
    return
  fi
  if [[ -d "$STATE_DIR" ]] && find "$STATE_DIR" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
    die "refusing non-empty unmarked state directory: $STATE_DIR"
  fi

  mkdir -p -m 700 "$STATE_DIR/control" "$STATE_DIR/compute" "$STATE_DIR/probe-io"
  chmod 700 "$STATE_DIR" "$STATE_DIR/control" "$STATE_DIR/compute" "$STATE_DIR/probe-io"
  printf 'celld-acceptance-state-v1\n' > "$STATE_DIR/.celld-acceptance-state"
  chmod 600 "$STATE_DIR/.celld-acceptance-state"
  local state_id bucket access_key secret_key
  state_id="$(printf '%s' "$STATE_DIR" | sha256sum | cut -c1-8)"
  bucket="ansts-celld-acceptance-$state_id"
  access_key="acceptance$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
  secret_key="local$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"

  cat > "$STATE_DIR/control/state.conf" <<CONFIG
STATE_ID=$state_id
BUCKET=$bucket
ACCESS_KEY=$access_key
SECRET_KEY=$secret_key
S3_PORT=$s3_port
CONSOLE_PORT=$console_port
WORKER_PORT=$worker_port
CONFIG
  chmod 600 "$STATE_DIR/control/state.conf"
  cat > "$STATE_DIR/control/compose.env" <<COMPOSE_ENV
CELLD_ACCEPTANCE_VOLUME=celld-acceptance-$state_id
CELLD_ACCEPTANCE_S3_PORT=$s3_port
CELLD_ACCEPTANCE_CONSOLE_PORT=$console_port
CELLD_ACCEPTANCE_ACCESS_KEY=$access_key
CELLD_ACCEPTANCE_SECRET_KEY=$secret_key
COMPOSE_ENV
  chmod 600 "$STATE_DIR/control/compose.env"
  printf 'Celld acceptance state initialized at %s\n' "$STATE_DIR"
  printf 'Bucket: s3://%s\nS3 endpoint: http://127.0.0.1:%s\nWorker: http://127.0.0.1:%s\n' "$bucket" "$s3_port" "$worker_port"
  printf 'Test credentials are stored with mode 0600 in %s/control/state.conf\n' "$STATE_DIR"
}

bucket_up() {
  compose up -d rustfs
  local ready=0
  for _ in $(seq 1 60); do
    if aws_cli s3api list-buckets >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 2
  done
  ((ready == 1)) || die "RustFS did not become reachable; inspect 'docker compose logs rustfs'"
  if ! aws_cli s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
    aws_cli s3api create-bucket --bucket "$BUCKET" >/dev/null
  fi
  aws_cli s3api head-bucket --bucket "$BUCKET" >/dev/null
  printf 'RustFS is ready; confirmed isolated bucket s3://%s\n' "$BUCKET"
}

bucket_range_probe() {
  local object_key="harness-acceptance/$STATE_ID/exact-range.bin"
  local source="$IO_DIR/range-source.bin"
  local expected="$IO_DIR/range-expected.bin"
  local received="$IO_DIR/range-received.bin"
  local response="$CONTROL_DIR/range-get.json"
  mkdir -p -m 700 "$IO_DIR"
  printf 'abcdef0123456789XYZ' > "$source"
  printf '0123456789' > "$expected"
  chmod 600 "$source" "$expected"
  aws_cli_with_probe_io s3api put-object \
    --bucket "$BUCKET" --key "$object_key" --body /acceptance/range-source.bin >/dev/null
  aws_cli_with_probe_io s3api get-object \
    --bucket "$BUCKET" --key "$object_key" --range 'bytes=6-15' \
    /acceptance/range-received.bin > "$response"
  chmod 600 "$response"
  if ! cmp -s "$expected" "$received"; then
    die "RustFS returned bytes that do not match the requested exact range; inspect $received"
  fi
  aws_cli_with_probe_io s3api delete-object --bucket "$BUCKET" --key "$object_key" >/dev/null
  rm -f -- "$source" "$expected" "$received"
  printf 'PASS: explicit S3 ranged GET returned exactly bytes 6-15 (0123456789)\n'
}

bucket_down() {
  compose stop --timeout 2 rustfs
  printf 'RustFS stopped; its named bucket volume remains intact.\n'
}

credential_env() {
  env \
    -u AWS_PROFILE \
    -u AWS_SESSION_TOKEN \
    -u AWS_WEB_IDENTITY_TOKEN_FILE \
    -u AWS_ROLE_ARN \
    -u AWS_CONTAINER_CREDENTIALS_RELATIVE_URI \
    -u AWS_CONTAINER_CREDENTIALS_FULL_URI \
    -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
    -u http_proxy -u https_proxy -u all_proxy \
    AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
    AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    AWS_REGION=us-east-1 \
    AWS_DEFAULT_REGION=us-east-1 \
    AWS_CONFIG_FILE=/dev/null \
    AWS_SHARED_CREDENTIALS_FILE=/dev/null \
    NO_PROXY=127.0.0.1,localhost \
    no_proxy=127.0.0.1,localhost \
    S3_ENDPOINT="$S3_URL" \
    CELLD_BUCKET="s3://$BUCKET" \
    "$@"
}

find_esbuild() {
  if [[ -n "${CELLD_ESBUILD:-}" && -x "$CELLD_ESBUILD" ]]; then
    printf '%s' "$CELLD_ESBUILD"
  elif [[ -x "$REPO_ROOT/node_modules/.bin/esbuild" ]]; then
    printf '%s' "$REPO_ROOT/node_modules/.bin/esbuild"
  else
    command -v esbuild || true
  fi
}

deploy_worker() {
  local esbuild
  esbuild="$(find_esbuild)"
  [[ -n "$esbuild" ]] || die "esbuild is required by 'celld deploy'; install it or put it on PATH"
  (
    cd "$SCRIPT_DIR"
    credential_env CELLD_ESBUILD="$esbuild" "$CELLD_BIN" deploy "$SCRIPT_DIR/fixture" \
      --bucket "s3://$BUCKET" \
      --endpoint "$S3_URL" \
      --region us-east-1
  )
}

node_is_running() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid args process_state
  pid="$(cat "$PID_FILE")"
  [[ "$pid" =~ ^[0-9]+$ ]] || die "invalid Celld PID file; inspect $PID_FILE"
  kill -0 "$pid" 2>/dev/null || return 1
  process_state="$(ps -p "$pid" -o stat= 2>/dev/null || true)"
  [[ "$process_state" == *Z* ]] && return 1
  args="$(ps -p "$pid" -o args= 2>/dev/null || true)"
  [[ "$args" == *celld* && "$args" == *"$BUCKET"* ]] || die "PID $pid is alive but is not this harness's Celld process; refusing to signal it"
  return 0
}

wait_http_health() {
  for _ in $(seq 1 60); do
    if ! node_is_running; then
      printf 'Celld exited during startup. Recent log:\n' >&2
      tail -80 "$NODE_LOG" >&2 || true
      die "Celld did not stay running"
    fi
    if local_curl --silent --fail --max-time 2 "$BASE_URL/health" >/dev/null 2>&1; then
      printf 'Celld Worker is ready at %s\n' "$BASE_URL"
      return
    fi
    sleep 1
  done
  tail -80 "$NODE_LOG" >&2 || true
  die "Worker health endpoint did not become ready"
}

node_start() {
  if node_is_running; then
    die "Celld is already running with PID $(cat "$PID_FILE")"
  fi
  rm -f "$PID_FILE"
  mkdir -p -m 700 "$COMPUTE_DIR"
  [[ ! -L "$COMPUTE_DIR" ]] || die "compute directory may not be a symlink"
  : > "$NODE_LOG"
  chmod 600 "$NODE_LOG"
  (
    cd "$SCRIPT_DIR"
    nohup env \
      -u AWS_PROFILE \
      -u AWS_SESSION_TOKEN \
      -u AWS_WEB_IDENTITY_TOKEN_FILE \
      -u AWS_ROLE_ARN \
      -u AWS_CONTAINER_CREDENTIALS_RELATIVE_URI \
      -u AWS_CONTAINER_CREDENTIALS_FULL_URI \
      -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
      -u http_proxy -u https_proxy -u all_proxy \
      AWS_ACCESS_KEY_ID="$ACCESS_KEY" \
      AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
      AWS_REGION=us-east-1 \
      AWS_DEFAULT_REGION=us-east-1 \
      AWS_CONFIG_FILE=/dev/null \
      AWS_SHARED_CREDENTIALS_FILE=/dev/null \
      NO_PROXY=127.0.0.1,localhost \
      no_proxy=127.0.0.1,localhost \
      S3_ENDPOINT="$S3_URL" \
      CELLD_BUCKET="s3://$BUCKET" \
      CELLD_DURABILITY=bucket \
      CELLD_WATCH="$COMPUTE_DIR" \
      "$CELLD_BIN" \
      --bucket "s3://$BUCKET" \
      --endpoint "$S3_URL" \
      --region us-east-1 \
      --listen "127.0.0.1:$WORKER_PORT" \
      >> "$NODE_LOG" 2>&1 < /dev/null &
    printf '%s\n' "$!" > "$PID_FILE"
  )
  chmod 600 "$PID_FILE"
  wait_http_health
}

node_stop() {
  if ! node_is_running; then
    rm -f "$PID_FILE"
    printf 'Celld is already stopped.\n'
    return
  fi
  local pid
  pid="$(cat "$PID_FILE")"
  kill -TERM "$pid"
  for _ in $(seq 1 45); do
    if ! node_is_running; then
      rm -f "$PID_FILE"
      printf 'Celld stopped gracefully.\n'
      return
    fi
    sleep 1
  done
  die "Celld did not exit within 45 seconds; inspect $NODE_LOG before proceeding"
}

node_crash() {
  if ! node_is_running; then
    rm -f "$PID_FILE"
    printf 'Celld is already stopped.\n'
    return
  fi
  local pid
  pid="$(cat "$PID_FILE")"
  kill -KILL "$pid"
  for _ in $(seq 1 10); do
    if ! node_is_running; then
      rm -f "$PID_FILE"
      printf 'Celld process killed with SIGKILL.\n'
      return
    fi
    sleep 1
  done
  die "Celld PID $pid remains visible after SIGKILL"
}

compute_discard() {
  node_is_running && die "stop Celld before discarding compute state"
  [[ "$COMPUTE_DIR" == "$STATE_DIR/compute" ]] || die "compute path guard failed"
  [[ ! -L "$COMPUTE_DIR" ]] || die "compute directory may not be a symlink"
  rm -rf -- "$COMPUTE_DIR"
  mkdir -m 700 -- "$COMPUTE_DIR"
  printf 'Discarded only %s; the RustFS bucket volume was untouched.\n' "$COMPUTE_DIR"
}

http_post() {
  local key="$1" value="$2" output="$3" timeout="${4:-20}"
  local_curl --silent --show-error --max-time "$timeout" \
    --output "$output" --write-out '%{http_code}' \
    --request POST --header 'content-type: application/json' \
    --data "{\"key\":\"$key\",\"value\":\"$value\"}" \
    "$BASE_URL/probe"
}

http_get() {
  local key="$1" output="$2"
  local_curl --silent --show-error --fail --max-time 30 \
    --output "$output" "$BASE_URL/probe?key=$key"
}

assert_post_value() {
  local key="$1" value="$2" label="$3" output="$CONTROL_DIR/last-response.json" code
  code="$(http_post "$key" "$value" "$output")" || die "$label request failed; see $output and $NODE_LOG"
  [[ "$code" == 200 ]] || die "$label returned HTTP $code: $(cat "$output" 2>/dev/null || true)"
  [[ "$(cat "$output")" == "{\"ok\":true,\"key\":\"$key\",\"value\":\"$value\"}" ]] || die "$label response did not match its write"
  printf 'PASS: %s\n' "$label"
}

assert_get_value() {
  local key="$1" value="$2" label="$3" output="$CONTROL_DIR/last-read.json"
  http_get "$key" "$output" || die "$label GET failed; see $output and $NODE_LOG"
  [[ "$(cat "$output")" == "{\"key\":\"$key\",\"value\":\"$value\"}" ]] || die "$label returned unexpected state: $(cat "$output")"
  printf 'PASS: %s\n' "$label"
}

only_stale_peer_failures() {
  local diagnostic="$1" failures line found=0
  failures="$(grep '"verdict":"fail"' "$diagnostic" || true)"
  [[ -n "$failures" ]] || return 1
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    [[ "$line" == *'"detail":"connect directly to node '* ]] || return 1
    found=1
  done <<< "$failures"
  ((found == 1))
}

bucket_gate_probe() {
  local key="$1" value="$2" output="$CONTROL_DIR/bucket-gate.json" code curl_status
  bucket_down
  set +e
  code="$(http_post "$key" "$value" "$output" 5)"
  curl_status=$?
  set -e
  if [[ "$code" =~ ^2[0-9][0-9]$ ]]; then
    bucket_up >/dev/null
    die "Celld returned HTTP $code for a write while RustFS was stopped; bucket confirmation gate was not observed"
  fi
  if [[ -n "$code" && "$code" =~ ^[0-9]{3}$ ]]; then
    printf 'PASS: bucket-outage write did not receive a 2xx response (HTTP %s)\n' "$code"
  else
    printf 'PASS: bucket-outage write received no HTTP success before curl stopped waiting (curl exit %s)\n' "$curl_status"
  fi
  bucket_up
  assert_post_value "$key" "$value" "write after RustFS restoration"
  assert_get_value "$key" "$value" "bucket-gated value is readable after restoration"
}

write_report_header() {
  {
    printf '# Celld and bucket acceptance report\n\n'
    printf -- '- Started: %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf -- '- Celld: %s\n' "$("$CELLD_BIN" --version 2>&1 | head -1)"
    printf -- '- Bucket: `s3://%s`\n' "$BUCKET"
    printf -- '- RustFS endpoint: `%s`\n' "$S3_URL"
    printf -- '- Worker endpoint: `%s`\n' "$BASE_URL"
    printf -- '- Durability mode: `CELLD_DURABILITY=bucket`\n\n'
  } > "$REPORT_FILE"
  chmod 600 "$REPORT_FILE"
}

verify_all() {
  write_report_header
  trap 'result=$?; printf "\n- Result: %s\n" "$([[ $result -eq 0 ]] && printf PASS || printf FAIL)" >> "$REPORT_FILE"; printf "\nReport: %s\n" "$REPORT_FILE"' EXIT
  bucket_up
  bucket_range_probe
  deploy_worker
  printf '\n## Checks\n\n' >> "$REPORT_FILE"
  printf -- '- PASS: explicit S3 ranged GET returned exactly bytes `6-15` (`0123456789`)\n' >> "$REPORT_FILE"
  node_start

  local nonce="$(date -u +%Y%m%dT%H%M%SZ)-$STATE_ID" key="restart" value="acceptance-$STATE_ID"
  assert_post_value "$key" "$value" "HTTP response after a DO SQLite write"
  printf -- '- PASS: HTTP response after a DO SQLite write\n' >> "$REPORT_FILE"

  node_stop
  node_start
  assert_get_value "$key" "$value" "read after graceful Celld restart"
  printf -- '- PASS: read after graceful Celld restart\n' >> "$REPORT_FILE"

  node_stop
  compute_discard
  node_start
  assert_get_value "$key" "$value" "read after compute working-directory discard"
  printf -- '- PASS: read after compute working-directory discard\n' >> "$REPORT_FILE"

  bucket_gate_probe "bucket-gate-$nonce" "confirmed-$STATE_ID"
  printf -- '- PASS: no successful HTTP response was observed while RustFS was stopped; retry/read succeeded after restoration\n' >> "$REPORT_FILE"

  local diagnostic="$CONTROL_DIR/diagnose.jsonl" attempt attempt_file diagnosed=0
  for attempt in $(seq 1 9); do
    attempt_file="$CONTROL_DIR/diagnose-attempt-$attempt.jsonl"
    if credential_env "$CELLD_BIN" diagnose \
      --bucket "s3://$BUCKET" --endpoint "$S3_URL" --region us-east-1 --json > "$attempt_file" 2>&1; then
      chmod 600 "$attempt_file"
      cp "$attempt_file" "$diagnostic"
      diagnosed=1
      break
    fi
    chmod 600 "$attempt_file"
    if ((attempt < 9)) && only_stale_peer_failures "$attempt_file"; then
      printf 'Waiting for exited single-node leases to expire before retrying diagnose (%s/8).\n' "$attempt"
      sleep 3
      continue
    fi
    cp "$attempt_file" "$diagnostic"
    cat "$diagnostic" >&2
    printf -- '- FAIL: `celld diagnose` writable bucket checks (details: `%s`)\n' "$diagnostic" >> "$REPORT_FILE"
    die "celld diagnose failed; inspect $diagnostic"
  done
  ((diagnosed == 1)) || die "celld diagnose did not finish successfully"
  printf 'PASS: celld diagnose writable bucket checks\n'
  if ((attempt > 1)); then
    printf -- '- PASS: `celld diagnose` writable bucket checks after %s retry attempt(s); the first stale peer lease diagnostic is preserved in `%s/diagnose-attempt-1.jsonl` (final output: `%s`)\n' "$((attempt - 1))" "$CONTROL_DIR" "$diagnostic" >> "$REPORT_FILE"
  else
    printf -- '- PASS: `celld diagnose` writable bucket checks (details: `%s`)\n' "$diagnostic" >> "$REPORT_FILE"
  fi

  node_stop
  printf '\n## Limits\n\n' >> "$REPORT_FILE"
  cat >> "$REPORT_FILE" <<'LIMITS'
- RustFS data lives in a Docker named volume and Celld's local working directory lives under the supplied state directory. Both are on the same host and may share the same physical disk, power, kernel, and operator domain. This demonstrates process restart and deletion of the compute working directory while retaining the local bucket volume; it does not demonstrate independent hardware or site failure domains.
- The fixture is a minimal SQLite-backed Durable Object probe. It demonstrates runtime behavior for this fixture, not end-to-end recovery of the ANSTS microservices. Full application-level DR acceptance depends on issue #10.
- The bucket outage check observes HTTP responses from one Celld process against local RustFS. It does not qualify RustFS or another S3-compatible store for production; run the same conditional-write and exact-range requirements against the intended production object store.
LIMITS
  printf 'Acceptance checks passed.\n'
}

case "$COMMAND" in
  help|-h|--help)
    usage
    ;;
  init)
    init_state "$@"
    ;;
  *)
    load_state
    (($# == 0)) || die "$COMMAND accepts no positional arguments"
    case "$COMMAND" in
      bucket-up) bucket_up ;;
      bucket-down) bucket_down ;;
      deploy) deploy_worker ;;
      node-start) node_start ;;
      node-stop) node_stop ;;
      node-crash) node_crash ;;
      compute-discard) compute_discard ;;
      simulate-compute-loss) node_crash; compute_discard; node_start ;;
      diagnose)
        credential_env "$CELLD_BIN" diagnose \
          --bucket "s3://$BUCKET" --endpoint "$S3_URL" --region us-east-1 --json
        ;;
      verify) verify_all ;;
      *) usage >&2; die "unknown command: $COMMAND" ;;
    esac
    ;;
esac
