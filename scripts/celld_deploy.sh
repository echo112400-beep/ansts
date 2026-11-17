#!/usr/bin/env bash
# Publishes the ANSTS brain shard onto a real denoland/celld fleet.
#
# celld owns the deployment lifecycle itself: it bundles `main`, publishes the bundle to object storage,
# then every node pulls it, starts the isolate, health-checks it and only then registers the cell.
#
# Run inside the celld image (see docker-compose.dev-cluster.yml `celld-deploy`), or on a host where
# `celld`, `node_modules/.bin/esbuild` and the repo are available:
#   bash scripts/celld_deploy.sh
set -euo pipefail
umask 077

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="${CELLD_CONFIG:-$ROOT/deploy/celld/wrangler.celld.json}"
BUCKET="${CELLD_BUCKET:-s3://ansts-fleet}"
S3_ENDPOINT="${CELLD_S3_ENDPOINT:-${S3_ENDPOINT:-http://rustfs:9000}}"
REGION="${AWS_REGION:-us-east-1}"

# celld bundles with esbuild; prefer the platform binary shipped in node_modules so nothing is downloaded.
if [[ -z "${CELLD_ESBUILD:-}" ]]; then
  for candidate in \
    "$ROOT/node_modules/@esbuild/linux-x64/bin/esbuild" \
    "$ROOT/node_modules/@esbuild/linux-arm64/bin/esbuild" \
    "$ROOT/node_modules/esbuild/bin/esbuild"; do
    if [[ -x "$candidate" ]]; then
      CELLD_ESBUILD="$candidate"
      break
    fi
  done
fi
if [[ -z "${CELLD_ESBUILD:-}" ]]; then
  echo "[celld-deploy] ERROR: no esbuild binary found; run 'npm ci' first or set CELLD_ESBUILD" >&2
  exit 1
fi
export CELLD_ESBUILD

if [[ ! -f "$CONFIG" ]]; then
  echo "[celld-deploy] ERROR: config not found: $CONFIG" >&2
  exit 1
fi
if [[ ! -f "$ROOT/src/cloudflare/worker.ts" ]]; then
  echo "[celld-deploy] ERROR: src/cloudflare/worker.ts missing" >&2
  exit 1
fi

# celld requires `main` and the migrations dir to sit inside the project, so stage a self-contained
# project (config + worker source + migrations). celld bundles `main` with esbuild itself; the entry is
# the TypeScript source because its handler scan reads top-level handlers from the source module.
PROJECT="${CELLD_PROJECT_DIR:-$(mktemp -d)/ansts-celld-brain}"
mkdir -p "$PROJECT"
cp "$CONFIG" "$PROJECT/wrangler.json"
if [[ -n "${ANSTS_BODY_NAMESPACE_PREFIX:-}" ]]; then
  [[ "$ANSTS_BODY_NAMESPACE_PREFIX" =~ ^[a-zA-Z0-9:_-]+$ ]] || exit 1
  sed -i "s/\"ANSTS_BODY_NAMESPACE_PREFIX\": \"\"/\"ANSTS_BODY_NAMESPACE_PREFIX\": \"$ANSTS_BODY_NAMESPACE_PREFIX\"/" "$PROJECT/wrangler.json"
fi
if [[ -n "${ANSTS_BODY_WORKER_URL:-}" ]]; then
  [[ "$ANSTS_BODY_WORKER_URL" =~ ^https?://[a-zA-Z0-9._:-]+$ ]] || exit 1
  sed -i "s|\"ANSTS_BODY_WORKER_URL\": \"\"|\"ANSTS_BODY_WORKER_URL\": \"$ANSTS_BODY_WORKER_URL\"|" "$PROJECT/wrangler.json"
fi
if [[ -n "${ANSTS_BODY_AUTH_SECRET_FILE:-}" ]]; then
  BODY_KEY="$(cat "$ANSTS_BODY_AUTH_SECRET_FILE")"
  if [[ ! "$BODY_KEY" =~ ^[a-f0-9]{64}$ ]]; then
    echo "[celld-deploy] invalid Body signing key" >&2
    exit 1
  fi
  sed -i "s/\"ANSTS_BODY_AUTH_SECRET\": \"\"/\"ANSTS_BODY_AUTH_SECRET\": \"$BODY_KEY\"/" "$PROJECT/wrangler.json"
  unset BODY_KEY
  PREVIOUS_FILE="${ANSTS_BODY_AUTH_PREVIOUS_SECRET_FILE:-$ROOT/.dev-secrets/body-signing.key.previous}"
  if [[ -f "$PREVIOUS_FILE" && -f "$PREVIOUS_FILE-until" ]]; then
    PREVIOUS_KEY="$(cat "$PREVIOUS_FILE")"
    PREVIOUS_UNTIL="$(cat "$PREVIOUS_FILE-until")"
    [[ "$PREVIOUS_KEY" =~ ^[a-f0-9]{64}$ && "$PREVIOUS_UNTIL" =~ ^[0-9]+$ ]] || exit 1
    sed -i "s/\"ANSTS_BODY_AUTH_PREVIOUS_SECRET\": \"\"/\"ANSTS_BODY_AUTH_PREVIOUS_SECRET\": \"$PREVIOUS_KEY\"/; s/\"ANSTS_BODY_AUTH_PREVIOUS_UNTIL\": \"\"/\"ANSTS_BODY_AUTH_PREVIOUS_UNTIL\": \"$PREVIOUS_UNTIL\"/" "$PROJECT/wrangler.json"
    unset PREVIOUS_KEY PREVIOUS_UNTIL
  fi
fi
if [[ -n "${ANSTS_DIRECTORY_SERVICE_TOKEN_FILE:-}" ]]; then
  DIRECTORY_KEY="$(cat "$ANSTS_DIRECTORY_SERVICE_TOKEN_FILE")"
  [[ "$DIRECTORY_KEY" =~ ^[a-f0-9]{64}$ ]] || exit 1
  sed -i "s/\"ANSTS_DIRECTORY_SERVICE_TOKEN\": \"\"/\"ANSTS_DIRECTORY_SERVICE_TOKEN\": \"$DIRECTORY_KEY\"/" "$PROJECT/wrangler.json"
  unset DIRECTORY_KEY
fi
# Coordinator authentication and shadow credentials are independent from directory/Body identities.
if [[ -n "${ANSTS_ADMISSION_SERVICE_TOKEN_FILE:-}" || -n "${ANSTS_ADMISSION_SIGNING_KEY_FILE:-}" ]]; then
  ADMISSION_SERVICE_KEY="$(cat "${ANSTS_ADMISSION_SERVICE_TOKEN_FILE:?}")"
  ADMISSION_SIGNING_KEY="$(cat "${ANSTS_ADMISSION_SIGNING_KEY_FILE:?}")"
  [[ "$ADMISSION_SERVICE_KEY" =~ ^[a-f0-9]{64}$ && "$ADMISSION_SIGNING_KEY" =~ ^[a-f0-9]{64}$ && "$ADMISSION_SERVICE_KEY" != "$ADMISSION_SIGNING_KEY" ]] || exit 1
  if [[ -n "${ANSTS_DIRECTORY_SERVICE_TOKEN_FILE:-}" ]]; then
    DIRECTORY_COMPARISON_KEY="$(cat "$ANSTS_DIRECTORY_SERVICE_TOKEN_FILE")"
    [[ "$ADMISSION_SERVICE_KEY" != "$DIRECTORY_COMPARISON_KEY" && "$ADMISSION_SIGNING_KEY" != "$DIRECTORY_COMPARISON_KEY" ]] || exit 1
    unset DIRECTORY_COMPARISON_KEY
  fi
  sed -i "s/\"ANSTS_ADMISSION_SERVICE_TOKEN\": \"\"/\"ANSTS_ADMISSION_SERVICE_TOKEN\": \"$ADMISSION_SERVICE_KEY\"/; s/\"ANSTS_ADMISSION_SIGNING_KEY\": \"\"/\"ANSTS_ADMISSION_SIGNING_KEY\": \"$ADMISSION_SIGNING_KEY\"/" "$PROJECT/wrangler.json"
  unset ADMISSION_SERVICE_KEY ADMISSION_SIGNING_KEY
fi
cp -R "$ROOT/src" "$PROJECT/src"
if [[ -d "$ROOT/migrations" ]]; then
  cp -R "$ROOT/migrations" "$PROJECT/migrations"
fi

echo "[celld-deploy] config=$CONFIG project=$PROJECT bucket=$BUCKET s3=$S3_ENDPOINT region=$REGION esbuild=$CELLD_ESBUILD"

# `celld deploy` bundles `main` with esbuild, writes deploy/<name>/<version> and flips deploy/current.json.
exec celld deploy "$PROJECT" \
  --bucket "$BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$REGION"
