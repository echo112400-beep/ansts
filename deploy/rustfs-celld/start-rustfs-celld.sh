#!/usr/bin/env bash
# Production Startup Specimen: RustFS S3 Bucket + denoland/celld Fleet Node
set -euo pipefail

RUSTFS_DATA_DIR="${RUSTFS_DATA_DIR:-/var/lib/rustfs/data}"
S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:9000}"
CELLD_BUCKET="${CELLD_BUCKET:-s3://ansts-fleet}"
CELLD_LISTEN="${CELLD_LISTEN:-0.0.0.0:8080}"
CELLD_INTERNAL_LISTEN="${CELLD_INTERNAL_LISTEN:-10.240.16.11:9001}"
CELLD_ADVERTISE="${CELLD_ADVERTISE:-http://10.240.16.11:9001}"

mkdir -p "${RUSTFS_DATA_DIR}"
if [[ "$(stat -c '%u:%g' "${RUSTFS_DATA_DIR}")" != "10001:10001" ]]; then
  chown -R 10001:10001 "${RUSTFS_DATA_DIR}"
fi

exec celld \
  --bucket "${CELLD_BUCKET}" \
  --endpoint "${S3_ENDPOINT}" \
  --listen "${CELLD_LISTEN}" \
  --internal-listen "${CELLD_INTERNAL_LISTEN}" \
  --advertise "${CELLD_ADVERTISE}"
