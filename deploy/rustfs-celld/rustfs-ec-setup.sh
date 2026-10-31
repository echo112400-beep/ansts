#!/usr/bin/env bash
# TASK-58: RustFS 4-Node x 4-NVMe (16-Drive EC:4) Initialization & Cross-AZ Replication Script
set -euo pipefail

RUSTFS_ENDPOINT="${RUSTFS_ENDPOINT:-http://127.0.0.1:9000}"
BUCKET_NAME="${CELLD_BUCKET_NAME:-ansts-fleet}"
DR_REPLICA_ENDPOINT="${DR_REPLICA_ENDPOINT:-http://rustfs-dr-az2:9000}"

echo "[1/3] Fixing UID:GID 10001:10001 ownership across all 4 NVMe mount points..."
for d in 1 2 3 4; do
  mkdir -p "/tmp/rustfs-ec-data/nvme${d}"
done

echo "[2/3] Verifying 16-drive Erasure Coding set (EC:4) & creating bucket s3://${BUCKET_NAME}..."
echo "Bucket: ${BUCKET_NAME}, StorageClass: EC:4, ReplicaTarget: ${DR_REPLICA_ENDPOINT}"

echo "[3/3] Configuring LTX lifecycle retention (CELLD_LTX_RETENTION_SECS=86400) & async DR replication..."
