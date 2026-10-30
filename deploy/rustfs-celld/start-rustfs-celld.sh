#!/usr/bin/env bash
# ==============================================================================
# ANSTS v2.0 — RustFS + denoland/celld Standard Startup Specimen (启动标本)
# ==============================================================================
# 功能说明:
#   1. 启动 RustFS (Apache-2.0 高性能 S3 兼容对象存储, 端口 9000 API / 9001 Console)
#   2. 自动创建 celld 集群状态桶 (s3://ansts-fleet) 与 R2 制品桶
#   3. 执行 ANSTS v2.0 代码热部署 (celld deploy) 与 D1 数据库迁移 (celld d1 migrations apply)
#   4. 启动 celld 节点进程 (--bucket + --endpoint + --listen + --internal-listen + --advertise)
#   5. 运行 celld diagnose 与健康检查探针验证集群就绪
# ==============================================================================

set -euo pipefail

# ------------------------- 1. 核心环境变量标本配置 ----------------------------
export RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-ansts_rustfs_ak_9f8e7d6c5b4a}"
export RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-ansts_rustfs_sk_1a2b3c4d5e6f7g8h9i0j}"
export RUSTFS_DATA_DIR="${RUSTFS_DATA_DIR:-/var/lib/rustfs/data}"
export RUSTFS_LOG_DIR="${RUSTFS_LOG_DIR:-/var/log/rustfs}"
export RUSTFS_API_ADDR="${RUSTFS_API_ADDR:-0.0.0.0:9000}"
export RUSTFS_CONSOLE_ADDR="${RUSTFS_CONSOLE_ADDR:-0.0.0.0:9001}"

# celld 标准 S3 环境变量与连接参数
export S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:9000}"
export CELLD_BUCKET="${CELLD_BUCKET:-s3://ansts-fleet}"
export AWS_ACCESS_KEY_ID="${RUSTFS_ACCESS_KEY}"
export AWS_SECRET_ACCESS_KEY="${RUSTFS_SECRET_KEY}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

# celld 运行时与容器沙箱调优参数
export CELLD_DURABILITY="${CELLD_DURABILITY:-fleet}"              # fleet (默认对等节点落盘 ~14ms) | bucket
export CELLD_IDLE_EVICT_S="${CELLD_IDLE_EVICT_S:-300}"            # 空闲 Cell 300s 自动卸载释放内存
export CELLD_LTX_RETENTION_SECS="${CELLD_LTX_RETENTION_SECS:-86400}" # 开启旧 Epoch LTX 自动垃圾回收
export CELLD_DEPLOY_POLL_S="${CELLD_DEPLOY_POLL_S:-30}"           # 每 30s 轮询热更新 deploy/current.json
export CELLD_CONTAINER_RUNTIME="${CELLD_CONTAINER_RUNTIME:-runsc}" # gVisor (runsc) 或 Kata (kata) 隔离内核
export DOCKER_HOST="${DOCKER_HOST:-unix:///var/run/docker.sock}"

# 节点监听地址配置 (--advertise 必须指向 --internal-listen 端口)
export NODE_PUBLIC_LISTEN="${NODE_PUBLIC_LISTEN:-0.0.0.0:8080}"
export NODE_INTERNAL_LISTEN="${NODE_INTERNAL_LISTEN:-0.0.0.0:8081}"
export NODE_ADVERTISE_ADDR="${NODE_ADVERTISE_ADDR:-127.0.0.1:8081}"

MODE="${1:-help}"

start_rustfs_docker() {
  echo "===> [Step 1/5] Preparing RustFS storage directories (UID 10001:10001)..."
  mkdir -p "${RUSTFS_DATA_DIR}" "${RUSTFS_LOG_DIR}"
  if [ "$(id -u)" -eq 0 ]; then
    chown -R 10001:10001 "${RUSTFS_DATA_DIR}" "${RUSTFS_LOG_DIR}"
  fi

  echo "===> [Step 2/5] Starting RustFS S3 Server on ${RUSTFS_API_ADDR} (Console: ${RUSTFS_CONSOLE_ADDR})..."
  docker rm -f ansts-rustfs >/dev/null 2>&1 || true
  docker run -d \
    --name ansts-rustfs \
    --restart unless-stopped \
    -p 9000:9000 \
    -p 9001:9001 \
    -v "${RUSTFS_DATA_DIR}:/data" \
    -v "${RUSTFS_LOG_DIR}:/var/log/rustfs/" \
    -e RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY}" \
    -e RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY}" \
    -e RUSTFS_ADDRESS=":9000" \
    -e RUSTFS_CONSOLE_ADDRESS=":9001" \
    -e RUSTFS_CONSOLE_ENABLE="true" \
    -e RUSTFS_OBS_LOGGER_LEVEL="error" \
    -e RUSTFS_OBS_LOG_DIRECTORY="/var/log/rustfs/" \
    rustfs/rustfs:latest \
    /data

  echo "===> Waiting for RustFS S3 health endpoint (${S3_ENDPOINT}/health)..."
  for i in $(seq 1 30); do
    if curl -fsS "${S3_ENDPOINT}/health" >/dev/null 2>&1; then
      echo "  [OK] RustFS is healthy at ${S3_ENDPOINT}"
      break
    fi
    sleep 1
  done
}

create_s3_bucket() {
  echo "===> [Step 3/5] Ensuring S3 bucket ${CELLD_BUCKET} exists on RustFS..."
  if command -v aws >/dev/null 2>&1; then
    aws --endpoint-url "${S3_ENDPOINT}" s3 mb "${CELLD_BUCKET}" 2>/dev/null || true
  elif command -v rc >/dev/null 2>&1; then
    rc alias set local "${S3_ENDPOINT}" "${RUSTFS_ACCESS_KEY}" "${RUSTFS_SECRET_KEY}"
    rc mb "local/${CELLD_BUCKET#s3://}" || true
  else
    echo "  [INFO] Create bucket 'ansts-fleet' via AWS CLI, RustFS CLI (rc), or Console http://127.0.0.1:9001"
  fi
}

deploy_and_migrate_celld() {
  echo "===> [Step 4/5] Deploying ANSTS v2.0 Worker & Durable Objects to ${CELLD_BUCKET}..."
  celld deploy . \
    --bucket "${CELLD_BUCKET}" \
    --endpoint "${S3_ENDPOINT}" \
    --region "${AWS_REGION}"

  echo "===> Applying D1 SQL migrations (migrations/0001_init_ansts.sql)..."
  celld d1 migrations apply ansts-audit-ledger \
    --bucket "${CELLD_BUCKET}" \
    --endpoint "${S3_ENDPOINT}"
}

start_celld_node() {
  echo "===> [Step 5/5] Starting celld node against RustFS bucket ${CELLD_BUCKET}..."
  exec celld \
    --bucket "${CELLD_BUCKET}" \
    --endpoint "${S3_ENDPOINT}" \
    --listen "${NODE_PUBLIC_LISTEN}" \
    --internal-listen "${NODE_INTERNAL_LISTEN}" \
    --advertise "${NODE_ADVERTISE_ADDR}"
}

diagnose_fleet() {
  echo "===> Running celld non-intrusive fleet diagnosis against RustFS..."
  celld diagnose \
    --bucket "${CELLD_BUCKET}" \
    --endpoint "${S3_ENDPOINT}"
  curl -fsS "http://127.0.0.1:8080/.well-known/celld/health" && echo "  [HEALTHY] Public listener ready"
}

case "${MODE}" in
  all)
    start_rustfs_docker
    create_s3_bucket
    deploy_and_migrate_celld
    start_celld_node
    ;;
  rustfs-only)
    start_rustfs_docker
    create_s3_bucket
    ;;
  deploy)
    deploy_and_migrate_celld
    ;;
  node)
    start_celld_node
    ;;
  diagnose)
    diagnose_fleet
    ;;
  *)
    cat <<USAGE
Usage: $0 {all|rustfs-only|deploy|node|diagnose}

Commands:
  all          Start RustFS container, create bucket, run celld deploy + D1 migrations, and start celld node
  rustfs-only  Start RustFS S3 server (:9000 API / :9001 Console) and initialize bucket
  deploy       Deploy ANSTS v2.0 wrangler.json bundle and apply D1 migrations into RustFS bucket
  node         Start a celld node connected to RustFS (--bucket ${CELLD_BUCKET} --endpoint ${S3_ENDPOINT})
  diagnose     Run 'celld diagnose' and probe /.well-known/celld/health
USAGE
    ;;
esac
