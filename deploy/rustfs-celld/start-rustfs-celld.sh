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

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

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
export CELLD_READY_TIMEOUT_S="${CELLD_READY_TIMEOUT_S:-120}"     # 节点就绪等待上限 (秒); 上次非优雅退出时需等 dead-node GC
# 诊断用独立端口 (all 模式下节点已占用 8080, diagnose 的 bind 检查会失败)
export CELLD_DIAG_PORT="${CELLD_DIAG_PORT:-18080}"
export DOCKER_HOST="${DOCKER_HOST:-unix:///var/run/docker.sock}"

# 节点监听地址配置 (--advertise 必须指向 --internal-listen 端口)
export NODE_PUBLIC_LISTEN="${NODE_PUBLIC_LISTEN:-0.0.0.0:8080}"
export NODE_INTERNAL_LISTEN="${NODE_INTERNAL_LISTEN:-0.0.0.0:8081}"
export NODE_ADVERTISE_ADDR="${NODE_ADVERTISE_ADDR:-127.0.0.1:8081}"

# ------------------- 2. 代理绕过 (修复 502 Bad Gateway) -----------------------
# celld 是 Rust 二进制 (reqwest / object_store)。与 curl 或 Go 不同, reqwest 不把
# NO_PROXY 中的通配条目 (如 `127.*`, `10.*`) 当作匹配 127.0.0.1, 因此发往本地 S3
# 端点 (${S3_ENDPOINT}) 的请求会被塞进 HTTP_PROXY, 由代理返回 502 Bad Gateway:
#   Error performing GET http://127.0.0.1:9000/... : 502 Bad Gateway
# 解决: 把端点主机名以其 "精确主机" 形式追加进 NO_PROXY / no_proxy。
# 保留代理用于其余出网流量, 仅本地 S3 直连。
s3_endpoint_host() {
  local host="${S3_ENDPOINT#*://}"   # 去掉 scheme
  host="${host%%/*}"                 # 去掉 path
  host="${host##*@}"                 # 去掉 userinfo
  case "${host}" in
    *:[0-9]*) host="${host%:*}" ;;   # 去掉 :port (不动 IPv6 裸地址)
  esac
  host="${host#[}"; host="${host%]}" # [::1] -> ::1
  printf '%s' "${host:-127.0.0.1}"
}

LOCAL_S3_HOST="$(s3_endpoint_host)"
add_no_proxy() {
  local list="${NO_PROXY:-${no_proxy:-}}" entry
  for entry in "${LOCAL_S3_HOST}" localhost 127.0.0.1 ::1; do
    case ",${list}," in
      *",${entry},"*) ;;                                  # 已存在
      *) list="${list:+${list},}${entry}" ;;
    esac
  done
  export NO_PROXY="${list}"
  export no_proxy="${list}"
}
add_no_proxy

# celld deploy 用 esbuild 打包, 且要求 esbuild >= 0.21 (需要 --target=es2024)。
# 优先使用仓库内版本, 避免依赖 PATH 上过旧的全局 esbuild。
if [ -z "${CELLD_ESBUILD:-}" ] && [ -x "${REPO_ROOT}/node_modules/.bin/esbuild" ]; then
  export CELLD_ESBUILD="${REPO_ROOT}/node_modules/.bin/esbuild"
fi

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
  local healthy=0
  for _ in $(seq 1 30); do
    if curl -fsS "${S3_ENDPOINT}/health" >/dev/null 2>&1; then
      echo "  [OK] RustFS is healthy at ${S3_ENDPOINT}"
      healthy=1
      break
    fi
    sleep 1
  done
  if [ "${healthy}" -ne 1 ]; then
    # 继续往下走只会得到难懂的 502/连接错误, 这里直接失败并留下日志路径
    echo "  [ERROR] RustFS not healthy after 30s at ${S3_ENDPOINT}/health" >&2
    echo "          Inspect: docker logs ansts-rustfs  (or ${RUSTFS_LOG_DIR})" >&2
    return 1
  fi
}

create_s3_bucket() {
  echo "===> [Step 3/5] Ensuring S3 bucket ${CELLD_BUCKET} exists on RustFS..."
  # RustFS CLI 常见于仓库根目录 (./rc), 不在 PATH 上, 也要能找到
  local rc_bin=""
  if command -v rc >/dev/null 2>&1; then
    rc_bin="$(command -v rc)"
  elif [ -x "${REPO_ROOT}/rc" ]; then
    rc_bin="${REPO_ROOT}/rc"
  fi

  if command -v aws >/dev/null 2>&1; then
    aws --endpoint-url "${S3_ENDPOINT}" s3 mb "${CELLD_BUCKET}" 2>/dev/null || true
  elif [ -n "${rc_bin}" ]; then
    "${rc_bin}" alias set local "${S3_ENDPOINT}" "${RUSTFS_ACCESS_KEY}" "${RUSTFS_SECRET_KEY}"
    "${rc_bin}" mb "local/${CELLD_BUCKET#s3://}" || true
  else
    echo "  [INFO] Create bucket 'ansts-fleet' via AWS CLI, RustFS CLI (rc), or Console http://127.0.0.1:9001"
  fi
}

deploy_celld_bundle() {
  echo "===> [Step 4/5] Deploying ANSTS v2.0 Worker & Durable Objects to ${CELLD_BUCKET}..."
  celld deploy . \
    --bucket "${CELLD_BUCKET}" \
    --endpoint "${S3_ENDPOINT}" \
    --region "${AWS_REGION}"
}

apply_d1_migrations() {
  # 注意: D1 绑定背后是跑在 fleet 节点上的 Durable Object,
  # 因此必须有活跃节点 (node lease) 才能迁移, 否则报:
  #   no node leases in the bucket; celld d1 needs a running fleet
  echo "===> Applying D1 SQL migrations (migrations/0001_init_ansts.sql)..."
  celld d1 migrations apply ansts-audit-ledger \
    --bucket "${CELLD_BUCKET}" \
    --endpoint "${S3_ENDPOINT}"
}

celld_node_cmd() {
  printf '%s\0' celld \
    --bucket "${CELLD_BUCKET}" \
    --endpoint "${S3_ENDPOINT}" \
    --listen "${NODE_PUBLIC_LISTEN}" \
    --internal-listen "${NODE_INTERNAL_LISTEN}" \
    --advertise "${NODE_ADVERTISE_ADDR}"
}

# 前台启动 (node / 手动调试用)
start_celld_node() {
  echo "===> [Step 5/5] Starting celld node against RustFS bucket ${CELLD_BUCKET}..."
  mapfile -d '' -t _node_argv < <(celld_node_cmd)
  exec "${_node_argv[@]}"
}

# 后台启动, 等就绪后再返回 (all 模式用: 迁移需要活跃节点)
start_celld_node_background() {
  echo "===> [Step 5/5] Starting celld node in background against ${CELLD_BUCKET}..."
  mapfile -d '' -t _node_argv < <(celld_node_cmd)
  "${_node_argv[@]}" &
  CELLD_NODE_PID=$!
  echo "  [OK] celld node pid ${CELLD_NODE_PID}"
}

wait_for_node_ready() {
  local health_url="http://127.0.0.1:${NODE_PUBLIC_LISTEN##*:}/.well-known/celld/health"
  echo "===> Waiting for celld public listener readiness (${health_url})..."
  local code
  for _ in $(seq 1 "${CELLD_READY_TIMEOUT_S:-60}"); do
    # 503 = 进程已起但 fleet 尚未 settle, 属正常中间态
    code="$(curl -sS -o /dev/null -w '%{http_code}' "${health_url}" 2>/dev/null || echo 000)"
    if [ "${code}" = "200" ]; then
      echo "  [OK] celld node ready (${health_url})"
      return 0
    fi
    if ! kill -0 "${CELLD_NODE_PID}" 2>/dev/null; then
      echo "  [ERROR] celld node exited during startup" >&2
      wait "${CELLD_NODE_PID}" 2>/dev/null || true
      return 1
    fi
    sleep 1
  done
  echo "  [ERROR] celld node not ready after ${CELLD_READY_TIMEOUT_S:-60}s (last health=${code})" >&2
  return 1
}

# celld 优雅停机时会排空 lease / LTX 并退出, 实测约 16s, 上限由
# CELLD_SHUTDOWN_TOTAL_MS 控制 (默认 40000)。给足窗口再升级 SIGKILL。
# 注意: 若用 SIGKILL 硬杀, 残留 lease 会让下一个节点的 readiness
# 一直卡在 503, 直到 dead-node GC 扫完 (约 60s) 后 fleet 才能 settle。
stop_celld_node() {
  [ -n "${CELLD_NODE_PID:-}" ] || return 0
  kill -0 "${CELLD_NODE_PID}" 2>/dev/null || return 0
  echo "===> Stopping celld node (pid ${CELLD_NODE_PID})..."
  kill "${CELLD_NODE_PID}" 2>/dev/null || true
  local bound=$(( ${CELLD_SHUTDOWN_TOTAL_MS:-40000} / 1000 + 10 ))
  local i
  for i in $(seq 1 "${bound}"); do
    kill -0 "${CELLD_NODE_PID}" 2>/dev/null || { echo "  [OK] stopped gracefully after ${i}s"; return 0; }
    sleep 1
  done
  echo "  [WARN] still running after ${bound}s, escalating to SIGKILL" >&2
  kill -9 "${CELLD_NODE_PID}" 2>/dev/null || true
  echo "  [OK] stopped (SIGKILL)"
}

# diagnose 会在 --listen 上做 bind 检查 (不提供服务), 而 all 模式下节点已占据
# ${NODE_PUBLIC_LISTEN}, 因此诊断使用独立端口。
diagnose_fleet() {
  local diag_port="${CELLD_DIAG_PORT:-18080}"
  local diag_peer_port=$(( diag_port + 1 ))
  echo "===> Running celld non-intrusive fleet diagnosis against RustFS..."
  celld diagnose \
    --bucket "${CELLD_BUCKET}" \
    --endpoint "${S3_ENDPOINT}" \
    --listen "127.0.0.1:${diag_port}" \
    --internal-listen "127.0.0.1:${diag_peer_port}" \
    --advertise "127.0.0.1:${diag_peer_port}"
  local health_url="http://127.0.0.1:${NODE_PUBLIC_LISTEN##*:}/.well-known/celld/health"
  if curl -fsS "${health_url}" >/dev/null 2>&1; then
    echo "  [HEALTHY] Public listener ready (${health_url})"
  else
    echo "  [WARN] Public listener not responding at ${health_url} (node not running?)" >&2
  fi
}

case "${MODE}" in
  all)
    # 顺序很关键: 先建桶 → 部署代码 → 起节点 → 再迁移 D1。
    # D1 迁移必须有活跃节点, 不能先于节点执行。
    start_rustfs_docker
    create_s3_bucket
    deploy_celld_bundle
    start_celld_node_background
    # 立即挂上清理 trap: 后续任何一步失败都不能把后台节点变成孤儿进程
    trap 'stop_celld_node; exit 0' INT TERM
    trap 'stop_celld_node' EXIT
    wait_for_node_ready
    apply_d1_migrations
    diagnose_fleet || true
    echo "===> [READY] celld node (pid ${CELLD_NODE_PID}) is serving. Ctrl-C to stop."
    wait "${CELLD_NODE_PID}"
    ;;
  rustfs-only)
    start_rustfs_docker
    create_s3_bucket
    ;;
  deploy)
    # 仅部署代码包; 如需迁移 D1 请先起节点, 或用 'all'
    deploy_celld_bundle
    ;;
  migrate)
    apply_d1_migrations
    ;;
  node)
    start_celld_node
    ;;
  diagnose)
    diagnose_fleet
    ;;
  *)
    cat <<USAGE
Usage: $0 {all|rustfs-only|deploy|migrate|node|diagnose}

Commands:
  all          Start RustFS, create bucket, deploy, start node, wait ready, then apply D1 migrations
  rustfs-only  Start RustFS S3 server (:9000 API / :9001 Console) and initialize bucket
  deploy       Deploy ANSTS v2.0 wrangler.json bundle into the RustFS bucket (no migrations)
  migrate      Apply D1 migrations (requires a running node: 'celld d1 needs a running fleet')
  node         Start a celld node connected to RustFS (--bucket ${CELLD_BUCKET} --endpoint ${S3_ENDPOINT})
  diagnose     Run 'celld diagnose' and probe /.well-known/celld/health
USAGE
    ;;
esac
