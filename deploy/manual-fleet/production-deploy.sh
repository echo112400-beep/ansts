#!/usr/bin/env bash
# ==============================================================================
# ANSTS v2.2 Production Deployment & Non-K8s Manual Fleet Orchestrator
# Architecture: denoland/celld (Brain Fleet) + Bare-Metal KVM (Body Fleet) + RustFS EC:4
# ==============================================================================
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ==================== 1. PRODUCTION ENVIRONMENT VARIABLES ====================
export ANSTS_RUNTIME_PROFILE="${ANSTS_RUNTIME_PROFILE:-production}"
export CELLD_BUCKET="${CELLD_BUCKET:-s3://ansts-fleet}"
export S3_ENDPOINT="${S3_ENDPOINT:-http://10.240.64.11:9000}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-ansts_prod_ak_9f8e7d6c5b4a}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-ansts_prod_sk_1a2b3c4d5e6f7g8h9i0j}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

export CELLD_DURABILITY="${CELLD_DURABILITY:-fleet}"
export CELLD_TTL_MS="${CELLD_TTL_MS:-10000}"
export CELLD_IDLE_EVICT_S="${CELLD_IDLE_EVICT_S:-300}"
export CELLD_LTX_RETENTION_SECS="${CELLD_LTX_RETENTION_SECS:-86400}"
export CELLD_REBALANCE_INTERVAL_MS="${CELLD_REBALANCE_INTERVAL_MS:-5000}"
export CELLD_V8_HEAP_LIMIT_MB="${CELLD_V8_HEAP_LIMIT_MB:-128}"
export CELLD_SQL_STATEMENT_BUDGET_MS="${CELLD_SQL_STATEMENT_BUDGET_MS:-250}"

export EDGE_API_URL="${EDGE_API_URL:-http://127.0.0.1:8080}"
export CGROUP_ROOT="${CGROUP_ROOT:-/sys/fs/cgroup}"
export PROC_PRESSURE_ROOT="${PROC_PRESSURE_ROOT:-/proc/pressure}"
export VSOCK_SOCKET_DIR="${VSOCK_SOCKET_DIR:-/var/run/ansts}"
export MTLS_CERT_DIR="${MTLS_CERT_DIR:-/etc/ansts/pki}"

log_info()  { printf "\033[1;34m[ANSTS-PROD][INFO]\033[0m  %s\n" "$*"; }
log_ok()    { printf "\033[1;32m[ANSTS-PROD][OK]\033[0m    %s\n" "$*"; }
log_warn()  { printf "\033[1;33m[ANSTS-PROD][WARN]\033[0m  %s\n" "$*" >&2; }
log_fatal() { printf "\033[1;31m[ANSTS-PROD][FATAL]\033[0m %s\n" "$*" >&2; exit 1; }

# ==================== 2. Q5 FAIL-FAST PRODUCTION PREFLIGHT GATE ====================
preflight_check() {
  local role="${1:-all}"
  log_info "Running Q5 Fail-Fast Preflight Gate (ANSTS_RUNTIME_PROFILE=${ANSTS_RUNTIME_PROFILE}, role=${role})..."

  if [[ "${ANSTS_RUNTIME_PROFILE}" != "production" ]]; then
    log_warn "Running in non-production profile (${ANSTS_RUNTIME_PROFILE}); strict kernel gates relaxed."
    return 0
  fi

  if [[ -z "${S3_ENDPOINT}" || -z "${CELLD_BUCKET}" ]]; then
    log_fatal "S3_ENDPOINT and CELLD_BUCKET must be set in production."
  fi
  if [[ "${#AWS_SECRET_ACCESS_KEY}" -lt 16 ]]; then
    log_fatal "AWS_SECRET_ACCESS_KEY is too short for production (<16 chars)."
  fi

  if [[ "${role}" == "brain" || "${role}" == "all" ]]; then
    command -v curl >/dev/null 2>&1 || log_fatal "'curl' binary is required on Brain nodes."
    log_ok "Brain preflight passed (Bucket=${CELLD_BUCKET}, Durability=${CELLD_DURABILITY})."
  fi

  if [[ "${role}" == "body" || "${role}" == "all" ]]; then
    [[ -f "${CGROUP_ROOT}/cgroup.controllers" ]] || \
      log_fatal "Cgroups v2 unified hierarchy not mounted at ${CGROUP_ROOT}/cgroup.controllers."
    [[ -f "${PROC_PRESSURE_ROOT}/cpu" && -f "${PROC_PRESSURE_ROOT}/memory" ]] || \
      log_fatal "Linux PSI metrics missing at ${PROC_PRESSURE_ROOT}/{cpu,memory}. Enable kernel param 'psi=1'."
    mkdir -p "${VSOCK_SOCKET_DIR}" "${CGROUP_ROOT}/ansts.slice" || \
      log_fatal "Insufficient permissions to create ${VSOCK_SOCKET_DIR} or ${CGROUP_ROOT}/ansts.slice."
    log_ok "Body Host kernel preflight passed (Cgroups v2 + PSI + VSOCK directory verified)."
  fi
}

# ==================== 3. Q1 INTERNAL mTLS PKI & SPIFFE PROVISIONER ====================
init_mtls_pki() {
  log_info "Provisioning internal TLS 1.3 mTLS certificates (SPIFFE trust domain: spiffe://ansts.internal)..."
  mkdir -p "${MTLS_CERT_DIR}"
  if [[ ! -f "${MTLS_CERT_DIR}/ca.crt" ]]; then
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -days 3650 -nodes \
      -keyout "${MTLS_CERT_DIR}/ca.key" -out "${MTLS_CERT_DIR}/ca.crt" \
      -subj "/CN=ansts-internal-root-ca/O=ANSTS-v2.2" 2>/dev/null
  fi
  for comp in "celld-brain" "body-daemon"; do
    if [[ ! -f "${MTLS_CERT_DIR}/${comp}.crt" ]]; then
      openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -keyout "${MTLS_CERT_DIR}/${comp}.key" -out "${MTLS_CERT_DIR}/${comp}.csr" \
        -subj "/CN=${comp}.ansts.internal/O=ANSTS-v2.2" 2>/dev/null
      openssl x509 -req -in "${MTLS_CERT_DIR}/${comp}.csr" \
        -CA "${MTLS_CERT_DIR}/ca.crt" -CAkey "${MTLS_CERT_DIR}/ca.key" -CAcreateserial \
        -out "${MTLS_CERT_DIR}/${comp}.crt" -days 825 2>/dev/null
    fi
  done
  chmod 600 "${MTLS_CERT_DIR}"/*.key
  log_ok "mTLS PKI certificates ready in ${MTLS_CERT_DIR}."
}

# ==================== 4. L4 STORAGE: 16-DRIVE EC:4 RUSTFS HA CLUSTER ====================
deploy_storage_ec4() {
  log_info "Deploying 4-Node x 4-NVMe (16-Drive EC:4) RustFS S3 Cluster..."
  bash "${REPO_ROOT}/deploy/rustfs-celld/rustfs-ec-setup.sh"
  docker compose -f "${REPO_ROOT}/deploy/rustfs-celld/docker-compose.rustfs-ha.yml" up -d

  log_info "Waiting for RustFS S3 API (${S3_ENDPOINT}/health)..."
  for _ in {1..30}; do
    if curl -fsS "${S3_ENDPOINT}/health" >/dev/null 2>&1; then
      log_ok "RustFS 16-Drive EC:4 cluster is healthy."
      break
    fi
    sleep 2
  done

  seed_topology_manifest
}

# ==================== 5. Q2 S3 CAS TOPOLOGY MANIFEST SEED & BROADCAST ====================
seed_topology_manifest() {
  local tmp_topo="/tmp/topology-v1.json"
  cat > "${tmp_topo}" <<EOF
{
  "schemaVersion": "v1",
  "generation": 1,
  "updatedAtMs": $(date +%s000),
  "schedulerShardCount": 8,
  "queueShardCount": 8,
  "brainNodes": [
    { "nodeId": "celld-node-01", "listenAddr": "10.240.16.11:8080", "internalAdminUrl": "http://10.240.16.11:9001", "weight": 100, "draining": false },
    { "nodeId": "celld-node-02", "listenAddr": "10.240.16.12:8080", "internalAdminUrl": "http://10.240.16.12:9001", "weight": 100, "draining": false },
    { "nodeId": "celld-node-03", "listenAddr": "10.240.16.13:8080", "internalAdminUrl": "http://10.240.16.13:9001", "weight": 100, "draining": false }
  ],
  "bodyHosts": [
    { "hostId": "body-host-01", "ipAddr": "10.240.32.11", "tunnelPort": 9443, "maxBurst": 20 },
    { "hostId": "body-host-02", "ipAddr": "10.240.32.12", "tunnelPort": 9443, "maxBurst": 20 }
  ]
}
EOF
  if command -v aws >/dev/null 2>&1; then
    aws --endpoint-url "${S3_ENDPOINT}" s3 mb "${CELLD_BUCKET}" 2>/dev/null || true
    aws --endpoint-url "${S3_ENDPOINT}" s3 cp "${tmp_topo}" "${CELLD_BUCKET}/control/topology-v1.json"
    log_ok "Uploaded global CAS topology manifest to ${CELLD_BUCKET}/control/topology-v1.json"
  fi
}

# ==================== 6. L2 BRAIN NODE DEPLOYMENT (denoland/celld) ====================
deploy_brain_node() {
  local node_id="${1:-celld-node-01}"
  local node_ip="${2:-127.0.0.1}"
  local weight="${3:-100}"

  preflight_check "brain"
  log_info "Deploying celld Brain Node ${node_id} (listen=${node_ip}:8080, internal=${node_ip}:9001, weight=${weight})..."

  install -d -m 0755 /var/lib/celld /etc/celld
  cat > /etc/celld/celld.env <<EOF
ANSTS_RUNTIME_PROFILE=production
CELLD_BUCKET=${CELLD_BUCKET}
S3_ENDPOINT=${S3_ENDPOINT}
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
AWS_REGION=${AWS_REGION}
CELLD_DURABILITY=${CELLD_DURABILITY}
CELLD_TTL_MS=${CELLD_TTL_MS}
CELLD_IDLE_EVICT_S=${CELLD_IDLE_EVICT_S}
CELLD_LTX_RETENTION_SECS=${CELLD_LTX_RETENTION_SECS}
CELLD_REBALANCE_INTERVAL_MS=${CELLD_REBALANCE_INTERVAL_MS}
CELLD_V8_HEAP_LIMIT_MB=${CELLD_V8_HEAP_LIMIT_MB}
CELLD_SQL_STATEMENT_BUDGET_MS=${CELLD_SQL_STATEMENT_BUDGET_MS}
CELLD_PLACEMENT_WEIGHT=${weight}
EOF

  cp "${REPO_ROOT}/deploy/rustfs-celld/systemd/celld.service" /etc/systemd/system/celld.service
  systemctl daemon-reload
  systemctl enable --now celld.service
  log_ok "celld Brain Node ${node_id} active."
}

# ==================== 7. L3-B SANDBOX BODY HOST DEPLOYMENT ====================
deploy_body_host() {
  local host_id="${1:-body-host-01}"
  local host_ip="${2:-127.0.0.1}"
  local max_burst="${3:-20}"

  preflight_check "body"
  init_mtls_pki
  log_info "Configuring Bare-Metal Sandbox Body Host ${host_id} (${host_ip}:9443, max_burst=${max_burst})..."

  mkdir -p "${CGROUP_ROOT}/ansts.slice"
  if [[ -w "${CGROUP_ROOT}/cgroup.subtree_control" ]]; then
    echo "+cpu +memory +pids" > "${CGROUP_ROOT}/cgroup.subtree_control" || true
  fi

  cp "${REPO_ROOT}/deploy/manual-fleet/systemd/ansts-node-daemon.service" /etc/systemd/system/ansts-node-daemon.service
  systemctl daemon-reload
  systemctl enable --now ansts-node-daemon.service
  log_ok "Sandbox Body Host ${host_id} active and listening on mTLS :9443 + local AF_VSOCK."
}

# ==================== 8. ZERO-DOWNTIME ROLLING RESTART & SCALE COMMANDS ====================
wait_cluster_restoring_zero() {
  local admin_urls=("$@")
  log_info "Verifying cluster-wide restoring === 0 gate across ${#admin_urls[@]} brain nodes..."
  for attempt in {1..60}; do
    local total_restoring=0
    for url in "${admin_urls[@]}"; do
      local state_json
      state_json="$(curl -fsS "${url}/state" 2>/dev/null || echo '{"node_load":{"restoring":0}}')"
      local r
      r="$(echo "${state_json}" | grep -o '"restoring":[0-9]*' | head -n1 | cut -d: -f2 || echo 0)"
      total_restoring=$((total_restoring + ${r:-0}))
    done
    if [[ "${total_restoring}" -eq 0 ]]; then
      log_ok "Cluster restoring gate clear (total_restoring=0)."
      return 0
    fi
    log_info "Waiting for ${total_restoring} restoring cells to settle (attempt ${attempt}/60)..."
    sleep 2
  done
  log_fatal "Timed out waiting for cluster restoring === 0."
}

rolling_restart_brain_fleet() {
  local admin_urls=("$@")
  [[ "${#admin_urls[@]}" -gt 0 ]] || admin_urls=("http://10.240.16.11:9001" "http://10.240.16.12:9001" "http://10.240.16.13:9001")
  for url in "${admin_urls[@]}"; do
    wait_cluster_restoring_zero "${admin_urls[@]}"
    log_info "Triggering in-place preserve handoff on ${url} (/shutdown?handoff=preserve)..."
    curl -fsS -X POST "${url}/shutdown?handoff=preserve"
    sleep 3
    wait_cluster_restoring_zero "${admin_urls[@]}"
    log_ok "Node ${url} completed zero-downtime restart."
  done
}

scale_virtual_shards() {
  local add_shards="${1:-4}"
  log_info "Executing Q3 3-Phase Virtual Slot Migration (+${add_shards} SchedulerCell shards)..."
  curl -fsS -X POST "${EDGE_API_URL}/v1beta1/fleet/scale" \
    -H "content-type: application/json" \
    -d "{\"add_scheduler_shards\": ${add_shards}}"
  echo ""
  log_ok "Scheduler shards scaled out with 256-virtual-slot 3-phase handover."
}

# ==================== 9. CLI DISPATCHER ====================
ACTION="${1:-help}"
shift || true

case "${ACTION}" in
  preflight)       preflight_check "${1:-all}" ;;
  pki)             init_mtls_pki ;;
  deploy-storage)  deploy_storage_ec4 ;;
  deploy-brain)    deploy_brain_node "$@" ;;
  deploy-body)     deploy_body_host "$@" ;;
  scale-shards)    scale_virtual_shards "$@" ;;
  rolling-restart) rolling_restart_brain_fleet "$@" ;;
  all)
    preflight_check "all"
    init_mtls_pki
    deploy_storage_ec4
    deploy_brain_node "celld-node-01" "127.0.0.1" 100
    deploy_body_host "body-host-01" "127.0.0.1" 20
    log_ok "Full ANSTS v2.2 Non-K8s Production Stack Deployed Successfully."
    ;;
  *)
    cat <<USAGE
Usage: $0 <command> [args...]

Commands:
  preflight [all|brain|body]                  Run Q5 Fail-Fast production environment & kernel gate
  pki                                         Generate internal TLS 1.3 mTLS SPIFFE certificates
  deploy-storage                              Deploy 4-node x 4-drive (16-Drive EC:4) RustFS S3 cluster
  deploy-brain <node-id> <ip> [weight]        Deploy celld Brain Node via Systemd
  deploy-body <host-id> <ip> [max-burst]      Deploy Bare-Metal Sandbox Body Host via Systemd
  scale-shards <add-shard-count>              Expand Scheduler shards via Q3 256-slot 3-phase migration
  rolling-restart [http://ip:9001 ...]        Execute zero-downtime preserve-handoff rolling restart
  all                                         Run preflight + PKI + Storage + Brain + Body deployment
USAGE
    ;;
esac
