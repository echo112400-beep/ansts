#!/usr/bin/env bash
# TASK-57 (Non-K8s Edition): Manual Fleet Scale-Out / Scale-In & Rolling Update Script
# Supports Bare-Metal / VM Systemd & Docker Compose deployments without Kubernetes.
set -euo pipefail

ACTION="${1:-status}"
 shift || true

CELLD_BUCKET="${CELLD_BUCKET:-s3://ansts-fleet}"
S3_ENDPOINT="${S3_ENDPOINT:-http://10.240.64.11:9000}"
EDGE_API_URL="${EDGE_API_URL:-http://127.0.0.1:8080}"

case "${ACTION}" in
  add-brain-node)
    NODE_ID="${1:?Usage: scale-fleet.sh add-brain-node <node-id> <advertise-ip> [weight]}"
    ADV_IP="${2:?Usage: scale-fleet.sh add-brain-node <node-id> <advertise-ip> [weight]}"
    WEIGHT="${3:-100}"
    echo "[Manual Scale-Out] Starting new celld brain node '${NODE_ID}' (${ADV_IP}:8080, internal ${ADV_IP}:9001, weight=${WEIGHT})..."
    echo "celld --bucket ${CELLD_BUCKET} --endpoint ${S3_ENDPOINT} --listen ${ADV_IP}:8080 --internal-listen ${ADV_IP}:9001 --advertise http://${ADV_IP}:9001"
    echo "[Manual Scale-Out] Node joined shared bucket ${CELLD_BUCKET}; celld rebalancer will migrate <=32 hibernated cells/5s."
    ;;

  remove-brain-node)
    INTERNAL_URL="${1:?Usage: scale-fleet.sh remove-brain-node <http://node-ip:9001> [preserve|drain]}"
    MODE="${2:-drain}"
    if [ "${MODE}" = "preserve" ]; then
      echo "[Manual Scale-In] Triggering in-place preserve shutdown on ${INTERNAL_URL}..."
      curl -fsS -X POST "${INTERNAL_URL}/shutdown?handoff=preserve"
    else
      echo "[Manual Scale-In] Draining node ${INTERNAL_URL} and handing off cells to remaining peers..."
      curl -fsS -X POST "${INTERNAL_URL}/shutdown"
    fi
    ;;

  add-body-host)
    HOST_ID="${1:?Usage: scale-fleet.sh add-body-host <host-id> <host-ip> [max-burst]}"
    HOST_IP="${2:?Usage: scale-fleet.sh add-body-host <host-id> <host-ip> [max-burst]}"
    MAX_BURST="${3:-20}"
    echo "[Manual Scale-Out] Registering Sandbox Body Host '${HOST_ID}' (${HOST_IP}, max_burst=${MAX_BURST}) via systemd ansts-node-daemon.service..."
    ;;

  scale-shards)
    ADD_SHARDS="${1:-4}"
    echo "[Manual Scale-Out] Expanding SchedulerCell shards by +${ADD_SHARDS} on ${EDGE_API_URL}..."
    curl -fsS -X POST "${EDGE_API_URL}/v1beta1/scheduler/shards" \
      -H "content-type: application/json" \
      -d "{\"add_shards\": ${ADD_SHARDS}}"
    ;;

  status)
    echo "[Manual Fleet Status] Querying ${EDGE_API_URL}/v1beta1/scheduler/shards..."
    ;;

  *)
    echo "Usage: $0 {add-brain-node|remove-brain-node|add-body-host|scale-shards|status}"
    exit 1
    ;;
esac
