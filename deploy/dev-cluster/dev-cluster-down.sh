#!/usr/bin/env bash
set -euo pipefail
if [[ -n "${INIT_CWD:-}" && -f "$INIT_CWD/scripts/restart_dev_cluster.py" ]]; then
  cd "$INIT_CWD"
elif [[ ! -f scripts/restart_dev_cluster.py ]]; then
  cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi
exec python3 scripts/restart_dev_cluster.py --skip-build --stop-only
