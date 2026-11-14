# celld development deployment

> 2026-11-14：Body 已拆分为独立 celld 控制模块（18085）与原生执行 Daemon（19501/19502）。Compose 共 14 个服务、两个一次性作业；下文旧的 Brain 内 Body 与通用执行器描述以 [拆分说明](../../docs/body-celld-daemon-split.md) 为准。


Current code baseline: `8eef81e`, checked 2026-11-13. See [current architecture](../../docs/current-architecture.md) for limits and evidence.

## Runtime boundaries

- Three `ghcr.io/denoland/celld:0.6.3` Brain nodes adopt the published scheduler Worker from RustFS. It exports Scheduler, Harness and Tunnel cells; the legacy Body binding remains available for migration export.
- Two Gateway containers run `celld dev` with `wrangler.gateway.json`, staged by `deploy/dev-cluster/start-celld-gateway.sh`. They are independent local stores in container temporary directories, not a replicated Gateway fleet.
- Two Node Body containers run the native host plus `body_cell_agent.mjs`. Bodies register capacity directly with the scheduler, poll leases, renew during execution and persist results. Tasks do execute; they no longer remain leased awaiting a gateway-side executor.
- Body agents point at `http://body-celld:8080`; Gateway and Body control point at `http://celld-brain-01:8081`. Client endpoint failover is not implemented.

## Compose deployment

```bash
python3 scripts/prepare_body_dev_credentials.py
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml up -d --build
# Republish scheduler source changes:
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml up -d --force-recreate celld-deploy
# Restage changed Gateway source:
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml up -d --force-recreate ansts-gateway-01 ansts-gateway-02 tanstack-console
```

`celld-deploy` exits with code zero after publishing; it is not a long-running service. Nodes adopt the new version on their pointer poll. Body source changes require image rebuilds. Development Body agent scripts are mounted read-only and require process restart after edits.

## Standalone launchers

After installing dependencies and building with Node.js 22, set `CELLD_BIN` to an installed celld executable and run these in separate terminals:

```bash
bash scripts/celld_scheduler_up.sh
bash scripts/celld_gateway_up.sh
```

Defaults: Scheduler `127.0.0.1:9876`, Gateway `127.0.0.1:9877`. These local launchers use separate staged projects and stores. Gateway reads `ANSTS_SCHEDULER_URL`; an absent URL fails closed. They differ from the object-store-backed Compose Brain fleet.

`npm start` runs the legacy Node host, not either celld Worker. Operator release requires a configured `ANSTS_CONSOLE_RBAC_SECRET`, a valid bearer token and explicit confirmation. The development Gateway shell launcher currently injects the scheduler URL only; do not assume it forwards the operator secret.

## Recovery contract

Per-host Body cells store heartbeat and receipts. Per-task cells arbitrate execution ownership across hosts. The native Body volume holds an exclusive execution journal. Completed results are reused; uncertain execution blocks automatic replay and needs reconciliation. Cancelling an HTTP request does not undo native effects. No strict exactly-once or production RPO/RTO guarantee is claimed.
