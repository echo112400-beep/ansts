# ANSTS — Agent-Native Sandbox Task Scheduler

TypeScript implementation with celld Gateway and Scheduler workers, durable Body control cells, and native Body execution agents. Current implementation baseline: `8eef81e` (2026-11-13).

See [current architecture and limitations](docs/current-architecture.md) and [Body execution recovery](docs/body-cell-development.md). Earlier design documents describe targets and historical designs; their production, capacity and test-count claims are not current verification results.

## Development cluster

Requires Docker Compose. Use the explicit development file; the root Compose file still starts the Node host architecture.

```bash
python3 scripts/prepare_body_dev_credentials.py
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml up -d --build
docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml ps -a
```

Console: http://localhost:8088/; Gateway: http://localhost:18080/health and http://localhost:18081/health. The independent Gateway Worker implements a subset of the legacy Node host APIs; not every console action is connected.

Task and data APIs require tenant-scoped bearer credentials. See [console login](docs/console-session.md) and [Gateway user authorization and development key rotation](docs/gateway-user-authorization.md). The default console shows live authorized tasks; production OIDC still requires provider configuration.

The cluster has three celld Brain nodes, two independent celld dev Gateways, two native Body containers, RustFS, Nginx, Prometheus, Grafana and a one-shot Worker deployment service. Body execution currently writes a task trace and commits a LayerFS snapshot. Uncertain execution requires reconciliation rather than automatic cross-host replay.

## Build and checks

Requires Node.js 22 and installed dependencies.

```bash
npm install
npm run lint
npm run build
npm test
```

Test totals change with the source tree. Run the commands to establish the current result; historical reports do not establish that all current suites pass.
