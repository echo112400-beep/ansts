# celld fleet deployment (dev cluster)

The dev cluster runs a real [denoland/celld](https://github.com/denoland/celld) fleet as the Brain layer:
three celld nodes (`celld-brain-01..03`) own `AnstsSchedulerCell` shards, and the gateways submit work to
them instead of scheduling in-process.

## Start the unified control plane (one celld process)

This is the primary way to run the gateway and the scheduler. celld loads `src/cloudflare/worker.ts`
and serves both. `AnstsGatewayCell` holds dispatch, lease, and operator-session rows.
`AnstsSchedulerCell` holds tasks. Both are SQLite Durable Objects in the same celld process, under
`.celld/worker-service/.celld/dev`.

`npm start` / `node dist/src/server_main.js` is the host process: Body execution, LayerFS, cgroups,
and the dashboard. It is not a second control plane. Set `ANSTS_CELLD_WORKER_URL` (or
`celldWorkerUrl`) and that process forwards `/v1/tasks`, `/v1beta1/tasks/dispatch`, and gateway lease
routes to this worker instead of scheduling them in memory. Its `/healthz` reports
`controlPlane: "legacy-node-host"`.

Operator lease release (`POST /v1beta1/gateway/leases/release`) requires `Authorization: Bearer` signed
with `ANSTS_CONSOLE_RBAC_SECRET` and a JSON body `"confirm": true`. An empty secret, the wrangler
default, fail-closes with 503. Put the secret in `.dev.vars` beside the staged wrangler config for
`celld dev`, or in the process environment for a fleet. Do not commit it.

```bash
npm install
npm run build
export CELLD_BIN=/path/to/celld    # denoland/celld 0.6.3; or put `celld` on PATH
bash scripts/celld_worker_up.sh
```

| Boundary | What it is |
| --- | --- |
| celld process | `celld dev`. Public worker listener `127.0.0.1:9876` (`CELLD_PORT`). Internal operator listener is an ephemeral loopback port printed as `internal listening`. |
| Worker | `src/cloudflare/worker.ts` `fetch`. Gateway routes and scheduler routes. |
| Gateway Durable Object | `AnstsGatewayCell`. Dispatch, lease, and operator session rows. |
| Scheduler Durable Object | `AnstsSchedulerCell`. Task rows. Harness and tunnel cells stay available on `/v1/harness` and `/v1/tunnel`. |
| Gateway HTTP | `GET /health`, `/healthz`, `/readyz`; `POST /v1/tasks`; `GET /v1/tasks/:id`; `POST /v1beta1/tasks/dispatch`; `GET /v1beta1/console/gateway`; `GET /v1beta1/gateway/state`; `POST /v1beta1/gateway/leases/release`. |
| Body / host process | Sandbox execution, LayerFS, cgroups, and the dashboard. Not inside the worker. Talks to the worker over HTTP when `ANSTS_CELLD_WORKER_URL` is set. |

`CELLD_CLEAN=1` deletes `.celld/dev` before start. Without it, a second run of the same project directory reads the same Durable Object back, including tasks submitted before the process exited.

Verified against celld `0.6.3` (`celld dev`). esbuild comes from `node_modules` after `npm install`, or `CELLD_ESBUILD`.

## What runs where

| Piece | Container | Port | Role |
| --- | --- | --- | --- |
| celld fleet | `ansts-celld-brain-01..03` | `8081` public, `8082` raft | owns Brain cells, published fleet control plane |
| Worker bundle | inside celld | — | `src/cloudflare/worker.ts` served by celld (`/internal/v1beta1/brain/*`) |
| Object store | `ansts-rustfs` | `9000` | `s3://ansts-fleet` bucket holding `deploy/` + `fleet/` |
| Gateways | `ansts-gateway-01/02` | `8080` | public API, body-host registry, celld client |

## Deploy flow

`scripts/celld_deploy.sh` stages a self-contained project (config + worker source + `migrations/`), runs
`celld deploy` against the bucket, and celld nodes adopt the new version on their next poll. Wire it back in
with `docker compose -f deploy/dev-cluster/docker-compose.dev-cluster.yml up -d celld-deploy` once the
worker changes; the gateways only need `npm run build` + an image rebuild.

## Gateway ↔ celld contract

Verified against celld `0.6.3`:

- `GET /internal/v1beta1/state` — node mode, public/internal listen addresses, owned cell count.
- `PUT /internal/v1beta1/worker` — deploy metadata + bundle; nodes report `deployment_adopted`.
- `POST /internal/v1beta1/brain/schedule` — brain-schedule contract (`taskId`, `tenantId`, `promptText`).
- `POST /v1/scheduler/register-node`, `/v1/scheduler/schedule-tick`, `/v1/scheduler/lease-renew`,
  `/v1/scheduler/complete` — the scheduler cell behind the worker.
- `fleet/capacity-v1.json` in the bucket — node leases with `expires_ms`, addresses and cell load.

## Operational notes

- Body hosts are heartbeated to **every** celld node every 5s (`CelldFleetClient.registerNodes`). celld keeps
  the body registry per node process and load-balances brain calls across nodes, so a single-node
  registration leaves siblings without body capacity.
- Leases placed through celld are renewed every 5s (`CelldFleetClient.renewLeases`). Without renewal the cell
  reconciles the task as orphaned, retries it 3 times and dead-letters it (`MAX_LEASE_RETRIES_EXCEEDED`).
- celld-placed tasks are scheduled but not yet executed: the body-side executor for celld-owned leases is not
  wired, so tasks stay `LEASED`. Task execution still uses the gateway's in-process scheduler for
  non-celld deployments.
