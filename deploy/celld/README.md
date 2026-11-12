# celld fleet deployment (dev cluster)

The dev cluster runs a real [denoland/celld](https://github.com/denoland/celld) fleet as the Brain layer:
three celld nodes (`celld-brain-01..03`) own `AnstsSchedulerCell` shards, and the gateways submit work to
them instead of scheduling in-process.

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
