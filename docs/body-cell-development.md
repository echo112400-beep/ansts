# Body control plane in the development cluster

The scheduler fleet exports `AnstsBodyCell`. Each host has a named cell with its
latest heartbeat and execution receipts keyed by task ID and fencing token.
`GET /v1/body/state?hostId=body-host-01` reports freshness and receipts.

The Body container runs the existing native execution server plus
`scripts/body_cell_agent.mjs`. The agent registers capacity directly with the
scheduler, polls assigned leases, renews the fence before execution, calls the
local LayerFS execution endpoint, persists a receipt and completes the scheduler
lease. A persisted receipt lets a retried completion skip execution.

Run the cluster with `docker compose -f
deploy/dev-cluster/docker-compose.dev-cluster.yml up -d --build`. Redeploy changes
to the brain worker by starting the one-shot `celld-deploy` service again.

This is a development implementation. It preserves the existing trace-writing
execution contract rather than introducing a general command runner. Body RPCs
use the trusted development network. During execution the agent renews the lease
and refreshes capacity and Body Cell heartbeats every three seconds. An unsuccessful
renewal aborts the waiting HTTP request; this does not kill an already running
native operation. Task timeout bounds the wait.

Fenced requests use an exclusive, fsynced execution journal on the Body volume.
After restart a completed result is returned without replay. An interrupted attempt
returns HTTP 409 (`EXECUTION_OUTCOME_UNCERTAIN`) and requires reconciliation; it
is never automatically replayed on that host. This prioritizes avoiding duplicate
side effects over automatic progress. Cross-host exactly-once side effects are
not guaranteed: external tools still require idempotency keys and fence validation.
Scheduler node expiration remains the authority for recovery.
The two development Gateway workers retain separate local cell stores.
