# Rivet and OpenOrange migration

The target topology runs the Scheduler, Gateway, and Body Worker entry points on OpenOrange, with Rivet hosting their Durable Objects. Run the compatibility checks from the repository root with OpenOrange on PATH:

```sh
node scripts/check_open_orange.mjs scheduler
node scripts/check_open_orange.mjs gateway
node scripts/check_open_orange.mjs body
```

The diagnostics read the current binding and migration definitions and rebase entry and migration paths in a temporary configuration.

## Self-hosted Rivet

The Compose file starts Rivet Engine 3.1.1 with persistent log and object volumes and a loopback API. Supply `ANSTS_RIVET_ADMIN_TOKEN` through the environment before starting it.

```sh
docker compose -f deploy/open-orange/compose.yml up -d
```

Create a runner pool in the target namespace and configure the Orange Workers with the Engine's `RIVET_ENDPOINT`, `RIVET_TOKEN`, and `RIVET_NAMESPACE`. The Gateway and Body runtime configuration must point `ANSTS_SCHEDULER_URL` at the Scheduler Worker endpoint. Scheduler and Body must share the authorized Body service signing key. For Workers running in Docker, use an Engine address reachable from the worker container instead of the host loopback address.

Use the repository launcher to run the Workers in separate processes on the same host. The Scheduler defaults to port 8788, Gateway to 8789, and Body to 8791.

```sh
node scripts/check_open_orange.mjs --serve scheduler
node scripts/check_open_orange.mjs --serve gateway
node scripts/check_open_orange.mjs --serve body
```

Set `RIVET_ENDPOINT`, `RIVET_TOKEN`, and `RIVET_NAMESPACE` in each process environment. OpenOrange 1.0.3 uses `ORANGE_WORKER_VARS` and `ORANGE_WORKER_SECRETS` as comma-separated lists of environment variable names. Include `ANSTS_SCHEDULER_URL` in the variable list for Gateway and Body. Include `ANSTS_BODY_AUTH_SECRET` in the secret list for Scheduler and Body, and provide the same signing value to both. Use role-specific environments for the remaining Worker configuration. The launcher listens on loopback.

Run `python3 -u scripts/verify_open_orange.py` to exercise all three roles with Docker and the pinned Orange CLI on PATH. The verifier supplies temporary credentials and a dedicated Rivet Engine, checks both health routes per role, and verifies authenticated task submission and readback. The OpenOrange compatibility job in ANSTS CI runs all three doctor checks and this smoke test.

## Verified in isolation

OpenOrange 1.0.3 passed `doctor` for Scheduler, Gateway, and Body. Each Worker generated a deployment build context in a dry run. Against a dedicated self-hosted Rivet Engine 3.1.1 container, all three Workers returned HTTP 200 from `/healthz` and `/readyz`, and the Scheduler accepted and read back a task with an isolated service identity. An independent `orange dev` restart test read back the same task after restart.

## Body snapshot rehearsal

An isolated celld 0.6.3 Body Worker recorded a completed execution, then exported its snapshot over the authenticated Body operator API. The OpenOrange Body Worker on self-hosted Rivet Engine 3.1.1 imported it and returned identical snapshot rows and the same Gateway-visible execution result. The target rejected an unauthenticated import with HTTP 401. A subsequent import left the initialized target intact, and re-export confirmed the source still held the original snapshot.

This transfer covers the Body state rows for one terminal attempt. Body import records the execution as an inventory baseline, so historical coverage remains unproven. The full cutover remains open.

## Financial archive rehearsal

An isolated celld fleet generated a signed tenant archive with a nonzero priced ledger and an outstanding admission reservation. A dedicated verification Worker on OpenOrange and self-hosted Rivet restored it into an empty SQLite Durable Object with the existing offline restore primitive. It returned the exact source revision, cost summary and reservation. Missing authorization and an altered signature returned HTTP 401. Replaying the archive into the same database returned HTTP 409 and retained the restored state. The verification object did not host tenant control routes or alarms, and the restore returned `operationalResume: DISABLED`.

## Tenant authority rehearsal

Signed Directory and Tenant archives from the same isolated celld run restored into two new SQLite objects on OpenOrange and self-hosted Rivet. The verification worker initialized the production TenantDirectoryCell and TenantControlCell only after each restore. Production Worker routes returned the same policy, admission reservation, priced ledger entries and summary, and directory audit. A cross-tenant request returned HTTP 404 and the directory credential on an admission route returned HTTP 401. The initialized objects rejected another restore with HTTP 409.

A version-checked policy write advanced the restored policy from version 1 to 2. An OpenOrange process restart retained that write and the original ledger and reservation. The fixture did not route alarms or enable admission delivery. The offline restore receipt continues to report `operationalResume: DISABLED`; production alarm and outbox resumption require a separate rehearsal.

## Admission alarm and outbox rehearsal

A celld tenant took a signed snapshot while delivery was paused and one capacity message was pending. OpenOrange restored the paused state without an alarm. The production version-checked delivery control then resumed processing and scheduled a new Rivet alarm. Without a manual dispatch request, the production TenantControlCell delivered the pending message to the production shadow mailbox. The outbox recorded one acknowledgement for the original event ID, one additional attempt, and a `RECORDED` receipt. The original reservation and priced ledger remained identical.

This verifies deliberate resumption of the shadow outbox after restore. The source alarm timestamp, hard enforcement actions, and messages outside this admission outbox were outside the rehearsal.

## Scheduler lease and inventory rehearsal

A fixture using the production Scheduler cell created and leased one task on isolated celld. It copied persisted task, node and inventory rows into an empty OpenOrange Durable Object on self-hosted Rivet. The production Scheduler constructor read back the same task ID, fencing token, node, lease deadlines and resource occupancy. The restored lease retained its original deadline. Reconciliation immediately before that deadline retained the lease, and reconciliation at the deadline released the slot, recovered the CPU and memory, and requeued the task. The source task state still matched the captured state.

The target assigned its own source ID and coverage start. The historical entries and digest chain remained byte-equivalent, while the source-specific watermark changed. An altered event caused the import transaction to roll back, after which the intact snapshot restored successfully. Reimport into the initialized target was also rejected. This fixture did not transfer pending alarms, admission intents, or a complete production fleet snapshot. It does not establish continuous settlement coverage across the cutover.

## Read-only cutover and rollback rehearsal

An isolated loopback ingress served the same authenticated tenant cost summary from celld and then from the restored Rivet target. It rejected a valid policy write with HTTP 503 on both sides while the write fence was active. Stopping the target produced an HTTP 502 through the ingress. Repointing the ingress at celld restored the original cost read, and the source policy version and values still matched the original snapshot.

This test used an isolated proxy and one read route. Production routing and full-stack rollback still need separate verification. The four configured names `ANSTS_ROUTING_KV`, `ANSTS_SCHEDULER_D1`, `ANSTS_LAYERFS_BUCKET`, and `ANSTS_TELEMETRY_QUEUE` appear in the celld manifest. The Worker has a queue consumer that can submit tasks to the Scheduler. The repository contains no direct references to these four binding names outside the manifest.

## Reverse rollback after a tenant policy write

Rivet accepted a version-checked policy update from v1 to v2 after the isolated restore. The original celld still showed v1. The test took a new signed tenant archive, paired it with the unchanged directory archive, and restored both into fresh celld objects. After stopping the Rivet Worker, celld returned v2, the original priced ledger and reservation, and the original receipt for a replay of the same write. Cross-tenant access remained denied.

The first reverse restore returned HTTP 409 because a fresh celld object already contains celld-managed SQLite tables. The isolated verification adapter excluded only the observed engine tables from the offline restore's empty-schema check and retained the guard against any existing business table. A repeated restore into the initialized business schema returned HTTP 409. This fixture establishes reverse transfer for the tested tenant policy write. A production reverse importer and a full-stack rollback covering Scheduler, Body and in-flight delivery state remain open.

The repository now provides `restoreIntoFreshCelldObject` for a dedicated, fenced import object. It recognizes the exact celld engine tables and alarm triggers, rejects application schema or visible runtime KV and alarms, and preserves the archive signature, tenant scope, and transactional readback checks. A repeat using the captured source archives restored a Rivet v2 policy to clean celld through this primitive, without filtering SQLite results in the fixture. The restored write receipt, ledger, reservation, and tenant access checks passed. Coordinated rollback across services remains open.

### Dedicated authority import entry point

The import-only manifest is `deploy/open-orange/wrangler.authority-import.json`. It exposes POST `/restore/directory` and POST `/restore/tenant/<tenant-id>`. Use it with a fresh, isolated destination while application traffic and delivery are fenced. Preserve the Worker name, authority class names, deployment namespace, and IDs when replacing the importer with the normal Worker.

For OpenOrange, set `ANSTS_AUTHORITY_IMPORT_RUNTIME=open-orange` and include its name in `ORANGE_WORKER_VARS`. Include `ANSTS_AUTHORITY_IMPORT_TOKEN` and `ANSTS_AUTHORITY_ARCHIVE_KEY` in `ORANGE_WORKER_SECRETS`. Set the latter to the key that verified the source archive and use a separate operator token for the Bearer credential. Start the importer with `orange serve --config deploy/open-orange/wrangler.authority-import.json --ip 127.0.0.1` and the target Rivet environment. For a dedicated celld import process, bundle the same entry point into the celld project and set the runtime selector to `celld`.

The import response includes the source revision and `operationalResume: DISABLED`. Compare the source revision to the signed manifest before stopping the importer and starting the normal Worker. In isolation, both runtimes rejected unauthenticated, wrong-tenant, and repeated imports. Both OpenOrange and celld retained restored authority after the import Worker stopped and the normal Scheduler Worker opened the same destination. Authenticated reads matched the source cost summary, ledger entries, quota policy, reservation and directory audit. The normal Workers rejected the import route and cross-tenant access. This rehearsal reused signed archives from an isolated fleet and used the same celld container storage across the Worker replacement.

## Reverse rollback after Scheduler and Body writes

The native Body operator API copied a terminal execution from celld to OpenOrange. After the Rivet target acquired, started, finished, and acknowledged a second execution under a different task ID, a clean celld runtime imported both snapshots. Each re-export and authenticated Gateway result matched the Rivet copy exactly. The original celld runtime returned HTTP 404 for the target-only result. The clean runtime rejected unauthenticated imports and kept its initialized data on replay.

In a separate Scheduler rehearsal, OpenOrange accepted a target-only task through the production Scheduler class after the forward transfer. A fixture transaction imported both tasks into clean celld. The original lease and node occupancy matched the Rivet checkpoint, both inventory entries and the head digest survived, and the clean celld Scheduler read the target-only task. The original celld remained unchanged. An unauthenticated import and a replay into the initialized destination were rejected. The Scheduler fixture suppressed scheduling alarms at the checkpoint and copied only tasks, nodes, and inventory rows. Those initial rehearsals used separate checkpoints.

A follow-up Scheduler fixture included the claim, admission intent, and acceptance tables and captured the pending alarm timestamp. The original claimant still received HTTP 200 and a different claimant received HTTP 409 on both the forward and reverse targets. The production expiry check recognized the expired, unaccepted intent and continued to reject expiry for an accepted task whose task row had already been purged. The forward target stored the original alarm timestamp. After a new target checkpoint, both OpenOrange and the clean rollback celld instance invoked the production Scheduler alarm handler at or after the restored timestamp. The fixture seeded the admission records in SQLite and rearmed the alarm after the SQL transaction. Synchronized cross-plane rollback and an operational importer remain open.

Run the Scheduler round-trip fixture with `python3 -u scripts/verify_open_orange_scheduler.py`. The same command runs in the OpenOrange compatibility CI job. It creates and removes its own celld and Rivet test containers.

## Pilot enforcement rehearsal

The existing capacity acceptance fixture ran on OpenOrange and a dedicated self-hosted Rivet Engine. Scheduler object IDs resolved after string reconstruction. The graceful-restart run verified one winner for two Gateway submissions against one slot, no admission while the control plane was unavailable, replay after lost bind and start authorization responses, and no budget or slot release on an expired Body start check. It used two Gateway objects in one Worker and synthetic signed host reports.

In a separate SIGKILL run, the Worker HTTP listener returned before both Scheduler occupancy probes recovered. Admission returned HTTP 503 during this interval. With the fixture's 7-second Engine node-loss threshold, both probes recovered after 12.1 seconds in one run. The fixture's last host report had aged out by then, and the next submission returned `CAPACITY_TARGET_UNAVAILABLE`. A follow-up SIGKILL run verified both probes recovered after 12.2 seconds. The Scheduler still had zero submitted tasks at that point. A fresh signed host report then restored routable capacity. Both Gateway objects competed again, exactly one submission won, and the full fixture exited successfully. It also verified alarm-driven expiry, expired start rejection, retained occupied quota, and Scheduler acceptance evidence after task deletion and restart. These results use two Gateway objects within one Worker. The independent Gateway and native Body Agent rehearsals below cover the separate processes.

## Independent Gateway rehearsal

Two independent OpenOrange processes ran the production Gateway Worker entry point with distinct endpoints and worker names. Both reached the same isolated Scheduler and capacity authority on self-hosted Rivet. With allocation paused and one tenant queue position available, simultaneous authenticated capacity requests returned HTTP 200 and HTTP 409 `QUEUE_QUOTA_EXCEEDED`. Replaying the accepted request returned the original response, both Gateways read the same queued request, and cancellation through the other Gateway allowed the rejected request to succeed on retry.

A separate full run exercised normal task submission through the independent Gateways. Existing legacy-shard occupancy and a simulated shard outage each blocked admission. After restoring the shard, exactly one Gateway won the competition for one pilot execution slot. A tenant control outage admitted no task, and replay recovered from dropped bind and start authorization responses across a control Worker restart. The fixture then verified expiry, occupied quota retention, and persisted Scheduler acceptance evidence. Both Gateway processes stayed up during the control Worker restart. The host reports were synthetic and the fixture did not run an in-flight Body attempt.

## Native Body Agent rehearsal

The existing native evidence fixture ran against OpenOrange and a dedicated self-hosted Rivet Engine using a temporary lifecycle adapter. The native Body Daemon recovered the signed terminal proof and pending acknowledgement from an independent stopped host copy. The production Body Agent received a capacity grant, started the task, reached the fixture's first model call, and recorded a RUNNING reservation before the test killed and restarted the OpenOrange Worker.

After the Worker recovered on the same endpoint, the reservation retained the same attempt ID, fencing token, generation, and capacity receipt. Its phase advanced to UNCERTAIN and its evidence sequence advanced from 2 to 3. The fixture stopped at its assertion that the entire RUNNING reservation would remain identical. A follow-up retained assertions on the reservation identity and capacity receipt and continued past the phase change. The original attempt did not reach both Scheduler COMPLETED and tenant SETTLING within the fixture's completion window. A diagnostic repeat ended with the Scheduler task QUEUED, the tenant reservation UNCERTAIN at evidence sequence 3, and zero pending evidence and usage records on the host. The Body Agent reported an abort and the model adapter saw only the first turn. The underlying Rivet Engine stayed running during the hard-restart runs.

With the mid-execution kill disabled, the full fixture passed. The production Body Agent executed a local tool in Docker, delivered signed terminal evidence, and acknowledged its durable outbox. Independent recomputation matched seven unique ledger events, five buckets and five credit subunits. The final cost replaced the reservation estimate atomically, and the same settlement receipt survived Worker restart and offline archive restore. The fixture used a deterministic model adapter and isolated data.

Reducing the isolated Engine's node-loss threshold from 7 seconds to 2 seconds let one full hard-restart run complete the original task and settle it. An independent repeat recovered the reservation read after 8,013 ms while it still showed RUNNING. The Scheduler nevertheless ended with the task QUEUED and the tenant reservation SETTLING. Both host outboxes had drained, but Body result delivery kept deferring. The 2-second setting did not consistently preserve the execution lease, so the deployment configuration retains its existing default.

Two OpenOrange runners behind a stable loopback proxy shared the isolated Engine with the 2-second node-loss threshold. Killing the active HTTP runner during a real Body Agent execution returned the original RUNNING reservation through the surviving runner after 8.190, 8.253, and 8.277 seconds. Two runs completed and settled the original attempt. The third reached Scheduler FAILED and tenant SETTLING after one model turn, although signed evidence and usage had already drained from the host outboxes. The fixture switched the HTTP origin explicitly and did not establish which runner originally hosted each Durable Object.

Body Daemon now treats renewal HTTP 5xx as transient while the last acknowledged lease remains inside its existing safety window. It still aborts on an explicit lease rejection or when that window closes. With this change, two independent repetitions of the two-runner hard-stop fixture completed and settled the original attempt. The stable endpoint returned the original RUNNING reservation after 8.223 and 8.263 seconds. Both runs recomputed the same seven unique ledger events, five buckets, and seven credit subunits. This verifies two local failovers with the fixture's runner and Engine settings. Production actor placement, workload and routing remain to be verified.

## Development celld storage inventory

An authenticated, read-only RustFS S3 listing paged through the development fleet bucket `ansts-fleet`. It returned 58,802 objects totaling 1,803,347,012 bytes, including 58,596 under `cells/`. The bucket reported no versioning configuration or incomplete multipart uploads. Most bytes were celld cell snapshots and logs. Retain the fleet bucket for source recovery and rollback.

The celld v0.6.3 implementation stores R2 Worker blobs under `r2/<bucket_name>/` and uses reserved cell classes for KV, D1, and Queues. Exact-prefix S3 queries for `r2/ansts-fleet/`, `cells/__KvNamespace:`, `cells/__D1Database:`, and `cells/__Queue:` each returned zero keys. Read-only inspection of the three stopped Scheduler node volumes also found no directories for those reserved classes. The development source was inspected without starting the nodes or invoking a queue consumer. Repeat these binding and producer checks against the intended cutover source while writes are fenced.

## State and cutover

The current source is celld. [OpenOrange's built-in migration](https://openorange.dev/docs/migrate/) expects a Cloudflare source Worker and Cloudflare D1 export APIs. Use a separate celld export and Orange import rehearsal before routing any shared traffic. An isolated environment with newly created state can exercise the runtime while that transfer is developed.

The rehearsal must establish object identity mapping, transactional SQLite state, KV and pending alarms, along with any outstanding queue messages and objects in R2. In particular, execution inventories validate the recorded source ID. An import must verify the source identity and preserve the digest chain while accounting for the target runtime's object IDs. Use authenticated Scheduler, Gateway and Body requests and compare task identities, active leases, capacity reservations, audit and cost state before and after the transfer. Preserve the source backup and rehearse rollback with writes held at the cutover boundary.


Run `python3 scripts/check_rivet_cutover.py` to check the development source endpoints and both Body hosts before taking a synchronized checkpoint. It reports readiness, authenticated Scheduler task counts, Body drain state and pending result, evidence and usage counts. It does not change routes or drain hosts.

Freeze new submissions and operator writes before waiting for in-flight attempts and host outboxes to drain. The Gateway fence also needs to cover direct Scheduler, Body, capacity and tenant-control callers. Pause any external queue producers and account for Scheduler and tenant alarms separately. Keep the source and target fenced during export, import and readback. Once target writes are enabled, rollback needs a fresh target checkpoint and reverse transfer before pointing writers back to celld.

The development check found all three celld Scheduler endpoints, both Gateway endpoints and the Body Worker unreachable. Both Body hosts were reachable with zero active sandboxes, pending results, evidence and usage, and neither host was drained. The development Compose configuration passed its check-only run. Task and reservation coverage for a synchronized source checkpoint therefore remains unverified.
