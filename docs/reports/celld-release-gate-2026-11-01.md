# Celld Persistence Release Gate — 2026-11-01

## Decision

**Release status: NOT READY.** The current branch contains substantial application-level persistence and recovery work, but the evidence does not satisfy the independent disaster-recovery, migration, retention, and production capacity gates in [implementation spec #1](https://github.com/echo112400-beep/ansts/issues/1). This report records the evidence available on this date; it is not a production sign-off.

## Verified in this branch

- `npm run build` completed successfully.
- `npm test` passed **111/111** integration and chaos tests, with zero skipped tests on Node **24.21.0**.
- `rtk bypass npm run lint` passed the repository AST and schema checks.
- `git diff --check` passed.
- Tests exercise durable task turns and snapshot references, DAG allowance transfer/retry, tenant task queries, rebuild of D1 projections, task event replay, SSE snapshot/follow setup, and expired-cursor resynchronization through the Worker HTTP surface with test Durable Object, D1, and R2 bindings.

The suite now also uses real SQLite through `node:sqlite` for four recovery/projection tests. File-backed directory and TenantLedger databases are closed and reopened; the public Worker API recovers the complete normalized TaskSpec and original idempotent result after an injected confirmation failure. A 45-tenant directory scan resumes across file reopen without duplicate registrations. Real SQLite also verifies concurrent event identity conflicts, offset repair after interrupted projection writes, and stale task projection protection. These four tests explicitly skip on Node versions without `node:sqlite`; all four executed in this run.

The follow-up fixed three reproduced errors: a lost ledger response incorrectly reported `NOT_ACCEPTED`; concurrent conflicting projection inserts could both report success; and retrying an inserted event did not repair an interrupted offset write. Admission uncertainty now reports `UNKNOWN`, and projection delivery verifies the winning immutable event row and retries the offset update before the caller acknowledges it.

SQLite tests still use a simulated `storage.sync` binding, and file close/reopen is not a process-crash or independent-storage test. These results do not establish guarantees across independent production storage failure domains.

## Runtime evidence available from T9/T1 work

Earlier runtime work recorded Celld **0.6.3** with RustFS **1.0.4** and exercised exact-range object reads, awaited persistence synchronization, process restart, restore after loss of the Celld compute directory, object-store outage behavior, and `celld diagnose`. The runtime and RustFS were hosted on the same machine/container environment. This supports the tested local restart and compute-directory-loss cases only; it does **not** prove survival of an independent host/storage failure or restore from an independently retained backup. The runtime exercise was not rerun as part of this report.

## Open release gates

| Gate | Status | Remaining evidence |
|---|---|---|
| Confirmed admission survives loss of every compute-node disk while the bucket remains independently available | Partial | Prior evidence lost a compute directory in a same-host setup, not all disks across a fleet with a separate storage failure domain. |
| Independent backup restore into an isolated fleet | Not demonstrated | Create a verifiable backup cut, restore it independently, then reconcile tasks, ledger entries, operations, and all live snapshot references. |
| Migration rehearsal | Not demonstrated | Verify imported history and emit an explicit unresolved-state list without inventing outcomes. |
| Retention and reference-safe garbage collection | Incomplete | Seven-day task event projection pruning and payload removal for delivered task outbox rows exist; stable event IDs/digests remain for operation replay and pending deliveries are protected. The full policy is not enforced end to end: 30-day task content/snapshots, 365-day summaries/settlements/operation audits, tenant overrides, and unresolved references/holds/compensation need implementation and recovery tests. |
| Durable delivery and reconciliation | Partial | Task event projection durably acknowledges bounded source outbox batches after D1 writes succeed, and a durable 16-shard tenant directory lets scheduled repair rediscover tenants after total D1 projection loss. General delivery progress/ack/replay for every Cell and reconciliation of every unfinished business action remain incomplete. |
| Straggler boost quota enforcement | Partial | The TenantLedger monetary transfer is durable and idempotent. Boosted token/TPS limits are recorded in voucher/target metadata but are not dynamically enforced by active TaskCell execution. |
| Capacity, latency, RTO, and cost | Not measured | Run correctness and DR gates first, then record workload dimensions and measurements on the intended storage/runtime topology. No production performance or recovery-time claim is made. |

## Implementation boundaries

- D1 task and event projections are rebuildable from the implemented authoritative task query/event replay path. A separately durable tenant directory is registered before admission and is scanned by scheduled repair, so tenant discovery survives D1 loss. Task event projection writes are followed by a durable source outbox acknowledgment; failures leave the source event pending for a later replay. Delivered task event bodies older than seven days are removed while their identity/digest tombstones remain. Projection refresh and event capture are not yet a general projection consumer for every Cell.
- SSE follow mode polls the authoritative task outbox. Replay, snapshot, and expired-cursor behavior are tested; a complete general-purpose durable feed and seven-day policy across all business event feeds are not established.
- The release must remain blocked until the missing storage, backup, migration, retention, and capacity evidence is recorded and reviewed.

## Related tracking

- [#1 — Implement Celld microservice persistence and recovery](https://github.com/echo112400-beep/ansts/issues/1)
- [#9 — Rebuildable projections, durable delivery, SSE cursors, and retention](https://github.com/echo112400-beep/ansts/issues/9)
- [#10 — Celld bucket acceptance, migration, independent backup, and release gate](https://github.com/echo112400-beep/ansts/issues/10)
